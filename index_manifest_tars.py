import argparse
import io
import os
import tarfile
import time
from typing import Iterable, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

import psycopg2
import psycopg2.extras


SRC_BUCKET = "arxiv"


# ---------------------------
# S3 streaming adapter
# ---------------------------
class StreamingBodyFile(io.RawIOBase):
    """StreamingBody -> fileobj for tarfile streaming mode (r|*)."""
    def __init__(self, body):
        self.body = body

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        chunk = self.body.read(len(b))
        if not chunk:
            return 0
        b[:len(chunk)] = chunk
        return len(chunk)


def iter_arxiv_ids_from_tar(*, s3, tar_key: str, request_payer: bool) -> Iterable[str]:
    """
    Один GetObject на tar_key, стримингом.
    Достаём arxiv_id из имён PDF внутри tar.
    """
    get_kwargs = {"Bucket": SRC_BUCKET, "Key": tar_key}
    if request_payer:
        get_kwargs["RequestPayer"] = "requester"

    obj = s3.get_object(**get_kwargs)
    stream = StreamingBodyFile(obj["Body"])

    with tarfile.open(fileobj=stream, mode="r|*") as tf:
        for m in tf:
            if not m.isfile():
                continue

            name = m.name.replace("\\", "/")
            base = os.path.basename(name)

            if not base.lower().endswith(".pdf"):
                continue

            arxiv_id = base[:-4]  # strip ".pdf"
            if arxiv_id:
                yield arxiv_id


# ---------------------------
# DB helpers
# ---------------------------
def db_connect(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def claim_next_tar(conn, limit: int, worker_id: str) -> List[str]:
    """
    Атомарно забираем tar_key в PROCESSING.
    SKIP LOCKED позволяет параллельные воркеры без пересечений.
    """
    sql = """
    WITH cte AS (
      SELECT tar_key
      FROM pdf_tar_manifest
      WHERE status IN ('NEW', 'FAILED')
      ORDER BY tar_key
      FOR UPDATE SKIP LOCKED
      LIMIT %s
    )
    UPDATE pdf_tar_manifest m
    SET status='PROCESSING',
        worker_id=%s,
        locked_at=now(),
        last_started_at=now(),
        attempts=attempts+1,
        last_error=NULL,
        updated_at=now()
    FROM cte
    WHERE m.tar_key = cte.tar_key
    RETURNING m.tar_key;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit, worker_id))
        return [r[0] for r in cur.fetchall()]


def heartbeat(conn, tar_key: str, worker_id: str):
    """
    Продлеваем lease — чтобы никто не "revive" живую задачу.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pdf_tar_manifest
            SET locked_at=now(), updated_at=now()
            WHERE tar_key=%s AND status='PROCESSING' AND worker_id=%s
            """,
            (tar_key, worker_id),
        )


def revive_stuck_processing(conn, older_than_minutes: int) -> int:
    """
    Реанимация ТОЛЬКО по истекшему lease (locked_at).
    Это безопасно при многоворкерности.
    """
    sql = """
    UPDATE pdf_tar_manifest
    SET status='FAILED',
        last_error = COALESCE(last_error, '') ||
                     CASE WHEN last_error IS NULL OR last_error = '' THEN '' ELSE E'\n' END ||
                     'revive: lock expired',
        updated_at=now()
    WHERE status='PROCESSING'
      AND locked_at IS NOT NULL
      AND locked_at < now() - (%s || ' minutes')::interval
    """
    with conn.cursor() as cur:
        cur.execute(sql, (older_than_minutes,))
        return cur.rowcount


def bulk_insert_index(conn, tar_key: str, ids: List[str]):
    """
    Быстро грузим (tar_key, arxiv_id) пачкой.
    ON CONFLICT DO NOTHING защищает от повторов.
    """
    rows = [(tar_key, x) for x in ids]
    sql = """
    INSERT INTO pdf_tar_index (tar_key, arxiv_id)
    VALUES %s
    ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=10_000)


def mark_done(conn, tar_key: str, worker_id: str, total_ids: int):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pdf_tar_manifest
            SET status='DONE',
                worker_id=%s,
                locked_at=NULL,
                num_items_indexed=%s,
                last_finished_at=now(),
                last_error=NULL,
                updated_at=now()
            WHERE tar_key=%s
        """, (worker_id, total_ids, tar_key))

def mark_failed(conn, tar_key: str, worker_id: str, err: str):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pdf_tar_manifest
            SET status='FAILED',
                worker_id=%s,
                locked_at=NULL,
                last_error=left(%s, 8000),
                updated_at=now()
            WHERE tar_key=%s
        """, (worker_id, err, tar_key))


# ---------------------------
# Main worker loop
# ---------------------------
def run_worker(
    *,
    dsn: str,
    region: str,
    request_payer: bool,
    batch: int,
    poll_sleep_sec: int,
    once: bool,
    revive_processing_minutes: Optional[int],
    flush_every: int,
    worker_id: str,
):
    if not worker_id:
        # лучше иметь стабильный идентификатор (для дебага и безопасного heartbeat)
        worker_id = f"pid-{os.getpid()}"

    config = Config(
        max_pool_connections=50,
        retries={"max_attempts": 10, "mode": "standard"},
    )
    s3 = boto3.client("s3", region_name=region, config=config)

    conn = db_connect(dsn)
    try:
        while True:
            # 1) опционально реанимируем протухшие leases
            if revive_processing_minutes is not None:
                revived = revive_stuck_processing(conn, revive_processing_minutes)
                conn.commit()
                if revived:
                    print(f"[revive] revived_processing={revived}")

            # 2) забираем пачку tar_key
            tar_keys = claim_next_tar(conn, limit=batch, worker_id=worker_id)
            conn.commit()

            if not tar_keys:
                if once:
                    print("[exit] no work (once=true)")
                    return
                print(f"[idle] no work, sleep {poll_sleep_sec}s")
                time.sleep(poll_sleep_sec)
                continue

            # 3) обрабатываем каждый tar_key
            for tar_key in tar_keys:
                t0 = time.time()
                print(f"[start] worker={worker_id} {tar_key}")

                try:
                    buf: List[str] = []
                    total = 0

                    for arxiv_id in iter_arxiv_ids_from_tar(
                        s3=s3,
                        tar_key=tar_key,
                        request_payer=request_payer,
                    ):
                        buf.append(arxiv_id)

                        if len(buf) >= flush_every:
                            bulk_insert_index(conn, tar_key, buf)
                            conn.commit()
                            total += len(buf)
                            buf.clear()

                            # продлеваем lease
                            heartbeat(conn, tar_key, worker_id)
                            conn.commit()

                            print(f"[progress] {tar_key}: inserted~{total}")

                    if buf:
                        bulk_insert_index(conn, tar_key, buf)
                        conn.commit()
                        total += len(buf)
                        heartbeat(conn, tar_key, worker_id)
                        conn.commit()

                    # успех
                    mark_done(conn, tar_key, worker_id=worker_id, total_ids=total)
                    conn.commit()

                    dt = time.time() - t0
                    print(f"[done] {tar_key}: ids={total} time={dt:.1f}s")

                except KeyboardInterrupt:
                    conn.rollback()
                    mark_failed(conn, tar_key, worker_id=worker_id, err="KeyboardInterrupt")
                    conn.commit()
                    raise

                except (ClientError, tarfile.TarError, Exception) as e:
                    conn.rollback()
                    mark_failed(conn, tar_key, worker_id=worker_id, err=repr(e))
                    conn.commit()
                    print(f"[fail] {tar_key}: {e!r}")

            if once:
                print("[exit] processed batch (once=true)")
                return

    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True, help="Postgres DSN, напр: postgresql://user:pass@host:5432/dbname")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--request-payer", action="store_true", help="для arXiv bucket requester pays")
    ap.add_argument("--batch", type=int, default=1, help="сколько tar_key забирать за раз")
    ap.add_argument("--poll-sleep-sec", type=int, default=60, help="пауза если нет работы")
    ap.add_argument("--once", action="store_true", help="сделать один цикл и выйти (удобно для cron)")
    ap.add_argument(
        "--revive-processing-minutes",
        type=int,
        default=None,
        help="если задано: PROCESSING с протухшим locked_at старше N минут -> FAILED",
    )
    ap.add_argument("--flush-every", type=int, default=50_000, help="вставлять в БД пачками по N ids")
    ap.add_argument("--worker-id", default=os.getenv("WORKER_ID", ""), help="идентификатор воркера/инстанса")

    args = ap.parse_args()

    run_worker(
        dsn=args.dsn,
        region=args.region,
        request_payer=args.request_payer,
        batch=args.batch,
        poll_sleep_sec=args.poll_sleep_sec,
        once=args.once,
        revive_processing_minutes=args.revive_processing_minutes,
        flush_every=args.flush_every,
        worker_id=args.worker_id,
    )


if __name__ == "__main__":
    main()
