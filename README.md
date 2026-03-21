# index_manifest_tars

Индексатор содержимого tar-архивов arXiv.

Скрипт читает tar-файлы из S3 bucket `arxiv`, потоково проходит по содержимому архивов, извлекает arXiv id из имён PDF и сохраняет соответствие `tar_key -> arxiv_id` в PostgreSQL таблицу `pdf_tar_index`.

Этот проект закрывает промежуток между манифестом tar-архивов и реальной адресацией конкретных PDF внутри tar.

---

## Что делает проект

`index_manifest_tars.py`:

- забирает из `pdf_tar_manifest` очередную пачку tar для обработки;
- стримингом читает tar из arXiv S3;
- извлекает arXiv id из имён PDF внутри tar;
- пачками вставляет данные в `pdf_tar_index`;
- переводит tar в `DONE` или `FAILED`;
- умеет реанимировать протухшие `PROCESSING` задачи.

---

## Где этот проект находится в пайплайне

Обычно последовательность такая:

1. `oai_suprcon_sync` — загружает статьи в `arxiv_paper`.
2. `manifest_parser_sync` — загружает список tar в `pdf_tar_manifest`.
3. `index_manifest_tars` — строит индекс содержимого tar в `pdf_tar_index`.
4. `tar_workers_sync` — по `arxiv_paper.lookup_key` и `pdf_tar_index` находит нужный tar и выгружает PDF в целевое хранилище.

---

## Требования

- Python 3.10+
- PostgreSQL
- `boto3`
- `psycopg2-binary`
- доступ к arXiv S3 bucket `arxiv`

Установка:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install boto3 psycopg2-binary
```

---

## Быстрый старт

### Один цикл обработки

```bash
python index_manifest_tars.py \
  --dsn postgresql://postgres:postgres@localhost:5432/rag \
  --region us-east-1 \
  --request-payer \
  --batch 1 \
  --once
```

### Непрерывный режим

```bash
python index_manifest_tars.py \
  --dsn postgresql://postgres:postgres@localhost:5432/rag \
  --region us-east-1 \
  --request-payer \
  --batch 4 \
  --poll-sleep-sec 30 \
  --flush-every 50000 \
  --worker-id indexer-1
```

---

## Аргументы CLI

```text
--dsn                        Postgres DSN
--region                     AWS region source bucket, по умолчанию us-east-1
--request-payer              requester pays для arXiv bucket
--batch                      сколько tar забирать за раз
--poll-sleep-sec             пауза, если нет работы
--once                       выполнить один цикл и завершиться
--revive-processing-minutes  пометить зависшие PROCESSING как FAILED
--flush-every                вставлять ids в БД пачками по N
--worker-id                  идентификатор воркера
```

---

## Необходимые таблицы

### Очередь tar-файлов

Проект использует `pdf_tar_manifest`, которую заполняет `manifest_parser_sync`.

### Индекс содержимого tar

```sql
CREATE TABLE IF NOT EXISTS pdf_tar_index (
  tar_key    text NOT NULL,
  arxiv_id   text NOT NULL,
  PRIMARY KEY (tar_key, arxiv_id)
);
```

### Полезные индексы

```sql
CREATE INDEX IF NOT EXISTS idx_pdf_tar_index_arxiv_id
  ON pdf_tar_index(arxiv_id);

CREATE INDEX IF NOT EXISTS idx_pdf_tar_index_tar_key
  ON pdf_tar_index(tar_key);
```

---

## Логика обработки

1. Воркер claim'ит пачку `tar_key` из `pdf_tar_manifest`.
2. Статус переводится в `PROCESSING`, фиксируется `worker_id` и `locked_at`.
3. Для каждого tar выполняется `GetObject` из S3.
4. Архив читается потоково через `tarfile`.
5. Из имён `*.pdf` извлекаются arXiv id.
6. Вставка в `pdf_tar_index` идёт батчами через `execute_values`.
7. После успеха tar переводится в `DONE` и сохраняется `num_items_indexed`.
8. При исключении tar переводится в `FAILED`, а текст ошибки кладётся в `last_error`.

---

## Почему проект нужен

`pdf_tar_manifest` даёт только диапазоны `first_item/last_item`, но downstream-воркерам нужен точный ответ: в каком `tar_key` лежит конкретный `arxiv_id`.

Именно для этого и строится `pdf_tar_index`.

---

## Проверка результата

### Сколько tar уже проиндексировано

```sql
SELECT status, count(*)
FROM pdf_tar_manifest
GROUP BY status
ORDER BY status;
```

### Сколько записей уже в индексе

```sql
SELECT count(*)
FROM pdf_tar_index;
```

### Для какого tar найден конкретный arXiv id

```sql
SELECT tar_key
FROM pdf_tar_index
WHERE arxiv_id = '2501.01234';
```

---

## Типовые режимы запуска

### Локальная отладка

```bash
python index_manifest_tars.py \
  --dsn postgresql://postgres:postgres@localhost:5432/rag \
  --region us-east-1 \
  --request-payer \
  --batch 1 \
  --once \
  --worker-id local-dev
```

### Серверный воркер

```bash
python index_manifest_tars.py \
  --dsn postgresql://postgres:postgres@localhost:5432/rag \
  --region us-east-1 \
  --request-payer \
  --batch 8 \
  --poll-sleep-sec 10 \
  --flush-every 100000 \
  --worker-id indexer-prod-1
```

---

## Что важно учитывать

### 1. Requester Pays

Для bucket arXiv нужно включать `--request-payer`, иначе `GetObject` может не сработать.

### 2. Большие tar-файлы

Скрипт использует streaming-подход, поэтому не читает весь tar в память целиком.

### 3. Зависшие задачи

Если воркер упал в середине обработки, можно использовать `--revive-processing-minutes`, чтобы вернуть такие задачи в поток обработки.

### 4. Идемпотентность

Вставка в `pdf_tar_index` сделана через `ON CONFLICT DO NOTHING`, поэтому повторный запуск безопасен для уже вставленных пар.

---

## Возможные проблемы

### Нет записей для обработки

Проверьте, что таблица `pdf_tar_manifest` уже заполнена и есть строки со статусом `NEW`.

### Медленная вставка

Увеличьте `--flush-every`, если у вас хватает памяти и база справляется с более крупными батчами.

### Ошибки чтения tar

Проверьте права доступа, настройки `requester pays` и сетевую доступность S3.

---

## Идеи для развития

- добавить метрики по числу id на tar;
- хранить длительность обработки каждого tar;
- добавить Prometheus-метрики;
- вынести SQL и DDL в отдельные миграции;
- добавить Dockerfile и systemd unit примеры.
