# arXiv pdf manifest loader

Скрипт для загрузки id из всех tar в s3 arxiv в PostgreSQL таблицу.


---

## Возможности

### Первичный запуск 

```bash
python .\index_manifest_tars.py `
  --dsn "postgresql://postgres:postgres@localhost:5432/ragProm" `
  --region us-east-1 `
  --request-payer `
  --batch 1 `
  --poll-sleep-sec 60
```

todo

## Требования

todo

---

##  Сущность в бд

---

### Используемые сущности


### Тип `pdf_tar_manifest`

todo

```sql
CREATE TABLE IF NOT EXISTS pdf_tar_index (
  tar_key   text NOT NULL,
  arxiv_id  text NOT NULL,
  PRIMARY KEY (tar_key, arxiv_id),
  FOREIGN KEY (tar_key) REFERENCES pdf_tar_manifest(tar_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_pdf_tar_index_arxiv_id
  ON pdf_tar_index(arxiv_id);


```
---
```
Запуск в несколько инстансов bash

$env:DSN="postgresql://postgres:postgres@localhost:5432/ragProm"
$env:REGION="us-east-1"

mkdir logs -Force | Out-Null

Start-Process python "index_manifest_tars.py --dsn $env:DSN --region $env:REGION --request-payer --batch 1 --poll-sleep-sec 20 --revive-processing-minutes 240 --flush-every 100000 --worker-id w1" -NoNewWindow
Start-Process python "index_manifest_tars.py --dsn $env:DSN --region $env:REGION --request-payer --batch 1 --poll-sleep-sec 20 --revive-processing-minutes 240 --flush-every 100000 --worker-id w2" -NoNewWindow
Start-Process python "index_manifest_tars.py --dsn $env:DSN --region $env:REGION --request-payer --batch 1 --poll-sleep-sec 20 --revive-processing-minutes 240 --flush-every 100000 --worker-id w3" -NoNewWindow
```