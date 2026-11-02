# Enterprise Data Lakehouse (Spark · Delta Lake · Airflow · MinIO · Trino)

Consolidates structured and semi-structured data (CSV, JDBC sources such as Oracle/PostgreSQL, JSON/API) **without losing history**,
using the medallion architecture. Data is **synthetic**.

```
Oracle / PostgreSQL / CSV / API
        |  ingestion (lakehouse/bronze.py)
   BRONZE  raw, append-only, + _batch_id/_source/_ingested_at   (never cleaned, full history)
        |  Spark (lakehouse/silver.py): typing, standardising, dedupe (latest wins), quarantine of bad rows
   SILVER  partitioned by order_date
        |  Spark (lakehouse/gold.py): business models
   GOLD    daily_sales, customer_value      ->  Trino SQL  ->  Superset / Power BI
```
Primary table format: **Delta Lake** (`TABLE_FORMAT=delta`, default). Plain Parquet (`TABLE_FORMAT=parquet`) is used by the offline tests.
Object storage: MinIO via `LAKE_ROOT=s3a://lake`. Orchestration: `airflow/dags/lakehouse_dag.py`.

## Features
raw storage · cleaning/standardising · partitioning (`order_date`) · historical versions (bronze history; Delta time travel) · schema evolution (`mergeSchema`) ·
query optimisation via partition pruning · run-level **lineage** (`lake/_lineage/lineage.jsonl`) · **data-quality gate** (`lakehouse/quality.py`: critical failures stop the pipeline).

## Run it
```bash
pip install -r requirements.txt
cp .env.example .env                       # set strong, unique values in .env
set -a; source .env; set +a
python data_gen/generate.py --out data/raw --days 3
python -m lakehouse.pipeline --orders data/raw/orders_day1.csv --customers data/raw/customers.csv --batch day1
python -m lakehouse.pipeline --orders data/raw/orders_day2.csv --batch day2       # later batches add history
TABLE_FORMAT=parquet python -m pytest -q                                          # offline tests
docker compose up -d minio minio-init trino superset                              # S3 storage + SQL + BI
LAKE_ROOT=s3a://lake python -m lakehouse.pipeline --orders ... --batch day1       # run against MinIO
```

## What was verified (build log)
With local Spark 3.5 and Parquet tables, **7 tests pass**: 3 batches ingested end to end; bronze keeps every row (including duplicates and bad rows);
silver is unique by key with the latest correction winning and bad rows quarantined; gold revenue reconciles with silver;
**partition pruning confirmed in the physical plan** (`PartitionFilters` on `order_date`); a new source column flows through (schema evolution);
the quality gate **fails** when duplicate keys are injected.

**Not verified in this build:** the Delta Lake write path (Delta jars are downloaded from Maven, unavailable offline), S3A/MinIO, JDBC ingestion, Airflow, Trino and Superset (config provided).
