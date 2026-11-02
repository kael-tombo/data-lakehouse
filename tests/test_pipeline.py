"""Offline tests: run with TABLE_FORMAT=parquet (Delta needs jars downloaded from Maven)."""
import io
import os
import shutil
import contextlib

import pytest

os.environ["TABLE_FORMAT"] = "parquet"
os.environ["LAKE_ROOT"] = "/tmp/lakehouse_test"

from data_gen.generate import generate  # noqa: E402
from lakehouse import bronze, gold, pipeline, quality, silver  # noqa: E402
from lakehouse.spark_session import get_spark, path  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    shutil.rmtree("/tmp/lakehouse_test", ignore_errors=True)
    shutil.rmtree("/tmp/lh_raw", ignore_errors=True)
    generate("/tmp/lh_raw", days=3)
    s = get_spark("test")
    yield s
    s.stop()


def test_full_pipeline_three_batches_and_quality(spark):
    for i in (1, 2, 3):
        res = pipeline.run(f"/tmp/lh_raw/orders_day{i}.csv", "/tmp/lh_raw/customers.csv" if i == 1 else None, f"day{i}")
        assert res["ok"], res
    assert all(r["passed"] for r in res["report"] if r["critical"])


def test_bronze_keeps_all_history_including_duplicates_and_bad_rows(spark):
    b = spark.read.parquet(path("bronze", "orders"))
    assert b.count() == 3 * 200 + 2 * 5          # 3 daily batches + 5 re-sent corrections on days 2 and 3
    assert b.select("_batch_id").distinct().count() == 3


def test_silver_dedupes_by_key_latest_wins_and_quarantines_bad_rows(spark):
    s = spark.read.parquet(path("silver", "orders"))
    assert s.count() == s.select("order_id").distinct().count()
    q = spark.read.parquet(path("silver", "orders_quarantine"))
    assert q.count() > 0
    assert q.filter("quantity <= 0 or order_date is null").count() == q.count()
    # corrected rows (re-sent later with qty 9) replaced the originals
    assert s.filter("quantity = 9").count() >= 5
    # standardisation
    assert {r[0] for r in s.select("region").distinct().collect()} <= {"Atsimo", "Atsinanana", "Analamanga", "Boeny"}


def test_gold_reconciles_with_silver(spark):
    import pyspark.sql.functions as F
    silver_rev = spark.read.parquet(path("silver", "orders")).filter("status != 'CANCELLED'").agg(F.sum("revenue")).first()[0]
    gold_rev = spark.read.parquet(path("gold", "daily_sales")).agg(F.sum("revenue")).first()[0]
    assert abs(silver_rev - gold_rev) < 0.05


def test_partition_pruning_on_order_date(spark):
    df = spark.read.parquet(path("silver", "orders")).filter("order_date = '2026-10-02'")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain()
    plan = buf.getvalue()
    assert "PartitionFilters" in plan and "order_date" in plan.split("PartitionFilters")[1].split("]")[0]


def test_schema_evolution_new_column_flows_into_bronze(spark, tmp_path):
    f = tmp_path / "orders_new.csv"
    f.write_text("order_id,customer_id,order_date,quantity,unit_price,region,status,channel\n9001,1,2026-10-04,2,10.0,Boeny,PAID,web\n")
    bronze.ingest_csv(spark, str(f), "orders", "day4")
    b = spark.read.option("mergeSchema", "true").parquet(path("bronze", "orders"))
    assert "channel" in b.columns and b.filter("channel = 'web'").count() == 1


def test_quality_gate_fails_on_corrupted_silver(spark):
    base = spark.read.parquet(path("silver", "orders")).limit(3)
    base.unionByName(base).write.mode("overwrite").parquet("/tmp/lakehouse_test/silver/orders_tmp")
    shutil.rmtree(path("silver", "orders"))
    shutil.copytree("/tmp/lakehouse_test/silver/orders_tmp", path("silver", "orders"))   # duplicated keys injected
    result = quality.run_checks(spark)
    assert result["ok"] is False
    assert any(r["check"] == "silver.orders.order_id unique" and not r["passed"] for r in result["report"])
