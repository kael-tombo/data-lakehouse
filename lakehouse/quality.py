"""Data-quality checks. `run_checks` returns a report; critical failures should stop the pipeline."""
import pyspark.sql.functions as F

from .spark_session import FORMAT, path


def run_checks(spark):
    o = spark.read.format(FORMAT).load(path("silver", "orders"))
    c = spark.read.format(FORMAT).load(path("silver", "customers"))
    g = spark.read.format(FORMAT).load(path("gold", "daily_sales"))
    q = spark.read.format(FORMAT).load(path("silver", "orders_quarantine"))
    total_o = o.count()
    checks = [
        ("silver.orders.order_id unique", total_o == o.select("order_id").distinct().count(), True),
        ("silver.orders no null keys/dates", o.filter("order_id is null or order_date is null").count() == 0, True),
        ("silver.orders revenue = qty*price", o.filter(F.abs(F.col("revenue") - F.col("quantity") * F.col("unit_price")) > 0.01).count() == 0, True),
        ("silver.orders referential: customer exists", o.join(c, "customer_id", "left_anti").count() == 0, False),
        ("gold.daily_sales revenue reconciles with silver", abs((g.agg(F.sum("revenue")).first()[0] or 0)
            - (o.filter("status != 'CANCELLED'").agg(F.sum("revenue")).first()[0] or 0)) < 0.05, True),
        ("quarantine ratio < 5%", q.count() <= 0.05 * (total_o + q.count()), False),
    ]
    report = [{"check": n, "passed": bool(ok), "critical": crit} for n, ok, crit in checks]
    failed_critical = [r for r in report if not r["passed"] and r["critical"]]
    return {"report": report, "ok": not failed_critical}
