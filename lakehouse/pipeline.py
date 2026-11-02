"""Run bronze -> silver -> gold -> quality for one batch.

    python -m lakehouse.pipeline --orders data/raw/orders_day1.csv --customers data/raw/customers.csv --batch day1
"""
import argparse

from . import bronze, gold, lineage, quality, silver
from .spark_session import get_spark


def run(orders_csv, customers_csv, batch_id):
    spark = get_spark()
    n_o = bronze.ingest_csv(spark, orders_csv, "orders", batch_id)
    n_c = bronze.ingest_csv(spark, customers_csv, "customers", batch_id) if customers_csv else 0
    lineage.record("bronze", [orders_csv, customers_csv], ["bronze/orders", "bronze/customers"], {"orders": n_o, "customers": n_c}, batch_id)
    if customers_csv:
        lineage.record("silver.customers", ["bronze/customers"], ["silver/customers"], silver.build_customers(spark), batch_id)
    lineage.record("silver.orders", ["bronze/orders"], ["silver/orders", "silver/orders_quarantine"], silver.build_orders(spark), batch_id)
    lineage.record("gold", ["silver/orders", "silver/customers"], ["gold/daily_sales", "gold/customer_value"],
                   {"daily_sales": gold.build_daily_sales(spark), "customer_value": gold.build_customer_value(spark)}, batch_id)
    result = quality.run_checks(spark)
    if not result["ok"]:
        raise RuntimeError(f"critical data-quality failure: {result['report']}")
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", required=True)
    ap.add_argument("--customers")
    ap.add_argument("--batch", required=True)
    a = ap.parse_args()
    print(run(a.orders, a.customers, a.batch))
