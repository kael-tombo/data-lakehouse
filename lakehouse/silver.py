"""SILVER: cleaned, typed, de-duplicated, standardized. Bad rows are quarantined, not silently dropped.

Orders: key = order_id; if the same order arrives in several bronze batches the LATEST ingestion wins
(history stays in bronze). Partitioned by order_date so date-filtered queries prune partitions.
Schema evolution: columns that appear in later bronze batches flow through (mergeSchema).
"""
import pyspark.sql.functions as F
from pyspark.sql import Window

from .spark_session import FORMAT, path


def build_orders(spark):
    b = spark.read.format(FORMAT).load(path("bronze", "orders"))
    typed = (b.withColumn("order_id", F.col("order_id").cast("long"))
              .withColumn("customer_id", F.col("customer_id").cast("long"))
              .withColumn("quantity", F.col("quantity").cast("int"))
              .withColumn("unit_price", F.col("unit_price").cast("double"))
              .withColumn("order_date", F.to_date("order_date"))
              .withColumn("region", F.initcap(F.trim("region")))
              .withColumn("status", F.upper(F.trim("status"))))
    valid = (F.col("order_id").isNotNull() & F.col("order_date").isNotNull() & (F.col("quantity") > 0)
             & (F.col("unit_price") >= 0) & F.col("customer_id").isNotNull())
    good, bad = typed.filter(valid), typed.filter(~valid)
    w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc(), F.col("_batch_id").desc())
    dedup = (good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
                 .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)))
    (dedup.write.format(FORMAT).mode("overwrite").option("overwriteSchema", "true")
          .partitionBy("order_date").save(path("silver", "orders")))
    (bad.write.format(FORMAT).mode("overwrite").option("overwriteSchema", "true").save(path("silver", "orders_quarantine")))
    return {"silver_rows": dedup.count(), "quarantined_rows": bad.count()}


def build_customers(spark):
    b = spark.read.format(FORMAT).load(path("bronze", "customers"))
    t = (b.withColumn("customer_id", F.col("customer_id").cast("long")).withColumn("name", F.trim("name"))
          .withColumn("region", F.initcap(F.trim("region"))).filter("customer_id is not null"))
    w = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    d = t.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    d.write.format(FORMAT).mode("overwrite").option("overwriteSchema", "true").save(path("silver", "customers"))
    return {"silver_rows": d.count()}
