"""GOLD: business models ready for SQL analytics / BI."""
import pyspark.sql.functions as F

from .spark_session import FORMAT, path


def build_daily_sales(spark):
    o = spark.read.format(FORMAT).load(path("silver", "orders")).filter("status != 'CANCELLED'")
    g = (o.groupBy("order_date", "region")
          .agg(F.countDistinct("order_id").alias("orders"), F.sum("quantity").alias("units"), F.round(F.sum("revenue"), 2).alias("revenue"))
          .withColumn("avg_order_value", F.round(F.col("revenue") / F.col("orders"), 2)))
    g.write.format(FORMAT).mode("overwrite").option("overwriteSchema", "true").save(path("gold", "daily_sales"))
    return g.count()


def build_customer_value(spark):
    o = spark.read.format(FORMAT).load(path("silver", "orders")).filter("status != 'CANCELLED'")
    c = spark.read.format(FORMAT).load(path("silver", "customers"))
    g = (o.groupBy("customer_id").agg(F.sum("revenue").alias("lifetime_revenue"), F.countDistinct("order_id").alias("orders"),
                                      F.max("order_date").alias("last_order_date"))
          .join(c.select("customer_id", "name", F.col("region").alias("customer_region")), "customer_id", "left"))
    g.write.format(FORMAT).mode("overwrite").option("overwriteSchema", "true").save(path("gold", "customer_value"))
    return g.count()
