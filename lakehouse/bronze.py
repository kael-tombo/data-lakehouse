"""BRONZE: raw, append-only copy of the sources + ingestion metadata. Nothing is cleaned or dropped,
so the original history is never lost and any downstream table can be rebuilt."""
import pyspark.sql.functions as F

from .spark_session import FORMAT, path


def ingest_csv(spark, csv_path: str, dataset: str, batch_id: str, source: str = "csv"):
    df = (spark.read.option("header", True).option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record").csv(csv_path))   # everything as string
    df = (df.withColumn("_batch_id", F.lit(batch_id)).withColumn("_source", F.lit(source))
          .withColumn("_ingested_at", F.current_timestamp()).withColumn("_ingest_date", F.current_date()))
    (df.write.format(FORMAT).mode("append").option("mergeSchema", "true")
       .partitionBy("_ingest_date").save(path("bronze", dataset)))
    return df.count()


def ingest_jdbc(spark, url: str, table: str, dataset: str, batch_id: str, user: str, password: str, driver: str):
    """Oracle / PostgreSQL source (needs the JDBC driver jar on the Spark classpath)."""
    df = (spark.read.format("jdbc").options(url=url, dbtable=table, user=user, password=password, driver=driver).load()
          .withColumn("_batch_id", F.lit(batch_id)).withColumn("_source", F.lit("jdbc:" + table))
          .withColumn("_ingested_at", F.current_timestamp()).withColumn("_ingest_date", F.current_date()))
    df.write.format(FORMAT).mode("append").option("mergeSchema", "true").partitionBy("_ingest_date").save(path("bronze", dataset))
    return df.count()


def ingest_json_api(spark, records: list, dataset: str, batch_id: str, source: str = "api"):
    df = (spark.createDataFrame([{k: str(v) for k, v in r.items()} for r in records])
          .withColumn("_batch_id", F.lit(batch_id)).withColumn("_source", F.lit(source))
          .withColumn("_ingested_at", F.current_timestamp()).withColumn("_ingest_date", F.current_date()))
    df.write.format(FORMAT).mode("append").option("mergeSchema", "true").partitionBy("_ingest_date").save(path("bronze", dataset))
    return df.count()
