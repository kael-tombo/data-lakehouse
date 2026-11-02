"""Spark session factory.

TABLE_FORMAT=delta (default)  -> Delta Lake tables (time travel, MERGE, schema evolution)
TABLE_FORMAT=parquet          -> plain Parquet (used by the offline unit tests)
LAKE_ROOT=s3a://lake          -> MinIO / S3-compatible object storage (see docker-compose.yml)
"""
import os

from pyspark.sql import SparkSession

FORMAT = os.getenv("TABLE_FORMAT", "delta")
LAKE_ROOT = os.getenv("LAKE_ROOT", "lake")


def get_spark(app="lakehouse"):
    b = (SparkSession.builder.appName(app).master(os.getenv("SPARK_MASTER", "local[2]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))
    if FORMAT == "delta":
        from delta import configure_spark_with_delta_pip
        b = (b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        b = configure_spark_with_delta_pip(b)
    if LAKE_ROOT.startswith("s3a://"):
        b = (b.config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
             .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
             .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4"))
    return b.getOrCreate()


def path(layer: str, table: str) -> str:
    return f"{LAKE_ROOT}/{layer}/{table}"
