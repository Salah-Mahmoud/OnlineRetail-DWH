from pyspark.sql import SparkSession
from upsert_to_db import upsert_to_db

PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def transform_store_dim():
    """
    Transforms store-related data and loads it into the StoreDim table in the DWH.
    """

    spark = SparkSession.builder \
        .appName("Transform StoreDim") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    print("Spark session created for StoreDim transformation.")

    staging_df = spark.read.format("jdbc").option(
        "url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/postgres"
    ).option(
        "dbtable", "public.sales_data_staging"
    ).option(
        "user", PG_USER
    ).option(
        "password", PG_PASSWORD
    ).option(
        "driver", "org.postgresql.Driver"
    ).load()

    print("Data loaded from staging area.")

    store_dim = staging_df.select(
        "StoreName",
        "StoreLocation",
        "StoreType",
        "OpeningDate",
        "ManagerName"
    ).dropDuplicates(["StoreName"])

    print("Store data transformed.")

    upsert_to_db(spark,store_dim, "StoreDim", ["StoreName"])

