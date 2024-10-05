from pyspark.sql import SparkSession
from upsert_to_db import upsert_to_db
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace

PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def transform_product_dim():
    """
    This function transforms the product dimension data by cleaning price fields
    and loading the data into the ProductDim table in the DWH.
    """

    spark = SparkSession.builder \
        .appName("Transform ProductDim") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    print("Spark session created for ProductDim transformation.")

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

    print("Data loaded from sales_data_staging.")

    product_df = staging_df.select(
        "ProductName",
        "Category",
        "Price",
        "PreviousPrice"
    ).dropDuplicates(["ProductName"])

    product_dim = (
        product_df
        .withColumn("Price",
                    regexp_replace("Price", "\\$", "").cast(FloatType()))  # Remove dollar sign and cast to Float
        .withColumn("PreviousPrice", regexp_replace("PreviousPrice", "\\$", "").cast(FloatType()))
    ).dropDuplicates(["ProductName"])

    print("Product data transformed with cleaned price columns.")

    upsert_to_db(spark,product_dim, "ProductDim", ["ProductName"])




