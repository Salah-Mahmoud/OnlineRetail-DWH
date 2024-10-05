from pyspark.sql import SparkSession
from upsert_to_db import upsert_to_db

PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def transform_payment_dim():
    """
    This function extracts payment information from the sales data staging area,
    and transforms it into the PaymentMethodDim table in the DWH.
    """

    spark = SparkSession.builder \
        .appName("Transform PaymentDim") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    print("Spark session created for PaymentDim transformation.")

    sales_data_staging = spark.read.format("jdbc").option(
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

    print("Sales data staging loaded.")

    payment_dim_df = sales_data_staging.select(
        "PaymentType",
        "Provider"
    ).dropDuplicates()

    print("Payment dimension data prepared.")

    upsert_to_db(spark, payment_dim_df, "PaymentMethodDim", ["PaymentType", "Provider"])

