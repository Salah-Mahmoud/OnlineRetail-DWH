from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col
from upsert_to_db import upsert_to_db

PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

def transform_customer_dim():
    """
    Transforms customer data from the staging area and loads it into the CustomerDim table in the DWH.
    """

    spark = SparkSession.builder \
        .appName("Transform CustomerDim") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    print("Spark session created for CustomerDim transformation.")

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

    customer_dim = staging_df.select(
        concat_ws(" ", col("FirstName"), col("LastName")).alias("Name"),
        col("Email"),
        col("PhoneNumber"),
        col("Address"),
        col("City"),
        col("State"),
        col("Country"),
        col("ZipCode"),
        col("PreviousCity")
    ).dropDuplicates(["Email"])

    print("Customer dimension transformed.")

    upsert_to_db(spark,customer_dim, "CustomerDim", ["Email"])

