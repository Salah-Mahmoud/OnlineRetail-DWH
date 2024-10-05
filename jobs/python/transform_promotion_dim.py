from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.types import FloatType
from upsert_to_db import upsert_to_db

PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def transform_promotion_dim():
    """
    This function transforms the promotion dimension data by cleaning and transforming
    promotion-related columns and then loading the data into the PromotionDim table in the DWH.
    """

    spark = SparkSession.builder \
        .appName("Transform PromotionDim") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    print("Spark session created for PromotionDim transformation.")

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

    promotion_dim = staging_df.select(
        "PromotionName",
        col("DiscountPercentage").cast(FloatType()),
        "PromotionType",
        when(col("IsActivePromotion") == 1, True).otherwise(False).alias("IsActivePromotion")
    ).dropDuplicates(["PromotionName"])

    print("Promotion data transformed.")

    upsert_to_db(spark,promotion_dim, "PromotionDim", ["PromotionName"])



