from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, regexp_replace
from pyspark.sql.types import DecimalType, IntegerType
from upsert_to_db import upsert_to_db

PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

def transform_sales_fact():
    """
    Transform the SalesFact table by joining the sales staging data
    with the dimension tables and inserting the necessary IDs into the fact table.
    """
    spark = SparkSession.builder \
        .appName("Transform SalesFact") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    # Read data from the Sales Staging area
    sales_data = spark.read.format("jdbc").option(
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

    # Clean the columns by removing non-numeric characters and casting them
    sales_data = sales_data \
        .withColumn("QuantitySold", regexp_replace(col("QuantitySold"), "[^0-9]", "").cast(IntegerType())) \
        .withColumn("TotalSales", regexp_replace(col("TotalSales"), "[$,]", "").cast(DecimalType(10, 2))) \
        .withColumn("DiscountAmount", regexp_replace(col("DiscountAmount"), "[$,]", "").cast(DecimalType(10, 2))) \
        .withColumn("NetSales", regexp_replace(col("NetSales"), "[$,]", "").cast(DecimalType(10, 2)))

    # Load each dimension table
    customer_dim = spark.read.format("jdbc").option(
        "url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/DwhOnlineRetail"
    ).option(
        "dbtable", "public.customerdim"
    ).option(
        "user", PG_USER
    ).option(
        "password", PG_PASSWORD
    ).load()

    product_dim = spark.read.format("jdbc").option(
        "url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/DwhOnlineRetail"
    ).option(
        "dbtable", "public.productdim"
    ).option(
        "user", PG_USER
    ).option(
        "password", PG_PASSWORD
    ).load()

    date_dim = spark.read.format("jdbc").option(
        "url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/DwhOnlineRetail"
    ).option(
        "dbtable", "public.datedim"
    ).option(
        "user", PG_USER
    ).option(
        "password", PG_PASSWORD
    ).load()

    store_dim = spark.read.format("jdbc").option(
        "url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/DwhOnlineRetail"
    ).option(
        "dbtable", "public.storedim"
    ).option(
        "user", PG_USER
    ).option(
        "password", PG_PASSWORD
    ).load()

    payment_dim = spark.read.format("jdbc").option(
        "url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/DwhOnlineRetail"
    ).option(
        "dbtable", "public.paymentmethoddim"
    ).option(
        "user", PG_USER
    ).option(
        "password", PG_PASSWORD
    ).load()

    promotion_dim = spark.read.format("jdbc").option(
        "url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/DwhOnlineRetail"
    ).option(
        "dbtable", "public.promotiondim"
    ).option(
        "user", PG_USER
    ).option(
        "password", PG_PASSWORD
    ).load()



    # Perform lookups to get the dimension IDs and calculations
    sales_fact = sales_data \
        .alias("s") \
        .join(customer_dim.alias("c"), col("s.Email") == col("c.Email"), "left") \
        .join(product_dim.alias("p"), col("s.ProductName") == col("p.ProductName"), "left") \
        .join(date_dim.alias("d"), col("s.PurchaseDate") == col("d.Date"), "left") \
        .join(store_dim.alias("st"), col("s.StoreName") == col("st.StoreName"), "left") \
        .join(payment_dim.alias("pm"), col("s.PaymentType") == col("pm.PaymentType"), "left") \
        .join(promotion_dim.alias("pr"), col("s.PromotionName") == col("pr.PromotionName"), "left") \
        .select(
        col("c.CustomerID"),
        col("p.ProductID"),
        col("d.TimeID"),
        col("st.StoreID"),
        col("pm.PaymentMethodID"),
        col("pr.PromotionID"),
        col("s.QuantitySold"),
        expr("p.Price * s.QuantitySold").alias("TotalSales"),
        expr("p.Price * pr.DiscountPercentage / 100").alias("DiscountAmount"),
        expr("(p.Price * s.QuantitySold) - (p.Price * pr.DiscountPercentage / 100)").alias("NetSales")
    )

    # Write the fact table data to the DwhOnlineRetail database
    upsert_to_db(spark, sales_fact, "SalesFact",
                 ["CustomerID", "ProductID", "TimeID", "StoreID", "PaymentMethodID", "PromotionID"])

