from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    DateType,
    StructField,
    StructType,
)

schema = StructType([
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("PhoneNumber", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("ZipCode", StringType(), True),
    StructField("PreviousCity", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("PreviousPrice", StringType(), True),
    StructField("PurchaseDate", DateType(), True),
    StructField("StoreName", StringType(), True),
    StructField("StoreLocation", StringType(), True),
    StructField("StoreType", StringType(), True),
    StructField("OpeningDate", DateType(), True),
    StructField("ManagerName", StringType(), True),
    StructField("PaymentType", StringType(), True),
    StructField("Provider", StringType(), True),
    StructField("PromotionName", StringType(), True),
    StructField("DiscountPercentage", StringType(), True),
    StructField("PromotionType", StringType(), True),
    StructField("IsActivePromotion", IntegerType(), True),
    StructField("QuantitySold", StringType(), True),
    StructField("TotalSales", StringType(), True),
    StructField("DiscountAmount", StringType(), True),
    StructField("NetSales", StringType(), True)
])


def staging_layer_jdbc(df):
    """
    Load DataFrame into PostgreSQL sales_data_staging table using JDBC.
    """
    jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
    connection_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(
        url=jdbc_url,
        table="public.sales_data_staging",
        mode="append",
        properties=connection_properties
    )

    print("Data successfully loaded into PostgreSQL via JDBC.")


def extract_data():
    """
    Extracts data from CSV and loads it into PostgreSQL using JDBC.
    """
    spark = SparkSession.builder \
        .appName("Extract and Load to PostgreSQL via JDBC") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    df = spark.read.csv('/opt/airflow/data/SrcData.csv', header=True, schema=schema)

    row_count = df.count()
    print(f"DataFrame Row Count: {row_count}")

    if row_count == 0:
        print("The DataFrame is empty. No data to load.")
    else:
        staging_layer_jdbc(df)
