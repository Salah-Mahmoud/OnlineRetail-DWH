import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, quarter, year, col
from pandas.tseries.holiday import USFederalHolidayCalendar as calendar
from upsert_to_db import upsert_to_db

PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def transform_date_dim():
    """
    Transforms the date data from the sales staging area and loads it into the DateDim table in the DWH.
    This includes deriving the day of the week, month, quarter, year, and whether a date is a holiday.
    """

    spark = SparkSession.builder \
        .appName("Transform DateDim") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    print("Spark session created for DateDim transformation.")

    online_retail = spark.read.format("jdbc").option(
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

    date_df = online_retail.select("PurchaseDate")

    start_date = pd.to_datetime("2021-01-01")
    end_date = pd.to_datetime("2026-12-31")

    cal = calendar()
    holidays = cal.holidays(start=start_date, end=end_date)
    holidays_list = [str(date.date()) for date in holidays]

    date_dim = (
        date_df
        .withColumnRenamed("PurchaseDate", "Date")
        .withColumn("DayOfWeek", date_format(col("Date"), "EEEE"))
        .withColumn("Month", date_format(col("Date"), "MMMM"))
        .withColumn("Quarter", quarter(col("Date")).cast("string"))
        .withColumn("Year", year(col("Date")))
        .withColumn("IsHoliday", date_format(col("Date"), "yyyy-MM-dd").isin(holidays_list))
        .dropDuplicates(["Date"])
    )

    print("DateDim transformation complete.")

    upsert_to_db(spark, date_dim, "DateDim", ["Date"])

