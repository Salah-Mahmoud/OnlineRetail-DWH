from pyspark.sql import SparkSession


def truncate_stage_table_spark(table_name='sales_data_staging'):
    """
    Truncate the given staging table using Spark's JDBC connection to PostgreSQL.
    """
    spark = SparkSession.builder \
        .appName("TruncateTable") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://postgres:5432/postgres"

    connection_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    try:
        spark.read \
            .jdbc(jdbc_url, f"(TRUNCATE TABLE {table_name} CASCADE) as dummy", properties=connection_properties)
        print(f"Staging table {table_name} successfully truncated.")
    except Exception as e:
        print(f"Error truncating staging table {table_name}: {e}")
    finally:
        spark.stop()
