from pyspark.sql import DataFrame, SparkSession

PG_HOST = "postgres"
PG_PORT = "5432"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def upsert_to_db(spark: SparkSession, df: DataFrame, table_name: str, composite_key: list, db_name="DwhOnlineRetail"):
    """
    Function to perform an upsert operation (update or insert) into a PostgreSQL table using JDBC.
    The composite_key parameter will be used as the primary key for checking duplicates.
    """
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{db_name}"
    connection_properties = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    existing_data = spark.read.format("jdbc").option(
        "url", jdbc_url
    ).option(
        "dbtable", table_name
    ).option(
        "user", PG_USER
    ).option(
        "password", PG_PASSWORD
    ).option(
        "driver", "org.postgresql.Driver"
    ).load()

    updated_records = df.join(existing_data, composite_key, how="leftanti")

    try:
        updated_records.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="append",
            properties=connection_properties
        )
        print(f"Data upserted into {table_name} in {db_name}.")
    except Exception as e:
        print(f"Error while upserting data to database: {e}")
