FROM apache/airflow:2.7.1-python3.11

USER root

RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk procps libpq-dev curl && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

RUN mkdir -p /opt/bitnami/spark/jars

RUN chmod -R 777 /opt/bitnami/spark/jars

RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -o /tmp/postgresql-42.2.18.jar && \
    mv /tmp/postgresql-42.2.18.jar /opt/bitnami/spark/jars/postgresql-42.2.18.jar

USER airflow

RUN pip uninstall -y apache-airflow-providers-openlineage

RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark \
    apache-airflow-providers-openlineage>=1.8.0 psycopg2
