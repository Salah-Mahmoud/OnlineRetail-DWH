from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/jobs/python/')

from load_data import extract_data
from truncate_stage_table import truncate_stage_table_spark
from transform_customer_dim import transform_customer_dim
from transform_date_dim import transform_date_dim
from transform_payment_dim import transform_payment_dim
from transform_promotion_dim import transform_promotion_dim
from transform_product_dim import transform_product_dim
from transform_store_dim import transform_store_dim
from transform_sales_fact import transform_sales_fact

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'ETL_for_DWH',
    default_args=default_args,
    description='ETL process using Spark to build DWH',
    schedule_interval='@daily',
)

truncate_stage_task = PythonOperator(
    task_id='truncate_staging_tables',
    python_callable=truncate_stage_table_spark,
    dag=dag,
)

stage_data = PythonOperator(
    task_id='stage_data',
    python_callable=extract_data,
    dag=dag,
)

CustomerDim_T = PythonOperator(
    task_id='customer_dim',
    python_callable=transform_customer_dim,
    dag=dag,
)

DateDim_T = PythonOperator(
    task_id='date_dim',
    python_callable=transform_date_dim,
    dag=dag,
)

PaymentDim_T = PythonOperator(
    task_id='payment_dim',
    python_callable=transform_payment_dim,
    dag=dag,
)

PromotionDim_T = PythonOperator(
    task_id='promotion_dim',
    python_callable=transform_promotion_dim,
    dag=dag,
)

StoreDim_T = PythonOperator(
    task_id='store_dim',
    python_callable=transform_store_dim,
    dag=dag,
)

ProductDim_T = PythonOperator(
    task_id='product_dim',
    python_callable=transform_product_dim,
    dag=dag,
)

SalesFact_T = PythonOperator(
    task_id='sales_fact',
    python_callable=transform_sales_fact,
    dag=dag,
)

truncate_stage_task >> stage_data >> [CustomerDim_T, DateDim_T, ProductDim_T, StoreDim_T, PromotionDim_T, PaymentDim_T] >> SalesFact_T
