from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from services.ETL_service import ETLService

SCHEDULE_DAILY = '0 0 * * *'  # Run daily
SCHEDULE_HOURLY = '0 * * * *'  # Run every hour
SCHEDULE_MINUTES = '* * * * *'  # Run every minute


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 12),
    'retries': False
    # ,
    # 'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    description='ETL DAG Transaction Banking and Flexible Schedule',
    schedule_interval=SCHEDULE_DAILY,
    catchup=False
)

def run_bronze(**kwargs):
    service = ETLService('2025-09-17')
    service.load_bronze()
    service.close_conn()

def run_silver(**kwargs):
    service = ETLService('2025-09-17')
    service.transform_silver()
    service.close_conn()

def run_gold(**kwargs):
    service = ETLService('2025-09-17')
    service.transform_gold()
    service.close_conn()

t1 = PythonOperator(task_id='load_bronze', python_callable=run_bronze, dag=dag)
t2 = PythonOperator(task_id='transform_silver', python_callable=run_silver, dag=dag)
# t3 = PythonOperator(task_id='transform_gold', python_callable=run_gold, dag=dag)

t1 >> t2