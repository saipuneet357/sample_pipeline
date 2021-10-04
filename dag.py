from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import tasks
from datetime import datetime,timedelta

DAG_ID = 'sample_pipeline'

default_args = {
    'owner': 'Puneet',
    'start_date': datetime(2020,8,13),
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'email': 'saipuneet357@gmail.com',
    'email_on_failure': 'saipuneet357@gmail.com',
    'email_on_retry': 'saipuneet357@gmail.com',
    'retries': 1
}


dag = DAG(dag_id=DAG_ID,
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False)


source_extract = PythonOperator(
    task_id='source_extract',
    python_callable=tasks.extract_data_from_source,
    provide_context=True,
    dag=dag
)

target_load = PythonOperator(
    task_id='target_load',
    python_callable=tasks.load_data_into_target,
    provide_context=True,
    dag=dag
)

target_load.set_upstream(source_extract)
