from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime , timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'DatapathHeylin',
    'depends_on_past': False,
    'email_on_failure': 'nilyeh@hotmail.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def start_process():
    print(" INICIO EL PROCESO!")

def fun_load_bi():
    print(" Hola Airflow!")

def load_raw_2():
    print(" Hola Raw 2!")

def load_raw_1():
    print(" Hola Raw 1!")

def load_step_master2():
    print(" Load Master2!")

def load_step_master():
    print(" Load Master1!")




with DAG(
    dag_id="dag_paralelo_tarea",
    schedule="20 04 * * *", 
    start_date=days_ago(1), 
    default_args=default_args,
    description='Prueba de Dag'
) as dag:
    step_start = PythonOperator(
        task_id='step_start',
        python_callable=start_process,
        dag=dag
    )
    step_load_raw_1 = PythonOperator(
        task_id='step_load_raw_1',
        python_callable=load_raw_1,
        dag=dag
    )
    step_load_raw_2 = PythonOperator(
        task_id='step_load_raw_2',
        python_callable=load_raw_2,
        dag=dag
   )
    load_master_2 = PythonOperator(
        task_id='load_step_master_2',
        python_callable=load_step_master2,
        dag=dag
    )
    load_master_ini = PythonOperator(
        task_id='load_step_master',
        python_callable=load_step_master,
        dag=dag
    )
    load_bi = PythonOperator(
        task_id='load_bi',
        python_callable=fun_load_bi,
        dag=dag
    )
    step_start>>step_load_raw_1
    step_start>>step_load_raw_2
    step_load_raw_1>>load_master_ini
    step_load_raw_2>>load_master_2
    load_master_ini>>load_bi
    load_master_2>>load_bi
