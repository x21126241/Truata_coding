from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
import time

dag_name = 'dagName'

default_args = {
    'owner': 'Airflow',
    'start_date':datetime(2022,2,16),
    'schedule_interval':'@once',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'catchup':False
}
with DAG(dag_name,default_args=default_args) as dag:
    t1 = DummyOperator(task_id="task_1")
    t2 = DummyOperator(task_id="task_2")
    t3 = DummyOperator(task_id="task_3")
    t4 = DummyOperator(task_id="task_4")
    t5 = DummyOperator(task_id="task_5")
    t6 = DummyOperator(task_id="task_6")
    
    t1 >> [t2,t3] >> [t4,t5,t6]