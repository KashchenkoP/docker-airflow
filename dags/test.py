import os
import airflow
from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'vit',
    # when set to True, keeps a task from getting triggered if the previous schedule for the task hasn’t succeeded
    'depends_on_past': True,
    # Dict of global variables to be used within DAG
    'provide_context': True,
    #
    'start_date': datetime.now(),
    'end_date': datetime(2050, 1, 1)
}


def dummy_python_operator(ds, **kwards):
    return 'Hello from pythonic world'


with airflow.DAG(
    dag_id='tiny-dag',
    schedule_interval="@once",
    default_args=args
) as dag:

    ############################################################
    # Service dummpy operators
    starter = DummyOperator(
        task_id='launcher',
        retries=3,
        dag=dag
    )

    finisher = DummyOperator(
        task_id='finisher',
        trigger_rule='all_success',
        dag=dag
    )
    
    receive_response = DummyOperator(
        task_id='receive_response',
        trigger_rule='all_success',
        dag=dag
    )
    ############################################################
    
    python_operator = PythonOperator(
        task_id='python-greetings',
        python_callable=dummy_python_operator,
        dag=dag
    )

    bash_operator = BashOperator(
        task_id='bash-greetings',
        bash_command='echo "Hello from bashistic world!"',
        dag=dag
    )

    starter >> python_operator >> receive_response >> finisher << receive_response << bash_operator
