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
        dag_id='panjiva-processing',
        schedule_interval="@once",
        default_args=args
) as dag:
    ############################################################
    # Service dummy operators
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
        task_id='receive-response',
        trigger_rule='all_success',
        dag=dag
    )
    ############################################################

    py_greeting = PythonOperator(
        task_id='python-greetings',
        python_callable=dummy_python_operator,
        dag=dag
    )

    launch_shellscript = BashOperator(
        task_id='bash-greetings',
        bash_command='${AIRFLOW_HOME}/dags/parsing-scripts/dummy.sh ',
        dag=dag
    )

    show_files = BashOperator(
        task_id='show-files',
        bash_command='ls -a ${AIRFLOW_HOME}/dags/parsing-scripts/',
        dag=dag
    )

    finisher << show_files << receive_response << launch_shellscript << starter >> py_greeting >> receive_response >> finisher
