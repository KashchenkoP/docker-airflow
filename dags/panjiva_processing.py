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

    download_import_data = BashOperator(
        task_id='download-panjiva-import-data',
        bash_command='${AIRFLOW_HOME}/dags/downloader-scripts/download-import-data.sh ',
        dag=dag
    )

    download_export_data = BashOperator(
        task_id='download-panjiva-export-data',
        bash_command='${AIRFLOW_HOME}/dags/downloader-scripts/download-export-data.sh ',
        dag=dag
    )

    show_files = BashOperator(
        task_id='show-files',
        bash_command='ls -a ${AIRFLOW_HOME}',
        dag=dag
    )

    starter >> download_export_data >> receive_response >> show_files >> finisher
    starter >> download_import_data >> receive_response >> show_files >> finisher
