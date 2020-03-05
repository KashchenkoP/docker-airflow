import os
import airflow
from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

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

    hadoop_hook = SSHHook(
        remote_host='10.1.25.37',
        username='kashchenko',
        password='Gee9lohphiey',
        timeout=30
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

    parse_and_put = BashOperator(
        task_id='parse-panjiva-archievies-and-put-2hdfs',
        bash_command='${AIRFLOW_HOME}/dags/parsing-scripts/parse-panjiva-and-put.sh ',
        dag=dag
    )

    launch_remote = SSHOperator(
        task_id='deploy-to-hive',
        remote_host='10.1.25.37',
        ssh_hook=hadoop_hook,
        command=u'echo $HOSTNAME',
        dag=dag
    )

    show_files = BashOperator(
        task_id='show-files',
        bash_command='ls -a ${AIRFLOW_HOME}',
        dag=dag
    )

    starter >> download_export_data >> receive_response >> launch_remote >> parse_and_put >> show_files >> finisher
    starter >> download_import_data >> receive_response >> launch_remote >> parse_and_put >> show_files >> finisher
