import airflow
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator


from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

args = {
    'owner': 'ivishnevsky',
    # when set to True, keeps a task from getting triggered if the previous schedule for the task failed
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
        dag_id='rec_tovar',
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

    hadoop_hook = SSHHook(
        remote_host='10.1.25.37',
        username='kashchenko',
        password='Gee9lohphiey',
        timeout=30
    )

    create_hive_table = SSHOperator(
        task_id='download_rec_tovar_to_hive',
        remote_host='10.1.25.37',
        ssh_hook=hadoop_hook,
        command=u'/home/kashchenko/fts-handler/run-parser.sh ',
        dag = dag
    )

    starter >> create_hive_table >> finisher