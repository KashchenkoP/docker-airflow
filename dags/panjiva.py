import airflow
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator


from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

args = {
    'owner': 'vit',
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
        dag_id='panjiva',
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

    wait_unzipping = DummyOperator(
        task_id='local-achieves-barrier',
        trigger_rule='all_success',
        dag=dag
    )

    wait_uploading = DummyOperator(
        task_id='hive-uploading-barrier',
        trigger_rule='all_success',
        dag=dag
    )

    export_barrier = DummyOperator(
        task_id='export-downloading-barrier',
        trigger_rule='all_success',
        dag=dag
    )

    import_barrier = DummyOperator(
        task_id='import-downloading-barrier',
        trigger_rule='all_success',
        dag=dag
    )

    hadoop_hook = SSHHook(
        remote_host='10.1.25.37',
        username='kashchenko',
        password='pwd',
        timeout=30
    )
    ############################################################
    '''
    # Use bash scripts if there is an interconnect with Hadoop/HDFS
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
    #
    '''

    download_import_data = SSHOperator(
        task_id='download-panjiva-import-data',
        remote_host='10.1.25.37',
        ssh_hook=hadoop_hook,
        command=u'/data/demo/download-import-data.sh '
    )

    download_export_data = SSHOperator(
        task_id='download-panjiva-export-data',
        remote_host='10.1.25.37',
        ssh_hook=hadoop_hook,
        command=u'/data/demo/download-export-data.sh '
    )

    parse_and_put = SSHOperator(
        task_id='parse-panjiva-archievies-and-put-2hdfs',
        remote_host='10.1.25.37',
        ssh_hook=hadoop_hook,
        command=u'/data/demo/parse-panjiva-and-put.sh ',
        dag=dag
    )

    starter >> download_export_data >> export_barrier >> parse_and_put
    starter >> download_import_data >> import_barrier >> parse_and_put

    upload_exp_data = SSHOperator(
        task_id='upload-export-data-to-hive',
        remote_host='10.1.25.37',
        ssh_hook=hadoop_hook,
        command=u'echo $HOSTNAME',
        dag=dag
    )

    upload_imp_data = SSHOperator(
        task_id='upload-import-data-to-hive',
        remote_host='10.1.25.37',
        ssh_hook=hadoop_hook,
        command=u'/data/demo/submit-df-handler.sh ',
        dag=dag
    )

    parse_and_put >> wait_unzipping >> upload_exp_data >> wait_uploading
    parse_and_put >> wait_unzipping >> upload_imp_data >> wait_uploading

    companies_info = HiveOperator(
        task_id='united-companies-table',
        #hql='${AIRFLOW_HOME}/sql/companies_info.sql ',
        hql='select * from companies_info',
        hive_cli_conn_id='hive'
    )

    wait_uploading >> finisher
