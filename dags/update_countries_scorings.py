# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.http_operator import SimpleHttpOperator

from templates.sql import *


args = {
    'owner': 'airflow',
    'start_date': datetime(2000, 2, 27),
    'end_date': datetime(2050, 1, 1),
    'provide_context': True,
    'depends_on_past': True
}

dag = airflow.DAG(
    'data_vault.update_countries_scoring',
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath='/usr/local/airflow/sql',
    default_args=args,
    max_active_runs=1)

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    trigger_rule='all_success',
    dag=dag
)

receive_response = DummyOperator(
    task_id='receive_response',
    trigger_rule='all_success',
    dag=dag
)


def http_response_check():
    pass


def create_get_config_task():
    t = SimpleHttpOperator(
        endpoint='/api/services/a305b9c3-17e9-4d98-b27b-5323e26fdd6d/schemas/b341dc52-dee4-4d01-81d1-a5c3daab257a/settings',
        http_conn_id='configuration',
        method='GET',
        response_check=lambda r: r.status_code == 200,
        xcom_push=True,
        log_response=True,
        task_id='get-config',
        dag=dag
    )
    start >> t >> receive_response
    return t


def update_countries_scorings(
):
    t = PostgresOperator(
        sql=UPDATE_COUNTRY_SCORING,
        params=dict(
            from_dtm=datetime.now() - timedelta(1)
        ),
        postgres_conn_id='greenplum_connection_id',
        autocommit=True,
        database='exporters_register',
        task_id='data_vault.update_countries_scoring',
        dag=dag
    )

    receive_response >> t >> end
    return t


create_get_config_task()
update_countries_scorings()
