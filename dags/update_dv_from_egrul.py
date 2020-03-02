# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from templates.sql import *


hubs = (
    ('h_local_company_egrul_main', 'local_company_pk'),
    ('h_test', 'test_pk'),
)

sats = (
    ('s_local_company_legal_name', 'svnaimul_grndata__grn', 'local_company_pk'),
    ('s_local_company_main_okved', 'svokved_svokvedosn_grndata__grn', 'local_company_pk')
)

args = {
    'owner': 'airflow',
    'start_date': datetime(2000, 2, 27),
    'end_date': datetime(2050, 1, 1),
    'provide_context': True,
    'depends_on_past': True
}

dag = airflow.DAG(
    'data_vault.load_egrul',
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
    dag=dag
)

hubs_loaded = DummyOperator(
    task_id='hubs_loaded',
    dag=dag
)

sats_loaded = DummyOperator(
    task_id='sats_loaded',
    dag=dag
)


def create_load_hub_from_ext(
            hub_name,
            pk_name
):
    t = PostgresOperator(
        sql=LOAD_HUB_FROM_EXT_TEMP,
        params=dict(
            hub_name=hub_name,
            pk_name=pk_name,
            from_dtm=datetime.now() - timedelta(1)
        ),
        postgres_conn_id='greenplum_connection_id',
        autocommit=True,
        database='exporters_register',
        task_id='load.{}'.format(hub_name),
        dag=dag
    )

    start >> t >> hubs_loaded
    return t


def create_load_sat_from_ext(
        sat_name,
        *hash_diff_cols
):
    t = PostgresOperator(
        sql=LOAD_SAT_FROM_EXT_TEMP,
        params=dict(
            sat_name=sat_name,
            hash_diff_cols=hash_diff_cols,
            from_dtm=datetime.now() - timedelta(1)
        ),
        postgres_conn_id='greenplum_connection_id',
        autocommit=True,
        database='exporters_register',
        task_id='load.{}'.format(sat_name),
        dag=dag
    )
    hubs_loaded >> t >> sats_loaded
    return t


sats_loaded >> end

map(lambda x: create_load_hub_from_ext(*x), hubs)
map(lambda x: create_load_sat_from_ext(*x), sats)






