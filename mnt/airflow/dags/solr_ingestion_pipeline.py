from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.subdag_operator import SubDagOperator
from connection_index.connection_subdag import factory_subdag
from datetime import datetime

DAG_NAME = "solr_ingestion_pipeline"

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    'start_date': datetime(2024, 1, 1),
}

with DAG(DAG_NAME, schedule_interval="@weekly",
         default_args=default_args, catchup=False) as dag:
    register_load = MySqlOperator(
        task_id="register_load",
        sql='''
            INSERT INTO load_metadata (llk,status) VALUES 
            ('{{ macros.ds_format(ts_nodash, '%Y%m%dT%H%M%S', '%Y%m%d') }}','IN_PROGRESS')
        ''',
        mysql_conn_id="solr_pipeline_metadata_conn",
    )

    conn_ingestion = SubDagOperator(
        task_id='conn_ingestion',
        subdag=factory_subdag(DAG_NAME, 'conn_ingestion', default_args),
        # executor=SequentialExecutor()
    )

    update_load = MySqlOperator(
        task_id="update_load",
        sql='''
            UPDATE load_metadata SET STATUS = 'SUCCEEDED' WHERE id = 1
        ''',
        mysql_conn_id="solr_pipeline_metadata_conn",
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="rghoshal8989@gmail.com",
        subject="Load Completion Status",
        html_content="Hi, The load is now completed for Load Log Key "
                     "{{ macros.ds_format(ts_nodash, '%Y%m%dT%H%M%S', '%Y%m%d') }}"
    )

    register_load >> conn_ingestion >> update_load >> send_email_notification
