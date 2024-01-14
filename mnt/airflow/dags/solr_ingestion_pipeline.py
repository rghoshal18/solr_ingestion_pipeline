from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.subdag_operator import SubDagOperator
# from airflow.executors.sequential_executor import SequentialExecutor
from connection_index.connection_subdag import factory_subdag
from datetime import datetime

DAG_NAME="solr_ingestion_pipeline"

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    'start_date': datetime(2024, 1 ,1), 
}

with DAG(DAG_NAME, schedule_interval="@weekly", 
         default_args=default_args, catchup=False) as dag:

    register_load = MySqlOperator(
        task_id = "register_load",
        sql = '''
            INSERT INTO load_metadata (llk,status) VALUES ('{{ macros.ds_format(ts_nodash, '%Y%m%dT%H%M%S', '%Y%m%d%H%M%S') }}','IN_PROGRESS')
        ''',
        mysql_conn_id = "solr_pipeline_metadata_conn",
    )

    conn_ingestion = SubDagOperator(
        task_id='conn_ingestion',
        subdag=factory_subdag(DAG_NAME, 'conn_ingestion', default_args),
        # executor=SequentialExecutor()
    )

    # downloading_rates = PythonOperator(
    #     task_id="downloading_rates",
    #     python_callable=download_rates
    # )

    # forex_processing = SparkSubmitOperator(
    #     task_id="forex_processing",
    #     application="/usr/local/airflow/dags/scripts/forex_processing.py",
    #     conn_id="spark_conn",
    #     verbose=False
    # )

    # send_email_notification = EmailOperator(
    #     task_id="send_email_notification",
    #     to="airflow_course@yopmail.com",
    #     subject="forex_data_pipeline",
    #     html_content="<h3>forex_data_pipeline</h3>"
    # )
    
    # downloading_rates >> forex_processing 
    # forex_processing >> send_email_notification

    register_load >> conn_ingestion