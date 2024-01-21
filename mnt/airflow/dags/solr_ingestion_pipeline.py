"""
solr_ingestion_pipeline DAG

This DAG orchestrates the Solr ingestion pipeline, including the registration of a new load,
retrieving the load ID, executing entity ingestion sub-DAG, updating load status,
and sending email notifications upon completion.

Tasks:
- register_load: Register a new load in the load_metadata table with status 'IN_PROGRESS'.
- get_load_id: Retrieve the load ID for the current load_log_key and push it to XCom.
- conn_ingestion: SubDagOperator to execute the entity ingestion sub-DAG.
- update_load: Update the load_metadata table setting the status to 'SUCCEEDED'.
- send_email_notification: Send an email notification upon load completion.

"""
from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from airflow.executors.celery_executor import CeleryExecutor
from airflow.hooks.mysql_hook import MySqlHook
from entity_ingestion.entity_ingestion_sub_dag import factory_entity_sub_dag
from datetime import datetime

DAG_NAME = "solr_ingestion_pipeline"


def fetch_load_id(llk, **context):
    """
        Retrieve the load ID from the load_metadata table and push it to XCom.

        Parameters:
        - llk (str): The load log key.

    """
    request = f"SELECT id FROM load_metadata where load_log_key = '{llk}' "
    mysql_hook = MySqlHook(mysql_conn_id='solr_pipeline_metadata_conn')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    load_id = str(cursor.fetchall()[0][0])
    print(f"Fetched load id for {llk}: {load_id}")
    context['ti'].xcom_push(key="load_id", value=load_id)


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
    load_log_key = "{{ macros.ds_format(ts_nodash, '%Y%m%dT%H%M%S', '%Y%m%d') }}"

    register_load = MySqlOperator(
        task_id="register_load",
        sql=f'''
            INSERT INTO load_metadata (load_log_key,status) VALUES 
            ('{load_log_key}','IN_PROGRESS')
        ''',
        mysql_conn_id="solr_pipeline_metadata_conn",
    )

    get_load_id = PythonOperator(
        task_id="get_load_id",
        python_callable=fetch_load_id,
        op_args=[load_log_key],
        provide_context=True
    )

    conn_ingestion = SubDagOperator(
        task_id='conn_ingestion',
        subdag=factory_entity_sub_dag(DAG_NAME, 'conn_ingestion', default_args),
        executor=CeleryExecutor()
    )

    update_load = MySqlOperator(
        task_id="update_load",
        sql='''
            UPDATE load_metadata SET STATUS = 'SUCCEEDED' WHERE id = {{ ti.xcom_pull(key='load_id') }}
        ''',
        mysql_conn_id="solr_pipeline_metadata_conn",
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="rghoshal8989@gmail.com",
        subject=f"Load Completion Status {load_log_key}",
        html_content=f"Hi team, <br>The load is now completed for Load Log Key {load_log_key}."
    )

    register_load >> get_load_id >> conn_ingestion >> update_load >> send_email_notification
