from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from connection_index.connection_subdag import factory_subdag
from datetime import datetime

DAG_NAME = "solr_ingestion_pipeline"


def fetch_load_id(load_log_key, **context):
    request = f"SELECT id FROM load_metadata where load_log_key = '{load_log_key}' "
    mysql_hook = MySqlHook(mysql_conn_id='solr_pipeline_metadata_conn')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    load_id = str(cursor.fetchall()[0][0])
    print(f"Fetched load id for {load_log_key}: {load_id}")
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
        subdag=factory_subdag(DAG_NAME, 'conn_ingestion', default_args),
        # executor=SequentialExecutor()
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
        subject="Load Completion Status",
        html_content=f"Hi team, <br>The load is now completed for Load Log Key {load_log_key}."
    )

    register_load >> get_load_id >> conn_ingestion >> update_load >> send_email_notification
