import os
import sys

dir_ = os.path.dirname(os.path.abspath(__file__))
module_path = os.path.join(dir_, '..')
if module_path not in sys.path:
    sys.path.append(module_path)
from scripts import transformations

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
import pandas as pd


def factory_connection_subdag(parent_dag_name, child_dag_name, default_args):
    with DAG(
            dag_id='%s.%s' % (parent_dag_name, child_dag_name),
            default_args=default_args
    ) as dag:
        _resource_path = "~/connections_config.csv"
        connection_config_df = pd.read_csv(_resource_path)

        for _, row in connection_config_df.iterrows():
            connection_name = row['connection_name']
            connection_path = row['base_path']

            register_connection = MySqlOperator(
                task_id=f"register_connection_{connection_name}",
                sql='''
                    INSERT INTO connection_metadata (load_id,connection_name,status) 
                    VALUES ({{ ti.xcom_pull(dag_id='''+parent_dag_name+''',key='load_id') }},
                    '''
                    +
                    f"'{connection_name}','IN_PROGRESS')",
                mysql_conn_id="solr_pipeline_metadata_conn",
            )

            ingest_connection = PythonOperator(
                task_id=f"ingest_connection_{connection_name}",
                python_callable=transformations.ingest_data,
                provide_context=True,
                templates_dict={'name': connection_name, 'base_path': connection_path}
            )

            update_connection = MySqlOperator(
                task_id=f"update_connection_{connection_name}",
                sql=f'''
                    UPDATE connection_metadata set status = 'SUCCEEDED' WHERE
                    connection_name = '{connection_name}' AND 
                    '''
                    +
                    "load_id = {{ ti.xcom_pull(dag_id="+parent_dag_name+", key='load_id') }}",
                mysql_conn_id="solr_pipeline_metadata_conn",
            )

            register_connection >> ingest_connection >> update_connection

    return dag
