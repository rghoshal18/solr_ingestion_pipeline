import os
import sys

dir_ = os.path.dirname(os.path.abspath(__file__))
module_path = os.path.join(dir_, '..')
if module_path not in sys.path:
    sys.path.append(module_path)
from scripts import etl

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
import pandas as pd


def factory_entity_sub_dag(parent_dag_name, child_dag_name, default_args):
    """
        Factory function to create a sub-DAG for ingesting data for multiple entities.

        Parameters:
        - parent_dag_name (str): The name of the parent DAG.
        - child_dag_name (str): The name of the sub-DAG.
        - default_args (dict): Default arguments for the sub-DAG.

        Returns:
        DAG: The sub-DAG object.

        Tasks:
        - register_entity_{entity_name}: MySQLOperator task to register a new entity in the entity_metadata table with
          the status 'IN_PROGRESS'.
        - ingest_entity_{entity_name}: PythonOperator task to execute the ingest_data function from the
          etl module, ingesting data for the specified entity. It retrieves the parameters from the
          entities_config.csv file and uses XCom to pass information between tasks.
        - update_entity_{entity_name}: MySQLOperator task to update the entity_metadata table setting
          the status to 'SUCCEEDED' for the specified entity and load.

    """
    with DAG(
            dag_id='%s.%s' % (parent_dag_name, child_dag_name),
            default_args=default_args
    ) as dag:
        _resource_path = "~/entities_config.csv"
        entity_config_df = pd.read_csv(_resource_path)

        for _, row in entity_config_df.iterrows():
            entity_name = row['entity_name']
            file_id = row['file_id']

            register_entity = MySqlOperator(
                task_id=f"register_entity_{entity_name}",
                sql='''
                    INSERT INTO entity_metadata (load_id,entity_name,status) 
                    VALUES ({{ ti.xcom_pull(dag_id='''+parent_dag_name+''',key='load_id') }},
                    '''
                    +
                    f"'{entity_name}','IN_PROGRESS')",
                mysql_conn_id="solr_pipeline_metadata_conn",
            )

            ingest_entity = PythonOperator(
                task_id=f"ingest_entity_{entity_name}",
                python_callable=etl.ingest_data,
                provide_context=True,
                templates_dict={
                    'entity_name': entity_name,
                    'file_id': file_id,
                    'load_log_key': "{{ macros.ds_format(ts_nodash, '%Y%m%dT%H%M%S', '%Y%m%d') }}",
                    'load_id': "{{ ti.xcom_pull(dag_id="+parent_dag_name+", key='load_id') }}"
                }
            )

            update_entity_metadata = MySqlOperator(
                task_id=f"update_entity_{entity_name}_metadata",
                sql=f'''
                    UPDATE entity_metadata set status = 'SUCCEEDED' WHERE
                    entity_name = '{entity_name}' AND 
                    '''
                    +
                    "load_id = {{ ti.xcom_pull(dag_id="+parent_dag_name+", key='load_id') }}",
                mysql_conn_id="solr_pipeline_metadata_conn",
            )

            register_entity >> ingest_entity >> update_entity_metadata

    return dag
