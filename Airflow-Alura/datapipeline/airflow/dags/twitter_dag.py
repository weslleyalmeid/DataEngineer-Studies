from airflow import DAG
from operators.twitter_operator import TwitterOperator
from datetime import datetime
import os.path

OPERATORS_DIR = os.path.dirname(os.path.abspath(__file__))
PLUGINS_DIR = os.path.dirname(OPERATORS_DIR)
AIRFLOW_DIR = os.path.dirname(PLUGINS_DIR)
DATAPIPE_DIR = os.path.dirname(AIRFLOW_DIR)
DATALAKE_DIR = os.path.join(DATAPIPE_DIR, 'datalake')

# import ipdb; ipdb.set_trace()

with DAG(dag_id='twitter_dag', start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id='twitter_aluraonline',
        query='AluraOnline',
        file_path=os.path.join(
                DATALAKE_DIR,
                'twitter_aluraonline',
                'extract_date={{ ds }}',
                'AluraOnline_{{ ds_nodash }}.json',
            ),
    )
