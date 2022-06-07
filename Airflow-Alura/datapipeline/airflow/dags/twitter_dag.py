import os.path
from datetime import datetime
from email.mime import application

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator
from operators.twitter_operator import TwitterOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
AIRFLOW_DIR = os.path.dirname(DAG_DIR)
DATAPIPE_DIR = os.path.dirname(AIRFLOW_DIR)
DATALAKE_DIR = os.path.join(DATAPIPE_DIR, "datalake")
SPARK_DIR = os.path.join(DATAPIPE_DIR, "spark")
BRONZE_DIR = os.path.join(DATALAKE_DIR, "bronze")
SILVER_DIR = os.path.join(DATALAKE_DIR, "silver")
GOLD_DIR = os.path.join(DATALAKE_DIR, "gold")
TWITTER_DIR = os.path.join(BRONZE_DIR, "twitter_aluraonline")

ARGS = {
    'owner': 'aiflow',
    'depends_on_past': False,
    'start_date': days_ago(6)
}

# formul.a de padronizacao do timestamp
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.00Z'

# max_active_runs = maximo de instancia em paralelo
with DAG(dag_id="twitter_dag", default_args=ARGS, schedule_interval='0 9 * * *', max_active_runs=1) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path=os.path.join(
            DATALAKE_DIR,
            "twitter_aluraonline",
            "extract_date={{ ds }}",
            "AluraOnline_{{ ds_nodash }}.json",
        ),
        start_time= f'{{ execution_date.strftime("{ TIMESTAMP_FORMAT }") }}',
        end_time= f'{{ next_execution_date.strftime("{ TIMESTAMP_FORMAT }") }}'
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_aluraonline",
        application=os.path.join(SPARK_DIR, "transformation.py"),
        name="twitter_transformation",
        application_args=[
            "--src",
            os.path.join(BRONZE_DIR, "twitter_aluraonline", "extract_date=2022-06-05"),
            "--dest",
            os.path.join(SILVER_DIR, "twitter_aluraonline"),
            "--process-date",
            "{{ ds }}",
        ],
    )

    # conectar o primeiro operador ao segundo
    twitter_operator >> twitter_transform

