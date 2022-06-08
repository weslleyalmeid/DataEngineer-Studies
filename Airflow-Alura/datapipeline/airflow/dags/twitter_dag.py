import os.path
from datetime import datetime
import ipdb
import pendulum

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator
from operators.twitter_operator import TwitterOperator

DAGS_DIR = os.path.dirname(os.path.abspath(__file__ ))
AIRFLOW_DIR = os.path.dirname(DAGS_DIR)
DATAPIPE_DIR = os.path.dirname(AIRFLOW_DIR)
DATALAKE_DIR = os.path.join(DATAPIPE_DIR, "datalake")
SPARK_DIR = os.path.join(DATAPIPE_DIR, "spark")
BRONZE_DIR = os.path.join(DATALAKE_DIR, "bronze")
SILVER_DIR = os.path.join(DATALAKE_DIR, "silver")
GOLD_DIR = os.path.join(DATALAKE_DIR, "gold")
TWITTER_DIR = os.path.join(BRONZE_DIR, "twitter_aluraonline")

BASE_DIR = os.path.join(DATALAKE_DIR, '{stage}','twitter_aluraonline', '{partition}')
PARTITION_DIR = 'extract_date={{ ds }}'
# formula de padronizacao do timestamp
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.00Z'

ARGS = {
    'owner': 'aiflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-6)
}



# max_active_runs = maximo de instancia em paralelo
with DAG(dag_id="twitter_dag", default_args=ARGS, schedule_interval='0 9 * * *', max_active_runs=1) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path= os.path.join(
                    BASE_DIR.format(stage='bronze', partition=PARTITION_DIR),
                    "AluraOnline_{{ ds_nodash }}.json"
                ),
        start_time=(
            "{{"
            f" data_interval_start.strftime('{ TIMESTAMP_FORMAT }') "
            "}}"
        ),
        end_time=(
            "{{"
            f" data_interval_end.strftime('{ TIMESTAMP_FORMAT }') "
            "}}"
        )
    )

    # start_time= (f'{{ execution_date.strftime("{ TIMESTAMP_FORMAT }") }}')
    # end_time= (f'{{ next_execution_date.strftime("{ TIMESTAMP_FORMAT }") }}')
    
    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_aluraonline",
        application=os.path.join(SPARK_DIR, "transformation.py"),
        name="twitter_transformation",
        application_args=[
            "--src",
            BASE_DIR.format(stage='bronze', partition=PARTITION_DIR),
            "--dest",
            BASE_DIR.format(stage='silver', partition=''),
            "--process-date",
            "{{ ds }}",
        ]
    )

    # conectar o primeiro operador ao segundo
    twitter_operator >> twitter_transform

