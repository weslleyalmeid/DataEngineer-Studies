import os.path
from datetime import datetime
from email.mime import application

from airflow import DAG
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

with DAG(dag_id="twitter_dag", start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path=os.path.join(
            DATALAKE_DIR,
            "twitter_aluraonline",
            "extract_date={{ ds }}",
            "AluraOnline_{{ ds_nodash }}.json",
        ),
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
