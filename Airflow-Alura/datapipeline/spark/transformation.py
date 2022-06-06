from pyspark.sql import SparkSession
import os

SPARK_DIR = os.path.dirname(os.path.abspath(__file__))
DATAPIPE_DIR = os.path.dirname(SPARK_DIR)
DATALAKE_DIR = os.path.join(DATAPIPE_DIR, 'datalake')
TWITTER_DIR = os.path.join(DATALAKE_DIR, 'twitter_aluraonline')

if __name__ == '__main__':

    spark = SparkSession\
        .builder\
        .appName('twitter_transformation')\
        .getOrCreate()

    df = spark.read.json(TWITTER_DIR)
    df.printSchema()
    df.show()