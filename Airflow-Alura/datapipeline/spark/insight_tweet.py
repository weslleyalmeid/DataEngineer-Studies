from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, countDistinct, sum, date_format
import os

SPARK_DIR = os.path.dirname(os.path.abspath(__file__ ))
DATAPIPE_DIR = os.path.dirname(SPARK_DIR)
DATALAKE_DIR = os.path.join(DATAPIPE_DIR, "datalake")

BASE_DIR = os.path.join(DATALAKE_DIR, '{stage}','twitter_aluraonline', '{folder_name}')

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName('twitter_insight_tweet')\
        .getOrCreate()

    tweet = spark.read.json(BASE_DIR.format(stage='silver', folder_name='tweet'))
    alura = tweet.where('author_id = "1566580880"').select('author_id', 'conversation_id')

    tweet = tweet.alias('tweet')\
        .join(
            alura.alias('alura'),
            [
                tweet.author_id != alura.author_id,
                tweet.conversation_id == tweet.conversation_id
            ],
            'left'
        )\
        .withColumn('alura_conversation', when(col('alura.conversation_id').isNotNull(), 1).otherwise(0))\
        .withColumn('reply_alura', when(col('tweet.in_reply_to_user_id') == "1566580880", 1).otherwise(0))\
        .groupBy(to_date('created_at').alias('created_date'))\
        .agg(
            countDistinct('id').alias('n_tweets'),
            countDistinct('tweet.conversation_id').alias('n_conversation'),
            sum('alura_conversation').alias('alura_conversation'),
            sum('reply_alura').alias('reply_alura'),

        )\
        .withColumn('weekday', date_format('created_date', 'E'))

    tweet.coalesce(1)\
        .write\
        .json(BASE_DIR.format(stage='gold', folder_name='twitter_insight_tweet'))