from pyspark.sql import SparkSession
import os
import ipdb
from pyspark.sql import functions as f

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

    # FLATTEN DATA

    # selectiona data.id da primeira linha e não comprimir
    df.select('data.id').show(1, False)

    # subiu os niveis na linha e subiu para uma nova linha cada tweet
    df.select(f.explode('data')).show(1, False)
    # renomear colunas com alias
    tweet_df = df.select(f.explode('data').alias('tweets')).select( 'tweets.author_id',
                                                                    'tweets.conversation_id',
                                                                    'tweets.created_at',
                                                                    'tweets.id',
                                                                    'tweets.in_reply_to_user_id',
                                                                    'tweets.public_metrics.*',
                                                                    'tweets.text'
                                                                )
    tweet_df.printSchema()
    tweet_df.show()


    # FLATTEN USERS
    df.printSchema()
    df.select(f.explode('includes.users')).printSchema()
    # minha tentiva, basicamente e preciso espeficiar quando tem subnivel aninhado, melhor usar formato abaixo
    # users_df = df.select(f.explode('includes.users').alias('users')).select(  'users.created_at',
    #                                                                 'users.id',
    #                                                                 'users.name',
    #                                                                 'users.username',
    #                                                             )

    # solution simplificado
    users_df = df.select(f.explode('includes.users').alias('users')).select('users.*')
    users_df.printSchema()
    users_df.show(1, False)