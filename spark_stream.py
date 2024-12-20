import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace():
    #create keypsace for cassandra

def create_table():


def insert_data(session,**kwargs):


def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDatastreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.41,org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host','localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("Error")
        logging.info('Spark Connection created successfully')
    except Exception as e:
        logging.error(f'Couldnt create Spark connection due to error {e}')

    return s_conn


def cassandra_connection():


if __name__ == "__main__":
    spark_conn = create_spark_connection()


