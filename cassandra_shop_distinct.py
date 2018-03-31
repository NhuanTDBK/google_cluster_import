import logging
import sys
import os

sys.path.append(os.getcwd())

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext

from utils.load_config import load_config

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

FILE_PATH = sys.argv[1]
CONFIG = load_config(FILE_PATH)

JOB_NAME = "spark_cassandra_count"

CASSANDRA_CLUSTER_IP = ','.join(CONFIG['CASSANDRA']['CLUSTER_IP'])
USERNAME, PASSWORD = CONFIG['CASSANDRA']['USERNAME'], CONFIG['CASSANDRA']['PASSWORD']
KEYSPACE = CONFIG['CASSANDRA']['KEYSPACE']


def get_spark_session_instance(context):
    sql_context = SQLContext(context)
    return sql_context


def batch():
    SPARK_CONF = SparkConf().setAppName(JOB_NAME) \
        .set('spark.cassandra.connection.host', CASSANDRA_CLUSTER_IP) \
        .set('spark.cassandra.auth.username', USERNAME) \
        .set('spark.cassandra.auth.password', PASSWORD)

    sc = SparkContext(conf=SPARK_CONF)
    sc.setLogLevel('OFF')
    sql_context = SQLContext(sc)
    keyspace = "beeketing"
    table_name = "product_shop"

    table_cassandra = sql_context.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace) \
        .load()
    table_cassandra.createOrReplaceTempView("tmp")
    df = sql_context.sql("select distinct(shop_id) from tmp")

    LOGGER.info("Number of rows: %s", df.count())
    df.coalesce(1).write.mode('overwrite').csv("data/beeketing_cassandra_product")


if __name__ == "__main__":
    batch()
