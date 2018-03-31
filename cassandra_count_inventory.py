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

POSTGRESQL_HOST = CONFIG['POSTGRESQL']['HOST']
POSTGRESQL_PORT = CONFIG['POSTGRESQL']['PORT']
POSTGRESQL_DBNAME = CONFIG['POSTGRESQL']['DBNAME']
POSTGRESQL_USERNAME = CONFIG['POSTGRESQL']['USERNAME']
POSTGRESQL_PASSWORD = CONFIG['POSTGRESQL']['PASSWORD']

POSTGRESQL_URI = "jdbc:postgresql://{HOST}:{PORT}/{DBNAME}".format(HOST=POSTGRESQL_HOST,
                                                                   PORT=POSTGRESQL_PORT,
                                                                   DBNAME=POSTGRESQL_DBNAME)


def get_sql_context_instance(context):
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
    POSTGRESQL_TABLE = "public.shop_inventory_policy"

    table_cassandra = sql_context.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace) \
        .load()
    table_cassandra.createOrReplaceTempView(table_name)

    df_extract = sql_context.sql("select shop_id, explode(variants) as data from %s"%table_name)
    df_extract.registerTempTable("Product_variant")
    sql_context.sql("select shop_id, data.inventory_policy as inventory_policy, count(*) as count from Product_variant group by shop_id, data.inventory_policy")\
        .write.format('jdbc')\
        .mode("append")\
        .option("url", POSTGRESQL_URI) \
        .option("dbtable",POSTGRESQL_TABLE) \
        .option("user", POSTGRESQL_USERNAME) \
        .option('password', POSTGRESQL_PASSWORD) \
        .save()

    logging.info("Done")

if __name__ == "__main__":
    batch()
