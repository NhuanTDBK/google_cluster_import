import logging
import os
import sys
from optparse import OptionParser

sys.path.append(os.getcwd())

from models.model import task_events
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)


def load_schema(type_table):
    return task_events


def main():
    parser = OptionParser()

    parser.add_option("-c", "--config_path", dest="config_path", default="config.yml",
                      help="config_path")
    parser.add_option("-i", "--input_path", dest="input_path", default=None,
                      help="input_path")
    parser.add_option("-t", "--table_name", dest="table_name", default=None,
                      help="config_path")
    parser.add_option("-m", "--save_mode", dest="save_mode", default="append",
                      help="method")
    parser.add_option("-s", "--sep", dest="sep", default=",",
                      help="method")

    (options, args) = parser.parse_args()

    CONFIG_PATH = options.config_path
    TABLE_NAME = options.table_name
    SAVE_MODE = options.save_mode
    INPUT_PATH = options.input_path
    SEP = options.sep

    config = yaml.load(open(CONFIG_PATH))

    MAX_FETCH_SIZE = 10000
    NUM_PARTITIONS = 50

    MYSQL_HOST = config['MYSQL']['HOST']
    MYSQL_PORT = config['MYSQL']['PORT']
    MYSQL_DBNAME = config['MYSQL']['DBNAME']
    MYSQL_USERNAME = config['MYSQL']['USERNAME']
    MYSQL_PASSWORD = config['MYSQL']['PASSWORD']

    MYSQL_URI = "jdbc:postgresql://{HOST}:{PORT}/{DBNAME}".format(HOST=MYSQL_HOST,
                                                                  PORT=MYSQL_PORT,
                                                                  DBNAME=MYSQL_DBNAME)

    logging.info(MYSQL_URI)

    spark_session = SparkSession \
        .builder \
        .appName("MySQL fetching") \
        .config("spark.rdd.compress", "false") \
        .config("spark.sql.shuffle.partitions", NUM_PARTITIONS) \
        .getOrCreate()

    logging.info("Loading database")
    df = spark_session.read.csv(INPUT_PATH, load_schema(TABLE_NAME), SEP)
    df.write.mode(SAVE_MODE).format('jdbc').option("url", MYSQL_URI) \
        .option("dbtable", TABLE_NAME) \
        .option("user", MYSQL_USERNAME) \
        .option('password', MYSQL_PASSWORD) \
        .option('fetchsize', MAX_FETCH_SIZE) \
        .option("numPartitions", NUM_PARTITIONS) \
        .save()


if __name__ == '__main__':
    main()
