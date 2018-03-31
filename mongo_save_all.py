import logging
import sys

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

import yaml



if __name__ == '__main__':

    config = yaml.load(open(sys.argv[1]))
    out_path = sys.argv[2]

    HOST = config['MONGODB_PRODUCTION']['HOST']
    PORT = config['MONGODB_PRODUCTION']['PORT']
    DBNAME = config['MONGODB_PRODUCTION']['DBNAME']

    COLLECTION = "Product"
    URI = "mongodb://{HOST}:{PORT}".format(HOST=HOST,PORT=PORT)
    logging.info(URI)

    spark_session = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri",URI) \
        .config("spark.mongodb.input.database",DBNAME)\
        .config("spark.mongodb.input.collection",COLLECTION)\
        .getOrCreate()

    df = spark_session.read.format("com.mongodb.spark.sql").load()
    logging.info("Loading database")
    df.registerTempTable(COLLECTION)
    df_extract = spark_session.sql("select _id, shopId,handle, tags,title, shortDescription from %s"%COLLECTION)
    df_extract.write.mode("overwrite").format('parquet').save(out_path)
    logging.info("Done")

