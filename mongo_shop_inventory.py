import logging
import sys

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

import yaml



if __name__ == '__main__':

    config = yaml.load(open(sys.argv[1]))

    HOST = config['MONGODB_PRODUCTION']['HOST']
    PORT = config['MONGODB_PRODUCTION']['PORT']
    DBNAME = config['MONGODB_PRODUCTION']['DBNAME']

    POSTGRESQL_HOST = config['POSTGRESQL']['HOST']
    POSTGRESQL_PORT = config['POSTGRESQL']['PORT']
    POSTGRESQL_DBNAME = config['POSTGRESQL']['DBNAME']
    POSTGRESQL_USERNAME = config['POSTGRESQL']['USERNAME']
    POSTGRESQL_PASSWORD = config['POSTGRESQL']['PASSWORD']
    POSTGRESQL_TABLE = "public.shop_inventory_policy"

    POSTGRESQL_URI = "jdbc:postgresql://{HOST}:{PORT}/{DBNAME}".format(HOST=POSTGRESQL_HOST,
                                                                       PORT=POSTGRESQL_PORT,
                                                                       DBNAME=POSTGRESQL_DBNAME)

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
    df_extract = spark_session.sql("select shopId, explode(variants) as data from %s"%COLLECTION)
    df_extract.registerTempTable("Product_variant")
    # spark_session.sql("select shopId, data.inventoryPolicy as inventoryPolicy, data.inventoryQuantity as inventoryQuantity from Product_variant ")\
    #     .write\
    #     .mode("overwrite")\
    #     .parquet(out_path)
    spark_session.sql("select shop_id, data.inventory_policy as inventory_policy, count(*) as count from Product_variant \
                    group by shop_id, data.inventory_policy") \
        .write.format('jdbc') \
        .mode("append") \
        .option("url", POSTGRESQL_URI) \
        .option("dbtable", POSTGRESQL_TABLE) \
        .option("user", POSTGRESQL_USERNAME) \
        .option('password', POSTGRESQL_PASSWORD) \
        .save()

    logging.info("Done")

