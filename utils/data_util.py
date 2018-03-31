import yaml
import inspect
import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table,sync_type


def _get_or_create_table(model_name, cluster_ip, cassandra_keyspace, username='', password=''):
    ap = PlainTextAuthProvider(username=username, password=password)
    connection.setup(cluster_ip, cassandra_keyspace, protocol_version=3, auth_provider=ap)
    cluster = Cluster(cluster_ip, port=9042, auth_provider=ap)
    cluster.connect()
    tables = cluster.metadata.keyspaces[cassandra_keyspace].tables.keys()
    table_instance = vars(__import__(
        'models.%s' % model_name, globals(), locals(), ['*']))[model_name]
    table_instance.__keyspace__ = cassandra_keyspace
    if table_instance.__table__ not in tables:
        sync_table(table_instance)
    # Create statistical table
    return table_instance.__table__

def sync_model(model_class,config_path="./config.yml",sync_method=sync_table):
    cfg = get_config(config_path)
    cluster_ip = cfg['CASSANDRA']['CLUSTER_IP']
    cassandra_keyspace = cfg['CASSANDRA']['KEYSPACE']
    username = cfg['CASSANDRA']['USERNAME']
    password = cfg['CASSANDRA']['PASSWORD']
    port = cfg['CASSANDRA']['PORT']
    ap = PlainTextAuthProvider(username=username, password=password)
    connection.setup(cluster_ip, cassandra_keyspace, protocol_version=3, auth_provider=ap)
    cluster = Cluster(cluster_ip, port=9042, auth_provider=ap)
    cluster.connect()
    sync_method(model_class)
    return model_class.__table__

def create_table(model_class,config_path='./config.yml'):
    return sync_model(model_class,config_path,sync_table)

def create_type(model_class,config_path='./config.yml'):
    cfg = get_config(config_path)
    cluster_ip = cfg['CASSANDRA']['CLUSTER_IP']
    cassandra_keyspace = cfg['CASSANDRA']['KEYSPACE']
    username = cfg['CASSANDRA']['USERNAME']
    password = cfg['CASSANDRA']['PASSWORD']
    port = cfg['CASSANDRA']['PORT']
    ap = PlainTextAuthProvider(username=username, password=password)
    connection.setup(cluster_ip, cassandra_keyspace, protocol_version=3, auth_provider=ap)
    cluster = Cluster(cluster_ip, port=port, auth_provider=ap)
    cluster.connect()
    sync_type("beeketing",model_class)
    return model_class


def create_table_if_not_exists(model_class, cluster_ip, cassandra_keyspace, username='', password=''):
    ap = PlainTextAuthProvider(username=username, password=password)
    connection.setup(cluster_ip, cassandra_keyspace, protocol_version=3, auth_provider=ap)
    cluster = Cluster(cluster_ip, port=9042, auth_provider=ap)
    cluster.connect()
    tables = cluster.metadata.keyspaces[cassandra_keyspace].tables.keys()
    # table_instance = getattr(models, model_name)
    table_instance = model_class
    table_instance.__keyspace__ = cassandra_keyspace

    if table_instance.__table__ not in tables:
        sync_table(table_instance)
        logging.info( "Create table %s successfully"%table_instance.__table__)

    return table_instance.__table__


def get_config(config_path='./config.yml'):
    with open(config_path) as ymlfile:
        cfg = yaml.load(ymlfile)

    return cfg


def create_connection(config_path='./config.yml'):
    cfg = get_config(config_path)
    cluster_ip = cfg['CASSANDRA']['CLUSTER_IP']
    cassandra_keyspace = cfg['CASSANDRA']['KEYSPACE']
    username = cfg['CASSANDRA']['USERNAME']
    password = cfg['CASSANDRA']['PASSWORD']
    ap = PlainTextAuthProvider(username=username, password=password)
    connection.setup(cluster_ip, cassandra_keyspace, protocol_version=3, auth_provider=ap)
    return connection

def default(obj):
    """Default JSON serializer."""
    import calendar, datetime

    if isinstance(obj, datetime.datetime):
        return int(obj.strftime("%s"))

    raise TypeError('Not sure how to serialize %s' % (obj,))