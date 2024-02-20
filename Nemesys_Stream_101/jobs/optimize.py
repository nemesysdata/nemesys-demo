import sys
import requests
import time
import os
import pyspark
# from distutils.log import ERROR
from azure.storage.blob import BlobServiceClient, BlobPrefix
from delta import *
from os import path
# from matplotlib import container
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, DateType, StructType, StructField
from pyspark.sql.avro.functions import *

APP_NAME = "OptimizeLakehouse"
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("STORAGE_KEY")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER")
LAKEHOUSE_PATH = os.getenv("LAKEHOUSE_PATH")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL")

postgresHostname = os.getenv("POSTGRES_HOSTNAME")
postgresDatabase = os.getenv("POSTGRES_DATABASE")
postgresPort = os.getenv("POSTGRES_PORT")
postgresUrl = f"jdbc:postgresql://{postgresHostname}:{postgresPort}/{postgresDatabase}?"
postgresUsername = os.getenv("POSTGRES_USERNAME")
postgresPassword = os.getenv("POSTGRES_PASSWORD")

spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#--------------------------------------------------------------------------
# Configurar o acesso ao Azure Data Lake (Blob)
#--------------------------------------------------------------------------
spark.conf.set(f'fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net', 'SharedKey')
spark.conf.set(f'fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net', STORAGE_KEY)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "False")
#--------------------------------------------------------------------------
# Retornar os caminhos da tabela
#--------------------------------------------------------------------------    
def paths(topico: str, db: str, tier: str):
    delta_path = f'abfss://{BLOB_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{LAKEHOUSE_PATH}/{tier}/{db}_{topico}'
    checkpoint_path = f'abfss://{BLOB_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{LAKEHOUSE_PATH}/{tier}/checkpoint/kafka/{db}_{topico}'

    return delta_path, checkpoint_path
#--------------------------------------------------------------------------
delta_path, _ = paths("stocks", "StockData", "bronze")
url = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};AccountKey={STORAGE_KEY};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(conn_str=url, container_name=BLOB_CONTAINER, blob_name=f'{LAKEHOUSE_PATH}/bronze/StockData_stocks')
container_client = blob_service_client.get_container_client(container="nemesys-stream-101")

def break_path(path:str):
    tokens = path.split("/")
    tier = tokens[1]
    tokens = tokens[2].split("_")
    db = tokens[0]
    table = tokens[1]
    return tier, db, table
    
def findTables(path:str="lakehouse/"):
    tables = []
    
    for blob_tier in container_client.walk_blobs(name_starts_with=path):
        for blob_table in container_client.walk_blobs(name_starts_with=blob_tier.name):
            if "checkpoint" not in blob_table.name:
                tier, db, table = break_path(blob_table.name)
                tables.append((tier, db, table))
    return tables

tables = findTables()

for tier, db, table in tables:
    delta_path, _ = paths(table, db, tier)
    print(f"Otimizando: {delta_path}")
    deltaTable = DeltaTable.forPath(spark, delta_path)
    deltaTable.optimize().executeCompaction()
    # deltaTable.vacuum(4)