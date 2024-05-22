import sys
import requests
import time
import os
import pyspark
# from distutils.log import ERROR
from azure.storage.blob import BlobClient
from delta import *
from os import path
# from matplotlib import container
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, DateType, StructType, StructField
from pyspark.sql.avro.functions import *

APP_NAME = "pipeline"
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
#--------------------------------------------------------------------------
# Checar se Delta Table existe
#--------------------------------------------------------------------------
def delta_exists(delta_path, topic, db, storage_account=None, storage_key=None):
    if 'abfss://':
        url = f'DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={storage_key};EndpointSuffix=core.windows.net'
        blob = BlobClient.from_connection_string(conn_str=url, container_name=BLOB_CONTAINER, blob_name=f'{LAKEHOUSE_PATH}/bronze/{db}_{topic}')
        return blob.exists()
    else:
        return path.exists(delta_path)
#--------------------------------------------------------------------------
# Retornar os caminhos da tabela
#--------------------------------------------------------------------------    
def paths(topico: str, db: str, tier: str):
    delta_path = f'abfss://{BLOB_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{LAKEHOUSE_PATH}/{tier}/{db}_{topico}'
    checkpoint_path = f'abfss://{BLOB_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{LAKEHOUSE_PATH}/{tier}/checkpoint/kafka/{db}_{topico}'

    return delta_path, checkpoint_path
#--------------------------------------------------------------------------
# Carregar uma delta table e registrar como temporária
#--------------------------------------------------------------------------
def loadAndRegister(table:str, db:str, tier:str = "bronze"):
    delta_path, _ = paths(table, db, tier)
    df = spark.read.format("delta").load(delta_path)
    df.createOrReplaceTempView(f"{tier}_{table}")
    return df
#--------------------------------------------------------------------------
# Stream de um topico
#--------------------------------------------------------------------------
def stream_topico(spark, bootstrap, topico, path_tabela, path_checkpoint, earliest=False, timeout=60):
    offset = 'earliest' if earliest else 'latest'

    print('=======================================================================')
    print('Configuração do STREAM')
    print(f'\tTopico..........: {topico}')
    print(f'\tPath Tabela.....: {path_tabela}')
    print(f'\tPath Checkpoint.: {path_checkpoint}')
    print(f'\tOffset..........: {offset}')

    # retrieve the latest schema
    response = requests.get('{}/subjects/{}-value/versions/latest/schema'.format(SCHEMA_REGISTRY, topico))    
    # error check
    response.raise_for_status()
    # extract the schema from the response
    schema = response.text    
    
    df = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topico)
        .option("startingOffsets", offset)
        .option("failOnDataLoss", False)
        .load()
        .selectExpr("substring(value, 6) as avro_value")
        .select(from_avro(col("avro_value"), schema).alias("value"))
        .select("value.*")
        .writeStream
        .format('delta')
        .partitionBy("ticker")
        .outputMode('append')
        .option('mergeSchema', 'true')
        .option('checkpointLocation', path_checkpoint)
        .trigger(once=True)
        .start(path_tabela)
    )
    
    return df

topico = "stocks.StockData.stocks"
tokens = topico.split(".")
db = tokens[1]
prefixo = ".".join(tokens[:-1])
nome = tokens[-1]

delta_path, checkpoint_path = paths(nome.lower(), db, "bronze")
delta_path_exists = delta_exists(delta_path, nome.lower(), db, STORAGE_ACCOUNT, STORAGE_KEY)
#
# Bronze Layer
# 
df = stream_topico(
    spark, 
    KAFKA_BOOTSTRAP, 
    f'{prefixo}.{nome}', 
    delta_path, 
    checkpoint_path, 
    earliest=not delta_path_exists
)

df.awaitTermination()
#
# Silver Layer
#
print("Silver Layer")
delta_path, _ = paths("stocks", "StockData", "bronze")
delta_path_final, _ = paths("stocks", "StockData", "silver")

df = loadAndRegister("stocks", db="StockData", tier="bronze")
#
# Data Transformation
#
df_silver = spark.sql("""
    SELECT
        _id,
        ticker,
        date_format(timestamp, "yyyy-MM-dd") as day,
        description,
        timestamp,
        open,
        high,
        low,
        close,
        volume,
        (close - LAG(close,1) OVER (PARTITION BY ticker ORDER BY timestamp)) AS osc,
        (osc * 100.0 / LAG(close,1) OVER (PARTITION BY ticker ORDER BY timestamp)) as osc_per,
        __op, 
        __collection, 
        (to_timestamp(__ts_ms / 1000) - interval 5 hours) as __ts_ms
    from bronze_stocks
    order by ticker, day, timestamp
""")
#
# Replace NaN with 0
# 
df_final = df_silver.fillna(value=0)
#
# Write to Delta Table
#
df_final.write.format("delta").mode("overwrite").partitionBy("ticker", "day").save(delta_path_final)
print("Silver Layer Done")
#
# Gold Layer
#
print("Gold Layer")
delta_path, _ = paths("stocks", "StockData", "silver")
delta_path_final, _ = paths("stocks", "StockData", "gold")

df = loadAndRegister("stocks", db="StockData", tier="silver")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW stocks_data AS 
    select * 
    from silver_stocks 
    order by ticker, day, timestamp
""")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW gold_daily AS
    select
        ticker,
        day,
        first(open) as open,
        min(low) as low,
        max(high) as high,
        last(close) as close,
        sum(volume) as volume
    from stocks_data
    group by all
    order by ticker, day
""")

df = spark.sql("""select *, (close - open) as osc, (osc / open) as osc_per from gold_daily""")
#
# Writes to Gold Layer
#
df.write.format("delta").mode("overwrite").partitionBy("ticker", "day").save(delta_path_final)
#
# Writes to Postgress DW
#
(df
  .write
  .format("jdbc")
  .option("url", postgresUrl)
  .option("driver", "org.postgresql.Driver")
  .option("dbtable", "stocks_daily")
  .option("user", postgresUsername)
  .option("password", postgresPassword)
  .mode("overwrite")
  .save()
)

print("Gold Layer Daily Done")
df = spark.sql("""
select
    _id,
    ticker,
    day,
    description,
    timestamp - interval 3 hours as timestamp,
    open,
    high,
    low,
    close,
    volume,
    osc,
    osc_per,
    __op,
    __collection,
    __ts_ms
from silver_stocks
order by ticker, timestamp desc
""")

(df
  .write
  .format("jdbc")
  .option("url", postgresUrl)
  .option("driver", "org.postgresql.Driver")
  .option("dbtable", "stocks_intraday")
  .option("user", postgresUsername)
  .option("password", postgresPassword)
  .mode("overwrite")
  .save()
)
print("Gold Layer Intraday Done")
spark.stop()
#--------------------------------------------------------------------------
