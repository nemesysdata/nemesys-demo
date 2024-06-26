import os
import pyspark.sql.functions

from delta import *
from pyspark.sql import SparkSession
from environment import *
#
# Cria uma sessão Spark
#

builder = (SparkSession.builder
         .appName(f"Nemesys-101")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
#
# Função para ler um Delta Lake e registrar como tabela
#
def load_table(camada, tabela):
    if os.path.exists(f"lakehouse/{camada}/{tabela}"):
        spark.read.format("delta").load(f"lakehouse/{camada}/{tabela}").createOrReplaceTempView(f"{camada}_{tabela}")

#
# Carregar a camada Bronze
#
load_table("bronze", "vendas")
#
# Carregar a camada Silver
#
load_table("silver", "vendas")
load_table("silver", "stock")
load_table("silver", "country")
#
# Carregar a camada Gold
#