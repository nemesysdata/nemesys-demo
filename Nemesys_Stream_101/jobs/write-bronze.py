from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.avro.functions import *

from LoadEnvironment import *
from schemas import *

APP_NAME = "write_bronze"

spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

tabela = "s3a://nemesys-demo1/lakehouse/bronze/stocks_intraday"
checkpoint = "s3a://nemesys-demo1/lakehouse/bronze/checkpoint/stocks_intraday"
offset = "latest"

(spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", offset)
    # .option("security.protocol", "SSL")
    .load()
    .select(from_avro(col("value"), sch_bronze_stocks).alias("value"))
    .select("value.*")
    .writeStream
    .format('delta')
    .outputMode('append')
    .option('mergeSchema', 'true')
    .option('checkpointLocation', checkpoint)
    .start(tabela)
    .awaitTermination()
)