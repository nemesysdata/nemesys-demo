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

spark.conf.set('fs.s3a.endpoint', S3_URL)
spark.conf.set('fs.s3a.access.key', S3_ACCESS_KEY)
spark.conf.set('fs.s3a.secret.key', S3_SECRET_KEY)
spark.conf.set("spark.sql.debug.maxToStringFields", "100")
spark.conf.set("fs.s3a.path.style.access", "true")
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.conf.set("fs.s3a.connection.ssl.enabled", "true")

tabela = "s3a://nemesys-demo1/lakehouse/bronze/stocks_intraday"
checkpoint = "s3a://nemesys-demo1/lakehouse/bronze/stocks_intraday/_checkpoint/kafka_raw"
offset = "latest"

(spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", offset)
    # .option("security.protocol", "SSL")
    .load()
    .select(from_avro(col("value"), sch_bronze_stocks).alias("value"), col("timestamp").alias("_capture_time"))
    .select(col("value.*"), col("_capture_time"))
    .writeStream
    .format('delta')
    .outputMode('append')
    .option('mergeSchema', 'true')
    .option('checkpointLocation', checkpoint)
    .start(tabela)
    .awaitTermination()
)