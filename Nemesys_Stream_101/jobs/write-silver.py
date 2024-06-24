from delta import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp
from pyspark.sql.avro.functions import *

from LoadEnvironment import *
from schemas import *

def config_upsert(delta):
    def upsertToDelta(microbatchdf, batchId):
        print(f'Batch {batchId} com {microbatchdf.count()} linhas')
        # Verificar e remover duplicatas no microbatch
        microbatchdf_clean = microbatchdf.dropDuplicates(["ticker", "timestamp"])
        
        # Garantir que os campos estejam no mesmo formato
        microbatchdf_clean = microbatchdf_clean.withColumn("ticker", col("ticker").cast("string"))
        microbatchdf_clean = microbatchdf_clean.withColumn("timestamp", col("timestamp").cast("timestamp"))
        
        delta.alias("t").merge(
          microbatchdf_clean.alias("s"),
          "s.ticker = t.ticker and s.timestamp = t.timestamp") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        print(f'Exportadas {microbatchdf_clean.count()} linhas')
        
    return upsertToDelta

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

stock_bronze, stock_bronze_checkpoint_dir = table_path("nemesys-demo1", "bronze", "stocks_intraday")
stock_silver, stock_silver_checkpoint_dir = table_path("nemesys-demo1", "silver", "stocks_intraday")

if not DeltaTable.isDeltaTable(spark, stock_silver):
    print("Criar tabela")
    schema = (StructType()
        .add("ticker", StringType())
        .add('ano', IntegerType())
        .add("timestamp", TimestampType())
        .add("open", DoubleType())
        .add("high", DoubleType())
        .add("low", DoubleType())
        .add("close", DoubleType())
        .add("volume", LongType())
        .add("_capture_time_kafka", TimestampType())
        .add("_capture_time_bronze", TimestampType())
        .add("_capture_time_silver", TimestampType())
    )
    emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    emptyDF.write.format('delta').mode('overwrite').partitionBy("ticker", "ano").save(stock_silver)

deltaTable = DeltaTable.forPath(spark, stock_silver)

(spark
    .readStream
    .format("delta")
    .option('startingOffsets', 'earliest')
    .load(stock_bronze)
    .withColumn('timestamp_real', to_timestamp("timestamp"))
    .withColumn('ano', date_format('timestamp', 'yyyy').cast(IntegerType()))
    .withColumn("_capture_time_silver", current_timestamp())
    .writeStream
    .format('delta')
    .foreachBatch(config_upsert(deltaTable))
    .outputMode('update')
    .option('checkpointLocation', stock_bronze_checkpoint_dir + "silver")
    .start()
    .awaitTermination()
)
