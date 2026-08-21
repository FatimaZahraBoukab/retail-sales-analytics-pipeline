from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("BronzeJob").getOrCreate()

raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "retail_transactions") \
    .option("startingOffsets", "earliest") \
    .load()

bronze = raw.select(col("value").cast("string").alias("json_data"))

query = bronze.writeStream \
    .format("parquet") \
    .option("path", "/opt/data/bronze") \
    .option("checkpointLocation", "/opt/data/bronze_checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()