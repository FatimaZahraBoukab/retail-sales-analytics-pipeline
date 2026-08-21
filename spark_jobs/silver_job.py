from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("SilverJob").getOrCreate()

schema = StructType() \
    .add("Invoice", StringType()) \
    .add("StockCode", StringType()) \
    .add("Description", StringType()) \
    .add("Quantity", IntegerType()) \
    .add("InvoiceDate", StringType()) \
    .add("Price", DoubleType()) \
    .add("Customer ID", DoubleType()) \
    .add("Country", StringType())

bronze = spark.read.parquet("/opt/data/bronze")
parsed = bronze.withColumn("data", from_json(col("json_data"), schema)).select("data.*")

silver = parsed.dropDuplicates(["Invoice", "StockCode", "InvoiceDate"]) \
    .filter(col("Quantity").isNotNull() & col("Price").isNotNull())

silver.write.mode("overwrite").parquet("/opt/data/silver")
print(f"Silver écrit : {silver.count()} lignes")