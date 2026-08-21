from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count

spark = SparkSession.builder \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .appName("GoldJob").getOrCreate()

silver = spark.read.parquet("/opt/data/silver") \
    .withColumn("revenue", col("Price") * col("Quantity"))

by_country = silver.groupBy("Country") \
    .agg(_sum("revenue").alias("total_revenue"), count("*").alias("nb_transactions"))

by_product = silver.groupBy("StockCode", "Description") \
    .agg(_sum("revenue").alias("total_revenue"), _sum("Quantity").alias("total_quantity"))

jdbc_url = "jdbc:postgresql://postgres:5432/retail_db"
props = {"user": "retail", "password": "retail", "driver": "org.postgresql.Driver"}

by_country.write.jdbc(jdbc_url, "gold_revenue_by_country", mode="overwrite", properties=props)
by_product.write.jdbc(jdbc_url, "gold_revenue_by_product", mode="overwrite", properties=props)

print("Gold écrit dans PostgreSQL.")
