from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
    .master("local") \
    .appName("Kafka Consumer User Flow") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

    ss.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("session_id", StringType(), True), \
        StructField("user_id", StringType(), True), \
        StructField("webtoon_id", StringType(), True), \
        StructField("episode_id", StringType(), True), \
        StructField("action", StringType(), True), \
        StructField("scroll_ratio", FloatType(), True), \
        StructField("timestamp", StringType(), True), \
        StructField("country_code", StringType(), True), \
        StructField("ip_address", StringType(), True), \
        StructField("device_type", StringType(), True), \
        StructField("browser", StringType(), True)
    ])

    df = ss.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "webtoon-events") \
    .option("startingOffsets", "latest") \
    .load()

    df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

    query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

    query.awaitTermination()