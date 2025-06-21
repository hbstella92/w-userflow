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

    schema = StructType() \
    .add("user_id", StringType()) \
    .add("webtoon_id", StringType()) \
    .add("episode_id", StringType()) \
    .add("action", StringType()) \
    .add("scroll_ratio", DoubleType()) \
    .add("timestamp", StringType())

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