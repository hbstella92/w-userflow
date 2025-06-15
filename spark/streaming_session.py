from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

if __name__ == "__main__":
    schema = StructType() \
        .add("user_id", StringType()) \
        .add("webtoon_id", StringType()) \
        .add("episode_id", StringType()) \
        .add("action", StringType()) \
        .add("scroll_ratio", DoubleType()) \
        .add("timestamp", StringType())

    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("WebtoonUserBehaviorStream") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .getOrCreate()

    kafka_df = ss.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "webtoon-events") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed = kafka_df.selectExpr("CAST(value AS STRING) AS json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*").withColumn("event_time", to_timestamp("timestamp"))

    query = parsed.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()