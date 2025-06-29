import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pyspark.sql.types import *

from config import jdbc_properties, jdbc_url

schema = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("signup_date", StringType(), False),
        StructField("last_date", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("referrer", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("app_version", StringType(), False),
        StructField("country", StringType(), False),
        StructField("webtoon_id", StringType(), False),
        StructField("episode_id", StringType(), False),
        StructField("episode_index_in_session", IntegerType(), False),
        StructField("scroll_ratio", DoubleType(), False),
        StructField("duration", IntegerType(), False),
        StructField("dropoff_position", DoubleType(), False),
        StructField("is_returning_episode", BooleanType(), False),
        StructField("thumbnail_clicked", BooleanType(), False),
    ]
)


def write_to_db(batch_df, batch_id):
    try:
        batch_df.write.jdbc(
            url=jdbc_url, table="user_events", mode="append", properties=jdbc_properties
        )
    except Exception as e:
        print(f"[ERROR] : {e}")
        raise e


# Just load to Postgres DB
if __name__ == "__main__":
    ss: SparkSession = (
        SparkSession.builder.master("local[2]")
        .appName("Kafka Consumer User Flow")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
        )
        .config("spark.jars", "../jars/postgresql-42.7.7.jar")
        .getOrCreate()
    )

    ss.sparkContext.setLogLevel("WARN")

    df = (
        ss.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "webtoon-events")
        .option("startingOffsets", "latest")
        .load()
    )

    df_parsed = (
        df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    query = df_parsed.writeStream.foreachBatch(write_to_db).outputMode("append").start()

    query.awaitTermination()
