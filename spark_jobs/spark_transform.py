from pyspark.sql import SparkSession

# CREATE df and WRITE TO parquet & csv
# spark = SparkSession.builder.appName("UserFlowETL").getOrCreate()
# df = spark.read.option("header", True).csv("user_event_log.csv")
# df.groupBy("event").count().write.mode("overwrite").parquet("output/event_counts")
# df.groupBy("event").count().write.mode("overwrite").csv("output/event_counts_csv", header=True)

# LOAD df using JDBC
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.27") \
    .getOrCreate()

df = spark.read.option("header", True).csv("user_event_log.csv")
df = df.withColumn("event_time", df["event_time"].cast("timestamp"))

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/userflowdb") \
    .option("dbtable", "user_events") \
    .option("user", "byeolong2") \
    .option("password", "adminadmin12") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()