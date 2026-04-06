from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("TrackingStreamProcessor") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cdc.public.tracking_events") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
after_df = json_df.withColumn("after_json", get_json_object(col("json_str"), "$.payload.after")) \
                  .filter(col("after_json").isNotNull())

tracking_schema = StructType([
    StructField("uuid", StringType(), True),
    StructField("create_time", StringType(), True),
    StructField("job_id", StringType(), True),
    StructField("custom_track", StringType(), True),
    StructField("bid", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("group_id", StringType(), True),
    StructField("publisher_id", StringType(), True),
    StructField("ev", StringType(), True),
    StructField("ts", StringType(), True)
])

parsed_df = after_df.withColumn("data", from_json(col("after_json"), tracking_schema)).select("data.*")

def write_to_mysql(batch_df, epoch_id):
    count = batch_df.count()
    print(f"[TRACKING STREAM] Da nhan va xu ly {count} dong tai lo (batch) {epoch_id}")
    if count > 0:
        batch_df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:mysql://localhost:3307/etl_db?useSSL=false&allowPublicKeyRetrieval=true") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "tracking_events") \
            .option("user", "root") \
            .option("password", "123456") \
            .save()

query = parsed_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .option("checkpointLocation", "data/checkpoint/spark_tracking") \
    .start()

query.awaitTermination()