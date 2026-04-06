from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import *
import logging

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("SearchStreamProcessor") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()

# Tắt bớt log rác của Spark để dễ nhìn terminal
spark.sparkContext.setLogLevel("WARN")

# 2. Đọc dữ liệu Real-time từ Kafka Topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cdc.public.search_by_jobid") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Ép kiểu dữ liệu nhị phân của Kafka thành chuỗi String (JSON)
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

# 4. Dùng get_json_object bóc tách lấy lõi dữ liệu mới (after)
after_df = json_df.withColumn("after_json", get_json_object(col("json_str"), "$.payload.after")) \
                  .filter(col("after_json").isNotNull())

# 5. Khai báo Schema (Cấu trúc cột) tương ứng với MySQL
search_schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("title", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("major_category", StringType(), True),
    StructField("minor_category", StringType(), True),
    StructField("pay_from", StringType(), True),
    StructField("pay_to", StringType(), True),
    StructField("pay_type", StringType(), True),
    StructField("work_schedule", StringType(), True)
])

# 6. Ép chuỗi JSON thành các cột riêng biệt và đổi kiểu dữ liệu
parsed_df = after_df.withColumn("data", from_json(col("after_json"), search_schema)).select("data.*")
final_df = parsed_df.withColumn("pay_from", col("pay_from").cast("float")) \
                    .withColumn("pay_to", col("pay_to").cast("float"))

# 7. Hàm ghi dữ liệu vào MySQL (Sink)
def write_to_mysql(batch_df, epoch_id):
    count = batch_df.count()
    print(f"[SEARCH STREAM] Da nhan va xu ly {count} dong tai lo (batch) {epoch_id}")
    if count > 0:
        # LƯU Ý: Đang dùng port 3307 vì đã đổi ở Giai đoạn 1 để tránh trùng lặp
        batch_df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:mysql://localhost:3307/etl_db?useSSL=false&allowPublicKeyRetrieval=true") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "search_by_jobid") \
            .option("user", "root") \
            .option("password", "123456") \
            .save()

# 8. Kích hoạt luồng chảy liên tục
query = final_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .option("checkpointLocation", "data/checkpoint/spark_search") \
    .start()

query.awaitTermination()