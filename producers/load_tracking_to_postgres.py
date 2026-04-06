from pyspark.sql import SparkSession
import psycopg2
import uuid
import os
import time
from datetime import datetime

# 1. Cấu hình kết nối Postgres
conn = psycopg2.connect(
    host="localhost", port=5433, database="etl_db", user="postgres", password="123456"
)
conn.autocommit = True
cursor = conn.cursor()

# 2. Cơ chế Checkpoint
def read_offset(path):
    if not os.path.exists(path): return 0
    with open(path) as f: return int(f.read().strip() or 0)

def write_offset(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f: f.write(str(value))

def clean_str(val):
    if val is None: return None
    val = str(val).strip()
    return val if val != "" else None

# 3. Hàm Load bằng Spark (Đã cập nhật để đọc file chuẩn)
def load_tracking_csv_spark(csv_path, offset_file):
    spark = SparkSession.builder \
        .appName("TrackingProducer") \
        .master("local[*]") \
        .getOrCreate()

    # ĐỌC TRỰC TIẾP FILE GỐC (Không cần clean_csv)
    df = spark.read \
        .option("header", True) \
        .option("delimiter", ",") \
        .option("quote", '"') \
        .option("escape", '"') \
        .csv(csv_path)
        
    rows = df.collect()
    last_offset = read_offset(offset_file)
    columns = df.columns

    print(f"[TRACKING] Bắt đầu insert từ dòng {last_offset}...")

    for idx, row in enumerate(rows[last_offset:], start=last_offset):
        uuid_key = str(uuid.uuid4())
        values = []

        for col_name in columns:
            val = row[col_name] if col_name in row else None
            values.append(clean_str(val))

        values.insert(0, uuid_key)
        values.append(datetime.now().isoformat())

        placeholders = ",".join(["%s"] * len(values))
        sql = f"""
            INSERT INTO tracking_events (
                uuid, {",".join(columns)}, updated_at
            ) VALUES ({placeholders})
        """
        try:
            cursor.execute(sql, tuple(values))
            print(f"[TRACKING] Đã insert event: uuid={uuid_key[:8]}...")
            write_offset(offset_file, idx + 1)
            time.sleep(1.5) 
        except Exception as e:
            print(f"[TRACKING] Lỗi tại dòng {idx}: {e}")

    spark.stop()

if __name__ == '__main__':
    # TRỎ TRỰC TIẾP VÀO FILE TRACKING.CSV GỐC
    load_tracking_csv_spark("data/source/tracking.csv", "data/checkpoint/tracking_offset.txt")