import pandas as pd
import psycopg2
from datetime import datetime
import os
import time

# 1. Cấu hình kết nối Postgres (Nguồn)
conn = psycopg2.connect(
    host="localhost", port=5433, database="etl_db", user="postgres", password="123456"
)
conn.autocommit = True
cursor = conn.cursor()

# 2. Cơ chế Checkpoint (Lưu trạng thái dòng đã đọc)
def read_offset(path):
    if not os.path.exists(path): return 0
    with open(path, "r") as f: return int(f.read().strip() or 0)

def write_offset(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f: f.write(str(value))

def none_if_nan(val):
    return None if pd.isna(val) else str(val)

# 3. Hàm Load dữ liệu
def load_search_csv(file_path, offset_file):
    df = pd.read_csv(file_path, header=0)
    # Loại bỏ cột index thừa do export
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.reset_index(drop=True, inplace=True)
    columns = df.columns.tolist()

    last_offset = read_offset(offset_file)
    new_rows = df.iloc[last_offset:]
    print(f"[SEARCH] Bắt đầu insert {len(new_rows)} dòng từ vị trí {last_offset}...")

    for idx, row in new_rows.iterrows():
        values = [none_if_nan(row[col]) for col in columns]
        values.append(datetime.now())  # Thêm timestamp cho updated_at

        placeholders = ','.join(['%s'] * len(values))
        sql = f"""
        INSERT INTO search_by_jobid ({','.join(columns)}, updated_at)
        VALUES ({placeholders})
        ON CONFLICT (job_id) DO NOTHING
        """
        try:
            cursor.execute(sql, tuple(values))
            print(f"[SEARCH] Đã insert: job_id={row['job_id']}")
            write_offset(offset_file, idx + 1)
            time.sleep(1) # Giả lập delay 1 giây giữa mỗi lần search
        except Exception as e:
            print(f"[SEARCH] Lỗi tại dòng {idx}: {e}")

if __name__ == '__main__':
    load_search_csv("data/source/search.csv", "data/checkpoint/search_offset.txt")