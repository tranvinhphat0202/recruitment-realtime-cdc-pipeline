import requests
import json

# URL của Debezium API (đã được bật trong docker-compose ở cổng 8083)
DEBEZIUM_URL = "http://localhost:8083/connectors"

# Cấu hình Connector
config_payload = {
    "name": "cdc_tracking_conn",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "plugin.name": "pgoutput",
        # Thông tin kết nối nội bộ trong mạng Docker
        "database.hostname": "postgres_source", 
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "123456",
        "database.dbname": "etl_db",
        # Tên prefix cho các Kafka topic sẽ được tạo ra
        "topic.prefix": "cdc", 
        "database.server.name": "cdc",
        "publication.name": "dbz_pub",
        "slot.name": "cdc_slot",
        # Chỉ theo dõi 2 bảng này
        "table.include.list": "public.tracking_events,public.search_by_jobid",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "string"
    }
}

def create_connector():
    print("Đang gửi yêu cầu khởi tạo Debezium Connector...")
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(DEBEZIUM_URL, data=json.dumps(config_payload), headers=headers)
        if response.status_code in [200, 201]:
            print("Đăng ký Connector thành công!")
            print("Chi tiết:", response.json())
        elif response.status_code == 409:
            print("⚠️ Connector này đã tồn tại rồi, không cần tạo lại.")
        else:
            print(f"Lỗi {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Không thể kết nối tới Debezium (localhost:8083). Lỗi: {e}")

if __name__ == "__main__":
    create_connector()