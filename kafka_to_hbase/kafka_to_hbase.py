import json
import time
from kafka import KafkaConsumer
import happybase

KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]
TOPIC = "orders"

HBASE_HOST = "hbase"
HBASE_PORT = 9090
HBASE_TABLE = "orders"
HBASE_CF = "info"


def wait_for_hbase():
    while True:
        try:
            print("Trying to connect HBase Thrift ...")
            conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
            conn.open()
            print("Connected to HBase.")
            return conn
        except Exception as e:
            print("HBase not ready yet:", e)
            time.sleep(5)


def ensure_table(conn):
    tables = [t.decode() for t in conn.tables()]
    if HBASE_TABLE not in tables:
        print(f"Creating HBase table '{HBASE_TABLE}' ...")
        conn.create_table(
            HBASE_TABLE,
            {HBASE_CF: dict()}
        )
    else:
        print(f"HBase table '{HBASE_TABLE}' already exists.")


def main():
    # 1. 先连 HBase
    conn = wait_for_hbase()
    ensure_table(conn)
    table = conn.table(HBASE_TABLE)

    # 2. 再连 Kafka
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="kafka-to-hbase"
    )

    print("Start consuming from Kafka and writing to HBase...")

    for msg in consumer:
        order = msg.value
        row_key = order["order_id"]

        data = {
            b"info:user_id": order["user_id"].encode(),
            b"info:product_id": order["product_id"].encode(),
            b"info:category": order["category"].encode(),
            b"info:status": order["status"].encode(),
            b"info:price": str(order["price"]).encode(),
            b"info:quantity": str(order["quantity"]).encode(),
            b"info:order_time": order["order_time"].encode()
        }

        # ===== 关键：写 HBase 时加重试、重连 =====
        while True:
            try:
                table.put(row_key, data)
                print("Wrote order to HBase:", row_key)
                break   # 写成功就跳出 while True
            except Exception as e:
                print("Error writing to HBase, will reconnect and retry:", e)
                # 重新连 HBase
                conn = wait_for_hbase()
                ensure_table(conn)
                table = conn.table(HBASE_TABLE)
                # 然后 while True 继续下一轮，重新 table.put(row_key, data)

if __name__ == "__main__":
    main()
