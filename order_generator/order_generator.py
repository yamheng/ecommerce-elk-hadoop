import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable  # 新增

KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]
TOPIC = "orders"

def create_producer():
    """循环重试，直到连上 Kafka 为止"""
    while True:
        try:
            print("Trying to connect Kafka ...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            # 试探一下 metadata
            producer.bootstrap_connected()
            print("Kafka connected.")
            return producer
        except NoBrokersAvailable as e:
            print("Kafka not ready yet, retry in 5s ...", e)
            time.sleep(5)
        except Exception as e:
            print("Unexpected error when connecting Kafka, retry in 5s ...", e)
            time.sleep(5)

def random_order():
    return {
        "order_id": f"ORD{int(time.time()*1000)}",
        "user_id": f"U{random.randint(1,1000):04d}",
        "product_id": f"P{random.randint(1,500):04d}",
        "category": random.choice(["food","clothes","electronics","books","sports"]),
        "price": round(random.uniform(10,1000),2),
        "quantity": random.randint(1,5),
        "order_time": datetime.utcnow().isoformat(),
        "status": random.choice(["CREATED","PAID","CANCELLED"])
    }

def main():
    producer = create_producer()
    print("Start producing orders...")

    try:
        while True:
            order = random_order()
            producer.send(TOPIC, order)
            print("New order:", order)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
