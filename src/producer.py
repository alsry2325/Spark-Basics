"""
Kafka Producer - API 이벤트 생성기

실행 방법:
    docker exec -it python-dev python producer.py --rate 10
"""
import json
import time
import random
import uuid
import argparse
from datetime import datetime
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "api-events"
NUM_PARTITIONS = 4

ENDPOINTS = ["/api/users", "/api/orders", "/api/products", "/api/payments", "/api/search"]
METHODS = ["GET", "POST", "PUT", "DELETE"]
STATUS_CODES = [
    (200, 60), (201, 10), (400, 8), (401, 5),
    (404, 7), (500, 6), (502, 2), (503, 2),
]


def weighted_choice(choices):
    total = sum(weight for _, weight in choices)
    r = random.uniform(0, total)
    cumulative = 0
    for item, weight in choices:
        cumulative += weight
        if r <= cumulative:
            return item
    return choices[-1][0]

def generate_event():
    status_code = weighted_choice(STATUS_CODES)
    if status_code >= 500:
        response_time = random.randint(500, 2000)
    elif status_code >= 400:
        response_time = random.randint(100, 500)
    else:
        response_time = random.randint(10, 300)
    return {
        "request_id": str(uuid.uuid4())[:8],
        "user_id": f"user-{random.randint(1, 100):03d}",
        "endpoint": random.choice(ENDPOINTS),
        "method": random.choice(METHODS),
        "status_code": status_code,
        "response_time_ms": response_time,
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=10)
    parser.add_argument("--duration", type=int, default=0)
    args = parser.parse_args()

    admin = AdminClient({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    interval = 1.0 / args.rate
    total_sent = 0
    start_time = time.time()

    topic = NewTopic(topic=TOPIC_NAME, num_partitions=NUM_PARTITIONS, replication_factor=1)
    admin.create_topics([topic])[TOPIC_NAME].result()
    try:
        while True:
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                break
            event = generate_event()
            producer.produce(TOPIC_NAME,key=event["endpoint"], value=json.dumps(event))
            total_sent += 1
            if total_sent % 10 == 0:
                print(f"전송: {total_sent}건")
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"총 전송: {total_sent}건")

if __name__ == "__main__":
    main()