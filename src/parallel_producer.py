"""
병렬 Consumer 실습용 Producer
=============================

사용법:
    python parallel_producer.py [메시지수]

예시:
    python parallel_producer.py 100000  # 10만건 전송
    python parallel_producer.py         # 기본 5만건 전송
"""

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time
import random
from datetime import datetime
import sys


# =============================================================================
# 설정
# =============================================================================
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_NAME = "parallel-test"
NUM_PARTITIONS = 4


def main():
    # 메시지 수 설정
    num_messages = int(sys.argv[1]) if len(sys.argv) > 1 else 50_000

    # 토픽 생성
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})

    try:
        admin.delete_topics([TOPIC_NAME]).get(TOPIC_NAME).result()
        print(f"기존 토픽 삭제")
        time.sleep(3)
    except:
        pass

    topic = NewTopic(topic=TOPIC_NAME, num_partitions=NUM_PARTITIONS, replication_factor=1)
    admin.create_topics([topic])[TOPIC_NAME].result()
    print(f"토픽 '{TOPIC_NAME}' 생성 (파티션: {NUM_PARTITIONS}개)")
    time.sleep(2)

    # Producer 설정
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    sent_count = 0

    def callback(err, msg):
        nonlocal sent_count
        if not err:
            sent_count += 1

    print(f"\n{'=' * 50}")
    print(f" {num_messages:,}건 메시지 전송")
    print(f"{'=' * 50}")

    start_time = time.time()

    for i in range(num_messages):
        event = {
            "id": i,
            "user_id": f"U{random.randint(1, 10000):05d}",
            "action": random.choice(["click", "view", "purchase"]),
            "amount": random.randint(1000, 100000),
            "timestamp": datetime.now().isoformat(),
            "payload": "x" * random.randint(200, 800),  # 처리 부하용
        }

        producer.produce(
            topic=TOPIC_NAME,
            key=str(i % NUM_PARTITIONS).encode(),
            value=json.dumps(event).encode("utf-8"),
            callback=callback,
        )

        if (i + 1) % 10000 == 0:
            producer.flush()
            elapsed = time.time() - start_time
            print(f"  {i + 1:,}건 전송 ({elapsed:.1f}초)")

    producer.flush()
    elapsed = time.time() - start_time

    print(f"\n전송 완료!")
    print(f"  총: {sent_count:,}건")
    print(f"  시간: {elapsed:.2f}초")
    print(f"  처리량: {sent_count / elapsed:,.0f} records/sec")

    print(f"\n{'=' * 50}")
    print(" 이제 Consumer를 실행하세요!")
    print(f"{'=' * 50}")
    print("""
터미널 1개로 실행 (단일 Consumer):
  python parallel_consumer.py 1

터미널 2개로 실행 (병렬 Consumer):
  터미널 1: python parallel_consumer.py 1
  터미널 2: python parallel_consumer.py 2
    """)


if __name__ == "__main__":
    main()
