"""
병렬 Consumer 성능 비교 실습 (v3)
=================================

subprocess를 사용하여 별도 프로세스로 Consumer 실행
"""

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time
import random
import hashlib
from datetime import datetime
import subprocess
import sys
import tempfile
import os


# =============================================================================
# 설정
# =============================================================================
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_NAME = "parallel-benchmark-v3"
NUM_PARTITIONS = 4
NUM_MESSAGES = 30_000  # 3만건


# =============================================================================
# 토픽 생성
# =============================================================================
def setup_topic():
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


# =============================================================================
# Producer
# =============================================================================
def produce_messages():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    sent_count = 0

    def callback(err, msg):
        nonlocal sent_count
        if not err:
            sent_count += 1

    print(f"\n[Producer] {NUM_MESSAGES:,}건 전송...")
    start_time = time.time()

    for i in range(NUM_MESSAGES):
        partition = i % NUM_PARTITIONS

        event = {
            "id": i,
            "data": "x" * 300,
            "ts": datetime.now().isoformat(),
        }

        producer.produce(
            topic=TOPIC_NAME,
            partition=partition,
            value=json.dumps(event).encode("utf-8"),
            callback=callback,
        )

        if (i + 1) % 10000 == 0:
            producer.flush()

    producer.flush()
    elapsed = time.time() - start_time
    print(f"  완료: {sent_count:,}건 ({elapsed:.2f}초)")
    return sent_count


# =============================================================================
# Consumer 스크립트 생성
# =============================================================================
CONSUMER_SCRIPT = '''
import sys
import json
import time
import hashlib
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_NAME = "parallel-benchmark-v3"

worker_id = int(sys.argv[1])
group_id = sys.argv[2]
result_file = sys.argv[3]

config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": group_id,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

consumer = Consumer(config)
consumer.subscribe([TOPIC_NAME])

count = 0
partitions = set()
start_time = time.time()
empty_polls = 0

try:
    while empty_polls < 3:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            empty_polls += 1
            continue

        if msg.error():
            continue

        empty_polls = 0

        event = json.loads(msg.value().decode("utf-8"))

        # CPU 부하 작업
        result = event["data"]
        for _ in range(30):
            result = hashlib.sha256(result.encode()).hexdigest()

        count += 1
        partitions.add(msg.partition())

        if count % 5000 == 0:
            elapsed = time.time() - start_time
            print(f"  [Consumer-{worker_id}] {count:,}건 (파티션 {sorted(partitions)})", flush=True)

finally:
    consumer.close()

elapsed = time.time() - start_time
with open(result_file, "w") as f:
    json.dump({
        "worker_id": worker_id,
        "count": count,
        "elapsed": elapsed,
        "partitions": list(partitions),
    }, f)
'''


# =============================================================================
# 테스트 함수
# =============================================================================
def run_test(num_consumers: int, group_suffix: str):
    print(f"\n{'=' * 55}")
    print(f" Consumer {num_consumers}개로 처리")
    print(f"{'=' * 55}")

    group_id = f"bench-{group_suffix}-{int(time.time())}"

    # Consumer 스크립트 파일 생성
    script_file = "/tmp/consumer_worker.py"
    with open(script_file, "w") as f:
        f.write(CONSUMER_SCRIPT)

    # 결과 파일 경로
    result_files = [f"/tmp/result_{i}.json" for i in range(num_consumers)]

    start_time = time.time()

    # Consumer 프로세스 시작
    processes = []
    for i in range(num_consumers):
        p = subprocess.Popen(
            ["python3", script_file, str(i + 1), group_id, result_files[i]],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        processes.append(p)

    # 프로세스 완료 대기 및 출력
    for p in processes:
        stdout, stderr = p.communicate()
        if stdout:
            print(stdout, end="")

    total_elapsed = time.time() - start_time

    # 결과 수집
    results = []
    total_count = 0
    for rf in result_files:
        try:
            with open(rf) as f:
                r = json.load(f)
                results.append(r)
                total_count += r["count"]
            os.remove(rf)
        except:
            pass

    # 출력
    print(f"\n[결과]")
    for r in sorted(results, key=lambda x: x["worker_id"]):
        rate = r["count"] / r["elapsed"] if r["elapsed"] > 0 else 0
        print(f"  Consumer-{r['worker_id']}: {r['count']:,}건 (파티션 {sorted(r['partitions'])}, {rate:.0f}/sec)")

    rate = total_count / total_elapsed if total_elapsed > 0 else 0
    print(f"\n  총: {total_count:,}건")
    print(f"  시간: {total_elapsed:.2f}초")
    print(f"  처리량: {rate:,.0f} records/sec")

    return total_elapsed, total_count


# =============================================================================
# 메인
# =============================================================================
def main():
    print("=" * 55)
    print("  병렬 Consumer 성능 비교")
    print("=" * 55)
    print(f"  토픽: {TOPIC_NAME}")
    print(f"  파티션: {NUM_PARTITIONS}개")
    print(f"  메시지: {NUM_MESSAGES:,}건")
    print(f"  처리: SHA256 해시 30회/메시지")
    print("=" * 55)

    # 테스트 1: 단일 Consumer
    setup_topic()
    produce_messages()
    time1, count1 = run_test(1, "single")

    # 테스트 2: 병렬 Consumer 2개
    setup_topic()
    produce_messages()
    time2, count2 = run_test(2, "parallel2")

    # 테스트 3: 병렬 Consumer 4개
    setup_topic()
    produce_messages()
    time4, count4 = run_test(4, "parallel4")

    # 최종 비교
    print(f"\n{'=' * 55}")
    print("  최종 결과 비교")
    print(f"{'=' * 55}")

    rate1 = count1 / time1 if time1 > 0 else 0
    rate2 = count2 / time2 if time2 > 0 else 0
    rate4 = count4 / time4 if time4 > 0 else 0

    print(f"  Consumer 1개: {time1:.2f}초 ({rate1:,.0f} records/sec)")
    print(f"  Consumer 2개: {time2:.2f}초 ({rate2:,.0f} records/sec)")
    print(f"  Consumer 4개: {time4:.2f}초 ({rate4:,.0f} records/sec)")

    if rate1 > 0 and rate2 > 0 and rate4 > 0:
        print(f"\n  2개 vs 1개: {rate2/rate1:.2f}배 ({(time1/time2-1)*100:.0f}% 빠름)")
        print(f"  4개 vs 1개: {rate4/rate1:.2f}배 ({(time1/time4-1)*100:.0f}% 빠름)")

    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
