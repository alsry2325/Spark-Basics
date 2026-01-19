"""
병렬 Consumer 실습용 Consumer
=============================

사용법:
    python parallel_consumer.py [consumer_id]

예시:
    # 터미널 1
    python parallel_consumer.py 1

    # 터미널 2 (동시에 실행하면 파티션 분배)
    python parallel_consumer.py 2

핵심 포인트:
- 같은 group.id를 사용하면 파티션이 자동 분배됨
- Consumer 1이 먼저 실행되면 4개 파티션 모두 담당
- Consumer 2가 추가되면 리밸런싱되어 각각 2개씩 담당
"""

from confluent_kafka import Consumer
import json
import time
import sys


# =============================================================================
# 설정
# =============================================================================
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_NAME = "parallel-test"
GROUP_ID = "parallel-consumer-group"  # 같은 그룹!


def main():
    consumer_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(config)
    consumer.subscribe([TOPIC_NAME])

    print(f"{'=' * 50}")
    print(f" Consumer {consumer_id} 시작")
    print(f" Group ID: {GROUP_ID}")
    print(f"{'=' * 50}")
    print(" 메시지 대기 중... (Ctrl+C로 종료)\n")

    count = 0
    partitions_seen = set()
    start_time = None
    empty_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                empty_count += 1
                # 처리 시작 후 5초간 메시지 없으면 완료 표시
                if start_time and empty_count >= 5:
                    elapsed = time.time() - start_time
                    print(f"\n처리 완료!")
                    print(f"  Consumer {consumer_id}: {count:,}건")
                    print(f"  담당 파티션: {sorted(partitions_seen)}")
                    print(f"  소요 시간: {elapsed:.2f}초")
                    print(f"  처리량: {count / elapsed:,.0f} records/sec")
                    print("\n새 메시지 대기 중...")
                    start_time = None
                    empty_count = 0
                continue

            if msg.error():
                print(f"에러: {msg.error()}")
                continue

            empty_count = 0

            # 첫 메시지 수신 시 타이머 시작
            if start_time is None:
                start_time = time.time()
                print(f"처리 시작!")

            count += 1
            partitions_seen.add(msg.partition())

            # 메시지 처리 (실제 연산 시뮬레이션)
            event = json.loads(msg.value().decode("utf-8"))
            _ = sum(ord(c) for c in event.get("payload", ""))

            # 진행 상황 출력
            if count % 5000 == 0:
                elapsed = time.time() - start_time
                current_rate = count / elapsed
                print(
                    f"  [Consumer {consumer_id}] {count:,}건 "
                    f"(파티션 {sorted(partitions_seen)}, "
                    f"{current_rate:,.0f}/sec)"
                )

    except KeyboardInterrupt:
        print(f"\n\n종료됨")
        if count > 0:
            elapsed = time.time() - start_time if start_time else 1
            print(f"  총 처리: {count:,}건")
            print(f"  담당 파티션: {sorted(partitions_seen)}")
            print(f"  처리량: {count / elapsed:,.0f} records/sec")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
