#!/usr/bin/env python3
"""
API 이벤트 Producer

Usage:
    python producer.py [--count N] [--delay D]

Options:
    --count N   전송할 메시지 수 (기본: 1000)
    --delay D   메시지 간 지연 시간(초) (기본: 0.01)
"""
import argparse
import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer


# API 이벤트 설정
ENDPOINTS = ["/api/products", "/api/users", "/api/orders", "/api/payments", "/api/search"]
METHODS = ["GET", "POST", "PUT", "DELETE"]
STATUS_CODES = [200, 200, 200, 200, 201, 400, 404, 500]


def generate_api_event():
    """API 이벤트 데이터 생성"""
    return {
        "request_id": f"REQ_{random.randint(1, 999999):06d}",
        "user_id": f"U{random.randint(1, 1000):04d}",
        "endpoint": random.choice(ENDPOINTS),
        "method": random.choice(METHODS),
        "status_code": random.choice(STATUS_CODES),
        "response_time_ms": random.randint(10, 500),
        "timestamp": datetime.now().isoformat(),
    }


def delivery_callback(err, msg):
    """전송 결과 콜백"""
    if err:
        print(f"전송 실패: {err}")


def main():
    parser = argparse.ArgumentParser(description="API 이벤트 Producer")
    parser.add_argument("--count", type=int, default=1000, help="전송할 메시지 수")
    parser.add_argument("--delay", type=float, default=0.01, help="메시지 간 지연 시간(초)")
    parser.add_argument("--topic", type=str, default="api-events", help="토픽 이름")
    args = parser.parse_args()

    # Producer 설정
    config = {
        "bootstrap.servers": "kafka:9092",
        "client.id": "api-event-producer",
    }
    producer = Producer(config)

    print(f"API 이벤트 {args.count}건 전송 시작...")
    print(f"  토픽: {args.topic}")
    print(f"  지연: {args.delay}초")
    print()

    start_time = time.time()
    sent_count = 0

    try:
        for i in range(args.count):
            event = generate_api_event()

            producer.produce(
                topic=args.topic,
                key=event["user_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_callback,
            )

            sent_count += 1

            if (i + 1) % 100 == 0:
                producer.poll(0)
                print(f"  {i + 1}건 전송 완료")

            if args.delay > 0:
                time.sleep(args.delay)

    except KeyboardInterrupt:
        print("\n중단됨")
    finally:
        producer.flush()

    elapsed = time.time() - start_time
    print()
    print(f"전송 완료!")
    print(f"  총 전송: {sent_count}건")
    print(f"  소요 시간: {elapsed:.2f}초")
    print(f"  처리량: {sent_count / elapsed:.0f} records/sec")


if __name__ == "__main__":
    main()
