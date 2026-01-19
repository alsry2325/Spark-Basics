#!/usr/bin/env python3
"""
API 이벤트 Consumer

Usage:
    python consumer.py [--group GROUP] [--count N]

Options:
    --group GROUP   Consumer Group ID (기본: api-event-consumer-group)
    --count N       수신할 메시지 수 (기본: 무제한)
"""
import argparse
import json
import signal
import sys
import time
from confluent_kafka import Consumer


def main():
    parser = argparse.ArgumentParser(description="API 이벤트 Consumer")
    parser.add_argument(
        "--group",
        type=str,
        default="api-event-consumer-group",
        help="Consumer Group ID",
    )
    parser.add_argument("--count", type=int, default=0, help="수신할 메시지 수 (0=무제한)")
    parser.add_argument("--topic", type=str, default="api-events", help="토픽 이름")
    args = parser.parse_args()

    # Consumer 설정
    config = {
        "bootstrap.servers": "kafka:9092",
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(config)
    consumer.subscribe([args.topic])

    print(f"Consumer 시작...")
    print(f"  토픽: {args.topic}")
    print(f"  그룹: {args.group}")
    print(f"  메시지 수: {'무제한' if args.count == 0 else args.count}")
    print()
    print("메시지 수신 대기 중... (Ctrl+C로 종료)")
    print()

    # 통계
    stats = {"total": 0, "by_status": {}, "by_endpoint": {}}
    start_time = time.time()

    # Graceful shutdown
    running = True

    def signal_handler(sig, frame):
        nonlocal running
        print("\n종료 신호 수신...")
        running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"에러: {msg.error()}")
                continue

            # 메시지 파싱
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError:
                continue

            # 통계 업데이트
            stats["total"] += 1

            status = event.get("status_code", "unknown")
            stats["by_status"][status] = stats["by_status"].get(status, 0) + 1

            endpoint = event.get("endpoint", "unknown")
            stats["by_endpoint"][endpoint] = stats["by_endpoint"].get(endpoint, 0) + 1

            # 메시지 출력
            print(
                f"[{stats['total']:6d}] "
                f"{event.get('request_id', 'N/A'):12s} | "
                f"{event.get('endpoint', 'N/A'):18s} | "
                f"{event.get('status_code', 'N/A'):3} | "
                f"{event.get('response_time_ms', 'N/A'):3}ms"
            )

            # 지정된 메시지 수에 도달하면 종료
            if args.count > 0 and stats["total"] >= args.count:
                break

    finally:
        consumer.close()

    # 통계 출력
    elapsed = time.time() - start_time

    print()
    print("=" * 60)
    print(f"총 수신: {stats['total']}건 ({elapsed:.2f}초)")
    if stats["total"] > 0:
        print(f"처리량: {stats['total'] / elapsed:.0f} records/sec")

        print(f"\n상태 코드별 분포:")
        for status, count in sorted(stats["by_status"].items()):
            pct = count / stats["total"] * 100
            print(f"  {status}: {count}건 ({pct:.1f}%)")

        print(f"\n엔드포인트별 분포:")
        for endpoint, count in sorted(
            stats["by_endpoint"].items(), key=lambda x: -x[1]
        ):
            pct = count / stats["total"] * 100
            print(f"  {endpoint}: {count}건 ({pct:.1f}%)")


if __name__ == "__main__":
    main()
