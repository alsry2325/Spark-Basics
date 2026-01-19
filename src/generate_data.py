#!/usr/bin/env python3
"""
테스트 데이터 생성 스크립트

Usage:
    python generate_data.py [--sizes 100000 1000000 5000000]

생성되는 파일:
    /data/api_events_100k.csv
    /data/api_events_1000k.csv
    /data/api_events_5000k.csv
"""
import argparse
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_api_events(num_records: int, seed: int = 42) -> pd.DataFrame:
    """API 이벤트 데이터 생성"""
    np.random.seed(seed)

    endpoints = [
        "/api/products",
        "/api/users",
        "/api/orders",
        "/api/payments",
        "/api/search",
    ]
    methods = ["GET", "POST", "PUT", "DELETE"]
    # 200이 더 자주 발생하도록 가중치 적용
    status_codes = [200, 200, 200, 200, 201, 400, 404, 500]

    base_time = datetime(2026, 1, 18, 0, 0, 0)

    print(f"  데이터 생성 중: {num_records:,}건...")

    df = pd.DataFrame(
        {
            "request_id": [f"REQ_{i:08d}" for i in range(num_records)],
            "user_id": [
                f"U{np.random.randint(1, 10001):05d}" for _ in range(num_records)
            ],
            "endpoint": np.random.choice(endpoints, num_records),
            "method": np.random.choice(methods, num_records),
            "status_code": np.random.choice(status_codes, num_records),
            "response_time_ms": np.random.randint(10, 500, num_records),
            "timestamp": [
                (base_time + timedelta(seconds=i * 0.1)).isoformat()
                for i in range(num_records)
            ],
        }
    )

    return df


def main():
    parser = argparse.ArgumentParser(description="테스트 데이터 생성")
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        default=[100_000, 1_000_000, 5_000_000],
        help="생성할 데이터 크기 목록",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/data",
        help="출력 디렉토리",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="기존 파일이 있어도 덮어쓰기",
    )
    args = parser.parse_args()

    # 출력 디렉토리 생성
    os.makedirs(args.output_dir, exist_ok=True)

    print("테스트 데이터 생성")
    print("=" * 50)

    for size in args.sizes:
        # 파일명 결정 (1000단위로 표시)
        if size >= 1_000_000:
            label = f"{size // 1_000_000}m"
        else:
            label = f"{size // 1_000}k"

        filename = f"api_events_{size // 1_000}k.csv"
        filepath = os.path.join(args.output_dir, filename)

        # 파일 존재 확인
        if os.path.exists(filepath) and not args.force:
            print(f"\n{filename}: 이미 존재 (--force로 덮어쓰기)")
            continue

        print(f"\n{filename} 생성 중...")

        # 데이터 생성
        df = generate_api_events(size)

        # CSV 저장
        df.to_csv(filepath, index=False)

        # 파일 크기 확인
        file_size = os.path.getsize(filepath)
        if file_size >= 1_000_000_000:
            size_str = f"{file_size / 1_000_000_000:.1f}GB"
        elif file_size >= 1_000_000:
            size_str = f"{file_size / 1_000_000:.1f}MB"
        else:
            size_str = f"{file_size / 1_000:.1f}KB"

        print(f"  저장 완료: {filepath}")
        print(f"  레코드 수: {len(df):,}건")
        print(f"  파일 크기: {size_str}")

    print("\n" + "=" * 50)
    print("데이터 생성 완료!")
    print(f"출력 디렉토리: {args.output_dir}")


if __name__ == "__main__":
    main()
