#!/usr/bin/env python3
"""
Spark 배치 처리: API 이벤트 분석

Usage:
    python spark_batch.py [--file FILE]

Options:
    --file FILE   분석할 CSV 파일 경로 (기본: /data/api_events_100k.csv)
"""
import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min, sum as spark_sum, when


def main():
    parser = argparse.ArgumentParser(description="Spark 배치 처리")
    parser.add_argument(
        "--file",
        type=str,
        default="/data/api_events_100k.csv",
        help="분석할 CSV 파일 경로",
    )
    args = parser.parse_args()

    # SparkSession 생성
    spark = (
        SparkSession.builder.appName("API-Events-Batch")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )

    print("SparkSession 생성 완료!")
    print(f"  버전: {spark.version}")
    print(f"  Master: {spark.sparkContext.master}")
    print()

    # 데이터 로드
    print(f"데이터 로드: {args.file}")
    start_time = time.time()

    df = spark.read.csv(args.file, header=True, inferSchema=True)
    df.cache()

    row_count = df.count()
    load_time = time.time() - start_time

    print(f"  총 레코드: {row_count:,}건")
    print(f"  로드 시간: {load_time:.2f}초")
    print()

    # 스키마 확인
    print("스키마:")
    df.printSchema()
    print()

    # 분석 1: 엔드포인트별 통계
    print("=" * 60)
    print("엔드포인트별 통계")
    print("=" * 60)

    endpoint_stats = df.groupBy("endpoint").agg(
        count("request_id").alias("request_count"),
        avg("response_time_ms").alias("avg_response_time"),
        spark_max("response_time_ms").alias("max_response_time"),
        spark_min("response_time_ms").alias("min_response_time"),
    )
    endpoint_stats.orderBy(col("request_count").desc()).show(truncate=False)

    # 분석 2: 상태 코드별 분포
    print("=" * 60)
    print("상태 코드별 분포")
    print("=" * 60)

    status_stats = df.groupBy("status_code").agg(
        count("request_id").alias("count"),
    )
    status_stats = status_stats.withColumn(
        "percentage", col("count") / row_count * 100
    )
    status_stats.orderBy("status_code").show()

    # 분석 3: 에러율 (endpoint별)
    print("=" * 60)
    print("엔드포인트별 에러율")
    print("=" * 60)

    error_stats = df.groupBy("endpoint").agg(
        count("request_id").alias("total"),
        spark_sum(when(col("status_code") >= 400, 1).otherwise(0)).alias("errors"),
    ).withColumn("error_rate", col("errors") / col("total") * 100)

    error_stats.orderBy(col("error_rate").desc()).show(truncate=False)

    # 분석 4: HTTP 메소드별 통계
    print("=" * 60)
    print("HTTP 메소드별 통계")
    print("=" * 60)

    method_stats = df.groupBy("method").agg(
        count("request_id").alias("count"),
        avg("response_time_ms").alias("avg_response_time"),
    )
    method_stats.orderBy(col("count").desc()).show()

    # 전체 요약
    total_time = time.time() - start_time

    print("=" * 60)
    print("분석 요약")
    print("=" * 60)
    print(f"  총 레코드: {row_count:,}건")
    print(f"  총 소요 시간: {total_time:.2f}초")
    print(f"  처리량: {row_count / total_time:,.0f} records/sec")

    # 에러 요약
    error_count = df.filter(col("status_code") >= 400).count()
    print(f"\n  전체 에러율: {error_count / row_count * 100:.2f}%")
    print(f"  평균 응답 시간: {df.agg(avg('response_time_ms')).collect()[0][0]:.1f}ms")

    # 세션 종료
    df.unpersist()
    spark.stop()
    print("\nSpark 세션 종료")


if __name__ == "__main__":
    main()
