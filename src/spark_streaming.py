#!/usr/bin/env python3
"""
Spark Structured Streaming: Kafka에서 실시간 API 이벤트 처리

Usage:
    python spark_streaming.py [--window MINUTES] [--output MODE]

Options:
    --window MINUTES   윈도우 크기 (분) (기본: 5)
    --output MODE      출력 모드: console, parquet, kafka (기본: console)
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    avg,
    sum as spark_sum,
    when,
    to_json,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


def main():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming")
    parser.add_argument("--window", type=int, default=5, help="윈도우 크기 (분)")
    parser.add_argument(
        "--output",
        type=str,
        default="console",
        choices=["console", "parquet", "kafka"],
        help="출력 모드",
    )
    parser.add_argument("--topic", type=str, default="api-events", help="소스 토픽")
    args = parser.parse_args()

    # SparkSession 생성
    spark = (
        SparkSession.builder.appName("API-Events-Streaming")
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
        )
        .config("spark.sql.streaming.checkpointLocation", f"/data/checkpoints/{args.output}")
        .getOrCreate()
    )

    print("SparkSession 생성 완료!")
    print(f"  출력 모드: {args.output}")
    print(f"  윈도우 크기: {args.window}분")
    print()

    # API 이벤트 스키마
    schema = StructType(
        [
            StructField("request_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("endpoint", StringType(), True),
            StructField("method", StringType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )

    # Kafka에서 스트림 읽기
    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # JSON 파싱
    df_parsed = (
        df_raw.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    # 윈도우 집계
    df_windowed = (
        df_parsed.withWatermark("event_time", "10 minutes")
        .groupBy(window(col("event_time"), f"{args.window} minutes"), col("endpoint"))
        .agg(
            count("request_id").alias("request_count"),
            avg("response_time_ms").alias("avg_response_time"),
            spark_sum(when(col("status_code") >= 400, 1).otherwise(0)).alias(
                "error_count"
            ),
        )
        .withColumn("error_rate", col("error_count") / col("request_count") * 100)
    )

    # 출력 모드에 따른 처리
    if args.output == "console":
        query = (
            df_windowed.writeStream.outputMode("update")
            .format("console")
            .option("truncate", False)
            .trigger(processingTime="10 seconds")
            .start()
        )
        print("콘솔 출력 시작! (Ctrl+C로 종료)")

    elif args.output == "parquet":
        query = (
            df_windowed.writeStream.outputMode("append")
            .format("parquet")
            .option("path", "/data/output/api_stats")
            .trigger(processingTime="1 minute")
            .start()
        )
        print("Parquet 저장 시작!")
        print("  경로: /data/output/api_stats")

    elif args.output == "kafka":
        df_output = df_windowed.select(
            col("endpoint").alias("key"), to_json(struct("*")).alias("value")
        )

        query = (
            df_output.writeStream.outputMode("update")
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("topic", "api-events-aggregated")
            .trigger(processingTime="30 seconds")
            .start()
        )
        print("Kafka 출력 시작!")
        print("  결과 토픽: api-events-aggregated")

    query.awaitTermination()


if __name__ == "__main__":
    main()
