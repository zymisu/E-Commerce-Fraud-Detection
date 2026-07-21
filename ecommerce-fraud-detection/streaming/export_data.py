"""
export_data.py — Xuất tập giao dịch từ HDFS ra một file CSV local.

Producer (producer.py) đọc dữ liệu từ file local cho nhẹ và nhanh, không cần
khởi động Spark mỗi lần. Script này chạy MỘT LẦN để tạo file local đó.

Cách chạy (từ thư mục streaming/):
    python export_data.py

Yêu cầu: HDFS đang chạy ở hdfs://localhost:9090 (giống các notebook khác).
Kết quả: ./data/transactions.csv
"""

import os
from pyspark.sql import SparkSession

HDFS_PATH = "hdfs://localhost:9090/ecommerce-fraud-detection/transactions.csv"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "transactions.csv")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("ExportTransactionsToLocal")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Đang đọc dữ liệu từ HDFS: {HDFS_PATH}")
    df = spark.read.csv(HDFS_PATH, header=True, inferSchema=True)

    n = df.count()
    print(f"Tổng số bản ghi: {n:,}")

    # Gom về 1 partition và lưu ra pandas -> 1 file CSV duy nhất, dễ cho producer đọc
    print(f"Đang xuất ra: {OUTPUT_CSV}")
    df.toPandas().to_csv(OUTPUT_CSV, index=False)

    print("Xuất dữ liệu hoàn tất!")
    spark.stop()


if __name__ == "__main__":
    main()
