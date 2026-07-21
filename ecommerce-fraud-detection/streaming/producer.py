"""
producer.py — Giả lập NGUỒN PHÁT giao dịch thời gian thực.

Đọc từng dòng từ file CSV local (đã xuất bằng export_data.py), serialize sang
JSON và đẩy vào Kafka topic "transactions". Giữa các lần gửi có delay ngẫu nhiên
để mô phỏng giao dịch đến theo thời gian thực.

Cách chạy (sau khi `docker compose up -d` và `python export_data.py`):
    python producer.py                 # gửi liên tục, tốc độ mặc định
    python producer.py --limit 500     # chỉ gửi 500 giao dịch
    python producer.py --min-delay 0.05 --max-delay 0.3   # tăng tốc

Yêu cầu: pip install kafka-python pandas
"""

import argparse
import json
import os
import random
import time

import pandas as pd
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "transactions"
DEFAULT_CSV = os.path.join(os.path.dirname(__file__), "data", "transactions.csv")

# Các cột số nguyên (ép kiểu khi đọc để JSON gửi đi đúng kiểu model mong đợi)
INT_COLS = [
    "transaction_id", "user_id", "account_age_days", "total_transactions_user",
    "promo_used", "avs_match", "cvv_result", "three_ds_flag", "is_fraud",
]
FLOAT_COLS = ["avg_amount_user", "amount", "shipping_distance_km"]


def parse_args():
    p = argparse.ArgumentParser(description="Kafka transaction producer (fraud demo)")
    p.add_argument("--csv", default=DEFAULT_CSV, help="Đường dẫn file CSV nguồn")
    p.add_argument("--limit", type=int, default=None, help="Số giao dịch tối đa cần gửi")
    p.add_argument("--min-delay", type=float, default=0.1, help="Delay tối thiểu (giây)")
    p.add_argument("--max-delay", type=float, default=1.0, help="Delay tối đa (giây)")
    p.add_argument("--shuffle", action="store_true", help="Trộn ngẫu nhiên thứ tự giao dịch")
    return p.parse_args()


def row_to_message(row: dict) -> dict:
    """Chuẩn hoá kiểu dữ liệu của một dòng trước khi gửi."""
    msg = {}
    for k, v in row.items():
        if pd.isna(v):
            msg[k] = None
        elif k in INT_COLS:
            msg[k] = int(v)
        elif k in FLOAT_COLS:
            msg[k] = float(v)
        else:
            # country, bin_country, channel, merchant_category, transaction_time
            msg[k] = str(v)
    return msg


def main():
    args = parse_args()

    if not os.path.exists(args.csv):
        raise SystemExit(
            f"Không tìm thấy file CSV: {args.csv}\n"
            f"Hãy chạy 'python export_data.py' trước để xuất dữ liệu từ HDFS."
        )

    print(f"Đọc dữ liệu từ: {args.csv}")
    df = pd.read_csv(args.csv)
    if args.shuffle:
        df = df.sample(frac=1.0).reset_index(drop=True)
    if args.limit:
        df = df.head(args.limit)
    print(f"Sẽ gửi {len(df):,} giao dịch tới topic '{TOPIC}' @ {BOOTSTRAP_SERVERS}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        linger_ms=10,
    )

    sent = 0
    fraud_sent = 0
    try:
        for record in df.to_dict(orient="records"):
            msg = row_to_message(record)
            key = msg.get("transaction_id")
            producer.send(TOPIC, key=key, value=msg)

            sent += 1
            if msg.get("is_fraud") == 1:
                fraud_sent += 1

            if sent % 50 == 0:
                producer.flush()
                print(f"  Đã gửi {sent:,} giao dịch (trong đó {fraud_sent} fraud)")

            time.sleep(random.uniform(args.min_delay, args.max_delay))
    except KeyboardInterrupt:
        print("\nDừng producer theo yêu cầu (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        print(f"Hoàn tất. Tổng đã gửi: {sent:,} giao dịch ({fraud_sent} fraud thực tế).")


if __name__ == "__main__":
    main()
