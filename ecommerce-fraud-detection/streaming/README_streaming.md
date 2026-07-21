# Hệ thống phát hiện gian lận THỜI GIAN THỰC (Kafka + Spark Structured Streaming)

> Phần mở rộng (bonus) của đồ án **E-Commerce Fraud Detection** — môn Big Data.

## 1. Vấn đề & Đề xuất

Mô hình Random Forest hiện tại ([fraud_detection_sparkml.ipynb](../fraud_detection_sparkml.ipynb)) chạy theo **lô (batch)** trên dữ liệu tĩnh đã có sẵn — chỉ phân tích được gian lận *sau khi sự việc đã xảy ra*. Trong thực tế, một giao dịch gian lận cần bị phát hiện và chặn trong **vài trăm mili-giây**, ngay khi nó đang diễn ra.

**Đề xuất:** Xây dựng pipeline **streaming** dùng **Apache Kafka** + **Spark Structured Streaming** để áp mô hình đã train lên luồng giao dịch realtime và phát cảnh báo gian lận tức thời.

## 2. Kiến trúc

```
 transactions.csv (HDFS)
        │  export_data.py (1 lần)
        ▼
 data/transactions.csv ──► producer.py ──► Kafka topic: transactions
                            (giả lập         │
                             realtime)        ▼
                                    fraud_detection_streaming.ipynb
                                    (Spark Structured Streaming)
                                      1. readStream từ Kafka
                                      2. parse JSON theo schema
                                      3. feature engineering (giống batch)
                                      4. load RF model từ HDFS
                                      5. transform → prediction
                                      6. lọc prediction == 1
                                             │
                            ┌────────────────┼─────────────────┐
                            ▼                ▼                  ▼
                     foreachBatch     Kafka topic        (tuỳ chọn)
                     in ra notebook   fraud-alerts       HDFS parquet
```

**Vì sao Kafka?** Tách rời (decouple) nguồn phát giao dịch khỏi bộ xử lý, làm bộ đệm (buffer) chịu tải đột biến, và cho phép nhiều consumer cùng đọc. Có thể nạp hàng triệu sự kiện/giây.

**Vì sao Spark Structured Streaming?** Tái dùng nguyên API DataFrame của phần batch (cùng feature engineering, cùng `PipelineModel`), đảm bảo *exactly-once*, và mở rộng ngang ra cluster nhiều node mà không phải đổi code.

## 3. Thành phần

| File | Vai trò |
|---|---|
| `docker-compose.yml` | Dựng 1 Kafka broker (KRaft, không cần Zookeeper) + tạo sẵn 2 topic |
| `export_data.py` | Xuất dữ liệu từ HDFS ra `data/transactions.csv` (chạy 1 lần) |
| `producer.py` | Đọc CSV, đẩy từng giao dịch dạng JSON vào topic `transactions` |
| `fraud_detection_streaming.ipynb` | Consumer: đọc stream, áp model, phát cảnh báo |
| `requirements.txt` | Thư viện Python cần cài |

## 4. Yêu cầu môi trường

- Docker + Docker Compose
- HDFS đang chạy tại `hdfs://localhost:9090` (giống các notebook khác)
- Python có `pyspark==4.1.1`, `pandas`, `kafka-python`
- Đã chạy notebook batch tới cell cuối để **lưu model lên HDFS** (`hdfs://localhost:9090/models/fraud_detection_rf`)

Cài thư viện:
```bash
pip install -r requirements.txt
```

## 5. Các bước chạy demo (end-to-end)

```bash
# Bước 1: Khởi động Kafka (trong thư mục streaming/)
docker compose up -d
docker compose ps            # chờ container fraud-kafka ở trạng thái healthy
docker logs fraud-kafka-init # xem 2 topic transactions & fraud-alerts đã tạo

# Bước 2: Lưu model — chạy notebook batch fraud_detection_sparkml.ipynb
#         tới cell cuối (đã lưu model lên HDFS). Bỏ qua nếu model đã tồn tại.

# Bước 3: Xuất dữ liệu từ HDFS ra local (chạy 1 lần)
python export_data.py        # tạo data/transactions.csv

# Bước 4: Mở fraud_detection_streaming.ipynb, chạy lần lượt các cell
#         tới cell có .start() (Sink 1 & Sink 2). Stream bắt đầu lắng nghe.

# Bước 5: Bơm giao dịch vào Kafka (terminal khác)
python producer.py --limit 1000
#   Quan sát cảnh báo fraud in ra ngay dưới cell foreachBatch trong notebook.
```

Kiểm tra cảnh báo đã được đẩy ra topic `fraud-alerts`:
```bash
docker exec -it fraud-kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9094 --topic fraud-alerts --from-beginning
```

Dọn dẹp:
```bash
# Trong notebook: chạy cell dừng query (mục 10)
docker compose down -v       # tắt Kafka và xoá volume
```

## 6. Lưu ý kỹ thuật

- **Scala 2.13:** Spark 4.x dùng Scala 2.13, nên connector Kafka phải là
  `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1`. Lần chạy đầu Spark tự tải package từ Maven (cần internet).
- **Feature engineering khớp 100%:** 5 cột dẫn xuất (`transaction_hour`, `transaction_dayofweek`, `country_match`, `is_weekend`, `amount_ratio`) phải tạo y hệt phần batch trước khi `transform`, nếu không feature sẽ lệch.
- **`handleInvalid="keep"`** trong `StringIndexer` (đã có ở batch) giúp stream không crash khi gặp giá trị categorical lạ.
- **Hai listener Kafka:** host (notebook/producer) dùng `localhost:9092`; container nội bộ dùng `kafka:9094`.

## 7. So sánh Batch vs Streaming

| Tiêu chí | Batch (hiện có) | Streaming (mở rộng) |
|---|---|---|
| Thời điểm phát hiện | Sau khi giao dịch xong | Ngay khi giao dịch đến |
| Độ trễ | Phút → giờ | Giây (micro-batch) |
| Nguồn dữ liệu | File tĩnh trên HDFS | Luồng sự kiện Kafka |
| Ứng dụng | Phân tích, báo cáo | Chặn/cảnh báo realtime |
| Mô hình ML | Cùng một `PipelineModel` Random Forest | |

## 8. Hướng mở rộng

- **Stateful aggregation** phát hiện *velocity attack* (nhiều giao dịch/user trong cửa sổ thời gian) bằng windowed aggregation của Structured Streaming.
- **Online learning** cập nhật model định kỳ theo dữ liệu mới.
- **Dashboard realtime** (Grafana / Streamlit) đọc từ topic `fraud-alerts`.
- Mở rộng ra **Spark cluster đa node** để xử lý hàng triệu giao dịch/ngày.
