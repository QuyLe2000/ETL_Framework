# Hướng Dẫn Sử Dụng Pipeline Bronze to Gold

## Mục lục

1. Tổng quan
2. Cấu trúc cấu hình pipeline
3. Full Load – Mô tả chi tiết
4. Incremental Load – Mô tả chi tiết
5. Transform – Các loại biến đổi dữ liệu
6. Ví dụ triển khai thực tế
7. Quy trình thêm bảng mới
8. Các lưu ý quan trọng
9. Troubleshooting
10. Tối ưu hiệu suất

---

## 1. Tổng quan

Pipeline Bronze to Gold được thiết kế nhằm chuyển đổi dữ liệu từ tầng **Bronze** (dữ liệu thô, chưa xử lý) sang tầng **Gold** (dữ liệu đã được làm sạch, chuẩn hóa và tối ưu cho mục đích phân tích và báo cáo).

Các đặc điểm chính của pipeline:

* Hỗ trợ hai phương pháp tải dữ liệu: **Full Load** và **Incremental Load**
* Tự động loại bỏ bản ghi trùng lặp dựa trên khóa định danh (deduplication)
* Hỗ trợ các phép biến đổi dữ liệu phổ biến (ví dụ: tạo cột năm/tháng/ngày, tính doanh thu)
* Hỗ trợ xử lý song song nhằm cải thiện hiệu suất
* Hỗ trợ phân vùng dữ liệu (partitioning) để tối ưu truy vấn

Pipeline phù hợp với kiến trúc Lakehouse và các mô hình dữ liệu Dimension / Fact.

---

## 2. Cấu trúc cấu hình pipeline

Mỗi bảng được khai báo thông qua một dictionary trong danh sách `TABLE_CONFIGS`. Cấu trúc tổng quát như sau:

```python
{
    "gold_table": "Tên bảng trong Gold layer",
    "bronze_table": "Tên bảng trong Bronze layer",
    "load_type": "FULL hoặc INCREMENTAL",
    "table_category": "DIMENSION hoặc FACT",

    "timestamp_col": "Cột timestamp dùng cho filter và dedup",
    "partition_timestamp_col": "Cột timestamp dùng để tạo partition",
    "dedup_cols": ["Danh sách cột khóa dùng cho deduplication"],

    "columns": ["Danh sách cột cần trích xuất"],
    "partition_cols": ["year", "month", "date"],

    "isLoad": True,
    "requires_transform": True,
    "transform_config": {
        "type": ["DERIVE_YMD", "PRICE_REVENUE"]
    }
}
```

---

## 3. Full Load – Mô tả chi tiết

### 3.1 Khái niệm

Full Load là phương pháp tải toàn bộ dữ liệu từ Bronze layer sang Gold layer trong mỗi lần chạy pipeline. Dữ liệu hiện có trong Gold sẽ bị xóa và được thay thế hoàn toàn bằng dữ liệu mới.

### 3.2 Trường hợp sử dụng

Full Load phù hợp trong các tình huống:

* Bảng master hoặc lookup có kích thước nhỏ
* Bảng snapshot định kỳ (daily snapshot, monthly snapshot)
* Dữ liệu có dung lượng nhỏ hoặc trung bình
* Không yêu cầu bảo toàn lịch sử dữ liệu

### 3.3 Ví dụ cấu hình Full Load

```python
{
    "gold_table": "dim_taxonomy",
    "bronze_table": "taxonomy",
    "load_type": "FULL",
    "table_category": "DIMENSION",
    "timestamp_col": None,
    "dedup_cols": ["taxonomy_id"],
    "columns": [
        "id",
        "taxonomy_id",
        "name",
        "parent_id",
        "created_at",
        "updated_at"
    ],
    "partition_cols": None,
    "isLoad": True,
    "requires_transform": False
}
```

---

## 4. Incremental Load – Mô tả chi tiết

### 4.1 Khái niệm

Incremental Load chỉ tải các bản ghi mới hoặc đã thay đổi kể từ lần chạy pipeline gần nhất. Pipeline sẽ xác định timestamp lớn nhất trong Gold layer và chỉ xử lý dữ liệu Bronze có timestamp lớn hơn giá trị này.

### 4.2 Trường hợp sử dụng

Incremental Load được khuyến nghị cho:

* Bảng fact hoặc bảng sự kiện có dung lượng lớn
* Dữ liệu được bổ sung liên tục theo thời gian
* Yêu cầu tối ưu thời gian xử lý
* Cần lưu trữ đầy đủ lịch sử dữ liệu

### 4.3 Ví dụ cấu hình Incremental Load

```python
{
    "gold_table": "dim_listing_information",
    "bronze_table": "analysis_listing_information",
    "load_type": "INCREMENTAL",
    "table_category": "DIMENSION",
    "timestamp_col": "creation_timestamp",
    "partition_timestamp_col": "creation_timestamp",
    "dedup_cols": ["listing_id"],
    "columns": [
        "listing_id",
        "shop_id",
        "user_id",
        "title",
        "description",
        "state",
        "url",
        "price",
        "currency_code",
        "taxonomy_id",
        "creation_timestamp"
    ],
    "partition_cols": ["year", "month", "date"],
    "isLoad": True,
    "requires_transform": True,
    "transform_config": {
        "type": ["DERIVE_YMD"]
    }
}
```

---

## 5. Transform – Các loại biến đổi dữ liệu

### 5.1 Transform DERIVE_YMD

**Mục đích:** Tạo các cột `year`, `month`, `date` từ một cột timestamp nhằm phục vụ partitioning.

**Trường hợp sử dụng:**

* Các bảng được partition theo ngày
* Các bảng fact hoặc dimension có dữ liệu theo thời gian

```python
"transform_config": {
    "type": ["DERIVE_YMD"]
}
```

### 5.2 Transform PRICE_REVENUE

**Mục đích:** Tính toán doanh thu dựa trên số lượng bán và đơn giá, thông qua việc join với bảng dimension chứa thông tin giá.

Công thức:

```
revenue = sales × price
```

```python
"transform_config": {
    "type": ["PRICE_REVENUE"],
    "join_table": "dim_listing_information",
    "join_key": "shop_id",
    "price_col": "price",
    "sales_col": "daily_sales"
}
```

### 5.3 Kết hợp nhiều transform

Pipeline cho phép áp dụng nhiều transform theo thứ tự được khai báo trong cấu hình.

---

## 6. Ví dụ triển khai thực tế

### Incremental Load cho bảng Fact có tính doanh thu

```python
{
    "gold_table": "fact_listing_performance_by_date",
    "bronze_table": "analysis_listing_performance_by_date",
    "load_type": "INCREMENTAL",
    "table_category": "FACT",
    "timestamp_col": "created_at",
    "partition_timestamp_col": "created_at",
    "dedup_cols": ["shop_id", "listing_id", "report_date"],
    "columns": [
        "shop_id",
        "listing_id",
        "report_date",
        "daily_sales",
        "daily_views",
        "created_at"
    ],
    "partition_cols": ["year", "month", "date"],
    "isLoad": True,
    "requires_transform": True,
    "transform_config": {
        "type": ["DERIVE_YMD", "PRICE_REVENUE"],
        "join_table": "dim_listing_information",
        "join_key": "shop_id",
        "price_col": "price",
        "sales_col": "daily_sales"
    }
}
```

---

## 7. Quy trình thêm bảng mới

1. Xác định loại load (Full hoặc Incremental)
2. Xác định danh sách cột cần trích xuất
3. Xác định khóa deduplication
4. Quyết định có sử dụng partition hay không
5. Xác định các transform cần áp dụng

---

## 8. Các lưu ý quan trọng

* Incremental Load bắt buộc phải có `timestamp_col`
* Các cột trong `dedup_cols` phải tạo thành khóa duy nhất
* Nếu sử dụng partition, cần đảm bảo các cột partition được tạo ra từ transform
* Các bảng dùng cho transform dạng join phải tồn tại trước khi pipeline chạy

---

## 9. Troubleshooting

**Lỗi: Table không tồn tại**

* Kiểm tra tên bảng, schema và lakehouse

**Lỗi: Column không tồn tại**

* Kiểm tra lại danh sách cột và kiểu chữ

**Lỗi: Timestamp chứa giá trị NULL**

* Làm sạch dữ liệu Bronze hoặc bổ sung logic xử lý NULL

---

## 10. Tối ưu hiệu suất

* Ưu tiên Incremental Load cho các bảng lớn
* Sử dụng partition theo ngày cho các bảng fact
* Điều chỉnh số lượng worker để tận dụng khả năng xử lý song song

```python
run_gold_pipeline(TABLE_CONFIGS, max_workers=8)
```

---

Tài liệu này đóng vai trò như một hướng dẫn chuẩn để triển khai, vận hành và mở rộng pipeline Bronze to Gold trong môi trường Lakehouse.
