# 📘 Hướng Dẫn Sử Dụng Pipeline Bronze to Gold

## Mục Lục
1. [Tổng Quan](#tổng-quan)
2. [Cấu Trúc Cơ Bản](#cấu-trúc-cơ-bản)
3. [FULL LOAD - Hướng Dẫn Chi Tiết](#full-load---hướng-dẫn-chi-tiết)
4. [INCREMENTAL LOAD - Hướng Dẫn Chi Tiết](#incremental-load---hướng-dẫn-chi-tiết)
5. [TRANSFORM - Các Loại Biến Đổi](#transform---các-loại-biến-đổi)
6. [Các Ví Dụ Thực Tế](#các-ví-dụ-thực-tế)
7. [Troubleshooting](#troubleshooting)

---

## 🎯 Tổng Quan

Pipeline này thực hiện công việc chuyển dữ liệu từ tầng **Bronze** (dữ liệu thô) sang tầng **Gold** (dữ liệu đã xử lý).

**Đặc điểm chính:**
- ✅ Hỗ trợ 2 loại load: **FULL LOAD** và **INCREMENTAL LOAD**
- ✅ Tự động **deduplication** theo khóa chỉ định
- ✅ Hỗ trợ **Transform** dữ liệu (Derive YMD, Price Revenue)
- ✅ Chạy **song song** (parallel) để tối ưu tốc độ
- ✅ Tự động **partition** dữ liệu

---

## 📋 Cấu Trúc Cơ Bản

Mỗi bảng được cấu hình bằng một **dictionary** trong danh sách `TABLE_CONFIGS`:

```python
{
    # ===== THÔNG TIN CƠ BẢN =====
    "gold_table": "Tên bảng trong Gold layer",
    "bronze_table": "Tên bảng trong Bronze layer",
    "load_type": "FULL hoặc INCREMENTAL",
    "table_category": "DIMENSION hoặc FACT",
    
    # ===== TIMESTAMP & DEDUP =====
    "timestamp_col": "Cột timestamp cho dedup",
    "partition_timestamp_col": "Cột timestamp cho partition",
    "dedup_cols": ["Danh sách cột khóa cho dedup"],
    
    # ===== CỘT DỮ LIỆU =====
    "columns": ["Danh sách cột cần lấy"],
    "partition_cols": ["year", "month", "date"],  # Cột partition (nếu có)
    
    # ===== CÀI ĐẶT =====
    "isLoad": True,  # True/False để bật/tắt load
    "requires_transform": True/False,
    "transform_config": {
        "type": ["DERIVE_YMD", "PRICE_REVENUE"],  # Loại transform
        # ... cài đặt transform khác ...
    }
}
```

---

## 🔄 FULL LOAD - Hướng Dẫn Chi Tiết

### Khái Niệm
- **FULL LOAD**: Tải **toàn bộ dữ liệu** từ Bronze mỗi lần chạy
- Sử dụng khi: Bảng nhỏ, thường xuyên thay đổi hoàn toàn, hoặc cần refresh toàn bộ
- Cách hoạt động: **Xóa hết dữ liệu cũ → Tải toàn bộ dữ liệu mới**

### Khi Nào Dùng FULL LOAD?
1. **Bảng Master/Lookup nhỏ**: Danh mục, phân loại, bảng tham chiếu
2. **Snapshot hàng ngày**: Trạng thái cơ sở dữ liệu tại một thời điểm
3. **Dữ liệu không lớn**: < 1GB mỗi lần load
4. **Yêu cầu cập nhật hoàn toàn**: Không cần giữ lại dữ liệu cũ

### Ví Dụ 1: FULL LOAD - Bảng Danh Mục

```python
{
    "gold_table": "dim_taxonomy",                 # Bảng danh mục
    "bronze_table": "taxonomy",
    "load_type": "FULL",                          # ⭐ FULL LOAD
    "table_category": "DIMENSION",
    "timestamp_col": None,                        # Không có timestamp
    "dedup_cols": ["taxonomy_id"],                # Khóa duy nhất
    "columns": [
        "id", "taxonomy_id", "name", "parent_id",
        "created_at", "updated_at"
    ],
    "partition_cols": None,                       # Không partition
    "isLoad": True,
    "requires_transform": False                   # Không cần transform
}
```

**Giải thích:**
- Mỗi chạy pipeline → Tải lại toàn bộ bảng taxonomy từ Bronze
- Đảm bảo danh mục luôn là phiên bản mới nhất
- Không partition → Toàn bộ dữ liệu trong 1 thư mục

### Ví Dụ 2: FULL LOAD - Bảng Thông Tin Shop (với Partition)

```python
{
    "gold_table": "dim_shop_informations",
    "bronze_table": "analysis_shop_informations",
    "load_type": "FULL",                          # ⭐ FULL LOAD
    "table_category": "DIMENSION",
    "timestamp_col": "updated_at",                # Dùng để dedup
    "partition_timestamp_col": "created_at",     # Dùng để derive YMD
    "dedup_cols": ["shop_id"],                    # 1 shop_id = 1 hàng
    "columns": [
        "id", "shop_id", "name", "status", "url", 
        "currency_code", "created_at", "updated_at",
        "created_timestamp", "updated_timestamp", 
        "is_vacation", "country"
    ],
    "partition_cols": ["year", "month", "date"],  # ⭐ Partition theo ngày
    "isLoad": True,
    "requires_transform": True,                   # ⭐ Cần transform
    "transform_config": {
        "type": ["DERIVE_YMD"]                    # Tạo cột year, month, date
    }
}
```

**Giải thích:**
- **DEDUP**: Vì 1 shop có nhiều record lịch sử, chỉ giữ bản ghi mới nhất (by `updated_at`)
- **PARTITION**: Dữ liệu được chia thành thư mục con theo năm/tháng/ngày
- **TRANSFORM DERIVE_YMD**: Lấy `created_at` → Tạo cột `year`, `month`, `date`

**Lợi ích Partition:**
- 📊 Query nhanh hơn (không cần scan toàn bộ)
- 💾 Dễ cleanup dữ liệu cũ
- ⚡ Tối ưu performance

---

## 📈 INCREMENTAL LOAD - Hướng Dẫn Chi Tiết

### Khái Niệm
- **INCREMENTAL LOAD**: Chỉ tải **dữ liệu mới/thay đổi** từ Bronze
- Sử dụng khi: Bảng lớn, thêm dữ liệu liên tục, cần tối ưu tốc độ
- Cách hoạt động: **Lấy timestamp cuối cùng → Tải dữ liệu sau timestamp đó → Merge vào Gold**

### Khi Nào Dùng INCREMENTAL LOAD?
1. **Bảng sự kiện lớn**: Transaction, log, activity
2. **Dữ liệu thêm liên tục**: Không bao giờ xóa/sửa dữ liệu cũ
3. **Cần tối ưu tốc độ**: Tải chỉ dữ liệu mới
4. **Lịch sử quan trọng**: Phải giữ lại tất cả bản ghi

### Ví Dụ 1: INCREMENTAL LOAD - Bảng Thông Tin Listing

```python
{
    "gold_table": "dim_listing_information",
    "bronze_table": "analysis_listing_information",
    "load_type": "INCREMENTAL",                  # ⭐ INCREMENTAL LOAD
    "table_category": "DIMENSION",
    "timestamp_col": "creation_timestamp",       # Dùng để filter & dedup
    "partition_timestamp_col": "creation_timestamp",
    "dedup_cols": ["listing_id"],                # 1 listing = 1 hàng
    "columns": [
        "listing_id", "shop_id", "user_id", "title", 
        "description", "state", "url", "price", 
        "currency_code", "taxonomy_id", 
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

**Quy Trình Chạy:**
1. ⏱️ **Lấy timestamp cuối**: `SELECT MAX(creation_timestamp) FROM gold.dim_listing_information`
   - VD: `2024-01-15 10:30:00`
2. 🔍 **Filter dữ liệu mới**: `WHERE creation_timestamp > 2024-01-15 10:30:00`
3. 🔄 **Dedup**: Giữ bản ghi mới nhất cho mỗi `listing_id`
4. 📝 **Transform**: Tạo cột year, month, date
5. ✅ **Merge**: 
   - Nếu `listing_id` tồn tại → **UPDATE** (nếu dữ liệu mới)
   - Nếu `listing_id` không tồn tại → **INSERT**

### Ví Dụ 2: INCREMENTAL LOAD - Bảng Performance (với Transform PRICE_REVENUE)

```python
{
    "gold_table": "fact_listing_performance_by_date",
    "bronze_table": "analysis_listing_performance_by_date",
    "load_type": "INCREMENTAL",                  # ⭐ INCREMENTAL LOAD
    "table_category": "FACT",
    "timestamp_col": "created_at",               # Filter & merge key
    "partition_timestamp_col": "created_at",
    "dedup_cols": ["shop_id", "listing_id", "report_date"],  # Key duy nhất
    "columns": [
        "site_id", "shop_id", "listing_id", "report_date",
        "daily_sales", "daily_views", "daily_favorers",
        "conversion_rate", "views", "favorers", "created_at"
    ],
    "partition_cols": ["year", "month", "date"],
    "isLoad": True,
    "requires_transform": True,
    "transform_config": {
        "type": ["DERIVE_YMD", "PRICE_REVENUE"],  # ⭐ 2 loại transform
        "join_table": "dim_listing_information",  # Join bảng này
        "join_key": "shop_id",                    # Khóa join
        "price_col": "price",                     # Cột giá từ dim_listing
        "sales_col": "daily_sales"                # Cột sales cần tính revenue
    }
}
```

**Quy Trình Chạy:**
1. 🔍 **Filter**: `WHERE created_at > (MAX created_at từ Gold)`
2. 📝 **Transform DERIVE_YMD**: Tạo cột year, month, date từ created_at
3. 📝 **Transform PRICE_REVENUE**: 
   - Join với `dim_listing_information` by `shop_id`
   - Lấy `price` từ dim_listing (bản ghi mới nhất)
   - Tính `revenue = daily_sales × price`
4. 🔄 **Dedup**: Giữ bản ghi mới nhất cho mỗi `(shop_id, listing_id, report_date)`
5. ✅ **Merge**: Cập nhật hoặc thêm mới

---

## 🔧 TRANSFORM - Các Loại Biến Đổi

### Transform 1: DERIVE_YMD (Tạo Cột Năm, Tháng, Ngày)

**Mục đích**: Tạo 3 cột `year`, `month`, `date` từ cột timestamp để dùng cho partition

**Khi nào dùng:**
- Hầu hết các bảng có partition theo ngày đều cần DERIVE_YMD
- Bảng có `partition_cols: ["year", "month", "date"]`

**Cấu hình:**
```python
"transform_config": {
    "type": ["DERIVE_YMD"]
}
```

**Ví dụ:**
```
Trước:
| listing_id | creation_timestamp  |
|------------|---------------------|
| 1001       | 2024-01-15 10:30:00 |
| 1002       | 2024-01-15 14:20:00 |

Sau:
| listing_id | creation_timestamp  | year | month | date       |
|------------|---------------------|------|-------|------------|
| 1001       | 2024-01-15 10:30:00 | 2024 | 1     | 2024-01-15 |
| 1002       | 2024-01-15 14:20:00 | 2024 | 1     | 2024-01-15 |
```

**Cấu hình hoàn chỉnh:**
```python
{
    "gold_table": "dim_tags",
    "bronze_table": "analysis_tags",
    "load_type": "INCREMENTAL",
    "timestamp_col": "updated_at",
    "partition_timestamp_col": "created_at",      # ⭐ Lấy từ cột này
    "partition_cols": ["year", "month", "date"],
    "isLoad": True,
    "requires_transform": True,
    "transform_config": {
        "type": ["DERIVE_YMD"]                    # Chỉ cần DERIVE_YMD
    }
}
```

---

### Transform 2: PRICE_REVENUE (Tính Doanh Thu)

**Mục đích**: Join với bảng giá để tính doanh thu = sales × price

**Khi nào dùng:**
- Bảng fact cần tính doanh thu
- Có giá từ bảng dimension khác
- Cần cột `price` và `revenue`

**Cấu hình:**
```python
"transform_config": {
    "type": ["PRICE_REVENUE"],
    "join_table": "dim_listing_information",  # Bảng chứa giá
    "join_key": "shop_id",                    # Khóa join
    "price_col": "price",                     # Cột giá trong bảng được join
    "sales_col": "daily_sales"                # Cột sales để tính revenue
}
```

**Ví dụ chi tiết:**

**Bronze dữ liệu:**
```
analysis_listing_performance_by_date:
| shop_id | listing_id | report_date | daily_sales |
|---------|------------|-------------|-------------|
| S001    | L001       | 2024-01-15  | 10          |
| S001    | L002       | 2024-01-15  | 5           |

analysis_listing_information:
| listing_id | price |
|------------|-------|
| L001       | 100   |
| L002       | 50    |
```

**Sau Transform:**
```
| shop_id | listing_id | report_date | daily_sales | price | revenue |
|---------|------------|-------------|-------------|-------|---------|
| S001    | L001       | 2024-01-15  | 10          | 100   | 1000    |
| S001    | L002       | 2024-01-15  | 5           | 50    | 250     |
```

**Cấu hình hoàn chỉnh:**
```python
{
    "gold_table": "fact_listing_performance_by_date",
    "bronze_table": "analysis_listing_performance_by_date",
    "load_type": "INCREMENTAL",
    "dedup_cols": ["shop_id", "listing_id", "report_date"],
    "columns": [
        "shop_id", "listing_id", "report_date",
        "daily_sales", "daily_views", "created_at"
    ],
    "partition_cols": ["year", "month", "date"],
    "isLoad": True,
    "requires_transform": True,
    "transform_config": {
        "type": ["DERIVE_YMD", "PRICE_REVENUE"],  # ⭐ Kết hợp 2 transform
        "join_table": "dim_listing_information",
        "join_key": "shop_id",
        "price_col": "price",
        "sales_col": "daily_sales"
    }
}
```

---

### Kết Hợp Nhiều Transform

Có thể chạy **nhiều transform** theo thứ tự:

```python
"transform_config": {
    "type": ["DERIVE_YMD", "PRICE_REVENUE"],  # ⭐ Thứ tự có ý nghĩa!
    # Cấu hình cho PRICE_REVENUE
    "join_table": "dim_listing_information",
    "join_key": "shop_id",
    "price_col": "price",
    "sales_col": "daily_sales"
}
```

**Thứ tự thực thi:**
1. Đầu tiên: `DERIVE_YMD` → Tạo cột year, month, date
2. Sau đó: `PRICE_REVENUE` → Join và tính revenue

---

## 📚 Các Ví Dụ Thực Tế

### ❌ Sai - INCREMENTAL không có MERGE khi bảng chưa tồn tại

```python
# ❌ KHÔNG ĐÚNG
{
    "gold_table": "new_table",
    "bronze_table": "new_bronze",
    "load_type": "INCREMENTAL",  # Bảng chưa tồn tại!
    "dedup_cols": ["id"],
    "columns": ["id", "name"],
    "isLoad": True,
    "requires_transform": False
}
```

**Vấn đề**: Lần đầu chạy, bảng Gold chưa tồn tại, code sẽ **tự động CREATE** bảng mới (không lỗi, nhưng log sẽ báo "Creating new table")

**Giải pháp**: Không có gì sai, code xử lý tự động. Lần chạy thứ 2 trở đi mới dùng MERGE.

---

### ✅ Đúng - FULL LOAD Bảng Danh Mục

```python
# ✅ ĐÚNG
{
    "gold_table": "dim_category",
    "bronze_table": "categories",
    "load_type": "FULL",
    "table_category": "DIMENSION",
    "timestamp_col": None,           # Không cần timestamp
    "dedup_cols": ["category_id"],   # Khóa duy nhất
    "columns": ["category_id", "name", "description"],
    "partition_cols": None,          # Không partition
    "isLoad": True,
    "requires_transform": False
}
```

---

### ✅ Đúng - INCREMENTAL Bảng Event với Transform

```python
# ✅ ĐÚNG
{
    "gold_table": "fact_user_events",
    "bronze_table": "user_events",
    "load_type": "INCREMENTAL",
    "table_category": "FACT",
    "timestamp_col": "event_timestamp",          # ⭐ Dùng để filter & dedup
    "partition_timestamp_col": "event_timestamp",
    "dedup_cols": ["user_id", "event_id", "event_date"],  # ⭐ Khóa duy nhất
    "columns": [
        "user_id", "event_id", "event_date",
        "event_type", "event_value", "event_timestamp"
    ],
    "partition_cols": ["year", "month", "date"],
    "isLoad": True,
    "requires_transform": True,
    "transform_config": {
        "type": ["DERIVE_YMD"]
    }
}
```

**Giải thích:**
- ✅ Có `timestamp_col` → Filter chỉ dữ liệu mới
- ✅ Có `dedup_cols` → Đảm bảo 1 event chỉ có 1 hàng
- ✅ Có `partition_cols` → Chia dữ liệu theo ngày
- ✅ Transform `DERIVE_YMD` → Tạo cột year, month, date

---

### ✅ Đúng - FULL LOAD Bảng Snapshot Hàng Ngày

```python
# ✅ ĐÚNG
{
    "gold_table": "dim_user_snapshot",
    "bronze_table": "user_daily_snapshot",
    "load_type": "FULL",                        # ⭐ FULL vì snapshot toàn bộ
    "table_category": "DIMENSION",
    "timestamp_col": "snapshot_date",
    "partition_timestamp_col": "snapshot_date",
    "dedup_cols": ["user_id", "snapshot_date"],
    "columns": [
        "user_id", "name", "status", "subscription_tier",
        "total_purchases", "snapshot_date"
    ],
    "partition_cols": ["year", "month", "date"],
    "isLoad": True,
    "requires_transform": True,
    "transform_config": {
        "type": ["DERIVE_YMD"]
    }
}
```

**Lý do FULL LOAD:**
- Snapshot hàng ngày → Cần reload toàn bộ
- Dữ liệu không lớn (khoảng 100K-1M hàng/ngày)
- Cần refresh hoàn toàn trạng thái người dùng

---

## 🚀 Cách Thêm Bảng Mới

### Bước 1: Chọn Load Type

| Chọn | Nếu |
|------|-----|
| **FULL LOAD** | Bảng nhỏ, lookup, snapshot, thay đổi hoàn toàn |
| **INCREMENTAL LOAD** | Bảng lớn, dữ liệu thêm liên tục, cần tối ưu |

### Bước 2: Xác Định Cột

```python
"columns": [
    # Chỉ lấy cột cần thiết
    # ❌ KHÔNG lấy cột không cần
    # ✅ Phải bao gồm cột timestamp
]
```

### Bước 3: Xác Định Dedup

```python
"dedup_cols": [
    # Khóa duy nhất của bảng
    # VD: ["id"] hoặc ["shop_id", "date"] hoặc ["user_id", "product_id", "timestamp"]
]
```

### Bước 4: Xác Định Partition (Nếu Cần)

```python
"partition_cols": ["year", "month", "date"]  # hoặc None
```

### Bước 5: Xác Định Transform (Nếu Cần)

```python
"requires_transform": True,
"transform_config": {
    "type": ["DERIVE_YMD"]  # hoặc ["DERIVE_YMD", "PRICE_REVENUE"]
    # ... cấu hình thêm nếu có ...
}
```

---

## 💡 Các Lưu Ý Quan Trọng

### ⚠️ Lưu Ý 1: Cột Timestamp
- **Bắt buộc** cho `INCREMENTAL LOAD`
- Dùng để filter dữ liệu mới
- Dùng để dedup (giữ bản ghi mới nhất)
- **Phải có giá trị** (không NULL)

```python
# ✅ Đúng
"timestamp_col": "created_at",

# ❌ Sai
"timestamp_col": None,  # Với INCREMENTAL
```

### ⚠️ Lưu Ý 2: Dedup Cols
- Phải là **khóa duy nhất** của bảng
- Không thể để trống cho FULL LOAD
- Có thể để trống nếu không cần dedup (hiếm gặp)

```python
# ✅ Đúng
"dedup_cols": ["listing_id"],  # 1 listing = 1 hàng

# ✅ Đúng  
"dedup_cols": ["shop_id", "report_date"],  # 1 shop/ngày = 1 hàng

# ❌ Sai
"dedup_cols": ["name"],  # Tên không phải khóa duy nhất!
```

### ⚠️ Lưu Ý 3: Cột Partition
- Giúp **tối ưu query**
- Không bắt buộc
- Thường là `["year", "month", "date"]`
- Phải có cột `year`, `month`, `date` trong dữ liệu (sau transform)

```python
# ✅ Đúng
"partition_cols": ["year", "month", "date"],
"requires_transform": True,
"transform_config": {"type": ["DERIVE_YMD"]}

# ❌ Sai
"partition_cols": ["year", "month", "date"],  # Nhưng không tạo cột này!
"requires_transform": False
```

### ⚠️ Lưu Ý 4: Transform PRICE_REVENUE
- Bảng join phải **tồn tại** trước
- Dùng **bản ghi mới nhất** của bảng join
- Nếu LEFT JOIN → NULL price = 0

```python
# ✅ Đúng
"transform_config": {
    "type": ["DERIVE_YMD", "PRICE_REVENUE"],
    "join_table": "dim_listing_information",  # Bảng này phải load trước
    "join_key": "shop_id",
    "price_col": "price",
    "sales_col": "daily_sales"
}
```

---

## ✅ Quy Trình Kiểm Tra Trước Khi Chạy

```
1. ✓ Bảng Bronze tồn tại?
   → SELECT * FROM lh_sidcorp_poc_bronze.dbo.<bronze_table> LIMIT 5
   
2. ✓ Cột trong "columns" có tồn tại không?
   → DESC lh_sidcorp_poc_bronze.dbo.<bronze_table>
   
3. ✓ Cột timestamp có giá trị không (không NULL)?
   → SELECT COUNT(*) FROM ... WHERE <timestamp_col> IS NULL
   
4. ✓ Cột dedup_cols có giá trị duy nhất không?
   → SELECT <dedup_cols>, COUNT(*) as cnt FROM ... GROUP BY <dedup_cols> HAVING cnt > 1
   
5. ✓ Nếu dùng PRICE_REVENUE, bảng join có tồn tại không?
   → SELECT COUNT(*) FROM lh_sidcorp_poc_gold.dbo.<join_table>
```

---

## 🐛 Troubleshooting

### ❌ Lỗi: "Table doesn't exist"
```
Error: Table lh_sidcorp_poc_bronze.dbo.<bronze_table> doesn't exist
```
**Giải pháp:**
- Kiểm tra tên bảng Bronze (case-sensitive)
- Kiểm tra schema (phải là `dbo`)
- Kiểm tra lakehouse name

### ❌ Lỗi: "Column not found"
```
Error: Column '<column_name>' doesn't exist
```
**Giải pháp:**
- Kiểm tra tên cột trong Bronze
- Kiểm tra spelling (case-sensitive)
- Dùng `DESC` để xem danh sách cột

### ❌ Lỗi: "Cannot dedup because timestamp is null"
**Giải pháp:**
- Filter dữ liệu Bronze để loại bỏ NULL timestamp
- Hoặc thêm cột timestamp mới

### ❌ Lỗi: "PRICE_REVENUE: Column 'price' not found in joined table"
**Giải pháp:**
- Kiểm tra bảng join có cột `price` không
- Kiểm tra spelling
- Kiểm tra cấu hình `join_table` và `price_col`

---

## 📊 Mẹo Tối Ưu Performance

### 1. Chọn Đúng Load Type
```python
# ✅ Nhanh (chỉ tải dữ liệu mới)
"load_type": "INCREMENTAL"

# ❌ Chậm (tải toàn bộ)
"load_type": "FULL"
```

### 2. Partition Đúng
```python
# ✅ Tối ưu (scan ít dữ liệu)
"partition_cols": ["year", "month", "date"]

# ❌ Không tối ưu (scan toàn bộ)
"partition_cols": None
```

### 3. Số Worker (Parallelization)
```python
# Tăng max_workers để chạy nhanh hơn
run_gold_pipeline(TABLE_CONFIGS, max_workers=8)  # Default 4
```

---

## 📝 Template Nhanh

### Template 1: FULL LOAD - Bảng Danh Mục
```python
{
    "gold_table": "dim_xxx",
    "bronze_table": "xxx",
    "load_type": "FULL",
    "table_category": "DIMENSION",
    "timestamp_col": None,
    "dedup_cols": ["id"],
    "columns": ["id", "name", "..."],
    "partition_cols": None,
    "isLoad": True,
    "requires_transform": False
}
```

### Template 2: INCREMENTAL LOAD - Bảng Sự Kiện
```python
{
    "gold_table": "fact_xxx",
    "bronze_table": "xxx",
    "load_type": "INCREMENTAL",
    "table_category": "FACT",
    "timestamp_col": "created_at",
    "partition_timestamp_col": "created_at",
    "dedup_cols": ["id"],
    "columns": ["id", "created_at", "..."],
    "partition_cols": ["year", "month", "date"],
    "isLoad": True,
    "requires_transform": True,
    "transform_config": {"type": ["DERIVE_YMD"]}
}
```

### Template 3: INCREMENTAL + PRICE_REVENUE
```python
{
    "gold_table": "fact_xxx",
    "bronze_table": "xxx",
    "load_type": "INCREMENTAL",
    "table_category": "FACT",
    "timestamp_col": "created_at",
    "partition_timestamp_col": "created_at",
    "dedup_cols": ["shop_id", "date"],
    "columns": ["shop_id", "sales", "created_at", "..."],
    "partition_cols": ["year", "month", "date"],
    "isLoad": True,
    "requires_transform": True,
    "transform_config": {
        "type": ["DERIVE_YMD", "PRICE_REVENUE"],
        "join_table": "dim_listing_information",
        "join_key": "shop_id",
        "price_col": "price",
        "sales_col": "sales"
    }
}
```

---

## 🎯 Tóm Tắt

| Tiêu Chí | FULL LOAD | INCREMENTAL LOAD |
|---------|-----------|-----------------|
| **Khi nào dùng** | Bảng nhỏ, lookup, snapshot | Bảng lớn, thêm liên tục |
| **Tốc độ** | Chậm (tải toàn bộ) | Nhanh (chỉ tải mới) |
| **Merge** | ❌ Không | ✅ Có |
| **Lần đầu** | Tạo bảng mới | Tạo bảng mới |
| **Lần 2+** | Xóa cũ → Tạo mới | Merge (UPDATE/INSERT) |
| **Timestamp** | Tùy chọn | Bắt buộc |
| **Dedup** | Thường có | Thường có |
| **Partition** | Tùy chọn | Nên có |

---

**📞 Hỗ trợ**: Xem mã nguồn `nb_bronze2gold.py` để hiểu chi tiết hơn về các hàm xử lý.
