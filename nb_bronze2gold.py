#!/usr/bin/env python
# coding: utf-8

# ## nb_bronze2gold
# 
# null

# ##### Import Libs and Spark Configs

# In[1]:


from pyspark.sql import functions as F
from pyspark.sql.functions import col, row_number, coalesce, lit
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "2000") 
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")


# ##### Config metadata

# In[2]:


# bronze_lakehouse = "lh_sidcorp_poc_bronze"
# gold_lakehouse = "lh_sidcorp_poc_gold"

# TABLE_CONFIGS = [
#     # ===============================
#     # dim_listing_information
#     # ===============================
#     {
#         "gold_table": "dim_listing_information",
#         "bronze_table": "analysis_listing_information",
#         "load_type": "INCREMENTAL", 
#         "table_category": "DIMENSION",
#         "timestamp_col": "creation_timestamp",
#         "dedup_cols": ["listing_id"],
#         "columns": [
#             "listing_id", "shop_id", "user_id", "title", "description", 
#             "state", "url", "price", "currency_code", "taxonomy_id", 
#             "year", "month", "date", "creation_timestamp"
#         ],
#         "partition_cols": ["year", "month", "date"],
#         "isLoad": True,
#         "requires_transform": False
#     },

#     # ===============================
#     # fact_listing_performance_by_date
#     # ===============================
#     {
#         "gold_table": "fact_listing_performance_by_date",
#         "bronze_table": "analysis_listing_performance_by_date",
#         "load_type": "FULL",  
#         "table_category": "FACT",
#         "timestamp_col": "created_at",
#         "dedup_cols": ["shop_id", "listing_id", "report_date"],
#         "columns": [
#             "site_id", "shop_id", "listing_id", "report_date", 
#             "daily_sales", "daily_views", "daily_favorers", 
#             "conversion_rate", "views", "favorers", "created_at", 
#             "year", "month", "date"
#         ],
#         "partition_cols": ["year", "month", "date"],
#         "isLoad": True,
#         "requires_transform": True,
#         "transform_config": {
#             "type": "PRICE_REVENUE",
#             "join_table": "dim_listing_information",
#             "join_key": "shop_id",
#             "price_col": "price",
#             "sales_col": "daily_sales"
#         }
#     },

#     # ===============================
#     # dim_listing_tags
#     # ===============================
#     {
#         "gold_table": "dim_listing_tags",
#         "bronze_table": "analysis_listing_tags",
#         "load_type": "INCREMENTAL",  
#         "table_category": "DIMENSION",
#         "timestamp_col": "created_at",
#         "dedup_cols": ["listing_id", "tag_name"],
#         "columns": [
#             "id", "listing_id", "tag_name", "created_at", 
#             "keyword_name", "version", "year", "month", "date"
#         ],
#         "partition_cols": ["year", "month", "date"],
#         "isLoad": True,
#         "requires_transform": False
#     },

#     # ===============================
#     # dim_shop_informations
#     # ===============================
#     {
#         "gold_table": "dim_shop_informations",
#         "bronze_table": "analysis_shop_informations",
#         "load_type": "FULL",  
#         "table_category": "DIMENSION",
#         "timestamp_col": "updated_at",
#         "dedup_cols": ["shop_id"],
#         "columns": [
#             "id", "shop_id", "name", "status", "url", "currency_code",
#             "created_at", "updated_at", "created_timestamp", "updated_timestamp",
#             "is_vacation", "country", "year", "month"
#         ],
#         "partition_cols": ["year", "month"],
#         "isLoad": True,
#         "requires_transform": False
#     },

#     # ===============================
#     # fact_shop_performance_by_date
#     # ===============================
#     {
#         "gold_table": "fact_shop_performance_by_date",
#         "bronze_table": "analysis_shop_performance_by_date",
#         "load_type": "FULL",  
#         "table_category": "FACT",
#         "timestamp_col": "report_date",
#         "dedup_cols": ["shop_id", "report_date"],
#         "columns": [
#             "shop_id", "report_date", "sales", "num_favorers", 
#             "listing_active_count", "digital_listing_count", 
#             "review_average", "review_count", "daily_sales",
#             "daily_favorers", "daily_active_listings", 
#             "daily_digital_listings", "daily_review_count", 
#             "daily_avg_review", "year", "month", "date"
#         ],
#         "partition_cols": ["year", "month", "date"],
#         "isLoad": True,
#         "requires_transform": True,
#         "transform_config": {
#             "type": "PRICE_REVENUE",
#             "join_table": "dim_listing_information",
#             "join_key": "shop_id",
#             "price_col": "price",
#             "sales_col": "daily_sales"
#         }
#     },

#     # ===============================
#     # dim_tags
#     # ===============================
#     {
#         "gold_table": "dim_tags",
#         "bronze_table": "analysis_tags",
#         "load_type": "INCREMENTAL",  
#         "table_category": "DIMENSION",
#         "timestamp_col": "updated_at",
#         "dedup_cols": ["keyword_name", "id"],
#         "columns": [
#             "id", "tag_name", "created_at", "updated_at",
#             "keyword_name", "normalized_tag", "year", "month", "date"
#         ],
#         "partition_cols": ["year", "month", "date"],
#         "isLoad": True,
#         "requires_transform": False
#     },

#     # ===============================
#     # dim_taxonomy
#     # ===============================
#     {
#         "gold_table": "dim_taxonomy",
#         "bronze_table": "taxonomy",
#         "load_type": "FULL",  # INCREMENTAL or FULL
#         "table_category": "DIMENSION",
#         "timestamp_col": None,
#         "dedup_cols": None,
#         "columns": [
#             "id", "taxonomy_id", "name", "parent_id", 
#             "created_at", "updated_at"
#         ],
#         "partition_cols": None,
#         "isLoad": True,
#         "requires_transform": False
#     }
# ]


# In[ ]:


bronze_lakehouse = "lh_sidcorp_poc_bronze"
gold_lakehouse   = "lh_sidcorp_poc_gold"

TABLE_CONFIGS = [

    # =========================================================
    # dim_listing_information
    # rule: creation_timestamp
    # =========================================================
    {
        "gold_table": "dim_listing_information",
        "bronze_table": "analysis_listing_information",
        "load_type": "INCREMENTAL",
        "table_category": "DIMENSION",
        "timestamp_col": "creation_timestamp",
        "partition_timestamp_col": "creation_timestamp",
        "dedup_cols": ["listing_id"],
        "columns": [
            "listing_id","shop_id","user_id","title","description",
            "state","url","price","currency_code","taxonomy_id",
            "creation_timestamp"
        ],
        "partition_cols": ["year","month","date"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD"]
        }
    },

    # =========================================================
    # fact_listing_performance_by_date
    # rule: created_at
    # =========================================================
    {
        "gold_table": "fact_listing_performance_by_date",
        "bronze_table": "analysis_listing_performance_by_date",
        "load_type": "FULL",
        "table_category": "FACT",
        "timestamp_col": "created_at",
        "partition_timestamp_col": "created_at",
        "dedup_cols": ["shop_id","listing_id","report_date"],
        "columns": [
            "site_id","shop_id","listing_id","report_date",
            "daily_sales","daily_views","daily_favorers",
            "conversion_rate","views","favorers","created_at"
        ],
        "partition_cols": ["year","month","date"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD","PRICE_REVENUE"],
            "join_table": "dim_listing_information",
            "join_key": "shop_id",
            "price_col": "price",
            "sales_col": "daily_sales"
        }
    },

    # =========================================================
    # dim_listing_tags
    # rule: created_at
    # =========================================================
    {
        "gold_table": "dim_listing_tags",
        "bronze_table": "analysis_listing_tags",
        "load_type": "INCREMENTAL",
        "table_category": "DIMENSION",
        "timestamp_col": "created_at",
        "partition_timestamp_col": "created_at",
        "dedup_cols": ["listing_id","tag_name"],
        "columns": [
            "id","listing_id","tag_name","created_at",
            "keyword_name","version"
        ],
        "partition_cols": ["year","month","date"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD"]
        }
    },

    # =========================================================
    # dim_shop_informations
    # rule: created_at (partition) / updated_at (dedup)
    # =========================================================
    {
        "gold_table": "dim_shop_informations",
        "bronze_table": "analysis_shop_informations",
        "load_type": "FULL",
        "table_category": "DIMENSION",
        "timestamp_col": "updated_at",
        "partition_timestamp_col": "created_at",
        "dedup_cols": ["shop_id"],
        "columns": [
            "id","shop_id","name","status","url","currency_code",
            "created_at","updated_at","created_timestamp",
            "updated_timestamp","is_vacation","country"
        ],
        "partition_cols": ["year","month","date"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD"]
        }
    },

    # =========================================================
    # fact_shop_performance_by_date
    # rule: report_date
    # =========================================================
    {
        "gold_table": "fact_shop_performance_by_date",
        "bronze_table": "analysis_shop_performance_by_date",
        "load_type": "FULL",
        "table_category": "FACT",
        "timestamp_col": "report_date",
        "partition_timestamp_col": "report_date",
        "dedup_cols": ["shop_id","report_date"],
        "columns": [
            "shop_id","report_date","sales","num_favorers",
            "listing_active_count","digital_listing_count",
            "review_average","review_count","daily_sales",
            "daily_favorers","daily_active_listings",
            "daily_digital_listings","daily_review_count",
            "daily_avg_review"
        ],
        "partition_cols": ["year","month","date"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD","PRICE_REVENUE"],
            "join_table": "dim_listing_information",
            "join_key": "shop_id",
            "price_col": "price",
            "sales_col": "daily_sales"
        }
    },

    # =========================================================
    # dim_tags
    # rule: created_at
    # =========================================================
    {
        "gold_table": "dim_tags",
        "bronze_table": "analysis_tags",
        "load_type": "INCREMENTAL",
        "table_category": "DIMENSION",
        "timestamp_col": "updated_at",
        "partition_timestamp_col": "created_at",
        "dedup_cols": ["id","keyword_name"],
        "columns": [
            "id","tag_name","created_at","updated_at",
            "keyword_name","normalized_tag"
        ],
        "partition_cols": ["year","month","date"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD"]
        }
    },

    # =========================================================
    # dim_taxonomy (NO partition)
    # =========================================================
    {
        "gold_table": "dim_taxonomy",
        "bronze_table": "taxonomy",
        "load_type": "FULL",
        "table_category": "DIMENSION",
        "timestamp_col": None,
        "dedup_cols": ["taxonomy_id"],
        "columns": [
            "id","taxonomy_id","name","parent_id",
            "created_at","updated_at"
        ],
        "partition_cols": None,
        "isLoad": True,
        "requires_transform": False
    }
]


# ##### Transform Functions
# - If more transform define more functions

# ##### Price_transform

# In[3]:


def transform_price_revenue(fact_df, cfg):

    transform_config = cfg["transform_config"]
    join_table = transform_config["join_table"]
    join_key = transform_config["join_key"]
    price_col = transform_config["price_col"]
    sales_col = transform_config["sales_col"]
    listing_df = spark.table(f"{gold_lakehouse}.dbo.{join_table}")
    w = Window.partitionBy(join_key).orderBy(col("creation_timestamp").desc())
    listing_dedup = (
        listing_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select(
            col(join_key),
            col(price_col).alias("listing_price")
        )
    )
    
    result_df = fact_df
    if "price" in fact_df.columns:
        result_df = result_df.drop("price")
    if "revenue" in fact_df.columns:
        result_df = result_df.drop("revenue")
    
    result_df = (
        result_df
        .join(listing_dedup, on=join_key, how="left")
        .withColumn(
            "price",
            coalesce(col("listing_price"), lit(0))
        )
        .withColumn(
            "revenue",
            col(sales_col) * col("price")
        )
        .drop("listing_price")
    )
    
    return result_df


# ##### Year month date transform

# In[ ]:


from pyspark.sql.functions import year, month, to_date, col

def transform_derive_year_month_date(df, cfg):
   
    ts_col = cfg.get("partition_timestamp_col") or cfg.get("timestamp_col")

    if not ts_col:
        return df

    if ts_col not in df.columns:
        raise ValueError(f"Partition timestamp column {ts_col} not found")

    print(f"  [TRANSFORM] DERIVE year/month/date from {ts_col}")

    df = (
        df
        .withColumn("year",  year(col(ts_col)))
        .withColumn("month", month(col(ts_col)))
        .withColumn("date",  to_date(col(ts_col)))
    )

    return df


# ##### Transform Router
# - Add to route difined function above

# In[4]:


TRANSFORM_FUNCTIONS = {
    "PRICE_REVENUE": transform_price_revenue,
    "DERIVE_YMD": transform_derive_year_month_date
}


# ##### Apply Transform

# In[5]:


def apply_transforms(df, cfg):

    if not cfg.get("requires_transform", False):
        return df
    
    transform_config = cfg.get("transform_config", {})
    transform_type = transform_config.get("type")
    
    if not transform_type:
        return df

    if isinstance(transform_type, str):
        transform_types = [transform_type]
    else:
        transform_types = transform_type
    
    result_df = df
    
    for t_type in transform_types:
        if t_type in TRANSFORM_FUNCTIONS:
            print(f"  [TRANSFORM] Applying {t_type}")
            transform_func = TRANSFORM_FUNCTIONS[t_type]
            result_df = transform_func(result_df, cfg)
        else:
            print(f"  Error : {t_type}")
    
    return result_df


# ##### Get Lastest Date for Increment

# In[ ]:


def get_last_processed_timestamp(gold_table, timestamp_col):
    full_table_name = f"{gold_lakehouse}.dbo.{gold_table}"
    
    try:
        result = spark.sql(f"""
            SELECT MAX({timestamp_col}) as max_ts 
            FROM {full_table_name}
        """).collect()
        
        if result and result[0]["max_ts"]:
            return result[0]["max_ts"]
        else:
            return None
    except Exception as e:
        print(f"Table {gold_table} doesn't exist")
        return None


# ##### Load Tables

# In[7]:


def load_incremental_table(cfg):

    bronze_table = cfg["bronze_table"]
    gold_table = cfg["gold_table"]
    columns = cfg["columns"]
    timestamp_col = cfg["timestamp_col"]
    dedup_cols = cfg["dedup_cols"]
    partition_cols = cfg.get("partition_cols")
    
    full_bronze_table = f"{bronze_lakehouse}.dbo.{bronze_table}"
    full_gold_table = f"{gold_lakehouse}.dbo.{gold_table}"
    
    print(f"[INCREMENTAL LOAD] {bronze_table} → {gold_table}")

    df = spark.table(full_bronze_table).select(*columns)
     
    if date_partition_col:
        last_date = get_last_processed_date(gold_table)
        print(f" Filter {last_date}")
        df = df.filter(col(date_partition_col) > last_date)
    elif timestamp_col:
     
        last_ts = get_last_processed_timestamp(gold_table, timestamp_col)
        if last_ts:
            print(f"  [FILTER] Loading records after {last_ts}")
            df = df.filter(col(timestamp_col) > last_ts)


    df = apply_transforms(df, cfg)
    if timestamp_col and dedup_cols:
        print(f"  [DEDUP] By {dedup_cols}, ordered by {timestamp_col} DESC")
        df = df.withColumn(timestamp_col, col(timestamp_col).cast("timestamp"))
        w = Window.partitionBy(*dedup_cols).orderBy(col(timestamp_col).desc())
        df = (
            df.withColumn("rn", row_number().over(w))
              .filter(col("rn") == 1)
              .drop("rn")
        )

    try:
        target = DeltaTable.forName(spark, full_gold_table)
        

        merge_cond = " AND ".join([f"t.{c} = s.{c}" for c in dedup_cols])
    
        update_cond = f"s.{timestamp_col} > t.{timestamp_col}"
        
        print(f"  [MERGE] Condition: {merge_cond}")
        print(f"  [UPDATE] When: {update_cond}")
        
        (
            target.alias("t")
            .merge(df.alias("s"), merge_cond)
            .whenMatchedUpdate(
                condition=update_cond,
                set={col_name: f"s.{col_name}" for col_name in df.columns}
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        print(f"[INCREMENTAL MERGE DONE] {gold_table}")
        log_merge_metrics(gold_table)
        
        return df.count()
        
    except Exception as e:

        print(f"Creating new table")
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.saveAsTable(full_gold_table)
        
        row_count = df.count()
        print(f"[INCREMENTAL CREATE DONE] {gold_table} - {row_count:,} rows")
        
        return row_count
def load_full_table(cfg):
    
    bronze_table = cfg["bronze_table"]
    gold_table = cfg["gold_table"]
    columns = cfg["columns"]
    timestamp_col = cfg.get("timestamp_col")
    dedup_cols = cfg.get("dedup_cols")
    partition_cols = cfg.get("partition_cols")
    
    full_bronze_table = f"{bronze_lakehouse}.dbo.{bronze_table}"
    full_gold_table = f"{gold_lakehouse}.dbo.{gold_table}"
    
    print(f"[FULL LOAD] {bronze_table} → {gold_table}")
    
    df = spark.table(full_bronze_table).select(*columns)

    df = apply_transforms(df, cfg)
    

    if dedup_cols and timestamp_col:
        print(f"  [DEDUP] By {dedup_cols}, ordered by {timestamp_col} DESC")
        df = df.withColumn(timestamp_col, col(timestamp_col).cast("timestamp"))
        w = Window.partitionBy(*dedup_cols).orderBy(col(timestamp_col).desc())
        df = (
            df.withColumn("rn", row_number().over(w))
              .filter(col("rn") == 1)
              .drop("rn")
        )
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.saveAsTable(full_gold_table)
    
    row_count = df.count()
    print(f"[FULL LOAD DONE] {gold_table} - {row_count:,} rows")
    
    return row_count


# ##### Log

# In[8]:


def log_merge_metrics(gold_table):
    """
    Log merge metrics for gold table
    """
    full_table_name = f"{gold_lakehouse}.dbo.{gold_table}"
    
    try:
        hist = (
            spark.sql(f"DESCRIBE HISTORY {full_table_name}")
            .select("timestamp", "operation", "operationMetrics")
            .orderBy(col("timestamp").desc())
            .limit(1)
            .collect()
        )
        
        if not hist:
            print(f"  [LOG] {gold_table}: No history found")
            return
        
        row = hist[0]
        metrics = row["operationMetrics"] or {}
        
        inserted = int(metrics.get("numTargetRowsInserted", 0))
        updated = int(metrics.get("numTargetRowsUpdated", 0))
        deleted = int(metrics.get("numTargetRowsDeleted", 0))
        
        print(
            f"  [MERGE METRICS] {gold_table} | "
            f"Inserted: {inserted:,} | "
            f"Updated: {updated:,} | "
            f"Deleted: {deleted:,}"
        )
    
    except Exception as e:
        print(f"  [LOG ERROR] {gold_table}: {str(e)}")


# ##### Process tables

# In[9]:


def process_gold_table(cfg):

    gold_table = cfg["gold_table"]
    load_type = cfg["load_type"]
    
    try:
        if load_type == "INCREMENTAL":
            row_count = load_incremental_table(cfg)
        elif load_type == "FULL":
            row_count = load_full_table(cfg)
        else:
            raise ValueError(f"Unknown load_type: {load_type}. Must be INCREMENTAL or FULL")
        
        return gold_table, "SUCCESS", row_count
    
    except Exception as e:
        import traceback
        error_msg = f"FAILED: {str(e)}\n{traceback.format_exc()}"
        print(f"[ERROR] {gold_table}: {error_msg}")
        return gold_table, f"FAILED: {str(e)}", 0


# #### Run 

# In[11]:


def run_gold_pipeline(configs, max_workers=4):

    configs_to_load = [cfg for cfg in configs if cfg.get("isLoad", True)]
    
    print("=" * 80)
    print("BRONZE TO GOLD PIPELINE - STARTING")
    print("=" * 80)
    print(f"Total tables configured: {len(configs)}")
    print(f"Tables to load: {len(configs_to_load)}")
    print(f"Tables skipped: {len(configs) - len(configs_to_load)}")
    print(f"Max workers: {max_workers}")
    print("=" * 80)
 
    print(f"\n[TABLES TO LOAD] ({len(configs_to_load)})")
    for cfg in configs_to_load:
        load_note = f"[{cfg['load_type']}]"
        category_note = f"[{cfg['table_category']}]"
        transform_note = " + TRANSFORM" if cfg.get("requires_transform") else ""
        dedup_note = f" (dedup by {cfg['dedup_cols']})" if cfg.get('dedup_cols') else ""
        print(f"  {load_note:15s} {category_note:12s} {cfg['bronze_table']} → {cfg['gold_table']}{dedup_note}{transform_note}")
    
    skipped = [cfg for cfg in configs if not cfg.get("isLoad", True)]
    if skipped:
        print(f"\n[TABLES SKIPPED] ({len(skipped)})")
        for cfg in skipped:
            print(f"  {cfg['gold_table']} - isLoad=False")
    
    print("=" * 80)
    print()
    
    if not configs_to_load:
        print("⚠ All tables have isLoad=False")
        return
    
    results = {}

    print("\n" + "=" * 80)
    print("LOADING ALL TABLES (PARALLEL)")
    print("=" * 80)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_gold_table, cfg): cfg["gold_table"]
            for cfg in configs_to_load
        }
        
        for f in as_completed(futures):
            gold_table, status, row_count = f.result()
     
            cfg = next(c for c in configs_to_load if c["gold_table"] == gold_table)
            results[gold_table] = (status, row_count, cfg["load_type"], cfg["table_category"])
            print(f"[COMPLETED] {gold_table}: {status}\n")

    print("\n" + "=" * 80)
    print("BRONZE2GOLD PIPELINE SUMMARY")
    print("=" * 80)
    
    success_count = sum(1 for status, _, _, _ in results.values() if status == "SUCCESS")
    failed_count = len(results) - success_count
    
    print(f"\nTotal tables processed: {len(results)}")
  
    
    if success_count > 0:
        print("\n[SUCCESSFUL TABLES]")
        for table, (status, row_count, load_type, category) in results.items():
            if status == "SUCCESS":
                print(f"  ✓ {table:40s} | {category:10s} | {load_type:12s} | {row_count:,} rows")
    
    if failed_count > 0:
        print("\n[FAILED TABLES]")
        for table, (status, row_count, load_type, category) in results.items():
            if status != "SUCCESS":
                print(f" {table:40s} | {category:10s} | {load_type:12s} | {status}")
    
    print("\n" + "=" * 80)
    print("BRONZE2GOLD PIPELINE COMPLETED")
    print("=" * 80)
run_gold_pipeline(TABLE_CONFIGS, max_workers=4)

