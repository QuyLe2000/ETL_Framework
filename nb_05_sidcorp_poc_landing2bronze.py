#!/usr/bin/env python
# coding: utf-8

# ## nb_05_sidcorp_poc_landing2bronze
# 
# null

# ##### Import Libs and Spark Configs

# In[1]:


from notebookutils import mssparkutils
from pyspark.sql import functions as F
from pyspark.sql.functions import col, row_number, year, month, to_date
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed

spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "2000")
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.merge.materializeSource", "none")


# ##### Config Metadata

# In[2]:


bronze_lakehouse = "lh_sidcorp_poc_bronze"

TABLE_CONFIGS = [

    # ===============================
    # analysis_listing_information
    # ===============================
    {
        "bronze_table": "analysis_listing_information",
        "load_type": "INCREMENTAL",
        "table_category": "DIMENSION",
        "timestamp_col": "original_creation_timestamp",
        "partition_timestamp_col": "original_creation_timestamp",
        "dedup_cols": ["listing_id"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD", "CAST_COLUMNS", "ALIAS_COLUMNS"],
            "cast_map": {
                "taxonomy_id": "long"
            },
            "alias_columns": {
                "original_creation_timestamp": "creation_timestamp"
            }
        },
        "schema": StructType([
            StructField("listing_id", LongType()),
            StructField("shop_id", LongType()),
            StructField("user_id", LongType()),
            StructField("title", StringType()),
            StructField("description", StringType()),
            StructField("state", StringType()),
            StructField("url", StringType()),
            StructField("price", DoubleType()),
            StructField("currency_code", StringType()),
            StructField("quantity", LongType()),
            StructField("views", LongType()),
            StructField("num_favorers", LongType()),
            StructField("creation_timestamp", TimestampType()),
            StructField("updated_timestamp", TimestampType()),
            StructField("last_modified_timestamp", TimestampType()),
            StructField("original_creation_timestamp", TimestampType()),
            StructField("listing_type", StringType()),
            StructField("taxonomy_id", LongType()),
            StructField("is_customizable", ShortType()),
            StructField("is_personalizable", ShortType()),
            StructField("who_made", StringType()),
            StructField("when_made", StringType()),
            StructField("updated_at", TimestampType()),
            StructField("version_hash", DecimalType(20, 0)),
            StructField("year", IntegerType()),
            StructField("month", IntegerType()),
            StructField("date", DateType())
        ])
    },

    # ===============================
    # analysis_listing_performance_by_date
    # ===============================
    {
        "bronze_table": "analysis_listing_performance_by_date",
        "load_type": "FULL",
        "table_category": "FACT",
        "timestamp_col": "created_at",
        "partition_timestamp_col": "created_at",
        "dedup_cols": ["shop_id", "listing_id", "report_date"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD"]
        },
        "schema": StructType([
            StructField("site_id", IntegerType()),
            StructField("shop_id", LongType()),
            StructField("listing_id", LongType()),
            StructField("report_date", DateType()),
            StructField("daily_sales", DoubleType()),
            StructField("daily_views", LongType()),
            StructField("daily_favorers", LongType()),
            StructField("conversion_rate", DoubleType()),
            StructField("views", LongType()),
            StructField("favorers", LongType()),
            StructField("created_at", TimestampType()),
            StructField("year", IntegerType()),
            StructField("month", IntegerType()),
            StructField("date", DateType())
        ])
    },

    # ===============================
    # analysis_listing_tags
    # ===============================
    {
        "bronze_table": "analysis_listing_tags",
        "load_type": "INCREMENTAL",
        "table_category": "DIMENSION",
        "timestamp_col": "created_at",
        "partition_timestamp_col": "created_at",
        "dedup_cols": ["listing_id", "tag_name"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD"]
        },
        "schema": StructType([
            StructField("id", StringType()),
            StructField("listing_id", DecimalType(20, 0)),
            StructField("tag_name", StringType()),
            StructField("created_at", TimestampType()),
            StructField("data_hash", StringType()),
            StructField("keyword_name", StringType()),
            StructField("version", DecimalType(20, 0)),
            StructField("year", IntegerType()),
            StructField("month", IntegerType()),
            StructField("date", DateType())
        ])
    },

    # ===============================
    # analysis_shop_informations
    # ===============================
    {
        "bronze_table": "analysis_shop_informations",
        "load_type": "FULL",
        "table_category": "DIMENSION",
        "timestamp_col": "updated_at",
        "partition_timestamp_col": "created_at",
        "dedup_cols": ["shop_id"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD"]
        },
        "schema": StructType([
            StructField("id", StringType()),
            StructField("shop_id", LongType()),
            StructField("name", StringType()),
            StructField("status", ByteType()),
            StructField("url", StringType()),
            StructField("currency_code", StringType()),
            StructField("created_at", TimestampType()),
            StructField("updated_at", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("updated_timestamp", TimestampType()),
            StructField("is_vacation", BooleanType()),
            StructField("listing_active_count", LongType()),
            StructField("digital_listing_count", LongType()),
            StructField("num_favorers", LongType()),
            StructField("sales", LongType()),
            StructField("review_average", FloatType()),
            StructField("review_count", IntegerType()),
            StructField("icon_url", StringType()),
            StructField("country", StringType()),
            StructField("year", IntegerType()),
            StructField("month", IntegerType()),
            StructField("date", DateType())
        ])
    },

    # ===============================
    # analysis_shop_performance_by_date
    # ===============================
    {
        "bronze_table": "analysis_shop_performance_by_date",
        "load_type": "FULL",
        "table_category": "FACT",
        "timestamp_col": "report_date",
        "partition_timestamp_col": "report_date",
        "dedup_cols": ["shop_id", "report_date"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["DERIVE_YMD"]
        },
        "schema": StructType([
            StructField("shop_id", LongType()),
            StructField("report_date", DateType()),
            StructField("sales", LongType()),
            StructField("num_favorers", LongType()),
            StructField("listing_active_count", IntegerType()),
            StructField("digital_listing_count", IntegerType()),
            StructField("review_average", FloatType()),
            StructField("review_count", FloatType()),
            StructField("daily_sales", LongType()),
            StructField("daily_favorers", LongType()),
            StructField("daily_active_listings", LongType()),
            StructField("daily_digital_listings", LongType()),
            StructField("daily_review_count", FloatType()),
            StructField("daily_avg_review", FloatType()),
            StructField("year", IntegerType()),
            StructField("month", IntegerType()),
            StructField("date", DateType())
        ])
    },

    # ===============================
    # taxonomy
    # ===============================
    {
        "bronze_table": "taxonomy",
        "load_type": "FULL",
        "table_category": "DIMENSION",
        "timestamp_col": None,
        "partition_timestamp_col": None,
        "dedup_cols": ["taxonomy_id"],
        "isLoad": True,
        "requires_transform": True,
        "transform_config": {
            "type": ["CAST_COLUMNS"],
            "cast_map": {
                "taxonomy_id": "long"
            }
        },
        "schema": StructType([
            StructField("id", StringType()),
            StructField("taxonomy_id", StringType()),
            StructField("name", StringType()),
            StructField("parent_id", StringType()),
            StructField("created_at", TimestampType()),
            StructField("updated_at", TimestampType())
        ])
    }
]


# ##### Transform Functions

# ##### Cast Columns Transform

# In[3]:


def transform_cast_columns(df, cfg):
    """
    Cast columns to specific types
    Example: {"taxonomy_id": "long"}
    """
    transform_config = cfg.get("transform_config", {})
    cast_map = transform_config.get("cast_map")
    
    if not cast_map:
        return df
    
    print(f"  [TRANSFORM] CAST_COLUMNS {cast_map}")
    
    for col_name, col_type in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
        else:
            print(f"  [WARNING] Column {col_name} not found for casting")
    
    return df


# ##### Derive Year Month Date Transform

# In[4]:


def transform_derive_year_month_date(df, cfg):
    """
    Derive year, month, date from partition timestamp column
    """
    ts_col = cfg.get("partition_timestamp_col") or cfg.get("timestamp_col")
    
    if not ts_col:
        return df
    
    if ts_col not in df.columns:
        raise ValueError(f"Partition timestamp column {ts_col} not found")
    
    print(f"  [TRANSFORM] DERIVE year/month/date from {ts_col}")
    
    df = (
        df
        .withColumn("year", year(col(ts_col)))
        .withColumn("month", month(col(ts_col)))
        .withColumn("date", to_date(col(ts_col)))
    )
    
    return df


# ##### Alias Columns Transform

# In[5]:


def transform_alias_columns(df, cfg):
    """
    Rename columns based on alias mapping
    Example: {"original_creation_timestamp": "creation_timestamp"}
    """
    transform_config = cfg.get("transform_config", {})
    alias_columns = transform_config.get("alias_columns")
    
    if not alias_columns:
        return df
    
    print(f"  [TRANSFORM] ALIAS_COLUMNS {alias_columns}")
    
    for src, tgt in alias_columns.items():
        if src not in df.columns:
            raise ValueError(f"Alias source column not found: {src}")
        df = df.withColumn(tgt, col(src))
    
    return df


# ##### Transform Router

# In[6]:


TRANSFORM_FUNCTIONS = {
    "CAST_COLUMNS": transform_cast_columns,
    "DERIVE_YMD": transform_derive_year_month_date,
    "ALIAS_COLUMNS": transform_alias_columns
}


# ##### Apply Transforms

# In[7]:


def apply_transforms(df, cfg):
    """
    Apply transformations to dataframe based on config
    """
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
            print(f"  [ERROR] Unknown transform type: {t_type}")
    
    return result_df


# ##### File Helper Functions

# In[8]:


def ls_safe(path):
    """
    Safely list files/folders in a path
    """
    try:
        items = mssparkutils.fs.ls(path)
        return items
    except Exception as e:
        print(f"  [DEBUG] ls_safe failed for {path}: {str(e)}")
        return []


def list_parquet_files(folder_path):
    """
    List all parquet files in a folder
    """
    files = []
    try:
        for f in mssparkutils.fs.ls(folder_path):
            if f.path.lower().endswith(".parquet"):
                files.append(f.path)
    except:
        pass
    return files


def get_latest_hour_folders(table_name, n=1):
    """
    Get the latest n hour folders for a table
    Searches in Files/increment/{table_name}/year/month/day/hour
    """
    base = f"Files/increment/{table_name}"
    results = []
    
    print(f"  [DEBUG] Searching for {table_name} in {base}")
    
    years = ls_safe(base)
    if not years:
        print(f"  [DEBUG] No years found in {base}")
        return results
    
    years_sorted = sorted(years, key=lambda x: x.name, reverse=True)
    
    for y in years_sorted:
        months = ls_safe(y.path)
        if not months:
            continue
        
        months_sorted = sorted(months, key=lambda x: x.name, reverse=True)
        
        for m in months_sorted:
            days = ls_safe(m.path)
            if not days:
                continue
            
            days_sorted = sorted(days, key=lambda x: x.name, reverse=True)
            
            for d in days_sorted:
                hours = ls_safe(d.path)
                if not hours:
                    continue
                
                hours_sorted = sorted(hours, key=lambda x: x.name, reverse=True)
                
                for h in hours_sorted:
                    results.append(h.path)
                    print(f"  [DEBUG] Found folder: {h.path}")
                    
                    if len(results) >= n:
                        return results
    
    print(f"  [DEBUG] Total folders found: {len(results)}")
    return results


# ##### Read Parquet Functions

# In[9]:


def read_incremental_parquet(table_name, schema, n_folders=1):
    """
    Read parquet files from the latest n hour folders for incremental load
    """
    hour_folders = get_latest_hour_folders(table_name, n_folders)
    
    if not hour_folders:
        raise ValueError(f"No incremental folders found for {table_name}")
    
    parquet_files = []
    for folder in hour_folders:
        parquet_files.extend(list_parquet_files(folder))
    
    if not parquet_files:
        raise ValueError(f"No parquet files found in folders for {table_name}")
    
    print(f"  [READ INCREMENTAL] {table_name}")
    for p in parquet_files:
        print(f"    {p}")
    
    return spark.read.schema(schema).parquet(*parquet_files)


def read_full_latest_parquet(table_name, schema):
    """
    Read the latest parquet file for full load
    """
    hour_folders = get_latest_hour_folders(table_name, 1)
    
    if not hour_folders:
        raise ValueError(f"No full folders found for {table_name}")
    
    parquet_files = list_parquet_files(hour_folders[0])
    
    if not parquet_files:
        raise ValueError(f"No parquet files found in folder for {table_name}")
    
    latest_file = sorted(parquet_files, reverse=True)[0]
    
    print(f"  [READ FULL] {table_name}")
    print(f"    Latest file: {latest_file}")
    
    return spark.read.schema(schema).parquet(latest_file)


# ##### Log Merge Metrics

# In[10]:


def log_merge_metrics(bronze_table):
    """
    Log merge metrics for bronze table
    """
    full_table_name = f"{bronze_lakehouse}.dbo.{bronze_table}"
    
    try:
        hist = (
            spark.sql(f"DESCRIBE HISTORY {full_table_name}")
            .select("timestamp", "operation", "operationMetrics")
            .orderBy(col("timestamp").desc())
            .limit(1)
            .collect()
        )
        
        if not hist:
            print(f"  [LOG] {bronze_table}: No history found")
            return
        
        row = hist[0]
        metrics = row["operationMetrics"] or {}
        
        inserted = int(metrics.get("numTargetRowsInserted", 0))
        updated = int(metrics.get("numTargetRowsUpdated", 0))
        deleted = int(metrics.get("numTargetRowsDeleted", 0))
        
        print(
            f"  [MERGE METRICS] {bronze_table} | "
            f"Inserted: {inserted:,} | "
            f"Updated: {updated:,} | "
            f"Deleted: {deleted:,}"
        )
    
    except Exception as e:
        print(f"  [LOG ERROR] {bronze_table}: {str(e)}")


# ##### Load Tables

# In[11]:


def load_incremental_table(cfg, n_folders=1):
    """
    Load incremental table using merge operation
    """
    bronze_table = cfg["bronze_table"]
    schema = cfg["schema"]
    timestamp_col = cfg["timestamp_col"]
    dedup_cols = cfg["dedup_cols"]
    
    full_bronze_table = f"{bronze_lakehouse}.dbo.{bronze_table}"
    
    print(f"[INCREMENTAL LOAD] {bronze_table}")
    
    # Read parquet files
    df = read_incremental_parquet(bronze_table, schema, n_folders)
    
    # Select schema columns
    df = df.select([f.name for f in schema.fields])
    
    # Apply transforms
    df = apply_transforms(df, cfg)
    
    # Deduplication
    if timestamp_col and dedup_cols:
        print(f"  [DEDUP] By {dedup_cols}, ordered by {timestamp_col} DESC")
        df = df.withColumn(timestamp_col, col(timestamp_col).cast("timestamp"))
        w = Window.partitionBy(*dedup_cols).orderBy(col(timestamp_col).desc())
        df = (
            df.withColumn("rn", row_number().over(w))
              .filter(col("rn") == 1)
              .drop("rn")
        )
    
    # Merge into target table
    try:
        target = DeltaTable.forName(spark, full_bronze_table)
        
        merge_cond = " AND ".join([f"t.{c} = s.{c}" for c in dedup_cols])
        update_cond = f"s.{timestamp_col} >= t.{timestamp_col}"
        
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
        
        print(f"[INCREMENTAL MERGE DONE] {bronze_table}")
        log_merge_metrics(bronze_table)
        
        return df.count()
        
    except Exception as e:
        print(f"  [ERROR] MERGE FAILED: {str(e)}")
        raise e


def load_full_table(cfg):
    """
    Load full table using overwrite operation
    """
    bronze_table = cfg["bronze_table"]
    schema = cfg["schema"]
    timestamp_col = cfg.get("timestamp_col")
    dedup_cols = cfg.get("dedup_cols")
    partition_timestamp_col = cfg.get("partition_timestamp_col")
    
    full_bronze_table = f"{bronze_lakehouse}.dbo.{bronze_table}"
    
    print(f"[FULL LOAD] {bronze_table}")
    
    # Read parquet files
    df = read_full_latest_parquet(bronze_table, schema)
    
    # Select schema columns
    df = df.select([f.name for f in schema.fields])
    
    # Apply transforms
    df = apply_transforms(df, cfg)
    
    # Deduplication
    if dedup_cols and timestamp_col:
        print(f"  [DEDUP] By {dedup_cols}, ordered by {timestamp_col} DESC")
        df = df.withColumn(timestamp_col, col(timestamp_col).cast("timestamp"))
        w = Window.partitionBy(*dedup_cols).orderBy(col(timestamp_col).desc())
        df = (
            df.withColumn("rn", row_number().over(w))
              .filter(col("rn") == 1)
              .drop("rn")
        )
    
    # Write to table
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    
    # Add partitioning if partition column exists
    if partition_timestamp_col and partition_timestamp_col in df.columns:
        writer = writer.partitionBy("year", "month", "date")
    
    writer.saveAsTable(full_bronze_table)
    
    row_count = df.count()
    print(f"[FULL LOAD DONE] {bronze_table} - {row_count:,} rows")
    
    return row_count


# ##### Process Tables

# In[12]:


def process_bronze_table(cfg):
    """
    Process a single bronze table
    """
    bronze_table = cfg["bronze_table"]
    load_type = cfg["load_type"]
    
    try:
        if load_type == "INCREMENTAL":
            row_count = load_incremental_table(cfg, n_folders=2)
        elif load_type == "FULL":
            row_count = load_full_table(cfg)
        else:
            raise ValueError(f"Unknown load_type: {load_type}. Must be INCREMENTAL or FULL")
        
        return bronze_table, "SUCCESS", row_count
    
    except Exception as e:
        import traceback
        error_msg = f"FAILED: {str(e)}\n{traceback.format_exc()}"
        print(f"[ERROR] {bronze_table}: {error_msg}")
        return bronze_table, f"FAILED: {str(e)}", 0


# ##### Run Pipeline

# In[13]:


def run_bronze_pipeline(configs, max_workers=6):
    """
    Run the landing to bronze pipeline
    """
    configs_to_load = [cfg for cfg in configs if cfg.get("isLoad", True)]
    
    print("=" * 80)
    print("LANDING TO BRONZE PIPELINE - STARTING")
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
        print(f"  {load_note:15s} {category_note:12s} {cfg['bronze_table']}{dedup_note}{transform_note}")
    
    skipped = [cfg for cfg in configs if not cfg.get("isLoad", True)]
    if skipped:
        print(f"\n[TABLES SKIPPED] ({len(skipped)})")
        for cfg in skipped:
            print(f"  {cfg['bronze_table']} - isLoad=False")
    
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
            executor.submit(process_bronze_table, cfg): cfg["bronze_table"]
            for cfg in configs_to_load
        }
        
        for f in as_completed(futures):
            bronze_table, status, row_count = f.result()
            
            cfg = next(c for c in configs_to_load if c["bronze_table"] == bronze_table)
            results[bronze_table] = (status, row_count, cfg["load_type"], cfg["table_category"])
            print(f"[COMPLETED] {bronze_table}: {status}\n")
    
    print("\n" + "=" * 80)
    print("LANDING2BRONZE PIPELINE SUMMARY")
    print("=" * 80)
    
    success_count = sum(1 for status, _, _, _ in results.values() if status == "SUCCESS")
    failed_count = len(results) - success_count
    
    print(f"\nTotal tables processed: {len(results)}")
    print(f"Success: {success_count} | Failed: {failed_count}")
    
    if success_count > 0:
        print("\n[SUCCESSFUL TABLES]")
        for table, (status, row_count, load_type, category) in results.items():
            if status == "SUCCESS":
                print(f"  ✓ {table:40s} | {category:10s} | {load_type:12s} | {row_count:,} rows")
    
    if failed_count > 0:
        print("\n[FAILED TABLES]")
        for table, (status, row_count, load_type, category) in results.items():
            if status != "SUCCESS":
                print(f"  ✗ {table:40s} | {category:10s} | {load_type:12s} | {status}")
    
    print("\n" + "=" * 80)
    print("LANDING2BRONZE PIPELINE COMPLETED")
    print("=" * 80)


# ##### Run

# In[14]:


run_bronze_pipeline(TABLE_CONFIGS, max_workers=6)


# In[15]: