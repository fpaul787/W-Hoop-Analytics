# Databricks notebook source
# DBTITLE 1,Bronze Ingestion - WNBA Data
# MAGIC %md
# MAGIC ## Bronze Ingestion — wehoop WNBA Data
# MAGIC
# MAGIC ### Overview
# MAGIC This notebook ingests raw WNBA data from an S3 bucket into the **bronze layer** of the `hooplakehouse.whoop` schema using a **batch read + overwrite** pattern. All sources are managed through a single config-driven loop to prevent configuration drift.
# MAGIC
# MAGIC ### How it works
# MAGIC * **Batch read** reads all parquet files from each S3 source path.
# MAGIC * **`mode("overwrite")`** fully replaces the bronze table each run, ensuring it always reflects the latest state of the source files (which are overwritten upstream daily).
# MAGIC * **`overwriteSchema=True`** handles schema evolution across files (e.g., new columns added over seasons).
# MAGIC * **Ingestion metadata** is added to every row: `_ingestion_timestamp` and `_load_date`.
# MAGIC * **No checkpoints needed** — since the upstream overwrites the same files, we do a full refresh each run.
# MAGIC
# MAGIC ### Why not Auto Loader?
# MAGIC Auto Loader is optimized for **append-only** file sources (new files arriving). Our upstream regenerates the same parquet files daily with updated data, so a batch overwrite is simpler and guarantees we always have the latest state.
# MAGIC
# MAGIC ### Source → Target Mapping
# MAGIC
# MAGIC | Source | S3 Path | Target Table |
# MAGIC | --- | --- | --- |
# MAGIC | Play-by-Play | `s3://wehoop/wehoop-wnba-data/pbp/parquet/` | `hooplakehouse.whoop.bronze_pbp` |
# MAGIC | Player Box | `s3://wehoop/wehoop-wnba-data/player_box/parquet/` | `hooplakehouse.whoop.bronze_player_box` |
# MAGIC | Player Season Stats | `s3://wehoop/wehoop-wnba-data/player_season_stats/parquet/` | `hooplakehouse.whoop.bronze_player_season_stats` |
# MAGIC | Schedules | `s3://wehoop/wehoop-wnba-data/schedules/parquet/` | `hooplakehouse.whoop.bronze_schedules` |
# MAGIC | Team Box | `s3://wehoop/wehoop-wnba-data/team_box/parquet/` | `hooplakehouse.whoop.bronze_team_box` |
# MAGIC
# MAGIC ### Adding a new source
# MAGIC Append a tuple to the `sources` list in the ingestion cell: `("subfolder_name", "bronze_table_name")`

# COMMAND ----------

# DBTITLE 1,Create schema if not exists
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hooplakehouse.whoop;

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql.functions import current_timestamp, current_date

bucket_name = "wehoop"
folder = "wehoop-wnba-data"

# COMMAND ----------

# DBTITLE 1,Auto Loader - Ingest all bronze sources
from datetime import datetime, timezone
from pyspark.sql.functions import max as spark_max, coalesce, col, get_json_object

# Source configuration: (subfolder, target_table, schema_hints)
# schema_hints: optional string to resolve type conflicts across parquet files
sources = [
    ("pbp", "bronze_pbp", "id DOUBLE"),
    ("player_box", "bronze_player_box", None),
    ("player_season_stats", "bronze_player_season_stats", None),
    ("schedules", "bronze_schedules", None),
    ("team_box", "bronze_team_box", None),
]

def get_s3_last_modified(s3_path: str) -> datetime:
    """Get the most recent modification time of files in an S3 path using dbutils."""
    files = [f for f in dbutils.fs.ls(s3_path) if f.size > 0]
    if not files:
        raise FileNotFoundError(f"No parquet files found in {s3_path} (folder is empty or contains only placeholders)")
    latest_ms = max(f.modificationTime for f in files)
    return datetime.fromtimestamp(latest_ms / 1000, tz=timezone.utc)

def get_last_ingestion(full_table: str) -> datetime | None:
    """Get the last ingestion timestamp from the target table, or None if it doesn't exist."""
    try:
        result = spark.table(full_table).select(spark_max("_ingestion_timestamp")).collect()[0][0]
        return result
    except Exception:
        return None

def ingest_to_bronze(source_subfolder: str, table_name: str, schema_hints: str | None = None):
    """Read parquet from S3 and overwrite the bronze table if source has changed."""
    s3_path = f"s3://{bucket_name}/{folder}/{source_subfolder}/parquet/"
    full_table = f"hooplakehouse.whoop.{table_name}"

    # Check if source files have been modified since last ingestion
    s3_modified = get_s3_last_modified(s3_path)
    last_ingestion = get_last_ingestion(full_table)

    if last_ingestion and s3_modified <= last_ingestion.replace(tzinfo=timezone.utc):
        print(f"Skipping {full_table} — source unchanged (last modified: {s3_modified})")
        return

    print(f"Ingesting: {s3_path} → {full_table}")

    # Use read_files() with schemaHints when type conflicts exist, otherwise standard read
    if schema_hints:
        df = spark.sql(f"""
            SELECT * FROM read_files(
                '{s3_path}',
                format => 'parquet',
                schemaHints => '{schema_hints}'
            )
        """)
        # Recover rescued values caused by type mismatches across files
        for hint in schema_hints.split(","):
            parts = hint.strip().split()
            col_name, col_type = parts[0], parts[1]
            df = df.withColumn(
                col_name,
                coalesce(
                    col(col_name),
                    get_json_object(col("_rescued_data"), f"$.{col_name}").cast(col_type)
                )
            )
        df = df.drop("_rescued_data")
    else:
        df = spark.read.option("mergeSchema", "true").parquet(s3_path)

    (
        df.withColumns({
            "_ingestion_timestamp": current_timestamp(),
            "_load_date": current_date()
        })
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )

    print(f"  ✓ {full_table} complete")

# Run all ingestions sequentially
for source_subfolder, table_name, schema_hints in sources:
    ingest_to_bronze(source_subfolder, table_name, schema_hints)

# COMMAND ----------


