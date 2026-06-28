# Databricks notebook source
# DBTITLE 1,Bronze Ingestion - WNBA Data
# MAGIC %md
# MAGIC ## Bronze Ingestion — wehoop WNBA Data
# MAGIC
# MAGIC ### Overview
# MAGIC This notebook ingests raw WNBA data from an S3 bucket into the **bronze layer** of the `hooplakehouse.whoop` schema using **Auto Loader** (Structured Streaming with `cloudFiles` format). All sources are managed through a single config-driven loop to prevent configuration drift.
# MAGIC
# MAGIC ### How it works
# MAGIC * **Auto Loader** incrementally discovers and processes new parquet files from S3 using **directory listing mode** — no cloud notification infrastructure required.
# MAGIC * **`trigger(availableNow=True)`** ensures the stream processes all new files and then stops, making it safe to run as a scheduled daily job.
# MAGIC * **`.awaitTermination()`** guarantees each stream completes before the next starts (sequential execution).
# MAGIC * **`mergeSchema=True`** handles schema evolution across files (e.g., new columns added over seasons).
# MAGIC * **Ingestion metadata** is added to every row: `_ingestion_timestamp` and `_load_date`.
# MAGIC * A **`_rescued_data`** column captures any values that don't conform to the inferred schema (e.g., type mismatches between older and newer files). This is intentionally kept at bronze and handled in the silver layer.
# MAGIC * **Checkpoints** track which files have already been processed, so re-running only picks up new data.
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
# Source configuration: (subfolder, target_table)
sources = [
    ("pbp", "bronze_pbp"),
    ("player_box", "bronze_player_box"),
    ("player_season_stats", "bronze_player_season_stats"),
    ("schedules", "bronze_schedules"),
    ("team_box", "bronze_team_box"),
]

def ingest_to_bronze(source_subfolder: str, table_name: str):
    """Run Auto Loader stream for a single source into the bronze layer."""
    s3_path = f"s3://{bucket_name}/{folder}/{source_subfolder}/parquet/"
    checkpoint_path = f"/tmp/checkpoints/wehoop/{table_name}"
    schema_path = f"{checkpoint_path}/_schema"
    full_table = f"hooplakehouse.whoop.{table_name}"

    print(f"Ingesting: {s3_path} → {full_table}")

    (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.schemaLocation", schema_path)
        .load(s3_path)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_load_date", current_date())
        .writeStream
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(full_table)
    ).awaitTermination()

    print(f"  ✓ {full_table} complete")

# Run all ingestions sequentially
for source_subfolder, table_name in sources:
    ingest_to_bronze(source_subfolder, table_name)
