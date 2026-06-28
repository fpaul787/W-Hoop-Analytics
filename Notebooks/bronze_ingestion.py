# Databricks notebook source
# DBTITLE 1,Bronze Ingestion - WNBA Data
# MAGIC %md
# MAGIC ## Bronze Ingestion — wehoop WNBA Data
# MAGIC
# MAGIC ### Overview
# MAGIC This notebook ingests raw WNBA data from an S3 bucket into the **bronze layer** of the `hooplakehouse.whoop` schema using **Auto Loader** (Structured Streaming with `cloudFiles` format).
# MAGIC
# MAGIC ### How it works
# MAGIC * **Auto Loader** incrementally discovers and processes new parquet files from S3 using **directory listing mode** — no cloud notification infrastructure required.
# MAGIC * **`trigger(availableNow=True)`** ensures the stream processes all new files and then stops, making it safe to run as a scheduled daily job.
# MAGIC * **`mergeSchema=True`** handles schema evolution across files (e.g., new columns added over seasons).
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

# DBTITLE 1,Auto Loader - bronze_pbp
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.schemaLocation", "/tmp/checkpoints/wehoop/bronze_pbp/_schema")
    .load(f"s3://{bucket_name}/{folder}/pbp/parquet/")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_load_date", current_date())
    .writeStream
    .option("checkpointLocation", "/tmp/checkpoints/wehoop/bronze_pbp")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("hooplakehouse.whoop.bronze_pbp")
)

# COMMAND ----------

# DBTITLE 1,Auto Loader - bronze_player_box
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.schemaLocation", "/tmp/checkpoints/wehoop/bronze_player_box/_schema")
    .load(f"s3://{bucket_name}/{folder}/player_box/parquet/")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_load_date", current_date())
    .writeStream
    .option("checkpointLocation", "/tmp/checkpoints/wehoop/bronze_player_box")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("hooplakehouse.whoop.bronze_player_box")
)

# COMMAND ----------

# DBTITLE 1,Auto Loader - bronze_player_season_stats
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.schemaLocation", "/tmp/checkpoints/wehoop/bronze_player_season_stats/_schema")
    .load(f"s3://{bucket_name}/{folder}/player_season_stats/parquet/")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_load_date", current_date())
    .writeStream
    .option("checkpointLocation", "/tmp/checkpoints/wehoop/bronze_player_season_stats")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("hooplakehouse.whoop.bronze_player_season_stats")
)

# COMMAND ----------

# DBTITLE 1,Auto Loader - bronze_schedules
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.schemaLocation", "/tmp/checkpoints/wehoop/bronze_schedules/_schema")
    .load(f"s3://{bucket_name}/{folder}/schedules/parquet/")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_load_date", current_date())
    .writeStream
    .option("checkpointLocation", "/tmp/checkpoints/wehoop/bronze_schedules")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("hooplakehouse.whoop.bronze_schedules")
)

# COMMAND ----------

# DBTITLE 1,Auto Loader - bronze_team_box
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.schemaLocation", "/tmp/checkpoints/wehoop/bronze_team_box/_schema")
    .load(f"s3://{bucket_name}/{folder}/team_box/parquet/")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_load_date", current_date())
    .writeStream
    .option("checkpointLocation", "/tmp/checkpoints/wehoop/bronze_team_box")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("hooplakehouse.whoop.bronze_team_box")
)
