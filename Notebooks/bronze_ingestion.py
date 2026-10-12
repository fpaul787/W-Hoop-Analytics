# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Bronze Ingestion - WNBA Data
# MAGIC %md
# MAGIC ## Bronze Ingestion — wehoop WNBA Data
# MAGIC
# MAGIC ### Overview
# MAGIC This notebook ingests raw WNBA data from an S3 bucket into the **bronze layer** of the `hooplakehouse.whoop` schema using a **batch read + overwrite** pattern. All sources are managed through a single config-driven loop to prevent configuration drift.
# MAGIC
# MAGIC ### How it works
# MAGIC * **Batch read** reads all parquet files from each S3 source path.
# MAGIC * **Hash-based MERGE** replaces `mode("overwrite")`: all business columns are hashed (SHA-256) into `_row_hash`. On each run, unchanged rows keep their original `_ingestion_timestamp` and `_load_date`; only new or changed rows receive the current timestamp.
# MAGIC * **`overwriteSchema=True`** / `mergeSchema=True` handles schema evolution. A one-time seed overwrites the table to add `_row_hash` if the column is missing (migration from old overwrite pattern).
# MAGIC * **Ingestion metadata** added to every row: `_ingestion_timestamp`, `_load_date`, and `_row_hash` (SHA-256 of `to_json(struct(...))` over the business columns, sorted by name, so null positions and separators can't cause collisions).
# MAGIC * **Hash versioning**: the table property `whoop.row_hash_version` records the hash algorithm. If it differs from `HASH_VERSION`, `_row_hash` is recomputed in place (timestamps kept) before the MERGE.
# MAGIC * **No checkpoints needed** — since the upstream overwrites the same files, the S3 modification check + MERGE guarantees we always have the latest state.
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
# MAGIC ### Validation test
# MAGIC The timestamp-preservation test (last cell) forces a re-ingest of `bronze_schedules`, so it only runs when the `run_tests` parameter is `true` (default `false`). Set it from the widget, or pass `run_tests=true` as a job parameter for a dedicated test run.
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

# Test parameter: validation cells only run when run_tests = "true".
# Defaults to "false" so scheduled production runs never force extra writes.
dbutils.widgets.dropdown("run_tests", "false", ["false", "true"], "Run validation tests")
RUN_TESTS = dbutils.widgets.get("run_tests").lower() == "true"

# COMMAND ----------

# Source configuration: (subfolder, target_table, schema_hints)
# schema_hints: optional string to resolve type conflicts across parquet files
sources = [
    ("pbp", "bronze_pbp", "id DOUBLE"),
    ("player_box", "bronze_player_box", None),
    ("player_season_stats", "bronze_player_season_stats", None),
    ("schedules", "bronze_schedules", None),
    ("team_box", "bronze_team_box", None)
]

# COMMAND ----------

# DBTITLE 1,Ingest all bronze sources (hash MERGE)
from datetime import datetime, timezone
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Row-hash versioning: bump HASH_VERSION whenever row_hash_expr changes so existing
# tables get their _row_hash recomputed in place (timestamps preserved) before the next MERGE.
HASH_VERSION = "2"
HASH_VERSION_PROP = "whoop.row_hash_version"
METADATA_COLS = {"_row_hash", "_ingestion_timestamp", "_load_date"}

def row_hash_expr(business_cols: list[str]):
    """SHA-256 over a JSON serialization of the business columns.

    to_json(struct(...)) keeps the column name next to every value, so nulls can't shift
    between columns and separators don't need escaping (fixes concat_ws collisions).
    Columns are sorted so the hash doesn't depend on column order; timestamps are
    serialized in UTC so the hash doesn't depend on the session time zone.
    """
    cols = [F.col(f"`{c}`") for c in sorted(business_cols)]
    return F.sha2(F.to_json(F.struct(*cols), {"timeZone": "UTC"}), 256)

def get_hash_version(full_table: str) -> str | None:
    """Read the row-hash version recorded in the table properties (None if never set)."""
    props = spark.sql(f"DESCRIBE DETAIL {full_table}").select("properties").collect()[0][0] or {}
    return props.get(HASH_VERSION_PROP)

def set_hash_version(full_table: str):
    spark.sql(f"ALTER TABLE {full_table} SET TBLPROPERTIES ('{HASH_VERSION_PROP}' = '{HASH_VERSION}')")

def migrate_row_hash(full_table: str):
    """One-time recompute of _row_hash on an existing table, keeping _ingestion_timestamp / _load_date."""
    tbl = spark.table(full_table)
    business_cols = [c for c in tbl.columns if c not in METADATA_COLS]
    (tbl.withColumn("_row_hash", row_hash_expr(business_cols))
        .write
        .mode("overwrite")
        .saveAsTable(full_table))
    set_hash_version(full_table)
    print(f"  ✓ {full_table} _row_hash recomputed (hash v{HASH_VERSION}), timestamps preserved")

def get_s3_last_modified(s3_path: str) -> datetime:
    """Get the most recent modification time of files in an S3 path using dbutils."""
    files = [f for f in dbutils.fs.ls(s3_path) if f.size > 0]
    if not files:
        raise FileNotFoundError(f"No parquet files found in {s3_path} (folder is empty or contains only placeholders)")
    latest_ms = max(f.modificationTime for f in files)
    return datetime.fromtimestamp(latest_ms / 1000, tz=timezone.utc)

def get_last_ingestion(full_table: str) -> datetime | None:
    """Get the last ingestion timestamp from the target table, or None if table doesn't exist."""
    if not spark.catalog.tableExists(full_table):
        return None
    return spark.table(full_table).select(F.max("_ingestion_timestamp")).collect()[0][0]

def ingest_to_bronze(source_subfolder: str, table_name: str, schema_hints: str | None = None, force: bool = False):
    """Read parquet from S3 and merge into the bronze table, preserving timestamps for unchanged rows."""
    s3_path = f"s3://{bucket_name}/{folder}/{source_subfolder}/parquet/"
    full_table = f"hooplakehouse.whoop.{table_name}"

    table_exists = spark.catalog.tableExists(full_table)
    needs_seed = not table_exists or "_row_hash" not in spark.table(full_table).columns

    # Only skip tables that already have _row_hash and whose source hasn't changed
    s3_modified = get_s3_last_modified(s3_path)
    if not force and not needs_seed:
        last_ingestion = get_last_ingestion(full_table)
        if last_ingestion and s3_modified <= last_ingestion.replace(tzinfo=timezone.utc):
            print(f"Skipping {full_table} — source unchanged (last modified: {s3_modified})")
            return

    print(f"Ingesting: {s3_path} → {full_table}")

    # Read source data
    if schema_hints:
        df = spark.sql(f"""
            SELECT * FROM read_files(
                '{s3_path}',
                format => 'parquet',
                schemaHints => '{schema_hints}'
            )
        """)
        # Recover rescued values in one .withColumns() call to avoid nested execution plans
        rescue_exprs = {}
        for hint in schema_hints.split(","):
            parts = hint.strip().split()
            col_name, col_type = parts[0], parts[1]
            rescue_exprs[col_name] = F.coalesce(
                F.col(col_name),
                F.get_json_object(F.col("_rescued_data"), f"$.{col_name}").cast(col_type)
            )
        df = df.withColumns(rescue_exprs).drop("_rescued_data")
    else:
        df = spark.read.option("mergeSchema", "true").parquet(s3_path)

    # SHA-256 hash over a null-preserving, column-named serialization
    business_cols = df.columns
    df = df.withColumns({
        "_row_hash": row_hash_expr(business_cols),
        "_ingestion_timestamp": F.current_timestamp(),
        "_load_date": F.current_date()
    })

    # First load or migration: table missing _row_hash → seed with a full overwrite
    if needs_seed:
        (df.write
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table))
        set_hash_version(full_table)
        print(f"  ✓ {full_table} seeded (initial / migration load)")
        return

    # Hash algorithm changed since this table was written → recompute existing hashes first,
    # otherwise every row would look new and the MERGE would reset all timestamps.
    if get_hash_version(full_table) != HASH_VERSION:
        migrate_row_hash(full_table)

    # Hash-based MERGE — three outcomes per row:
    #   MATCHED (hash in both source & target):      row unchanged → do nothing, timestamps preserved
    #   NOT MATCHED BY TARGET (new hash in source):  new or updated row → insert with current timestamps
    #   NOT MATCHED BY SOURCE (hash gone from source): row removed upstream → delete from bronze
    (DeltaTable.forName(spark, full_table)
        .alias("target")
        .merge(df.alias("source"), "target._row_hash = source._row_hash")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )

    print(f"  ✓ {full_table} merge complete")


# COMMAND ----------

# Run all ingestions sequentially
for source_subfolder, table_name, schema_hints in sources:
    ingest_to_bronze(source_subfolder, table_name, schema_hints)

# COMMAND ----------

# DBTITLE 1,Test: unchanged rows preserve timestamps
# ── Test: verify unchanged rows keep their original timestamps ────────────────
# Forces re-ingestion against the same source data and asserts that no
# _ingestion_timestamp values were modified (all rows matched by _row_hash).
# Gated behind the run_tests parameter (Configuration cell) so production runs skip it.

TEST_SOURCE     = "schedules"
TEST_TABLE      = "bronze_schedules"
full_test_table = f"hooplakehouse.whoop.{TEST_TABLE}"
schema_hints    = next((s[2] for s in sources if s[1] == TEST_TABLE), None)

if not RUN_TESTS:
    print("Skipping validation test — set run_tests = 'true' to run it.")
elif not spark.catalog.tableExists(full_test_table):
    print(f"⚠  {full_test_table} doesn't exist yet — run the ingestion cell first.")
else:
    # Snapshot before
    before = (
        spark.table(full_test_table)
        .select(
            F.count("*").alias("row_count"),
            F.countDistinct("_ingestion_timestamp").alias("distinct_ts"),
            F.min("_ingestion_timestamp").alias("min_ts"),
            F.max("_ingestion_timestamp").alias("max_ts"),
        )
        .collect()[0]
    )
    print(f"Before — {before.row_count} rows | {before.distinct_ts} distinct timestamp(s)")
    print(f"         {before.min_ts}  →  {before.max_ts}")

    # Force re-ingestion (bypasses S3 modification check)
    ingest_to_bronze(TEST_SOURCE, TEST_TABLE, schema_hints, force=True)

    # Snapshot after
    after = (
        spark.table(full_test_table)
        .select(
            F.count("*").alias("row_count"),
            F.countDistinct("_ingestion_timestamp").alias("distinct_ts"),
            F.min("_ingestion_timestamp").alias("min_ts"),
            F.max("_ingestion_timestamp").alias("max_ts"),
        )
        .collect()[0]
    )
    print(f"\nAfter  — {after.row_count} rows | {after.distinct_ts} distinct timestamp(s)")
    print(f"         {after.min_ts}  →  {after.max_ts}")

    # Assertions
    assert before.row_count == after.row_count, \
        f"❌ Row count changed: {before.row_count} → {after.row_count}"
    assert before.min_ts == after.min_ts and before.max_ts == after.max_ts, \
        "❌ Timestamps changed — MERGE updated rows it shouldn't have!"

    print(f"\n✓ {after.row_count} rows — all timestamps preserved")
