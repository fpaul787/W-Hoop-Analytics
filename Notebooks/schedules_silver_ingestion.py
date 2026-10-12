# Databricks notebook source
# DBTITLE 1,Silver Ingestion - Schedules
# MAGIC %md
# MAGIC # Silver Ingestion — Schedules
# MAGIC
# MAGIC ## Initial Silver Layer Goals
# MAGIC * Enforce consistent schema (cast integer IDs to string)
# MAGIC * Null filtering on primary key (`id`)
# MAGIC * Deduplication by primary key
# MAGIC * Drop internal `_row_hash` column
# MAGIC * MERGE upsert into `hooplakehouse.whoop.silver_schedules`

# COMMAND ----------

# DBTITLE 1,Read bronze table
from pyspark.sql.functions import col
from delta.tables import DeltaTable

bronze_table = "hooplakehouse.whoop.bronze_schedules"
silver_table = "hooplakehouse.whoop.silver_schedules"

bronze_df = spark.read.table(bronze_table)

# COMMAND ----------

# DBTITLE 1,Schema enforcement
# Cast integer ID columns to string for consistent join behaviour
int_id_fields = {
    "id", "type_id", "status_type_id",
    "home_id", "home_venue_id",
    "away_id", "away_venue_id",
    "game_id", "season", "season_type", "venue_id"
}

silver_df = bronze_df.select(
    *[
        col(c).cast("string").alias(c) if c in int_id_fields
        else col(c)
        for c in bronze_df.columns
    ]
)

# COMMAND ----------

# DBTITLE 1,Null filter on primary key
silver_df = silver_df.filter(col("id").isNotNull())

# COMMAND ----------

# DBTITLE 1,Deduplicate by primary key
silver_df = silver_df.dropDuplicates(["id"])

# COMMAND ----------

# DBTITLE 1,Drop _row_hash column
silver_df = silver_df.drop("_row_hash")

# COMMAND ----------

# DBTITLE 1,Save silver table
if spark.catalog.tableExists(silver_table):
    delta_table = DeltaTable.forName(spark, silver_table)
    delta_table.alias("target").merge(
        silver_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"✓ Merged into {silver_table}")
else:
    silver_df.write.saveAsTable(silver_table)
    print(f"✓ Created {silver_table}")
