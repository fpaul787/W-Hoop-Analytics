# Databricks notebook source
# DBTITLE 1,Silver Ingestion - Team Box
# MAGIC %md
# MAGIC # Silver Ingestion — Team Box
# MAGIC
# MAGIC ## Initial Silver Layer Goals
# MAGIC * Enforce consistent schema (cast integer IDs to string)
# MAGIC * Null filtering on composite primary key (`game_id`, `team_id`)
# MAGIC * Deduplication by primary key
# MAGIC * Drop internal `_row_hash` column
# MAGIC * MERGE upsert into `hooplakehouse.whoop.silver_team_box`

# COMMAND ----------

# DBTITLE 1,Read bronze table
from pyspark.sql.functions import col
from delta.tables import DeltaTable

bronze_table = "hooplakehouse.whoop.bronze_team_box"
silver_table = "hooplakehouse.whoop.silver_team_box"

bronze_df = spark.read.table(bronze_table)

# COMMAND ----------

# DBTITLE 1,Schema enforcement
# Cast integer ID columns to string for consistent join behaviour
int_id_fields = {"game_id", "season", "season_type", "team_id", "opponent_team_id"}

silver_df = bronze_df.select(
    *[
        col(c).cast("string").alias(c) if c in int_id_fields
        else col(c)
        for c in bronze_df.columns
    ]
)

# COMMAND ----------

# DBTITLE 1,Null filter on primary key
silver_df = silver_df.filter(
    col("game_id").isNotNull() & col("team_id").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Deduplicate by primary key
silver_df = silver_df.dropDuplicates(["game_id", "team_id"])

# COMMAND ----------

# DBTITLE 1,Drop _row_hash column
silver_df = silver_df.drop("_row_hash")

# COMMAND ----------

# DBTITLE 1,Save silver table
if spark.catalog.tableExists(silver_table):
    delta_table = DeltaTable.forName(spark, silver_table)
    delta_table.alias("target").merge(
        silver_df.alias("source"),
        "target.game_id = source.game_id AND target.team_id = source.team_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"✓ Merged into {silver_table}")
else:
    silver_df.write.saveAsTable(silver_table)
    print(f"✓ Created {silver_table}")
