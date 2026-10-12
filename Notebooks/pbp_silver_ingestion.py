# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Initial Silver Layer Goals
# MAGIC %md
# MAGIC # Initial Silver Layer Goals
# MAGIC * Enforce Consistent Schema
# MAGIC * Propagate upstream deletes (MERGE deletes rows no longer in bronze; aborts if source is empty or >5% of rows would be deleted)

# COMMAND ----------

bronze_pbp_table = "hooplakehouse.whoop.bronze_pbp"

# COMMAND ----------

bronze_play_by_play_df = spark.read.table(bronze_pbp_table)

# COMMAND ----------

display(bronze_play_by_play_df)

# COMMAND ----------

bronze_play_by_play_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema Enforcement

# COMMAND ----------

from pyspark.sql.functions import col

# id is double in bronze, so cast to long first to avoid scientific notation
double_id_fields = {"id"}
int_id_fields = {"type_id", "team_id", "athlete_id_1", "athlete_id_2", "athlete_id_3", "game_id", "home_team_id", "away_team_id"}

silver_play_by_play = bronze_play_by_play_df.select(
    *[
        col(c).cast("long").cast("string").alias(c) if c in double_id_fields
        else col(c).cast("string").alias(c) if c in int_id_fields
        else col(c)
        for c in bronze_play_by_play_df.columns
    ]
)

# COMMAND ----------

display(silver_play_by_play)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality & Validation

# COMMAND ----------

silver_play_by_play = (
    silver_play_by_play
    .filter(col("id").isNotNull())
)

# COMMAND ----------

# DBTITLE 1,Deduplicate by id
silver_play_by_play = silver_play_by_play.dropDuplicates(["id"])

# COMMAND ----------

# DBTITLE 1,Drop _row_hash column
silver_play_by_play = silver_play_by_play.drop("_row_hash")

# COMMAND ----------

# DBTITLE 1,Save silver table
from delta.tables import DeltaTable

silver_table_name = "hooplakehouse.whoop.silver_pbp"
pk_cols = ["id"]
MAX_DELETE_FRACTION = 0.05  # abort if more than 5% of silver rows would be deleted

if spark.catalog.tableExists(silver_table_name):
    # Guard: bronze is a complete snapshot, so rows missing from it are deleted from silver.
    # Refuse to run if the source is empty or the delete volume looks like a bad bronze load.
    source_count = silver_play_by_play.count()
    if source_count == 0:
        raise ValueError(f"Aborting: source for {silver_table_name} is empty — refusing to delete all silver rows")
    target_pks = spark.table(silver_table_name).select(*pk_cols)
    target_count = target_pks.count()
    delete_count = target_pks.join(silver_play_by_play.select(*pk_cols), pk_cols, "left_anti").count()
    if target_count and delete_count / target_count > MAX_DELETE_FRACTION:
        raise ValueError(
            f"Aborting: {delete_count:,} of {target_count:,} rows ({delete_count / target_count:.1%}) in "
            f"{silver_table_name} would be deleted — exceeds {MAX_DELETE_FRACTION:.0%}. Check bronze before re-running."
        )

    delta_table = DeltaTable.forName(spark, silver_table_name)
    delta_table.alias("target").merge(
        silver_play_by_play.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()
    print(f"✓ Merged into {silver_table_name} — {source_count:,} source rows, {delete_count:,} removed upstream and deleted")
else:
    silver_play_by_play.write.saveAsTable(silver_table_name)
    print(f"✓ Created {silver_table_name}")
