# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC SELECT * FROM hooplakehouse.whoop.bronze_pbp;

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial Silver Layer Goals
# MAGIC * Enforce Consistent Schema

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
