# Databricks notebook source
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

from pyspark.sql.functions import col

id_fields = {"id", "type_id", "team_id", "athlete_id_1", "athlete_id_2", "athlete_id_3", "game_id", "home_team_id", "away_team_id"}

silver_play_by_play = bronze_play_by_play_df.select(
    *[col(c).cast("string").alias(c) if c in id_fields else col(c) for c in bronze_play_by_play_df.columns]
)

# COMMAND ----------

display(silver_play_by_play)

# COMMAND ----------


