# Databricks notebook source
# MAGIC %md
# MAGIC # Load player_master into Bronze
# MAGIC
# MAGIC Source contract: `docs/player_master_data_dictionary.md` (schema 1.6).
# MAGIC Reads `/Volumes/ai_dev_demo/ai_dev_schema/ai_dev_raw/player_master.parquet`
# MAGIC and creates the managed Delta table `ai_dev_demo.bronze_layer.player_master`.
# MAGIC The original Parquet file remains in the volume.
# MAGIC
# MAGIC Run on Unity Catalog-enabled Databricks compute with volume support
# MAGIC (Databricks Runtime 13.3 LTS or newer). PySpark and Delta are provided by
# MAGIC the runtime. The executing identity needs USE CATALOG, USE SCHEMA and
# MAGIC READ VOLUME on the source, plus CREATE SCHEMA on the catalog if needed,
# MAGIC and USE SCHEMA / CREATE TABLE on the destination. Refreshing an existing
# MAGIC table also requires MODIFY; verification requires SELECT.
# MAGIC
# MAGIC This notebook loads one baseline file. `create` stops if the table already
# MAGIC exists. Select `full_refresh` to replace its contents with the baseline
# MAGIC on a subsequent demo run. Do not use full refresh for monthly incremental
# MAGIC releases: those contain deltas, not complete player snapshots.

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Volumes/ai_dev_demo/ai_dev_schema/ai_dev_raw/player_master.parquet",
)
dbutils.widgets.dropdown("load_mode", "create", ["create", "full_refresh"])

source_path = dbutils.widgets.get("source_path").strip()
load_mode = dbutils.widgets.get("load_mode")
target_schema = "ai_dev_demo.bronze_layer"
target_table = f"{target_schema}.player_master"

if not source_path.startswith("/Volumes/ai_dev_demo/ai_dev_schema/ai_dev_raw/"):
    raise ValueError("source_path must be inside the ai_dev_raw volume.")
if load_mode not in {"create", "full_refresh"}:
    raise ValueError("load_mode must be create or full_refresh.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the source and compare it with the dictionary
# MAGIC Infer types from Parquet to preserve its decimal precision and string dates.
# MAGIC Report contract differences without correcting, filtering, or deduplicating
# MAGIC raw records. Required-value and business-rule checks belong in Silver.

# COMMAND ----------

from pyspark.sql.types import DecimalType

# Ordered columns and logical types from the source dictionary.
expected_types = {
    "player_id": "bigint",
    "external_player_key": "string",
    "first_name": "string",
    "last_name": "string",
    "gender": "string",
    "birth_date": "string",
    "dominant_hand": "string",
    "home_region_id": "bigint",
    "country_code": "string",
    "registration_date": "string",
    "player_status": "string",
    "rating": "decimal_scale_3",
    "rating_value": "decimal_scale_3",
    "confidence_score": "decimal_scale_3",
    "volatility_score": "decimal_scale_3",
    "global_percentile": "decimal_scale_2",
    "match_count_used": "bigint",
    "rating_date": "string",
    "rating_batch_id": "bigint",
    "snapshot_month": "string",
}

raw_players = spark.read.parquet(source_path)
actual_types = {field.name: field.dataType for field in raw_players.schema.fields}
schema_issues = []

for name, expected in expected_types.items():
    actual = actual_types.get(name)
    if actual is None:
        schema_issues.append(f"Missing column: {name}")
    elif expected.startswith("decimal_scale_"):
        scale = int(expected.rsplit("_", 1)[1])
        if not isinstance(actual, DecimalType) or actual.scale != scale:
            schema_issues.append(f"{name}: expected decimal scale {scale}, got {actual}")
    elif actual.simpleString() != expected:
        schema_issues.append(f"{name}: expected {expected}, got {actual.simpleString()}")

extra_columns = sorted(set(actual_types) - set(expected_types))
if extra_columns:
    schema_issues.append(f"Additional source columns: {extra_columns}")
if raw_players.columns != list(expected_types):
    schema_issues.append("Source column order differs from the published contract.")

for issue in schema_issues:
    print(f"Source contract observation: {issue}")
if not schema_issues:
    print("Source column names, order, and types match dictionary version 1.6.")

raw_players.printSchema()
source_count = raw_players.count()
print(f"Source rows: {source_count:,} (the demo expects approximately 5,000).")
if source_count == 0:
    raise ValueError("The source is empty; no Bronze table will be created or refreshed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the managed Bronze Delta table
# MAGIC Preserve every source column and row, including nulls and duplicates.
# MAGIC Full refresh replaces all rows; it does not append duplicate copies.
# MAGIC Schema changes to an existing destination fail for review rather than
# MAGIC automatically replacing its schema. Keep the source file unchanged while
# MAGIC this notebook runs, and run only one writer against the destination.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
write_mode = "errorifexists" if load_mode == "create" else "overwrite"
(
    raw_players.write.format("delta")
    .mode(write_mode)
    .option("mergeSchema", "false")
    .option("overwriteSchema", "false")
    .saveAsTable(target_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the load
# MAGIC Compare complete rows in both directions, preserving duplicate counts.
# MAGIC This verification is suitable for the small classroom dataset.

# COMMAND ----------

bronze_players = spark.table(target_table)
bronze_count = bronze_players.count()
if bronze_count != source_count:
    raise RuntimeError(f"Row count mismatch: source={source_count}, Bronze={bronze_count}")
if (
    raw_players.exceptAll(bronze_players).limit(1).count()
    or bronze_players.exceptAll(raw_players).limit(1).count()
):
    raise RuntimeError("Bronze contents differ from the source; inspect before continuing.")

print(f"Loaded and verified {bronze_count:,} rows in {target_table}.")
display(spark.sql(f"DESCRIBE DETAIL {target_table}"))
display(bronze_players.limit(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS bronze_row_count
# MAGIC FROM ai_dev_demo.bronze_layer.player_master;
