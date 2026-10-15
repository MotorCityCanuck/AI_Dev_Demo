# Databricks notebook source
import sys
from pathlib import Path

# Bundle sync preserves notebooks/ and src/ as sibling directories.
sys.path.insert(0, str(Path.cwd().parent))
from src.player_master_bronze import ingest_player_master

# COMMAND ----------
defaults = {
    "source_path": "/Volumes/ai_dev_class_demo/ai_dev_schema/ai_dev_raw",
    "catalog": "ai_dev_class_demo",
    "schema": "bronze_layer",
    "table": "player_master",
    "state_volume": "ingestion_state",
}
for name, default in defaults.items():
    dbutils.widgets.text(name, default)

result = ingest_player_master(
    spark, **{name: dbutils.widgets.get(name) for name in defaults}
)
print(result)
