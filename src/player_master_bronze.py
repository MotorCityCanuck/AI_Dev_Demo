"""Append release rows to Bronze without applying player business rules.

Contract: docs/player_master_data_dictionary.md, source schema version 1.6.
PySpark imports are deferred so configuration checks can run locally.
"""

import re


# The dictionary defines scale, but exported decimal precision varies by file.
# Widen precision to 38 without changing scale or rounding source values.
CONTRACT = {
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
    "rating": "decimal(38,3)",
    "rating_value": "decimal(38,3)",
    "confidence_score": "decimal(38,3)",
    "volatility_score": "decimal(38,3)",
    "global_percentile": "decimal(38,2)",
    "match_count_used": "bigint",
    "rating_date": "string",
    "rating_batch_id": "bigint",
    "snapshot_month": "string",
}


def identifier(value):
    """Restrict interpolated SQL and volume path components to simple names."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value!r}")
    return value


def validate_source_path(source_path, state_path):
    source_path = source_path.rstrip("/")
    if not re.fullmatch(r"/Volumes/[^/]+/[^/]+/[^/]+(?:/[^/]+)*", source_path):
        raise ValueError("source_path must be a Unity Catalog /Volumes path")
    if any(part in {".", ".."} for part in source_path.split("/")):
        raise ValueError("source_path cannot contain relative path segments")
    if state_path == source_path or state_path.startswith(source_path + "/"):
        raise ValueError("Ingestion state must be outside the raw source directory")
    return source_path


def validate_schema(actual_types):
    """Fail on structural contract violations; retain business-quality issues."""
    actual_types = dict(actual_types)
    actual_types.pop("_rescued_data", None)
    if actual_types != CONTRACT:
        raise ValueError(
            f"player_master source schema differs from contract: {actual_types!r}; "
            f"expected {CONTRACT!r}"
        )


def ingest_player_master(spark, source_path, catalog, schema, table, state_volume):
    from pyspark.sql import functions as F

    catalog, schema, table, state_volume = map(
        identifier, (catalog, schema, table, state_volume)
    )
    destination = f"`{catalog}`.`{schema}`.`{table}`"
    state_path = f"/Volumes/{catalog}/{schema}/{state_volume}/{table}"
    source_path = validate_source_path(source_path, state_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{state_volume}`"
    )

    # Infer the Parquet schema rather than silently imposing missing columns.
    # Hints reconcile precision across releases; dates remain source strings.
    hints = ", ".join(
        f"{name} {dtype}" for name, dtype in CONTRACT.items()
        if dtype.startswith("decimal")
    )
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{state_path}/schema")
        .option("cloudFiles.schemaHints", hints)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.allowOverwrites", "false")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "player_master.parquet")
        .load(source_path)
    )
    validate_schema(raw.dtypes)
    bronze = raw.select(
        *[F.col(name) for name in CONTRACT],
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )
    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{state_path}/checkpoint")
        .trigger(availableNow=True)
        .toTable(destination)
    )
    query.awaitTermination()
    return {"destination": destination, "last_progress": query.lastProgress}
