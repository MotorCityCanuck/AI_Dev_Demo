import pytest

from src.player_master_bronze import (
    CONTRACT, identifier, validate_schema, validate_source_path,
)


def test_schema_contract_matches_dictionary():
    from pathlib import Path
    import re

    dictionary = (Path(__file__).parents[1] / "docs" /
                  "player_master_data_dictionary.md").read_text(encoding="utf-8")
    columns = re.findall(r"^\| `([^`]+)` \| (INT64|STRING[^|]*|DECIMAL[^|]*) \|", dictionary, re.M)
    assert list(CONTRACT) == [name for name, _ in columns]
    for name, dtype in columns:
        if dtype == "INT64":
            assert CONTRACT[name] == "bigint"
        elif dtype.startswith("STRING"):
            assert CONTRACT[name] == "string"
        else:
            scale = re.search(r"scale (\d+)", dtype).group(1)
            assert CONTRACT[name] == f"decimal(38,{scale})"


def test_contract_accepts_rescued_data_and_arbitrary_column_order():
    validate_schema(list(reversed(list(CONTRACT.items()))) + [("_rescued_data", "string")])


@pytest.mark.parametrize("change", ["missing", "extra", "wrong_type"])
def test_contract_rejects_structural_changes(change):
    actual = dict(CONTRACT)
    if change == "missing":
        actual.pop("player_id")
    elif change == "extra":
        actual["unexpected"] = "string"
    else:
        actual["birth_date"] = "date"
    with pytest.raises(ValueError, match="differs from contract"):
        validate_schema(actual)


@pytest.mark.parametrize("value", ["a.b", "a/b", "x`; DROP SCHEMA x", ""])
def test_identifiers_reject_sql_and_path_injection(value):
    with pytest.raises(ValueError):
        identifier(value)


def test_state_must_be_outside_source():
    with pytest.raises(ValueError, match="outside"):
        validate_source_path("/Volumes/c/s/v", "/Volumes/c/s/v/state")


@pytest.mark.parametrize("path", ["/tmp/raw", "/Volumes/c/s", "/Volumes/c/s/v/../other"])
def test_source_requires_absolute_volume_path(path):
    with pytest.raises(ValueError):
        validate_source_path(path, "/Volumes/c/bronze/state/player_master")


def test_ingestion_uses_append_and_stable_checkpoint(monkeypatch):
    """Check restart configuration, without claiming to simulate Auto Loader."""
    import sys
    from types import ModuleType
    from unittest.mock import MagicMock, call
    from src.player_master_bronze import ingest_player_master

    functions = ModuleType("pyspark.sql.functions")
    functions.col = MagicMock()
    functions.current_timestamp = MagicMock()
    sql = ModuleType("pyspark.sql")
    sql.functions = functions
    monkeypatch.setitem(sys.modules, "pyspark", ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)

    spark = MagicMock()
    reader = spark.readStream
    reader.format.return_value = reader
    reader.option.return_value = reader
    raw = reader.load.return_value
    raw.dtypes = list(CONTRACT.items()) + [("_rescued_data", "string")]
    writer = raw.select.return_value.writeStream
    for method in ("format", "outputMode", "option", "trigger"):
        getattr(writer, method).return_value = writer

    for _ in range(2):
        ingest_player_master(spark, "/Volumes/c/raw/files", "c", "bronze", "players", "state")

    assert reader.option.call_args_list.count(call("pathGlobFilter", "player_master.parquet")) == 2
    assert reader.option.call_args_list.count(call("cloudFiles.allowOverwrites", "false")) == 2
    assert writer.outputMode.call_args_list == [call("append"), call("append")]
    checkpoint = call("checkpointLocation", "/Volumes/c/bronze/state/players/checkpoint")
    assert writer.option.call_args_list == [checkpoint, checkpoint]
    assert writer.trigger.call_args_list == [call(availableNow=True), call(availableNow=True)]
    assert writer.toTable.call_args_list == [call("`c`.`bronze`.`players`")] * 2
    assert writer.toTable.return_value.awaitTermination.call_count == 2
