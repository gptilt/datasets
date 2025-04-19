import os
import tempfile
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from storage import StorageParquet


@pytest.fixture
def temp_env_root():
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["DIR_ROOT"] = tmpdir
        yield tmpdir


@pytest.fixture
def storage_parquet(temp_env_root):
    dataset = "gptilt"
    schema = "match_v5"
    region = "euw1"
    tables = ["games", "players"]
    return StorageParquet(dataset, schema, region, tables)


def test_initialization_defaults(storage_parquet):
    assert storage_parquet.file_extension == "parquet"
    assert storage_parquet.target_batch_size == 1_000_000_000
    for table in storage_parquet.tables:
        assert storage_parquet.buffer[table] == []
        assert storage_parquet.table_path(table).exists()


def test_has_record_false_when_no_dir(storage_parquet):
    result = storage_parquet.has_record("nonexistent_table", "some_column", "value")
    assert result is False


def test_has_record_false_when_column_missing(storage_parquet):
    # Create table with unrelated column
    table_path = storage_parquet.table_path("games")
    table = pa.table({"game_id": [1, 2, 3]})
    pq.write_table(table, table_path / "dummy.parquet")

    result = storage_parquet.has_record("games", "nonexistent_column", 123)
    assert result is False


def test_has_record_true_when_value_exists(storage_parquet):
    table_path = storage_parquet.table_path("players")
    table = pa.table({"summoner_id": ["a", "b", "c"]})
    pq.write_table(table, table_path / "dummy.parquet")

    assert storage_parquet.has_record("players", "summoner_id", "b") is True
    assert storage_parquet.has_record("players", "summoner_id", "z") is False


def test_has_records_in_all_tables(storage_parquet):
    for table_name in storage_parquet.tables:
        table_path = storage_parquet.table_path(table_name)
        table = pa.table({"game_id": ["abc", "def"]})
        pq.write_table(table, table_path / "file.parquet")

    assert storage_parquet.has_records_in_all_tables(game_id="abc") is True
    assert storage_parquet.has_records_in_all_tables(game_id="xyz") is False


def test_write_to_dataset_creates_file(storage_parquet):
    # Create a small table
    data = [{"id": i, "value": i * 10} for i in range(5)]
    table = pa.Table.from_pylist(data)

    storage_parquet.write_to_dataset("games", table)

    output_files = list(storage_parquet.table_path("games").glob("parquet_*.parquet"))
    assert len(output_files) == 1
    assert pq.read_table(output_files[0]).to_pylist() == data


def test_store_batch_buffers_and_writes(storage_parquet):
    storage_parquet.target_batch_size = 0  # Force write regardless of size

    data = [{"name": f"Player{i}", "score": i} for i in range(100)]
    storage_parquet.store_batch("players", data)

    output_files = list(storage_parquet.table_path("players").glob("parquet_*.parquet"))
    assert len(output_files) == 1

    table = pq.read_table(output_files[0])
    assert len(table) == 100
    assert table.column("name").to_pylist()[0] == "Player0"
