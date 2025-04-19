import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from storage import StorageParquet
from unittest.mock import MagicMock


@pytest.fixture
def storage_parquet(tmp_path):
    dataset = "gptilt"
    schema = "match_v5"
    tables = ["games", "players"]
    return StorageParquet(tmp_path, dataset, schema, tables)


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

    output_files = list(storage_parquet.table_path("games").glob("part*.parquet"))
    assert len(output_files) == 1
    assert pq.read_table(output_files[0]).to_pylist() == data


def test_store_batch_buffers_and_writes(storage_parquet):
    storage_parquet.target_batch_size = 0  # Force write regardless of size

    data = [{"name": f"Player{i}", "score": i} for i in range(1000)]
    storage_parquet.store_batch("players", data)

    output_files = list(storage_parquet.table_path("players").glob("part*.parquet"))
    assert len(output_files) == 1

    table = pq.read_table(output_files[0])
    assert len(table) == 1000
    assert table.column("name").to_pylist()[0] == "Player0"


def test_flush_calls_store_batch_with_partition_columns(storage_parquet):
    # Put dummy data into the buffer
    table_name = storage_parquet.tables[0]
    storage_parquet.buffer[table_name] = [{"foo": "bar"}, {"foo": "baz"}]

    # Mock store_batch so it doesn't actually write
    storage_parquet.store_batch = MagicMock()

    # Call flush with a fake partition column
    storage_parquet.flush(region="europe")

    # Check store_batch was called once with the correct arguments
    storage_parquet.store_batch.assert_called_once()
    args, kwargs = storage_parquet.store_batch.call_args
    assert args[0] == table_name
    assert isinstance(args[1], pa.Table)
    assert kwargs == {"region": "europe"}

    # After flush, buffer should be empty
    assert storage_parquet.buffer[table_name] == []
