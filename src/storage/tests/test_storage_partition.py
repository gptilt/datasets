import polars as pl
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from storage import StoragePartition


@pytest.fixture
def sample_partitioned_parquet(tmp_path):
    """Create a small hive-partitioned Parquet dataset."""
    base_path = tmp_path / "basic" / "matches" / "players"
    europe_path = base_path / "region=europe"
    europe_path.mkdir(parents=True)

    # Create dummy data
    europe_df = pl.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "region": ["europe", "europe"]
    })

    # Save as partitioned Parquet
    europe_df.write_parquet(europe_path / "data.parquet")

    return tmp_path


@pytest.fixture
def storage_partition(sample_partitioned_parquet):
    schema = "basic"
    dataset = "matches"
    tables = ["games", "players"]
    return StoragePartition(
        sample_partitioned_parquet,
        schema,
        dataset,
        tables,
        region="europe"
    )


def test_initialization_defaults(storage_partition):
    assert storage_partition.file_extension == "parquet"
    assert storage_partition.target_batch_size == 1_000_000_000
    for table in storage_partition.tables:
        assert storage_partition.buffer[table] == []
        assert storage_partition.table_path(table).exists()


def test_has_record_false_when_no_dir(storage_partition):
    result = storage_partition.has_record("nonexistent_table", "some_column", "value")
    assert result is False


def test_has_record_false_when_column_missing(storage_partition):
    # Create table with unrelated column
    table_path = storage_partition.table_path("games")
    table = pa.table({"game_id": [1, 2, 3]})
    pq.write_table(table, table_path / "dummy.parquet")

    result = storage_partition.has_record("games", "nonexistent_column", 123)
    assert result is False


def test_has_record_true_when_value_exists(storage_partition):
    table_path = storage_partition.table_path("players")
    table = pa.table({"summoner_id": ["a", "b", "c"]})
    pq.write_table(table, table_path / "dummy.parquet")

    assert storage_partition.has_record("players", "summoner_id", "b") is True
    assert storage_partition.has_record("players", "summoner_id", "z") is False


def test_has_records_in_all_tables(storage_partition):
    for table_name in storage_partition.tables:
        table_path = storage_partition.table_path(table_name)
        table = pa.table({"game_id": ["abc", "def"]})
        pq.write_table(table, table_path / "file.parquet")

    assert storage_partition.has_records_in_all_tables(game_id="abc") is True
    assert storage_partition.has_records_in_all_tables(game_id="xyz") is False


def test_flush_table_creates_file(storage_partition):
    # Create a small table
    data = [{"id": i, "value": i * 10, "region": "europe"} for i in range(5)]
    table = storage_partition._arrow_table_from_list_of_records(data)

    storage_partition._flush_table("games", table)
       
    partition_dir = storage_partition.partition_path("games")
    output_files = list(partition_dir.glob("*.parquet"))
    assert len(output_files) == 1
    print(output_files)
    assert pq.read_table(partition_dir).to_pylist() == data


def test_store_batch_buffers_and_writes(storage_partition):
    storage_partition.target_batch_size = 0  # Force write regardless of size

    data = [{"name": f"Player{i}", "score": i} for i in range(1000)]
    storage_partition.store_batch("players", data)

    partition_dir = storage_partition.partition_path("players")
    output_files = list(partition_dir.glob("*.parquet"))
    assert len(output_files) == 2

    table = pq.read_table(partition_dir)
    assert len(table) == 1002
    assert table.column("name").to_pylist()[0] == "Alice"
    assert table.column("name").to_pylist()[2] == "Player0"


def test_flush_writes_and_clears_buffer(storage_partition, tmp_path):
    # Don't include 'region' in data, let partitioning logic add it
    storage_partition.buffer["games"] = [{"gameId": 1}, {"gameId": 2}]
    storage_partition.buffer["players"] = [{"playerId": "abc"}]

    # Flush with region partition
    nbytes = storage_partition.flush()

    assert nbytes > 0
    assert storage_partition.buffer["games"] == []
    assert storage_partition.buffer["players"] == []

    # Validate file creation and content
    games_path = tmp_path / "basic/matches/games/region=europe"
    players_path = tmp_path / "basic/matches/players/region=europe"

    assert any(f.suffix == ".parquet" for f in games_path.iterdir())
    assert any(f.suffix == ".parquet" for f in players_path.iterdir())

    table = pq.read_table(games_path)
    assert table.num_rows == 2
    assert set(table.column_names) == {"gameId", "region"}
    assert table.column("region").to_pylist() == ["europe", "europe"]


def test_load_to_polars_with_partition_filter(storage_partition):
    # Load only europe region
    df = storage_partition.load_to_polars("players", ["name"])

    assert df.shape[0] == 2
    assert "name" in df.columns
    assert df["name"].to_list() == ["Alice", "Bob"]


def test_store_polars_writes_partitioned_file(storage_partition):
    df = pl.DataFrame({
        "summoner_id": ["x1", "x2"],
        "kills": [5, 6],
    })

    nbytes = storage_partition.store_polars("players", df)

    assert nbytes > 0

    partition_dir = storage_partition.partition_path("players")
    assert partition_dir.exists(), "Expected partition directory to exist"

    output_files = list(partition_dir.glob("*.parquet"))
    assert len(output_files) == 2

    table = pq.read_table(partition_dir)
    print(table.schema.names)
    assert table.column("region").to_pylist() == ["europe"] * 4
