import pytest
from storage import Storage


@pytest.fixture
def storage_instance(tmp_path):
    dataset = "my_dataset"
    schema = "my_schema"
    tables = ["table1", "table2"]
    return Storage(tmp_path, dataset, schema, tables)

def test_directories_created(storage_instance):
    for table in storage_instance.tables:
        assert storage_instance.table_path(table).exists()
        assert storage_instance.table_path(table).is_dir()

def test_file_extension_default(storage_instance):
    assert storage_instance.file_extension == "json"

def test_total_size_in_gb(storage_instance):
    dummy_file = storage_instance.table_path("table1") / "test.json"
    dummy_file.write_text("a" * 1024 * 1024)  # ~1MB
    size_gb = storage_instance.total_size_in_gb()
    assert 0.0009 < size_gb < 0.0011  # Allow for minor float variance
