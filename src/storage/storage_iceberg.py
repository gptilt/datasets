from pyiceberg.catalog.rest import RestCatalog
from .storage import Storage


class StorageIceberg(Storage):
    def __init__(
        self, root, schema, dataset, tables,
        warehouse_name: str,
        catalog_uri: str,
        token: str
    ):
        super().__init__(root, schema, dataset, tables)
        
        self.catalog = RestCatalog(
            name=root,
            warehouse_name=warehouse_name,
            catalog_uri=catalog_uri,
            token=token
        )

        self.catalog.create_namespace_if_not_exists(dataset)
