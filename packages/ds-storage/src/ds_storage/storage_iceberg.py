from pydantic import PrivateAttr
from pyiceberg.catalog.rest import RestCatalog
from .storage_base import Storage, NonEmptyStr


class StorageIceberg(Storage):
    warehouse_name: NonEmptyStr
    catalog_uri: NonEmptyStr
    token: NonEmptyStr
    
    # Declare a private attribute that Pydantic/Dagster ignores during serialization
    _catalog: object = PrivateAttr(default=None)

    @property
    def catalog(self):

        if self._catalog is None:
            self._catalog = RestCatalog(
                name=self.root,
                warehouse_name=self.warehouse_name,
                catalog_uri=self.catalog_uri,
                token=self.token
            )

        self.catalog.create_namespace_if_not_exists(f"{self.dataset}.{self.schema_name}")

        return self._catalog
