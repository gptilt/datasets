import dagster as dg
import huggingface_hub as hub
from pydantic import PrivateAttr


class HuggingFaceHub(dg.ConfigurableResource):
    """
    huggingface_hub API client.
    Token injected via dg.EnvVar in definitions.py
    """
    token: str
    org: str = "gptilt"

    _api: hub.HfApi | None = PrivateAttr(default=None)

    def api(self) -> hub.HfApi:
        if self._api is None:
            self._api = hub.HfApi(token=self.token)
        return self._api

    def repo_id(self, dataset_id: str) -> str:
        return f"{self.org}/{dataset_id}"
