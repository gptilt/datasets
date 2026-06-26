from dagster import ConfigurableResource
from pydantic import PrivateAttr


class LocalLLM(ConfigurableResource):
    model_id: str = "Qwen/Qwen2.5-3B-Instruct"
    device: str = "cuda"
    torch_dtype: str = "auto"
    temperature: float = 0.3
    models_directory: str | None = None

    _pipeline: object = PrivateAttr(default=None)

    @property
    def pipeline(self):
        """
        Lazy-loads the pipeline.
        If it exists in memory, return it.
        If not, create it.
        """
        if self._pipeline is None:
            from transformers import pipeline

            self._pipeline = pipeline(
                task="text-generation",
                model=self.model_id,
                device_map=self.device,
                torch_dtype=self.torch_dtype,
                model_kwargs={"cache_dir": self.models_directory} if self.models_directory else {},
            )
        return self._pipeline

    def generate(
        self,
        system: str,
        user: str,
        max_new_tokens: int = 256
    ) -> str:
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        output = self.pipeline(
            messages,
            max_new_tokens=max_new_tokens,
            temperature=self.temperature,
            do_sample=self.temperature > 0,
            return_full_text=False,
        )
        return output[0]["generated_text"].strip()
