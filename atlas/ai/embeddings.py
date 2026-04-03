"""Optional local embedding generation for semantic vector search."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from atlas.ai.client import LocalLLMClient
from atlas.ai.types import AIConnectionError, AIGenerationError, AITimeoutError


class EmbeddingGenerator:
    """Generate embeddings through a supported local HTTP backend."""

    def __init__(self, client: LocalLLMClient) -> None:
        self.client = client

    def is_supported(self) -> bool:
        return self.provider_name in {"ollama", "openai_compatible"}

    @property
    def provider_name(self) -> str:
        return self.client.config.provider.lower()

    @property
    def model_name(self) -> str:
        return self.client.config.model

    def generate_embedding(self, text: str) -> list[float]:
        if not self.is_supported():
            raise AIGenerationError(
                f"Provider {self.provider_name!r} does not support embeddings in Atlas Phase 10B."
            )

        normalized_text = " ".join(text.split())
        if not normalized_text:
            raise AIGenerationError("Embedding text must not be empty.")

        if self.provider_name == "ollama":
            payload = self._request_json(
                "/api/embed",
                {"model": self.model_name, "input": normalized_text},
            )
            vector = self._extract_ollama_embedding(payload)
        else:
            payload = self._request_json(
                "/v1/embeddings",
                {"model": self.model_name, "input": normalized_text},
            )
            vector = None
            data = payload.get("data")
            if isinstance(data, list) and data:
                first = data[0]
                if isinstance(first, dict):
                    vector = first.get("embedding")

        if not isinstance(vector, list) or not vector:
            raise AIGenerationError(
                f"Embedding response for provider {self.provider_name!r} did not contain a numeric vector."
            )
        try:
            return [float(value) for value in vector]
        except (TypeError, ValueError) as exc:
            raise AIGenerationError("Embedding response contained non-numeric values.") from exc

    def _extract_ollama_embedding(self, payload: dict[str, Any]) -> list[Any] | None:
        embeddings = payload.get("embeddings")
        if isinstance(embeddings, list) and embeddings:
            first = embeddings[0]
            if isinstance(first, list) and first:
                return first

        # Preserve compatibility with older Ollama-compatible responses.
        vector = payload.get("embedding")
        if isinstance(vector, list) and vector:
            return vector
        return None

    def _request_json(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.client.config.base_url.rstrip('/')}{endpoint}"
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self.client.config.timeout_seconds,
            ) as response:
                decoded = json.loads(response.read().decode("utf-8"))
        except TimeoutError as exc:
            raise AITimeoutError(
                f"Embedding call to {url!r} timed out after {self.client.config.timeout_seconds}s."
            ) from exc
        except urllib.error.URLError as exc:
            raise AIConnectionError(f"Cannot reach embedding backend at {url!r}: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise AIGenerationError(f"Embedding backend returned non-JSON from {url!r}: {exc}") from exc

        if not isinstance(decoded, dict):
            raise AIGenerationError(f"Embedding backend returned non-object JSON from {url!r}.")
        return decoded
