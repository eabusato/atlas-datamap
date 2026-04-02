"""Abstract LLM client and concrete provider implementations."""

from __future__ import annotations

import abc
import json
import logging
import urllib.error
import urllib.request
from typing import Any

from atlas.ai.types import (
    AIConfig,
    AIConfigError,
    AIConnectionError,
    AIGenerationError,
    AITimeoutError,
    ModelInfo,
)

logger = logging.getLogger(__name__)


class LocalLLMClient(abc.ABC):
    """Abstract contract for all local LLM clients."""

    def __init__(self, config: AIConfig) -> None:
        self._config = config

    @property
    def config(self) -> AIConfig:
        return self._config

    @abc.abstractmethod
    def is_available(self) -> bool:
        """Return True if the LLM server is online and accepting requests."""

    @abc.abstractmethod
    def get_model_info(self) -> ModelInfo:
        """Return runtime information about the currently loaded model."""

    @abc.abstractmethod
    def generate(
        self,
        prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        """Send a prompt to the provider and return the raw generated text."""

    def _eff_max_tokens(self, override: int | None) -> int:
        return override if override is not None else self._config.max_tokens

    def _eff_temperature(self, override: float | None) -> float:
        return override if override is not None else self._config.temperature

    def _base_url(self) -> str:
        return self._config.base_url.rstrip("/")

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"model={self._config.model!r}, base_url={self._config.base_url!r})"
        )


class OllamaClient(LocalLLMClient):
    """Client for Ollama."""

    def _make_request(
        self,
        endpoint: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url()}{endpoint}"
        method = "POST" if payload is not None else "GET"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Content-Type": "application/json"} if data else {}
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(
                request,
                timeout=self._config.timeout_seconds,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if not isinstance(payload, dict):
                    raise AIGenerationError(f"Ollama returned non-object JSON from {url!r}.")
                return payload
        except TimeoutError as exc:
            raise AITimeoutError(
                f"Ollama call to {url!r} timed out after {self._config.timeout_seconds}s."
            ) from exc
        except urllib.error.URLError as exc:
            raise AIConnectionError(f"Cannot reach Ollama at {url!r}: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise AIGenerationError(f"Ollama returned non-JSON from {url!r}: {exc}") from exc

    def is_available(self) -> bool:
        try:
            self._make_request("/api/version")
            return True
        except (AIConnectionError, AITimeoutError):
            return False

    def get_model_info(self) -> ModelInfo:
        data = self._make_request("/api/version")
        return ModelInfo(
            provider_name="ollama",
            model_name=self._config.model,
            is_local=True,
            version=str(data.get("version")) if data.get("version") is not None else None,
        )

    def generate(
        self,
        prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        payload: dict[str, Any] = {
            "model": self._config.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": self._eff_temperature(temperature),
                "num_predict": self._eff_max_tokens(max_tokens),
            },
        }
        data = self._make_request("/api/generate", payload)
        response = data.get("response")
        if not isinstance(response, str):
            raise AIGenerationError(
                f"Ollama /api/generate response missing 'response' key: {data!r}"
            )
        return response


class LlamaCppClient(LocalLLMClient):
    """Client for the llama.cpp HTTP server."""

    def _make_request(
        self,
        endpoint: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url()}{endpoint}"
        method = "POST" if payload is not None else "GET"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Content-Type": "application/json"} if data else {}
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(
                request,
                timeout=self._config.timeout_seconds,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if not isinstance(payload, dict):
                    raise AIGenerationError(f"llama.cpp returned non-object JSON from {url!r}.")
                return payload
        except TimeoutError as exc:
            raise AITimeoutError(
                f"llama.cpp call to {url!r} timed out after {self._config.timeout_seconds}s."
            ) from exc
        except urllib.error.URLError as exc:
            raise AIConnectionError(
                f"Cannot reach llama.cpp server at {url!r}: {exc.reason}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise AIGenerationError(f"llama.cpp returned non-JSON from {url!r}: {exc}") from exc

    def is_available(self) -> bool:
        try:
            data = self._make_request("/health")
            status = data.get("status")
            return isinstance(status, str) and status in {
                "ok",
                "no slot available",
                "loading model",
            }
        except (AIConnectionError, AITimeoutError):
            return False

    def get_model_info(self) -> ModelInfo:
        try:
            data = self._make_request("/props")
            model_name = data.get("default_generation_settings", {}).get("model")
        except (AIConnectionError, AIGenerationError, AttributeError):
            model_name = None
        return ModelInfo(
            provider_name="llamacpp",
            model_name=str(model_name or self._config.model),
            is_local=True,
            version=None,
        )

    def generate(
        self,
        prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        payload: dict[str, Any] = {
            "prompt": prompt,
            "n_predict": self._eff_max_tokens(max_tokens),
            "temperature": self._eff_temperature(temperature),
            "stream": False,
            "stop": ["</s>", "\n\n\n"],
        }
        data = self._make_request("/completion", payload)
        content = data.get("content")
        if not isinstance(content, str):
            raise AIGenerationError(
                f"llama.cpp /completion response missing 'content' key: {data!r}"
            )
        return content


class OpenAICompatibleClient(LocalLLMClient):
    """Client for local OpenAI-compatible chat completion servers."""

    def _headers(self, *, has_body: bool) -> dict[str, str]:
        headers: dict[str, str] = {}
        if has_body:
            headers["Content-Type"] = "application/json"
        if self._config.api_key:
            headers["Authorization"] = f"Bearer {self._config.api_key}"
        return headers

    def _make_request(
        self,
        endpoint: str,
        payload: dict[str, Any] | None = None,
        method: str = "POST",
    ) -> dict[str, Any]:
        url = f"{self._base_url()}{endpoint}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = self._headers(has_body=data is not None)
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(
                request,
                timeout=self._config.timeout_seconds,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if not isinstance(payload, dict):
                    raise AIGenerationError(
                        f"OpenAI-compatible server returned non-object JSON from {url!r}."
                    )
                return payload
        except TimeoutError as exc:
            raise AITimeoutError(
                f"OpenAI-compatible call to {url!r} timed out after "
                f"{self._config.timeout_seconds}s."
            ) from exc
        except urllib.error.URLError as exc:
            raise AIConnectionError(
                f"Cannot reach OpenAI-compatible server at {url!r}: {exc.reason}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise AIGenerationError(
                f"OpenAI-compatible server returned non-JSON from {url!r}: {exc}"
            ) from exc

    def is_available(self) -> bool:
        try:
            self._make_request("/v1/models", method="GET")
            return True
        except (AIConnectionError, AITimeoutError):
            return False

    def get_model_info(self) -> ModelInfo:
        try:
            data = self._make_request(f"/v1/models/{self._config.model}", method="GET")
            model_name = data.get("id")
        except (AIConnectionError, AIGenerationError, AttributeError):
            model_name = None

        base_url = self._config.base_url.lower()
        is_local = "localhost" in base_url or "127.0.0.1" in base_url
        return ModelInfo(
            provider_name="openai_compatible",
            model_name=str(model_name or self._config.model),
            is_local=is_local,
            version=None,
        )

    def generate(
        self,
        prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        payload: dict[str, Any] = {
            "model": self._config.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": self._eff_max_tokens(max_tokens),
            "temperature": self._eff_temperature(temperature),
            "stream": False,
        }
        data = self._make_request("/v1/chat/completions", payload)
        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise AIGenerationError(
                "OpenAI-compatible /v1/chat/completions response has unexpected "
                f"structure: {data!r}"
            ) from exc
        if not isinstance(content, str):
            raise AIGenerationError(
                "OpenAI-compatible /v1/chat/completions returned non-string content: "
                f"{data!r}"
            )
        return content


_PROVIDER_MAP: dict[str, type[LocalLLMClient]] = {
    "ollama": OllamaClient,
    "llamacpp": LlamaCppClient,
    "openai_compatible": OpenAICompatibleClient,
}

_DETECTION_ORDER: tuple[type[LocalLLMClient], ...] = (
    OllamaClient,
    LlamaCppClient,
    OpenAICompatibleClient,
)


def build_client(config: AIConfig) -> LocalLLMClient:
    """Instantiate the concrete client specified by config.provider."""
    if config.provider.lower() == "auto":
        return auto_detect_client(config)
    client_cls = _PROVIDER_MAP.get(config.provider.lower())
    if client_cls is None:
        valid = ", ".join(sorted([*list(_PROVIDER_MAP), "auto"]))
        raise AIConfigError(
            f"Unknown AI provider {config.provider!r}. Supported values: {valid}."
        )
    return client_cls(config)


def auto_detect_client(config: AIConfig) -> LocalLLMClient:
    """Return the first responding local client in the canonical detection order."""
    for client_cls in _DETECTION_ORDER:
        probe_config = AIConfig(
            provider={
                OllamaClient: "ollama",
                LlamaCppClient: "llamacpp",
                OpenAICompatibleClient: "openai_compatible",
            }[client_cls],
            model=config.model,
            base_url=config.base_url,
            api_key=config.api_key,
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            timeout_seconds=config.timeout_seconds,
        )
        client = client_cls(probe_config)
        logger.debug("Probing %s at %s", client_cls.__name__, config.base_url)
        if client.is_available():
            logger.info("Auto-detected LLM provider: %s", client_cls.__name__)
            return client
    raise AIConnectionError(
        f"No LLM provider responded at {config.base_url!r}. "
        "Make sure Ollama, llama.cpp server, or a local OpenAI-compatible server is running."
    )
