"""Canonical types and error hierarchy for Atlas AI integration."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class AIError(Exception):
    """Base exception for all Atlas AI integration errors."""


class AIConfigError(AIError):
    """Raised when AIConfig is missing or contains invalid values."""


class AIConnectionError(AIError):
    """Raised when the LLM server is unreachable or refuses the connection."""


class AITimeoutError(AIConnectionError):
    """Raised when a generation call exceeds the configured timeout."""


class AIGenerationError(AIError):
    """Raised when the LLM returns an unexpected or unparseable response."""


@dataclass(slots=True)
class AIConfig:
    """Configuration for a local or OpenAI-compatible LLM provider."""

    provider: str = "auto"
    model: str = "llama3"
    base_url: str = "http://localhost:11434"
    api_key: str | None = None
    temperature: float = 0.1
    max_tokens: int = 300
    timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.temperature < 0.0:
            raise AIConfigError(f"'temperature' must be >= 0.0, got {self.temperature}.")
        if self.max_tokens < 1:
            raise AIConfigError(f"'max_tokens' must be >= 1, got {self.max_tokens}.")
        if self.timeout_seconds <= 0.0:
            raise AIConfigError(
                f"'timeout_seconds' must be > 0.0, got {self.timeout_seconds}."
            )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AIConfig:
        """Build an AIConfig from a plain dict."""
        fields = set(cls.__dataclass_fields__)
        kwargs = {key: value for key, value in data.items() if key in fields}
        return cls(**kwargs)

    @classmethod
    def from_file(cls, path: str | Path) -> AIConfig:
        """Load AIConfig from the [ai] section of an atlas.toml file."""
        config_path = Path(path)
        try:
            with config_path.open("rb") as handle:
                data = tomllib.load(handle)
        except FileNotFoundError as exc:
            raise AIConfigError(f"Configuration file not found: {config_path}.") from exc
        except Exception as exc:
            raise AIConfigError(f"Failed to parse TOML from {config_path}: {exc}") from exc

        ai_section = data.get("ai", {})
        if not isinstance(ai_section, dict):
            raise AIConfigError(f"Section [ai] in {config_path} must be a table.")
        if not ai_section:
            return cls()
        return cls.from_dict(ai_section)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "base_url": self.base_url,
            "api_key": self.api_key,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "timeout_seconds": self.timeout_seconds,
        }

    def __repr__(self) -> str:
        return (
            f"AIConfig(provider={self.provider!r}, model={self.model!r}, "
            f"base_url={self.base_url!r})"
        )


@dataclass(frozen=True, slots=True)
class ModelInfo:
    """Runtime information about a configured model."""

    provider_name: str
    model_name: str
    is_local: bool
    version: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider_name": self.provider_name,
            "model_name": self.model_name,
            "is_local": self.is_local,
            "version": self.version,
        }
