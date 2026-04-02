"""Tests for the Phase 8A local LLM client abstraction."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from atlas.ai import (
    AIConfig,
    AIConfigError,
    AIConnectionError,
    AIGenerationError,
    AITimeoutError,
    LlamaCppClient,
    ModelInfo,
    OllamaClient,
    OpenAICompatibleClient,
    auto_detect_client,
    build_client,
)


def _make_mock_response(body: dict[str, Any] | bytes) -> MagicMock:
    response = MagicMock()
    response.read.return_value = (
        json.dumps(body).encode("utf-8") if isinstance(body, dict) else body
    )
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    return response


@pytest.fixture
def ollama_config() -> AIConfig:
    return AIConfig(
        provider="ollama",
        model="llama3",
        base_url="http://localhost:11434",
        temperature=0.1,
        max_tokens=300,
        timeout_seconds=5.0,
    )


@pytest.fixture
def llamacpp_config() -> AIConfig:
    return AIConfig(
        provider="llamacpp",
        model="mistral",
        base_url="http://localhost:8080",
        temperature=0.2,
        max_tokens=200,
        timeout_seconds=5.0,
    )


@pytest.fixture
def openai_config() -> AIConfig:
    return AIConfig(
        provider="openai_compatible",
        model="gpt-4o-mini",
        base_url="http://localhost:1234",
        temperature=0.0,
        max_tokens=250,
        timeout_seconds=5.0,
    )


@pytest.mark.integration
@pytest.mark.phase_8a
class TestAIConfig:
    def test_defaults_and_repr(self) -> None:
        config = AIConfig()
        assert config.provider == "auto"
        assert config.model == "llama3"
        assert config.timeout_seconds == 60.0
        assert "provider='auto'" in repr(config)

    def test_from_dict_ignores_unknown_keys(self) -> None:
        config = AIConfig.from_dict({"provider": "ollama", "unknown": "ignored"})
        assert config.provider == "ollama"

    def test_from_file_reads_ai_section(self, tmp_path: Path) -> None:
        config_path = tmp_path / "atlas.toml"
        config_path.write_text(
            "[ai]\nprovider = 'llamacpp'\nmodel = 'mistral'\nbase_url = 'http://127.0.0.1:8080'\n",
            encoding="utf-8",
        )
        config = AIConfig.from_file(config_path)
        assert config.provider == "llamacpp"
        assert config.model == "mistral"

    def test_from_file_raises_for_missing_path(self) -> None:
        with pytest.raises(AIConfigError, match="Configuration file not found"):
            AIConfig.from_file("tests/tmp/phase_8_missing_ai.toml")

    def test_invalid_numeric_values_raise(self) -> None:
        with pytest.raises(AIConfigError, match="max_tokens"):
            AIConfig(max_tokens=0)


@pytest.mark.integration
@pytest.mark.phase_8a
class TestModelInfo:
    def test_to_dict(self) -> None:
        info = ModelInfo("ollama", "llama3", True, "1.0")
        assert info.to_dict()["version"] == "1.0"


@pytest.mark.integration
@pytest.mark.phase_8a
class TestOllamaClient:
    @patch("urllib.request.urlopen")
    def test_is_available_true(self, mock_urlopen: MagicMock, ollama_config: AIConfig) -> None:
        mock_urlopen.return_value = _make_mock_response({"version": "0.1.30"})
        assert OllamaClient(ollama_config).is_available() is True

    @patch("urllib.request.urlopen")
    def test_generate_success(self, mock_urlopen: MagicMock, ollama_config: AIConfig) -> None:
        mock_urlopen.return_value = _make_mock_response({"response": '{"ok": true}'})
        assert OllamaClient(ollama_config).generate("prompt") == '{"ok": true}'

    @patch("urllib.request.urlopen")
    def test_generate_timeout_maps_to_ai_timeout(
        self, mock_urlopen: MagicMock, ollama_config: AIConfig
    ) -> None:
        mock_urlopen.side_effect = TimeoutError("timed out")
        with pytest.raises(AITimeoutError):
            OllamaClient(ollama_config).generate("prompt")


@pytest.mark.integration
@pytest.mark.phase_8a
class TestLlamaCppClient:
    @patch("urllib.request.urlopen")
    def test_is_available_accepts_loading_status(
        self, mock_urlopen: MagicMock, llamacpp_config: AIConfig
    ) -> None:
        mock_urlopen.return_value = _make_mock_response({"status": "loading model"})
        assert LlamaCppClient(llamacpp_config).is_available() is True

    @patch("urllib.request.urlopen")
    def test_generate_requires_content_key(
        self, mock_urlopen: MagicMock, llamacpp_config: AIConfig
    ) -> None:
        mock_urlopen.return_value = _make_mock_response({"stop": True})
        with pytest.raises(AIGenerationError, match="missing 'content' key"):
            LlamaCppClient(llamacpp_config).generate("prompt")

    @patch("urllib.request.urlopen")
    def test_get_model_info_uses_props(
        self, mock_urlopen: MagicMock, llamacpp_config: AIConfig
    ) -> None:
        mock_urlopen.return_value = _make_mock_response(
            {"default_generation_settings": {"model": "mistral-q4"}}
        )
        assert LlamaCppClient(llamacpp_config).get_model_info().model_name == "mistral-q4"


@pytest.mark.integration
@pytest.mark.phase_8a
class TestOpenAICompatibleClient:
    @patch("urllib.request.urlopen")
    def test_generate_extracts_choice_content(
        self, mock_urlopen: MagicMock, openai_config: AIConfig
    ) -> None:
        mock_urlopen.return_value = _make_mock_response(
            {"choices": [{"message": {"content": '{"hello": "world"}'}}]}
        )
        assert OpenAICompatibleClient(openai_config).generate("prompt") == '{"hello": "world"}'

    @patch("urllib.request.urlopen")
    def test_generate_sends_bearer_token_when_api_key_is_configured(
        self, mock_urlopen: MagicMock
    ) -> None:
        mock_urlopen.return_value = _make_mock_response(
            {"choices": [{"message": {"content": '{"hello": "world"}'}}]}
        )
        config = AIConfig(
            provider="openai_compatible",
            model="gpt-4o-mini",
            base_url="http://127.0.0.1:1234",
            api_key="secret-token",
        )

        OpenAICompatibleClient(config).generate("prompt")

        request = mock_urlopen.call_args.args[0]
        assert request.headers["Authorization"] == "Bearer secret-token"

    @patch("urllib.request.urlopen")
    def test_generate_invalid_structure_raises(
        self, mock_urlopen: MagicMock, openai_config: AIConfig
    ) -> None:
        mock_urlopen.return_value = _make_mock_response({"model": "broken"})
        with pytest.raises(AIGenerationError, match="unexpected structure"):
            OpenAICompatibleClient(openai_config).generate("prompt")

    @patch("urllib.request.urlopen")
    def test_model_info_non_local_host_marks_not_local(
        self, mock_urlopen: MagicMock
    ) -> None:
        mock_urlopen.return_value = _make_mock_response({"id": "gpt-4"})
        config = AIConfig(
            provider="openai_compatible",
            model="gpt-4",
            base_url="http://llm-gateway.internal:8080",
        )
        assert OpenAICompatibleClient(config).get_model_info().is_local is False


@pytest.mark.integration
@pytest.mark.phase_8a
class TestBuildAndAutoDetect:
    def test_build_client_returns_requested_class(
        self, ollama_config: AIConfig, llamacpp_config: AIConfig, openai_config: AIConfig
    ) -> None:
        assert isinstance(build_client(ollama_config), OllamaClient)
        assert isinstance(build_client(llamacpp_config), LlamaCppClient)
        assert isinstance(build_client(openai_config), OpenAICompatibleClient)

    def test_build_client_unknown_provider_raises(self) -> None:
        with pytest.raises(AIConfigError, match="Unknown AI provider"):
            build_client(AIConfig(provider="bogus"))

    @patch.object(OllamaClient, "is_available", return_value=False)
    @patch.object(LlamaCppClient, "is_available", return_value=True)
    def test_auto_detect_falls_back_to_llamacpp(
        self,
        _mock_llama: MagicMock,
        _mock_ollama: MagicMock,
    ) -> None:
        config = AIConfig(provider="auto", base_url="http://localhost:8080")
        assert isinstance(auto_detect_client(config), LlamaCppClient)

    @patch.object(OllamaClient, "is_available", return_value=False)
    @patch.object(LlamaCppClient, "is_available", return_value=False)
    @patch.object(OpenAICompatibleClient, "is_available", return_value=False)
    def test_auto_detect_raises_when_none_available(
        self,
        _mock_openai: MagicMock,
        _mock_llama: MagicMock,
        _mock_ollama: MagicMock,
    ) -> None:
        config = AIConfig(provider="auto", base_url="http://localhost:9999")
        with pytest.raises(AIConnectionError, match="No LLM provider responded"):
            auto_detect_client(config)
