"""Local LLM integration for semantic enrichment."""

from atlas.ai.cache import SemanticCache
from atlas.ai.client import (
    LlamaCppClient,
    LocalLLMClient,
    OllamaClient,
    OpenAICompatibleClient,
    auto_detect_client,
    build_client,
)
from atlas.ai.embeddings import EmbeddingGenerator
from atlas.ai.enricher import ResponseParser, SemanticEnricher
from atlas.ai.prompts import COLUMN_PROMPT_TEMPLATE, TABLE_PROMPT_TEMPLATE
from atlas.ai.sampler import SamplePreparer
from atlas.ai.types import (
    AIConfig,
    AIConfigError,
    AIConnectionError,
    AIError,
    AIGenerationError,
    AITimeoutError,
    ModelInfo,
)

__all__ = [
    "AIConfig",
    "AIConfigError",
    "AIConnectionError",
    "AIError",
    "AIGenerationError",
    "AITimeoutError",
    "LlamaCppClient",
    "LocalLLMClient",
    "ModelInfo",
    "OllamaClient",
    "OpenAICompatibleClient",
    "ResponseParser",
    "SamplePreparer",
    "SemanticCache",
    "SemanticEnricher",
    "COLUMN_PROMPT_TEMPLATE",
    "EmbeddingGenerator",
    "TABLE_PROMPT_TEMPLATE",
    "auto_detect_client",
    "build_client",
]
