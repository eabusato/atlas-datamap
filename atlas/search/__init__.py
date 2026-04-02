"""Public exports for the Atlas search subsystem."""

from atlas.search.discovery import AtlasDiscovery, CandidateRef, DiscoveryResult
from atlas.search.qa import AtlasQA, QACandidate, QAResult
from atlas.search.textual import AtlasSearch
from atlas.search.types import EntityType, SearchResult
from atlas.search.vector import VectorCandidate, VectorIndexEntry, VectorSearch

__all__ = [
    "AtlasDiscovery",
    "AtlasQA",
    "AtlasSearch",
    "CandidateRef",
    "DiscoveryResult",
    "EntityType",
    "QACandidate",
    "QAResult",
    "SearchResult",
    "VectorCandidate",
    "VectorIndexEntry",
    "VectorSearch",
]
