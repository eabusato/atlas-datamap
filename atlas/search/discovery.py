"""Heuristic domain discovery for Atlas metadata."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from atlas.analysis.scorer import ScoreBreakdown, TableScorer
from atlas.search.textual import AtlasSearch
from atlas.types import IntrospectionResult

STOP_WORDS: frozenset[str] = frozenset(
    {
        "o",
        "a",
        "os",
        "as",
        "um",
        "uma",
        "uns",
        "umas",
        "onde",
        "fica",
        "ficam",
        "esta",
        "estao",
        "estão",
        "de",
        "do",
        "da",
        "dos",
        "das",
        "em",
        "no",
        "na",
        "nos",
        "nas",
        "por",
        "para",
        "com",
        "sem",
        "sobre",
        "entre",
        "ate",
        "até",
        "qual",
        "quais",
        "que",
        "quem",
        "como",
        "quando",
        "tabela",
        "tabelas",
        "coluna",
        "colunas",
        "banco",
        "schema",
        "base",
        "dados",
        "me",
        "lhe",
        "se",
        "e",
        "ou",
        "mas",
        "pois",
        "porque",
        "eh",
        "é",
        "sao",
        "são",
        "foi",
        "eram",
        "the",
        "an",
        "is",
        "are",
        "was",
        "were",
        "be",
        "have",
        "has",
        "had",
        "does",
        "did",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "from",
        "with",
        "by",
        "and",
        "or",
        "but",
        "not",
        "where",
        "what",
        "which",
        "who",
        "how",
        "when",
        "table",
        "tables",
        "column",
        "columns",
        "database",
        "find",
        "show",
        "get",
        "list",
        "search",
    }
)

HEURISTIC_MAP: dict[str, list[str]] = {
    "cliente": ["customer", "client", "usuario", "user", "cliente", "person", "conta"],
    "pedido": ["order", "pedido", "compra", "purchase", "sale", "venda", "request"],
    "pagamento": [
        "payment",
        "pagamento",
        "fatura",
        "invoice",
        "transaction",
        "cobranca",
        "billing",
        "charge",
        "receipt",
    ],
    "produto": [
        "product",
        "produto",
        "item",
        "sku",
        "catalog",
        "catalogo",
        "goods",
        "merchandise",
        "stock",
    ],
    "historico": [
        "log",
        "history",
        "historico",
        "audit",
        "auditoria",
        "trace",
        "tracking",
        "changelog",
        "record",
        "event",
    ],
    "estoque": ["inventory", "estoque", "stock", "warehouse", "armazem", "supply"],
    "usuario": ["user", "usuario", "account", "conta", "profile", "perfil", "member", "login"],
    "empresa": ["company", "empresa", "organization", "org", "tenant", "client", "business", "corporacao"],
    "contrato": ["contract", "contrato", "agreement", "accord", "deal", "term"],
    "fatura": ["invoice", "fatura", "bill", "receipt", "nota", "nfe", "fiscal"],
    "envio": [
        "shipping",
        "envio",
        "delivery",
        "entrega",
        "logistic",
        "logistica",
        "dispatch",
        "freight",
        "carrier",
    ],
    "endereco": ["address", "endereco", "location", "localizacao", "zip", "cep", "postal", "geo"],
    "categoria": ["category", "categoria", "group", "grupo", "class", "classe", "segment", "segmento", "type"],
    "permissao": ["permission", "permissao", "role", "papel", "acl", "grant", "privilege", "access", "auth"],
    "sessao": ["session", "sessao", "token", "jwt", "refresh", "credential", "auth_token"],
    "relatorio": ["report", "relatorio", "summary", "sumario", "analytics", "dashboard", "metric", "indicador"],
    "config": ["config", "configuracao", "setting", "parametro", "parameter", "preference", "option"],
    "audit": ["audit", "auditoria", "log", "trail", "history", "historico", "change", "revision"],
    "evento": ["event", "evento", "activity", "atividade", "action", "acao", "occurrence", "webhook"],
    "item": ["item", "line", "linha", "detail", "detalhe", "entry", "entrada", "row", "record"],
}

_CONFIDENCE_SATURATOR = 40.0
_MAX_CANDIDATES = 5
_TOPOLOGY_BONUS = 0.5
_RE_TOKEN = re.compile(r"[\W_]+", re.UNICODE)


@dataclass(frozen=True, slots=True)
class CandidateRef:
    """Reference to a likely table candidate."""

    schema: str
    table: str
    score: float
    justification: str
    rank: int = 0
    breakdown: ScoreBreakdown | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.table}"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "table": self.table,
            "qualified_name": self.qualified_name,
            "score": round(self.score, 4),
            "justification": self.justification,
            "rank": self.rank,
            "breakdown": self.breakdown.to_dict() if self.breakdown is not None else None,
        }


@dataclass(slots=True)
class DiscoveryResult:
    """Heuristic discovery answer for a user question."""

    question: str
    candidates: list[CandidateRef]
    reasoning: str
    confidence: float

    def to_dict(self) -> dict[str, object]:
        return {
            "question": self.question,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "reasoning": self.reasoning,
            "confidence": round(self.confidence, 4),
        }


@dataclass(slots=True)
class _AccumulatedCandidate:
    schema: str
    table: str
    score: float = 0.0
    concepts: set[str] = field(default_factory=set)
    matched_terms: set[str] = field(default_factory=set)
    topology_bonus: bool = False

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.table}"


def _plural_variants(token: str) -> set[str]:
    variants = {token}
    if not token.endswith("s"):
        variants.add(f"{token}s")
    return variants


_HEURISTIC_ALIASES: dict[str, str] = {}
for _concept, _terms in HEURISTIC_MAP.items():
    for _variant in _plural_variants(_concept):
        _HEURISTIC_ALIASES.setdefault(_variant, _concept)
    for _term in _terms:
        for _variant in _plural_variants(_term):
            _HEURISTIC_ALIASES.setdefault(_variant, _concept)


class AtlasDiscovery:
    """Heuristic assisted discovery over an introspected database graph."""

    def __init__(self, result: IntrospectionResult) -> None:
        self._result = result
        self._search = AtlasSearch(result)

    def find_likely_location(self, question: str) -> DiscoveryResult:
        """Return the best table candidates for a near-natural-language question."""

        tokens = self._extract_intent_tokens(question)
        expanded = self._expand_tokens(tokens)
        candidates = self._search_candidates(expanded)
        self._apply_topology_bonus(candidates)
        return self._rank_and_synthesize(question, tokens, expanded, candidates)

    def _extract_intent_tokens(self, question: str) -> list[str]:
        """Tokenize the question while removing bilingual stop words."""

        seen: set[str] = set()
        tokens: list[str] = []
        for raw in _RE_TOKEN.sub(" ", question.casefold()).split():
            if len(raw) <= 1 or raw in STOP_WORDS or raw in seen:
                continue
            seen.add(raw)
            tokens.append(raw)
        return tokens

    def _expand_tokens(self, tokens: list[str]) -> dict[str, list[str]]:
        """Expand user intent tokens into known business-domain concepts."""

        expanded: dict[str, list[str]] = {}
        for token in tokens:
            concept = self._resolve_concept(token)
            if concept is None:
                expanded[token] = [token]
                continue
            synonyms = [concept, *HEURISTIC_MAP[concept]]
            expanded[concept] = list(dict.fromkeys(synonyms))
        return expanded

    def _resolve_concept(self, token: str) -> str | None:
        """Map a token or synonym to a canonical heuristic concept."""

        if token in _HEURISTIC_ALIASES:
            return _HEURISTIC_ALIASES[token]
        if token.endswith("es") and token[:-2] in _HEURISTIC_ALIASES:
            return _HEURISTIC_ALIASES[token[:-2]]
        if token.endswith("s") and token[:-1] in _HEURISTIC_ALIASES:
            return _HEURISTIC_ALIASES[token[:-1]]
        return None

    def _search_candidates(self, expanded: dict[str, list[str]]) -> dict[str, _AccumulatedCandidate]:
        """Search candidate tables for every expanded concept or raw token."""

        candidates: dict[str, _AccumulatedCandidate] = {}
        for concept, terms in expanded.items():
            for term in terms:
                for result in self._search.search_tables(term):
                    key = result.qualified_name
                    candidate = candidates.setdefault(
                        key,
                        _AccumulatedCandidate(schema=result.schema, table=result.table or ""),
                    )
                    candidate.score += result.score
                    candidate.concepts.add(concept)
                    candidate.matched_terms.add(term)
        return candidates

    def _apply_topology_bonus(
        self,
        candidates: dict[str, _AccumulatedCandidate],
    ) -> dict[str, _AccumulatedCandidate]:
        """Boost tables that act as FK hubs among the current candidates."""

        candidate_names = set(candidates)
        for qualified_name, candidate in candidates.items():
            inbound = self._result.fk_in_degree_map.get(qualified_name, [])
            if any(referrer in candidate_names for referrer in inbound):
                candidate.score *= 1.0 + _TOPOLOGY_BONUS
                candidate.topology_bonus = True
        return candidates

    def _rank_and_synthesize(
        self,
        question: str,
        tokens: list[str],
        expanded: dict[str, list[str]],
        candidates: dict[str, _AccumulatedCandidate],
    ) -> DiscoveryResult:
        """Rank candidates and synthesize a human-readable explanation."""

        ranked = sorted(
            candidates.values(),
            key=lambda item: (-item.score, item.schema, item.table),
        )[:_MAX_CANDIDATES]

        scorer = TableScorer(self._result)
        candidate_refs: list[CandidateRef] = []
        for rank, item in enumerate(ranked, start=1):
            table = self._result.get_table(item.schema, item.table)
            table_score = scorer.score_table(table) if table is not None else None
            candidate_refs.append(
                CandidateRef(
                    schema=item.schema,
                    table=item.table,
                    score=round(item.score, 4),
                    justification=self._build_justification(item),
                    rank=rank,
                    breakdown=table_score.breakdown if table_score is not None else None,
                )
            )

        confidence = 0.0
        if ranked:
            confidence = min(1.0, ranked[0].score / _CONFIDENCE_SATURATOR)

        return DiscoveryResult(
            question=question,
            candidates=candidate_refs,
            reasoning=self._build_reasoning(tokens, expanded, candidate_refs),
            confidence=round(confidence, 4),
        )

    def _build_justification(self, candidate: _AccumulatedCandidate) -> str:
        parts = [
            "concepts: " + ", ".join(sorted(candidate.concepts)),
            "terms: " + ", ".join(sorted(candidate.matched_terms)),
        ]
        if candidate.topology_bonus:
            parts.append("hub FK bonus")
        return "; ".join(parts)

    def _build_reasoning(
        self,
        tokens: list[str],
        expanded: dict[str, list[str]],
        candidates: list[CandidateRef],
    ) -> str:
        if not candidates:
            return (
                "No strong candidate tables were found. "
                f"Extracted terms: {', '.join(tokens) if tokens else 'none'}."
            )

        expanded_parts = [
            f"{concept} -> {', '.join(terms[:4])}"
            for concept, terms in expanded.items()
        ]
        top = ", ".join(
            f"{candidate.qualified_name} ({candidate.score:.1f})" for candidate in candidates[:3]
        )
        return (
            f"Extracted terms: {', '.join(tokens)}. "
            f"Expanded concepts: {' | '.join(expanded_parts)}. "
            f"Top candidates: {top}."
        )
