from typing import List, Optional, Dict, Any, Tuple
from backend.domain import SourceSystem, Mapping, SemanticEdge
from backend.engine.edge_index import EdgeIndex
from backend.core.constants import (
    CONFIDENCE_POSITIVE_PATTERN, CONFIDENCE_EXACT_FIELD, CONFIDENCE_PARTIAL_FIELD,
    CONFIDENCE_SYNONYM, CONFIDENCE_CONCEPT_IN_NAME, CONFIDENCE_CONTEXT_BOOST,
    CONFIDENCE_CONTEXT_CAP, CONFIDENCE_SEMANTIC_AMOUNT, CONFIDENCE_SEMANTIC_ID,
    AAM_EDGE_CONFIDENCE_MIN,
)
import re


class HeuristicMapper:
    
    NEGATIVE_PATTERNS = {
        'account': [
            r'^gl_',
            r'^general_ledger',
            r'_gl$',
            r'gl_account',
            r'ledger_account',
            r'chart_of_account',
            r'coa_',
            r'cofa_',
        ],
        'revenue': [
            r'^debit',
            r'^credit',
        ],
    }
    
    POSITIVE_PATTERNS = {
        'gl_account': [
            r'^gl_',
            r'gl_account',
            r'general_ledger',
            r'ledger_account',
            r'chart_of_account',
            r'coa_',
            r'cofa_',
        ],
        'currency': [
            r'currency$',
            r'currency_code',
            r'_currency$',
            r'^currency',
        ],
        'invoice': [
            r'invoice_',
            r'_invoice',
            r'invoice_number',
            r'invoice_id',
            r'invoicenumber',
        ],
        'subscription': [
            r'subscription_id',
            r'subscription_status',
            r'plan_id',
            r'plan_name',
            r'billing_period',
            r'^mrr$',
        ],
        'employee': [
            r'worker_id',
            r'employee_id',
            r'employee_name',
            r'position_id',
            r'hire_date',
            r'termination_date',
            r'headcount',
        ],
        'ticket': [
            r'ticket_id',
            r'ticket_number',
            r'satisfaction_rating',
            r'csat',
            r'first_response',
            r'resolution_time',
        ],
        'engineering_work': [
            r'issue_key',
            r'issue_id',
            r'sprint_id',
            r'sprint_name',
            r'story_points',
            r'issue_type',
        ],
        'incident': [
            r'incident_id',
            r'slo_id',
            r'slo_name',
            r'slo_target',
            r'mttr',
            r'mtta',
            r'severity',
        ],
        'date': [
            r'^date$',
            r'^timestamp$',
            r'^datetime$',
            r'created_at',
            r'updated_at',
            r'_date$',
            r'_time$',
            r'_at$',
        ],
        'ad': [
            r'^ad_id',
            r'^ad_name',
            r'^creative_id',
            r'^placement_id',
            r'_ad_',
        ],
    }
    
    FINANCIAL_TABLE_PATTERNS = [
        r'invoice',
        r'billing',
        r'payment',
        r'ledger',
        r'gl_',
        r'cost',
        r'expense',
        r'revenue',
        r'subscription',
        r'ar$',   # accounts receivable
        r'ap$',   # accounts payable
        r'rev.schedule',
    ]
    
    def __init__(self, ontology_concepts: List[Dict[str, Any]], edge_index: Optional[EdgeIndex] = None):
        self.concepts = ontology_concepts
        self._concept_by_id = {c['id']: c for c in ontology_concepts}
        self._edge_index = edge_index or EdgeIndex([])
        self.aam_edge_hits = 0
        self.aam_edge_misses = 0

    def create_mappings(self, sources: List[SourceSystem]) -> List[Mapping]:
        mappings = []

        for source in sources:
            for table in source.tables:
                table_context = self._get_table_context(table.name)

                # --- Pass 1: per-field candidates (Tier 0 AAM edge wins outright) ---
                edge_by_field: Dict[str, Mapping] = {}
                ranked_by_field: Dict[str, List[Tuple[Dict[str, Any], float]]] = {}
                for field in table.fields:
                    edge_mapping = self._try_aam_edge(source.id, table.name, field.name)
                    if edge_mapping:
                        self.aam_edge_hits += 1
                        edge_by_field[field.name] = edge_mapping
                        continue
                    self.aam_edge_misses += 1
                    ranked_by_field[field.name] = self._rank_field_candidates(
                        field.name, field.semantic_hint or "", field.type,
                        table.name, table_context,
                    )

                # --- Pipe co-occurrence: a domain is "native" to this pipe when a
                # field maps unambiguously into it (all that field's candidates
                # share one ontology domain). Anchors like customer_name->customer
                # (sales) and currency->currency (finance) establish the pipe's
                # context. Ambiguous fields are then routed by that context, not by
                # the raw name score — so amount_usd in an orders pipe binds to
                # revenue (finance, native here), not cloud_spend (foreign here). ---
                native_domains = self._native_domains(ranked_by_field)

                # --- Pass 2: emit, disambiguating each field against the context ---
                for field in table.fields:
                    if field.name in edge_by_field:
                        mappings.append(edge_by_field[field.name])
                        continue
                    ranked = ranked_by_field.get(field.name) or []
                    if not ranked:
                        continue
                    concept, confidence = self._resolve_by_context(ranked, native_domains)
                    mappings.append(Mapping(
                        id=f"{source.id}_{table.name}_{field.name}_{concept['id']}",
                        source_field=field.name,
                        source_table=table.name,
                        source_system=source.id,
                        ontology_concept=concept['id'],
                        confidence=confidence,
                        method="heuristic",
                        status="ok",
                    ))

        return mappings

    @staticmethod
    def _native_domains(
        ranked_by_field: Dict[str, List[Tuple[Dict[str, Any], float]]]
    ) -> set:
        """Ontology domains this pipe unambiguously contains.

        A field anchors a domain when every one of its candidates resolves to
        that single domain (no cross-domain contest). Those anchors are the
        pipe's context; ambiguous fields are read against them.
        """
        native = set()
        for ranked in ranked_by_field.values():
            domains = {(c.get('domain') or '') for c, _ in ranked}
            domains.discard('')
            if len(domains) == 1:
                native |= domains
        return native

    @staticmethod
    def _resolve_by_context(
        ranked: List[Tuple[Dict[str, Any], float]],
        native_domains: set,
    ) -> Tuple[Dict[str, Any], float]:
        """Pick a field's concept, letting pipe context override the name score.

        Default to the top name-scored candidate. But if that candidate's domain
        is foreign to the pipe (not among native_domains) while a lower-scored
        candidate sits in a native domain, the native candidate wins — the field
        belongs to the pipe's context, not to whatever its name matches globally.
        With no native context (empty set) the name score stands unchanged.
        """
        top_concept, top_conf = ranked[0]
        top_domain = top_concept.get('domain') or ''
        if top_domain and native_domains and top_domain not in native_domains:
            for concept, conf in ranked:
                if (concept.get('domain') or '') in native_domains:
                    return concept, conf
        return top_concept, top_conf

    def _try_aam_edge(
        self, system_id: str, table_name: str, field_name: str
    ) -> Optional[Mapping]:
        """Tier 0: check EdgeIndex for an AAM semantic edge."""
        if self._edge_index.empty:
            return None

        edge = self._edge_index.lookup(system_id, table_name, field_name)
        if edge is None or edge.confidence < AAM_EDGE_CONFIDENCE_MIN:
            return None

        concept_id = self._edge_to_concept(edge)
        return Mapping(
            id=f"{system_id}_{table_name}_{field_name}_{concept_id}",
            source_field=field_name,
            source_table=table_name,
            source_system=system_id,
            ontology_concept=concept_id,
            confidence=edge.confidence,
            method="aam_edge",
            status="ok",
            provenance=f"AAM {edge.fabric_plane}: {edge.extraction_source}",
            cross_system_mapping={
                "maps_to_system": edge.target_system,
                "maps_to_object": edge.target_object,
                "maps_to_field": edge.target_field,
                "edge_type": edge.edge_type,
                "transformation": edge.transformation,
            },
        )

    def _edge_to_concept(self, edge: SemanticEdge) -> str:
        """
        Resolve an AAM edge to an ontology concept ID.

        Uses lightweight lookups only (aliases, example_fields, concept IDs).
        Falls back to 'unclassified_but_mapped' if no concept matches cheaply.
        """
        # Check both field names from the edge against ontology
        candidates = [
            edge.source_field.lower(),
            edge.target_field.lower(),
            edge.source_object.lower(),
            edge.target_object.lower(),
        ]

        # Strategy 1: direct concept ID match
        for name in candidates:
            if name in self._concept_by_id:
                return name

        # Strategy 2: check aliases and example_fields
        for concept in self.concepts:
            concept_id = concept["id"]
            aliases = [a.lower() for a in concept.get("aliases", [])]
            examples = [e.lower() for e in concept.get("example_fields", [])]

            for name in candidates:
                if name in aliases or name in examples:
                    return concept_id
                # Substring: concept ID in field name or field name in concept ID
                if len(concept_id) >= 3 and concept_id in name:
                    return concept_id

        return "unclassified_but_mapped"
    
    def _get_table_context(self, table_name: str) -> str:
        table_lower = table_name.lower()
        for pattern in self.FINANCIAL_TABLE_PATTERNS:
            if re.search(pattern, table_lower):
                return "financial"
        if re.search(r'customer|contact|lead|account|opportunity', table_lower):
            return "crm"
        if re.search(r'resource|instance|host|service|aws|cloud', table_lower):
            return "infrastructure"
        if re.search(r'worker|employee|position|timeoff|wd[-_]', table_lower):
            return "hr"
        if re.search(r'ticket|zendesk|organization', table_lower):
            return "support"
        if re.search(r'issue|sprint|jira', table_lower):
            return "engineering"
        if re.search(r'incident|slo|datadog', table_lower):
            return "monitoring"
        return "general"
    
    def _is_blocked_by_negative_pattern(self, field_name: str, concept_id: str) -> bool:
        if concept_id not in self.NEGATIVE_PATTERNS:
            return False
        field_lower = field_name.lower()
        for pattern in self.NEGATIVE_PATTERNS[concept_id]:
            if re.search(pattern, field_lower):
                return True
        return False
    
    def _check_positive_patterns(self, field_name: str) -> Optional[str]:
        field_lower = field_name.lower()
        for concept_id, patterns in self.POSITIVE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, field_lower):
                    return concept_id
        return None
    
    def _match_field_to_concept(
        self,
        field_name: str,
        semantic_hint: str,
        field_type: str,
        table_name: str,
        table_context: str
    ) -> Tuple[Optional[Dict[str, Any]], float]:
        """Best single concept for a field by name/alias/pattern scoring.

        Thin wrapper over _rank_field_candidates returning its top candidate.
        Cross-field context disambiguation is applied by create_mappings, which
        consumes the full ranked candidate list directly.
        """
        ranked = self._rank_field_candidates(
            field_name, semantic_hint, field_type, table_name, table_context
        )
        return ranked[0] if ranked else (None, 0.0)

    def _rank_field_candidates(
        self,
        field_name: str,
        semantic_hint: str,
        field_type: str,
        table_name: str,
        table_context: str
    ) -> List[Tuple[Dict[str, Any], float]]:
        """All viable concept candidates for a field, highest confidence first.

        Pure name/alias/pattern scoring — it deliberately keeps EVERY positive
        candidate (not just the argmax) so create_mappings can break cross-domain
        ties with pipe co-occurrence. A field whose top name-match is contextually
        foreign (amount_usd -> cloud_spend in an orders pipe) still carries its
        in-context runner-up (revenue) here for context to choose.
        """
        field_lower = field_name.lower()

        # Positive patterns are authoritative — a single deterministic concept.
        positive_match = self._check_positive_patterns(field_name)
        if positive_match and positive_match in self._concept_by_id:
            return [(self._concept_by_id[positive_match], CONFIDENCE_POSITIVE_PATTERN)]

        scored: List[Tuple[Dict[str, Any], float]] = []
        for concept in self.concepts:
            concept_id = concept['id']

            if self._is_blocked_by_negative_pattern(field_name, concept_id):
                continue

            example_fields = concept.get('example_fields', [])
            synonyms = concept.get('aliases', [])

            match_confidence = 0.0

            for example in example_fields:
                example_lower = example.lower()
                if example_lower == field_lower:
                    match_confidence = max(match_confidence, CONFIDENCE_EXACT_FIELD)
                elif len(field_lower) >= 3 and (example_lower in field_lower or field_lower in example_lower):
                    match_confidence = max(match_confidence, CONFIDENCE_PARTIAL_FIELD)

            for synonym in synonyms:
                synonym_lower = synonym.lower()
                if synonym_lower in field_lower or field_lower in synonym_lower:
                    match_confidence = max(match_confidence, CONFIDENCE_SYNONYM)

            if len(concept_id) >= 3 and concept_id in field_lower:
                match_confidence = max(match_confidence, CONFIDENCE_CONCEPT_IN_NAME)

            if match_confidence > 0 and table_context == "financial":
                if concept_id in ['revenue', 'cost', 'invoice', 'currency', 'date', 'subscription']:
                    match_confidence = min(match_confidence + CONFIDENCE_CONTEXT_BOOST, CONFIDENCE_CONTEXT_CAP)
            if match_confidence > 0 and table_context == "hr":
                if concept_id in ['employee', 'date']:
                    match_confidence = min(match_confidence + CONFIDENCE_CONTEXT_BOOST, CONFIDENCE_CONTEXT_CAP)
            if match_confidence > 0 and table_context == "support":
                if concept_id in ['ticket', 'health', 'account']:
                    match_confidence = min(match_confidence + CONFIDENCE_CONTEXT_BOOST, CONFIDENCE_CONTEXT_CAP)
            if match_confidence > 0 and table_context == "engineering":
                if concept_id in ['engineering_work', 'date']:
                    match_confidence = min(match_confidence + CONFIDENCE_CONTEXT_BOOST, CONFIDENCE_CONTEXT_CAP)
            if match_confidence > 0 and table_context == "monitoring":
                if concept_id in ['incident', 'health', 'aws_resource']:
                    match_confidence = min(match_confidence + CONFIDENCE_CONTEXT_BOOST, CONFIDENCE_CONTEXT_CAP)

            if match_confidence > 0:
                scored.append((concept, match_confidence))

        if scored:
            scored.sort(key=lambda pair: pair[1], reverse=True)
            return scored

        # Semantic-hint fallback — only when nothing name-matched.
        if semantic_hint == "amount" and table_context == "financial":
            for c in ['revenue', 'cost']:
                if c in self._concept_by_id:
                    return [(self._concept_by_id[c], CONFIDENCE_SEMANTIC_AMOUNT)]
        if semantic_hint == "id":
            if "account" in field_lower and not self._is_blocked_by_negative_pattern(field_name, "account"):
                if "account" in self._concept_by_id:
                    return [(self._concept_by_id["account"], CONFIDENCE_SEMANTIC_ID)]

        return []
