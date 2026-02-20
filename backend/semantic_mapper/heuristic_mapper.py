from typing import List, Optional, Dict, Any, Tuple
from backend.domain import SourceSystem, Mapping
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
    ]
    
    def __init__(self, ontology_concepts: List[Dict[str, Any]]):
        self.concepts = ontology_concepts
        self._concept_by_id = {c['id']: c for c in ontology_concepts}
    
    def create_mappings(self, sources: List[SourceSystem]) -> List[Mapping]:
        mappings = []
        
        for source in sources:
            for table in source.tables:
                table_context = self._get_table_context(table.name)
                
                for field in table.fields:
                    matched_concept, confidence = self._match_field_to_concept(
                        field.name,
                        field.semantic_hint or "",
                        field.type,
                        table.name,
                        table_context
                    )
                    
                    if matched_concept:
                        mapping = Mapping(
                            id=f"{source.id}_{table.name}_{field.name}_{matched_concept['id']}",
                            source_field=field.name,
                            source_table=table.name,
                            source_system=source.id,
                            ontology_concept=matched_concept['id'],
                            confidence=confidence,
                            method="heuristic",
                            status="ok"
                        )
                        mappings.append(mapping)
        
        return mappings
    
    def _get_table_context(self, table_name: str) -> str:
        table_lower = table_name.lower()
        for pattern in self.FINANCIAL_TABLE_PATTERNS:
            if re.search(pattern, table_lower):
                return "financial"
        if re.search(r'customer|contact|lead|account', table_lower):
            return "crm"
        if re.search(r'resource|instance|host|service', table_lower):
            return "infrastructure"
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
        field_lower = field_name.lower()
        
        positive_match = self._check_positive_patterns(field_name)
        if positive_match and positive_match in self._concept_by_id:
            return self._concept_by_id[positive_match], 0.95
        
        best_match = None
        best_confidence = 0.0
        
        for concept in self.concepts:
            concept_id = concept['id']
            
            if self._is_blocked_by_negative_pattern(field_name, concept_id):
                continue
            
            metadata = concept.get('metadata', {})
            example_fields = metadata.get('example_fields', [])
            synonyms = metadata.get('aliases', metadata.get('synonyms', []))
            
            match_confidence = 0.0
            
            for example in example_fields:
                example_lower = example.lower()
                if example_lower == field_lower:
                    match_confidence = max(match_confidence, 0.95)
                elif example_lower in field_lower or field_lower in example_lower:
                    match_confidence = max(match_confidence, 0.75)
            
            for synonym in synonyms:
                synonym_lower = synonym.lower()
                if synonym_lower in field_lower or field_lower in synonym_lower:
                    match_confidence = max(match_confidence, 0.70)
            
            if concept_id in field_lower:
                match_confidence = max(match_confidence, 0.80)
            
            if match_confidence > 0 and table_context == "financial":
                if concept_id in ['revenue', 'cost', 'invoice', 'currency', 'date']:
                    match_confidence = min(match_confidence + 0.05, 0.95)
            
            if match_confidence > best_confidence:
                best_confidence = match_confidence
                best_match = concept
        
        if best_match is None and semantic_hint:
            if semantic_hint == "amount":
                if table_context == "financial":
                    for c in ['revenue', 'cost']:
                        if c in self._concept_by_id:
                            return self._concept_by_id[c], 0.65
            
            if semantic_hint == "id":
                if "account" in field_lower and not self._is_blocked_by_negative_pattern(field_name, "account"):
                    if "account" in self._concept_by_id:
                        return self._concept_by_id["account"], 0.60
        
        return best_match, best_confidence
