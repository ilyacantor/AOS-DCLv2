"""
LLM-based Mapping Validator for Prod mode.
Uses OpenAI to validate and correct ambiguous mappings.
"""

import os
import json
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass


@dataclass
class ValidationResult:
    field_name: str
    table_name: str
    source_id: str
    original_concept: str
    validated_concept: str
    confidence: float
    was_corrected: bool
    reasoning: str


class MappingValidator:
    
    VALIDATION_PROMPT = """You are an expert data engineer validating field-to-ontology mappings.

Given a field from a source system, determine if the current mapping is correct.

Field Information:
- Field Name: {field_name}
- Table Name: {table_name}
- Source System: {source_id}
- Current Mapping: {current_concept}
- Confidence: {confidence}

Available Ontology Concepts:
{concepts_list}

Rules for validation:
1. GL_ACCOUNT, ledger_account, chart_of_account fields should map to "gl_account" (General Ledger), NOT "account" (Customer)
2. Currency fields (currency, currency_code) should map to "currency", NOT "opportunity" or "revenue"
3. Invoice fields (invoice_number, invoiceId) should map to "invoice"
4. Customer/Account ID fields should map to "account" (Customer Account)
5. Date fields should map to "date"
6. Amount/revenue fields should map to "revenue" or "cost" based on context
7. Consider the table name for context (e.g., INVOICES table = financial context)

Respond in JSON format:
{{
    "is_correct": true/false,
    "correct_concept": "concept_id",
    "confidence": 0.0-1.0,
    "reasoning": "brief explanation"
}}
"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.client = None
        self._init_client()
    
    def _init_client(self):
        if not self.api_key:
            return
        
        try:
            from openai import OpenAI
            
            base_url = os.getenv('AI_INTEGRATIONS_OPENAI_BASE_URL')
            if base_url:
                self.client = OpenAI(
                    api_key=os.getenv('AI_INTEGRATIONS_OPENAI_API_KEY', self.api_key),
                    base_url=base_url
                )
            else:
                self.client = OpenAI(api_key=self.api_key)
        except ImportError:
            pass
    
    def is_available(self) -> bool:
        return self.client is not None
    
    def validate_mapping(
        self,
        field_name: str,
        table_name: str,
        source_id: str,
        current_concept: str,
        confidence: float,
        ontology_concepts: List[Dict[str, Any]]
    ) -> ValidationResult:
        
        if not self.is_available():
            return ValidationResult(
                field_name=field_name,
                table_name=table_name,
                source_id=source_id,
                original_concept=current_concept,
                validated_concept=current_concept,
                confidence=confidence,
                was_corrected=False,
                reasoning="LLM not available - keeping original mapping"
            )
        
        concepts_list = "\n".join([
            f"- {c['id']}: {c.get('name', c['id'])} - {c.get('description', '')}"
            for c in ontology_concepts
        ])
        
        prompt = self.VALIDATION_PROMPT.format(
            field_name=field_name,
            table_name=table_name,
            source_id=source_id,
            current_concept=current_concept,
            confidence=confidence,
            concepts_list=concepts_list
        )
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                max_tokens=500,
                temperature=0.1
            )
            
            result = json.loads(response.choices[0].message.content)
            
            was_corrected = not result.get('is_correct', True)
            validated_concept = result.get('correct_concept', current_concept)
            
            return ValidationResult(
                field_name=field_name,
                table_name=table_name,
                source_id=source_id,
                original_concept=current_concept,
                validated_concept=validated_concept,
                confidence=result.get('confidence', confidence),
                was_corrected=was_corrected,
                reasoning=result.get('reasoning', '')
            )
            
        except Exception as e:
            return ValidationResult(
                field_name=field_name,
                table_name=table_name,
                source_id=source_id,
                original_concept=current_concept,
                validated_concept=current_concept,
                confidence=confidence,
                was_corrected=False,
                reasoning=f"LLM error: {str(e)}"
            )
    
    def validate_batch(
        self,
        mappings: List[Dict[str, Any]],
        ontology_concepts: List[Dict[str, Any]],
        confidence_threshold: float = 0.85
    ) -> Tuple[List[ValidationResult], Dict[str, Any]]:
        
        results = []
        stats = {
            'total_validated': 0,
            'corrections_made': 0,
            'skipped_high_confidence': 0,
            'errors': 0
        }
        
        for mapping in mappings:
            confidence = mapping.get('confidence', 0)
            
            if confidence >= confidence_threshold:
                stats['skipped_high_confidence'] += 1
                continue
            
            result = self.validate_mapping(
                field_name=mapping.get('field_name', mapping.get('source_field', '')),
                table_name=mapping.get('table_name', mapping.get('source_table', '')),
                source_id=mapping.get('source_id', mapping.get('source_system', '')),
                current_concept=mapping.get('concept_id', mapping.get('ontology_concept', '')),
                confidence=confidence,
                ontology_concepts=ontology_concepts
            )
            
            results.append(result)
            stats['total_validated'] += 1
            
            if result.was_corrected:
                stats['corrections_made'] += 1
            
            if 'error' in result.reasoning.lower():
                stats['errors'] += 1
        
        return results, stats


def validate_mappings_prod_mode(
    mappings: List[Dict[str, Any]],
    ontology_concepts: List[Dict[str, Any]],
    narration_callback=None
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Main entry point for Prod mode validation.
    Returns corrected mappings and validation stats.
    """
    
    validator = MappingValidator()
    
    if not validator.is_available():
        if narration_callback:
            narration_callback("LLM validation not available - OPENAI_API_KEY not set")
        return mappings, {'llm_available': False}
    
    if narration_callback:
        narration_callback(f"Validating {len(mappings)} mappings with LLM...")
    
    results, stats = validator.validate_batch(mappings, ontology_concepts)
    
    corrected_mappings = []
    corrections_log = []
    
    mapping_by_key = {}
    for m in mappings:
        key = f"{m.get('source_id', m.get('source_system'))}_{m.get('table_name', m.get('source_table'))}_{m.get('field_name', m.get('source_field'))}"
        mapping_by_key[key] = m.copy()
    
    for result in results:
        key = f"{result.source_id}_{result.table_name}_{result.field_name}"
        if key in mapping_by_key and result.was_corrected:
            mapping_by_key[key]['concept_id'] = result.validated_concept
            mapping_by_key[key]['ontology_concept'] = result.validated_concept
            mapping_by_key[key]['confidence'] = result.confidence
            mapping_by_key[key]['method'] = 'llm_validated'
            corrections_log.append({
                'field': f"{result.source_id}.{result.table_name}.{result.field_name}",
                'from': result.original_concept,
                'to': result.validated_concept,
                'reason': result.reasoning
            })
    
    corrected_mappings = list(mapping_by_key.values())
    
    if narration_callback:
        narration_callback(f"LLM validation complete: {stats['corrections_made']} corrections made")
        for correction in corrections_log[:5]:
            narration_callback(f"  Corrected {correction['field']}: {correction['from']} -> {correction['to']}")
    
    stats['corrections_log'] = corrections_log
    stats['llm_available'] = True
    
    return corrected_mappings, stats
