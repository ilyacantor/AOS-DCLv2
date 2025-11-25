"""
Mapping Evaluator - Identifies obvious mismappings in field-concept mappings.
Run after each pipeline to flag semantic errors.
"""

from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
import re


@dataclass
class MappingIssue:
    source_id: str
    table_name: str
    field_name: str
    current_concept: str
    issue_type: str
    severity: str
    suggested_concept: str
    explanation: str


class MappingEvaluator:
    
    MISMAP_RULES = [
        {
            'name': 'GL_to_customer_account',
            'field_pattern': r'(^gl_|_gl$|general_ledger|ledger_account|chart_of_account|coa_|cofa_)',
            'blocked_concepts': ['account'],
            'suggested_concept': 'gl_account',
            'severity': 'high',
            'explanation': 'General Ledger (GL) fields should map to gl_account, not customer account'
        },
        {
            'name': 'currency_field_mismap',
            'field_pattern': r'(currency$|currency_code|_currency$|^currency)',
            'blocked_concepts': ['opportunity', 'account', 'revenue'],
            'suggested_concept': 'currency',
            'severity': 'high',
            'explanation': 'Currency fields should map to currency concept'
        },
        {
            'name': 'invoice_field_mismap',
            'field_pattern': r'(invoice_number|invoicenumber|invoice_id)',
            'blocked_concepts': ['account', 'opportunity', 'revenue'],
            'suggested_concept': 'invoice',
            'severity': 'medium',
            'explanation': 'Invoice identifier fields should map to invoice concept'
        },
        {
            'name': 'debit_credit_to_revenue',
            'field_pattern': r'(^debit|^credit|debit_amount|credit_amount)',
            'blocked_concepts': ['revenue'],
            'suggested_concept': 'cost',
            'severity': 'medium',
            'explanation': 'Debit/credit fields are accounting entries, not revenue'
        },
        {
            'name': 'generic_id_to_account',
            'field_pattern': r'^id$',
            'blocked_concepts': ['account', 'currency'],
            'suggested_concept': None,
            'severity': 'low',
            'explanation': 'Generic "id" field is too ambiguous to map confidently'
        },
        {
            'name': 'aws_account_context',
            'field_pattern': r'aws_account',
            'blocked_concepts': [],
            'required_concept': 'account',
            'severity': 'info',
            'explanation': 'AWS account is a valid account mapping but semantically different from customer account'
        },
        {
            'name': 'unit_fields_mismap',
            'field_pattern': r'^unit$|^unit_',
            'blocked_concepts': ['opportunity', 'revenue'],
            'suggested_concept': 'usage',
            'severity': 'low',
            'explanation': 'Unit fields typically relate to usage metrics, not opportunities'
        },
    ]
    
    def __init__(self):
        self.issues: List[MappingIssue] = []
    
    def evaluate_mappings(self, mappings: List[Dict[str, Any]]) -> List[MappingIssue]:
        self.issues = []
        
        for mapping in mappings:
            field_name = mapping.get('field_name', mapping.get('source_field', ''))
            concept_id = mapping.get('concept_id', mapping.get('ontology_concept', ''))
            source_id = mapping.get('source_id', mapping.get('source_system', ''))
            table_name = mapping.get('table_name', mapping.get('source_table', ''))
            
            for rule in self.MISMAP_RULES:
                if re.search(rule['field_pattern'], field_name.lower()):
                    if 'blocked_concepts' in rule and concept_id in rule['blocked_concepts']:
                        issue = MappingIssue(
                            source_id=source_id,
                            table_name=table_name,
                            field_name=field_name,
                            current_concept=concept_id,
                            issue_type=rule['name'],
                            severity=rule['severity'],
                            suggested_concept=rule.get('suggested_concept', 'unknown'),
                            explanation=rule['explanation']
                        )
                        self.issues.append(issue)
        
        return self.issues
    
    def get_summary(self) -> Dict[str, Any]:
        high_count = sum(1 for i in self.issues if i.severity == 'high')
        medium_count = sum(1 for i in self.issues if i.severity == 'medium')
        low_count = sum(1 for i in self.issues if i.severity == 'low')
        
        return {
            'total_issues': len(self.issues),
            'high_severity': high_count,
            'medium_severity': medium_count,
            'low_severity': low_count,
            'issues_by_type': self._group_by_type(),
            'pass': high_count == 0
        }
    
    def _group_by_type(self) -> Dict[str, int]:
        counts = {}
        for issue in self.issues:
            counts[issue.issue_type] = counts.get(issue.issue_type, 0) + 1
        return counts
    
    def format_report(self) -> str:
        if not self.issues:
            return "No mapping issues detected. All mappings passed evaluation."
        
        summary = self.get_summary()
        lines = [
            "=" * 60,
            "MAPPING EVALUATION REPORT",
            "=" * 60,
            f"Total Issues: {summary['total_issues']}",
            f"  High Severity: {summary['high_severity']}",
            f"  Medium Severity: {summary['medium_severity']}",
            f"  Low Severity: {summary['low_severity']}",
            "",
            "Issues by Type:",
        ]
        
        for issue_type, count in summary['issues_by_type'].items():
            lines.append(f"  {issue_type}: {count}")
        
        lines.append("")
        lines.append("-" * 60)
        lines.append("DETAILED ISSUES:")
        lines.append("-" * 60)
        
        for issue in sorted(self.issues, key=lambda x: (x.severity != 'high', x.severity != 'medium', x.source_id)):
            lines.append(f"\n[{issue.severity.upper()}] {issue.issue_type}")
            lines.append(f"  Field: {issue.source_id}.{issue.table_name}.{issue.field_name}")
            lines.append(f"  Current Mapping: {issue.current_concept}")
            if issue.suggested_concept:
                lines.append(f"  Suggested: {issue.suggested_concept}")
            lines.append(f"  Reason: {issue.explanation}")
        
        lines.append("")
        lines.append("=" * 60)
        status = "PASSED" if summary['pass'] else "FAILED"
        lines.append(f"EVALUATION STATUS: {status}")
        lines.append("=" * 60)
        
        return "\n".join(lines)


def evaluate_from_database() -> Tuple[List[MappingIssue], str]:
    """Fetch mappings from database and evaluate them."""
    import os
    import psycopg2
    
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        return [], "ERROR: DATABASE_URL not set"
    
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT source_id, table_name, field_name, concept_id, confidence
        FROM field_concept_mappings
    """)
    
    mappings = []
    for row in cur.fetchall():
        mappings.append({
            'source_id': row[0],
            'table_name': row[1],
            'field_name': row[2],
            'concept_id': row[3],
            'confidence': row[4]
        })
    
    cur.close()
    conn.close()
    
    evaluator = MappingEvaluator()
    issues = evaluator.evaluate_mappings(mappings)
    report = evaluator.format_report()
    
    return issues, report


if __name__ == "__main__":
    issues, report = evaluate_from_database()
    print(report)
