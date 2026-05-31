"""SE-path record-identity resolver (AAM Blueprint v3.1 §3.6 decision (c)).

Brought into DCL from AAM so AAM's ingest-time fuzzy-match + HITL resolver can be
retired. DCL already owned source-SYSTEM normalization (backend/engine/
source_normalizer.py); this package adds entity-RECORD resolution (customer /
vendor / employee / account identity) plus the inbound record->triple converter.
"""
