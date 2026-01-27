"""
DCL (Data Connectivity Layer) - Metadata-only semantic mapping engine.

DCL maps raw technical fields from source systems to business concepts
and visualizes who uses what. It answers one question:
"What does this field mean to the business?"

DCL does NOT:
- Store raw data
- Process payloads
- Track lineage
- Perform ETL
- Execute queries (moved to AOS-NLQ)
- Parse natural language (moved to AOS-NLQ)
- Assemble answers (moved to AOS-NLQ)

DCL DOES:
- Manage schema structures (field names, types)
- Maintain semantic mappings (field → concept)
- Provide ontology management
- Support graph visualization
- Handle pointer buffering (NOT payload buffering)
"""
