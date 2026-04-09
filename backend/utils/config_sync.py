import os
import yaml
try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    psycopg2 = None  # type: ignore[assignment]
    Json = None  # type: ignore[assignment]
from typing import Dict, List, Any


def load_yaml_config(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)


def sync_ontology_concepts(conn):
    config_path = 'config/ontology_concepts.yaml'
    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}")
        return 0
    
    config = load_yaml_config(config_path)
    concepts = config.get('concepts', [])
    
    cursor = conn.cursor()
    synced = 0
    
    for concept in concepts:
        cursor.execute("""
            INSERT INTO ontology_concepts (id, name, description, cluster, metadata)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                cluster = EXCLUDED.cluster,
                metadata = EXCLUDED.metadata,
                updated_at = CURRENT_TIMESTAMP
        """, (
            concept['id'],
            concept['name'],
            concept['description'],
            concept['cluster'],
            Json(concept.get('metadata', {}))
        ))
        synced += 1
    
    conn.commit()
    cursor.close()
    print(f"Synced {synced} ontology concepts")
    return synced


def sync_all_configs():
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("DATABASE_URL not set - skipping config sync")
        return False

    try:
        conn = psycopg2.connect(database_url)
        print("Connected to database for config sync")

        sync_ontology_concepts(conn)

        conn.close()
        print("Config sync completed successfully")
        return True
    except Exception as e:
        print(f"Config sync failed: {str(e)}")
        return False


if __name__ == "__main__":
    sync_all_configs()
