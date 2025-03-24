import json
import os
from typing import Dict
import psycopg2.extensions

def load_config(config_path: str = "config.json") -> Dict:
    """Loads database configuration from a .env and sync settings from a JSON file."""
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    import dotenv
    load_dotenv = dotenv.load_dotenv(os.path.join(PROJECT_ROOT, '.env'))
    if not load_dotenv:
        raise FileNotFoundError("Database configuration file not found: .env")
    
    if not os.getenv("DATABASE_URL_SOURCE") or not os.getenv("DATABASE_URL_TARGET"):
        raise ValueError("Database URL not found in environment variables")
    
    source_db_config = psycopg2.extensions.parse_dsn(os.getenv("DATABASE_URL_SOURCE"))
    target_db_config = psycopg2.extensions.parse_dsn(os.getenv("DATABASE_URL_TARGET"))
    
    config = {
        "database": {
            "source_db":source_db_config,
            "target_db":target_db_config
        }
    }

    CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
    config_path = os.path.join(CONFIG_DIR, config_path)
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        config.update(json.load(f))

    return config

def get_sync_settings(config: Dict) -> Dict[str, str]:
    """Extracts table mapping from the configuration."""
    table_mapping = {}
    sync_config = config.get("sync")
    if sync_config:
        tables = sync_config.get("tables", [])
        for table_info in tables:
            source_table = table_info.get("source")
            target_table = table_info.get("target")
            if source_table and target_table:
                table_mapping[source_table] = target_table
    return table_mapping
