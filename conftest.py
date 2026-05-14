"""Root conftest — loads .env.development so DATABASE_URL points at aos-dev (not prod)."""

from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env.development")
