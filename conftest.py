"""Root conftest — loads .env so DATABASE_URL is available to the connection pool."""

from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")
