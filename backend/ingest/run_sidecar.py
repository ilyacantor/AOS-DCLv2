#!/usr/bin/env python3
"""
Run script for the Ingest Sidecar.

Usage:
    python backend/ingest/run_sidecar.py

Environment variables:
    SOURCE_URL - The Farm streaming endpoint URL
    REDIS_URL - Redis connection URL (default: redis://localhost:6379)
    SOURCE_NAME - Name to identify the source (default: mulesoft_mock)
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.ingest.ingest_agent import main

if __name__ == "__main__":
    asyncio.run(main())
