#!/usr/bin/env python3
"""
Run script for the Ingest Consumer.

Usage:
    python backend/ingest/run_consumer.py

Environment variables:
    REDIS_URL - Redis connection URL (default: redis://localhost:6379)
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.ingest.consumer import main

if __name__ == "__main__":
    asyncio.run(main())
