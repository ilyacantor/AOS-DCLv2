from typing import List, Dict
from datetime import datetime, timezone, timedelta
import json

PST = timezone(timedelta(hours=-8))
import os
import logging
try:
    import redis
except ImportError:
    redis = None  # type: ignore[assignment]


logger = logging.getLogger("NarrationService")
REDIS_LOG_KEY = "dcl.logs"


class NarrationService:
    
    def __init__(self):
        self.messages: Dict[str, List[Dict]] = {}
        self._redis_client = None
    
    def _get_redis(self):
        """Lazy initialization of Redis client."""
        if redis is None:
            return None
        if self._redis_client is None:
            redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
            self._redis_client = redis.from_url(redis_url, decode_responses=True)
        return self._redis_client
    
    def _fetch_ingest_logs(self) -> List[Dict]:
        """Fetch latest logs from Redis List dcl.logs."""
        try:
            r = self._get_redis()
            logs = r.lrange(REDIS_LOG_KEY, -100, -1)
            
            parsed_logs = []
            for log_entry in logs:
                try:
                    data = json.loads(log_entry)
                    parsed_logs.append({
                        "number": 0,
                        "timestamp": data.get("ts", datetime.now(PST).isoformat()),
                        "source": "Ingest",
                        "message": data.get("msg", ""),
                        "type": data.get("type", "info")
                    })
                except json.JSONDecodeError:
                    continue
            return parsed_logs
        except Exception as e:
            logger.error(f"Error fetching ingest logs: {e}")
            return []
    
    def add_message(self, run_id: str, source: str, message: str):
        if run_id not in self.messages:
            self.messages[run_id] = []
        
        self.messages[run_id].append({
            "number": len(self.messages[run_id]) + 1,
            "timestamp": datetime.now(PST).isoformat(),
            "source": source,
            "message": message
        })
    
    def get_messages(self, run_id: str) -> List[Dict]:
        run_messages = self.messages.get(run_id, [])
        # Note: Ingest logs moved to AAM - no longer fetching from Redis
        # ingest_logs = self._fetch_ingest_logs()
        return run_messages
    
    def clear_messages(self, run_id: str):
        if run_id in self.messages:
            self.messages[run_id] = []
