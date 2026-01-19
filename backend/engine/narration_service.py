from typing import List, Dict
from datetime import datetime
import json
import os
import redis


REDIS_LOG_KEY = "dcl.logs"


class NarrationService:
    
    def __init__(self):
        self.messages: Dict[str, List[Dict]] = {}
        self._redis_client = None
        self._last_log_index = 0
    
    def _get_redis(self):
        """Lazy initialization of Redis client."""
        if self._redis_client is None:
            redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
            self._redis_client = redis.from_url(redis_url, decode_responses=True)
        return self._redis_client
    
    def _fetch_ingest_logs(self) -> List[Dict]:
        """Fetch new logs from Redis List dcl.logs."""
        try:
            r = self._get_redis()
            logs = r.lrange(REDIS_LOG_KEY, self._last_log_index, -1)
            if logs:
                self._last_log_index += len(logs)
            
            parsed_logs = []
            for log_entry in logs:
                try:
                    data = json.loads(log_entry)
                    parsed_logs.append({
                        "number": 0,
                        "timestamp": data.get("ts", datetime.now().isoformat()),
                        "source": "Ingest",
                        "message": data.get("msg", ""),
                        "type": data.get("type", "info")
                    })
                except json.JSONDecodeError:
                    continue
            return parsed_logs
        except Exception:
            return []
    
    def add_message(self, run_id: str, source: str, message: str):
        if run_id not in self.messages:
            self.messages[run_id] = []
        
        self.messages[run_id].append({
            "number": len(self.messages[run_id]) + 1,
            "timestamp": datetime.now().isoformat(),
            "source": source,
            "message": message
        })
    
    def get_messages(self, run_id: str) -> List[Dict]:
        run_messages = self.messages.get(run_id, [])
        ingest_logs = self._fetch_ingest_logs()
        
        for i, log in enumerate(ingest_logs):
            log["number"] = len(run_messages) + i + 1
        
        return run_messages + ingest_logs
    
    def clear_messages(self, run_id: str):
        if run_id in self.messages:
            self.messages[run_id] = []
