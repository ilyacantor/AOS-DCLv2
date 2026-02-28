import threading
from typing import List, Dict
from datetime import datetime, timezone, timedelta
import logging

PST = timezone(timedelta(hours=-8))

logger = logging.getLogger("NarrationService")


class NarrationService:

    def __init__(self):
        self._lock = threading.Lock()
        self.messages: Dict[str, List[Dict]] = {}

    def add_message(self, run_id: str, source: str, message: str):
        with self._lock:
            if run_id not in self.messages:
                self.messages[run_id] = []

            self.messages[run_id].append({
                "number": len(self.messages[run_id]) + 1,
                "timestamp": datetime.now(PST).isoformat(),
                "source": source,
                "message": message
            })

    def get_messages(self, run_id: str) -> List[Dict]:
        with self._lock:
            return list(self.messages.get(run_id, []))

    def clear_messages(self, run_id: str):
        with self._lock:
            if run_id in self.messages:
                self.messages[run_id] = []
