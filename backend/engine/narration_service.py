from typing import List, Dict
from datetime import datetime


class NarrationService:
    
    def __init__(self):
        self.messages: Dict[str, List[Dict]] = {}
    
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
        return self.messages.get(run_id, [])
    
    def clear_messages(self, run_id: str):
        if run_id in self.messages:
            self.messages[run_id] = []
