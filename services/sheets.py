from datetime import datetime

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

class DummySheets:
    def find_one(self, *args, **kwargs):
        return None

    def upsert_user(self, *args, **kwargs):
        return None

    def create_lead(self, *args, **kwargs):
        return "LD-001"

    def get_active_agents(self):
        return []

sheets = DummySheets()
