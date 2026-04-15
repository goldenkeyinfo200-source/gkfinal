import os
import json


class Settings:
    def __init__(self):
        self.bot_token = os.getenv("BOT_TOKEN", "").strip()
        self.base_webhook_url = os.getenv("BASE_WEBHOOK_URL", "").rstrip("/")
        self.webhook_secret = os.getenv("WEBHOOK_SECRET", "").strip()
        self.google_sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
        self.google_credentials_raw = os.getenv("GOOGLE_CREDENTIALS", "").strip()
        self.admins = [
            int(x.strip())
            for x in os.getenv("ADMINS", "").split(",")
            if x.strip().isdigit()
        ]
        self.company_name = os.getenv("COMPANY_NAME", "Golden Key").strip()
        self.contact_phone = os.getenv("CONTACT_PHONE", "").strip()

    @property
    def webhook_path(self) -> str:
        return f"/{self.webhook_secret}"

    @property
    def webhook_url(self) -> str:
        return f"{self.base_webhook_url}{self.webhook_path}"

    @property
    def google_credentials_dict(self) -> dict:
        if not self.google_credentials_raw:
            raise ValueError("GOOGLE_CREDENTIALS is empty")
        return json.loads(self.google_credentials_raw)


settings = Settings()