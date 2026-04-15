import os


class Settings:
    def __init__(self):
        self.bot_token = os.getenv("BOT_TOKEN")
        self.base_webhook_url = os.getenv("BASE_WEBHOOK_URL", "").rstrip("/")
        self.webhook_secret = os.getenv("WEBHOOK_SECRET")
        self.google_sheet_id = os.getenv("GOOGLE_SHEET_ID")
        self.admins = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip()]
        self.company_name = os.getenv("COMPANY_NAME", "Golden Key")
        self.contact_phone = os.getenv("CONTACT_PHONE", "")

    @property
    def webhook_path(self):
        return f"/{self.webhook_secret}"

    @property
    def webhook_url(self):
        return f"{self.base_webhook_url}{self.webhook_path}"


settings = Settings()