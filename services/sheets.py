from datetime import datetime
from typing import Any

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import settings

DATE_FMT = "%Y-%m-%d %H:%M:%S"


def now_str() -> str:
    return datetime.now().strftime(DATE_FMT)


class GoogleSheetsService:
    def __init__(self):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]

        creds_dict = settings.google_credentials_dict
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

        self.gc = gspread.authorize(creds)
        self.sheet_id = settings.google_sheet_id
        self.sh = self.gc.open_by_key(self.sheet_id)

    def worksheet(self, name: str):
        return self.sh.worksheet(name)

    def get_headers(self, sheet_name: str) -> list[str]:
        ws = self.worksheet(sheet_name)
        return [str(x).strip() for x in ws.row_values(1)]

    def get_all_records(self, sheet_name: str) -> list[dict[str, Any]]:
        ws = self.worksheet(sheet_name)
        headers = self.get_headers(sheet_name)
        rows = ws.get_all_values()
        items: list[dict[str, Any]] = []

        for idx, row in enumerate(rows[1:], start=2):
            obj: dict[str, Any] = {}
            for i, h in enumerate(headers):
                obj[h] = row[i] if i < len(row) else ""
            obj["__row"] = idx
            items.append(obj)

        return items

    def find_one(self, sheet_name: str, column: str, value: Any) -> dict[str, Any] | None:
        target = str(value).strip()
        for row in self.get_all_records(sheet_name):
            if str(row.get(column, "")).strip() == target:
                return row
        return None

    def append_row_by_headers(self, sheet_name: str, row_data: dict[str, Any]) -> int:
        ws = self.worksheet(sheet_name)
        headers = self.get_headers(sheet_name)
        row = [row_data.get(h, "") for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return len(ws.get_all_values())

    def update_row_by_match(
        self,
        sheet_name: str,
        match_column: str,
        match_value: Any,
        fields: dict[str, Any],
    ) -> bool:
        row = self.find_one(sheet_name, match_column, match_value)
        if not row:
            return False

        ws = self.worksheet(sheet_name)
        headers = self.get_headers(sheet_name)
        row_index = row["__row"]

        for key, value in fields.items():
            if key in headers:
                col_index = headers.index(key) + 1
                ws.update_cell(row_index, col_index, value)
        return True

    def get_setting(self, key: str) -> str:
        row = self.find_one("Settings", "key", key)
        if not row:
            return ""
        return str(row.get("value", "")).strip()

    def upsert_user(
        self,
        tg_id: int,
        full_name: str,
        phone: str,
        username: str,
        role: str = "client",
        is_active: str = "TRUE",
    ) -> None:
        existing = self.find_one("Users", "tg_id", str(tg_id))
        payload = {
            "tg_id": str(tg_id),
            "full_name": full_name,
            "phone": phone if phone else (existing.get("phone", "") if existing else ""),
            "username": username,
            "role": role,
            "created_at": existing["created_at"] if existing else now_str(),
            "last_seen_at": now_str(),
            "is_active": is_active,
        }

        if existing:
            self.update_row_by_match("Users", "tg_id", str(tg_id), payload)
        else:
            self.append_row_by_headers("Users", payload)

    def upsert_agent(
        self,
        tg_id: int,
        full_name: str,
        phone: str,
        username: str,
        role: str = "agent",
        is_active: str = "TRUE",
        can_take_leads: str = "TRUE",
        is_special_agent: str = "FALSE",
        notes: str = "",
    ) -> None:
        existing = self.find_one("Agents", "tg_id", str(tg_id))
        payload = {
            "tg_id": str(tg_id),
            "full_name": full_name,
            "phone": phone if phone else (existing.get("phone", "") if existing else ""),
            "username": username,
            "role": role,
            "is_active": is_active,
            "can_take_leads": can_take_leads,
            "is_special_agent": is_special_agent,
            "registered_at": existing["registered_at"] if existing else now_str(),
            "notes": notes,
        }

        if existing:
            self.update_row_by_match("Agents", "tg_id", str(tg_id), payload)
        else:
            self.append_row_by_headers("Agents", payload)

    def get_active_agents(self) -> list[dict[str, Any]]:
        rows = self.get_all_records("Agents")
        result = []
        for row in rows:
            if (
                str(row.get("is_active", "")).upper() == "TRUE"
                and str(row.get("can_take_leads", "")).upper() == "TRUE"
                and str(row.get("tg_id", "")).isdigit()
            ):
                result.append(row)
        return result

    def next_lead_id(self) -> str:
        rows = self.get_all_records("Leads")
        max_num = 0

        for row in rows:
            lead_id = str(row.get("lead_id", "")).strip()
            if lead_id.startswith("LD-"):
                try:
                    max_num = max(max_num, int(lead_id.split("-")[1]))
                except Exception:
                    pass

        return f"LD-{max_num + 1:03d}"

    def create_lead(
        self,
        client_tg_id: int,
        client_name: str,
        client_phone: str,
        client_username: str,
        purpose: str,
        notes: str,
        property_id: str = "",
        special_agent_id: str = "",
        special_agent_name: str = "",
    ) -> str:
        lead_id = self.next_lead_id()

        self.append_row_by_headers(
            "Leads",
            {
                "lead_id": lead_id,
                "created_at": now_str(),
                "property_id": property_id,
                "client_tg_id": str(client_tg_id),
                "client_name": client_name,
                "client_phone": client_phone,
                "client_username": client_username,
                "lead_status": "new",
                "assigned_to_tg_id": "",
                "assigned_to_name": "",
                "taken_at": "",
                "finished_at": "",
                "result": purpose,
                "special_agent_id": special_agent_id,
                "special_agent_name": special_agent_name,
                "group_message_id": "",
                "notes": notes,
            },
        )

        return lead_id


sheets = GoogleSheetsService()
