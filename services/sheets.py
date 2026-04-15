import time
from datetime import datetime
from typing import Any

import gspread
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials

from config import settings

DATE_FMT = "%Y-%m-%d %H:%M:%S"


def now_str() -> str:
    return datetime.now().strftime(DATE_FMT)


def col_to_letter(col_num: int) -> str:
    result = ""
    while col_num > 0:
        col_num, rem = divmod(col_num - 1, 26)
        result = chr(65 + rem) + result
    return result


class TTLCache:
    def __init__(self):
        self._data: dict[str, tuple[float, Any]] = {}

    def get(self, key: str, ttl: float):
        item = self._data.get(key)
        if not item:
            return None
        created_at, value = item
        if time.time() - created_at > ttl:
            self._data.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any):
        self._data[key] = (time.time(), value)

    def delete_prefix(self, prefix: str):
        for key in list(self._data.keys()):
            if key.startswith(prefix):
                self._data.pop(key, None)


class GoogleSheetsService:
    def __init__(self):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]

        creds_dict = settings.google_credentials_dict
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

        self.gc = gspread.authorize(creds)
        self.sh = self.gc.open_by_key(settings.google_sheet_id)

        self.cache = TTLCache()
        self._last_touch_by_user: dict[str, float] = {}

    def _with_retry(self, fn, *args, **kwargs):
        last_error = None
        for delay in (0, 0.5, 1.0, 2.0):
            if delay:
                time.sleep(delay)
            try:
                return fn(*args, **kwargs)
            except APIError as e:
                last_error = e
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status not in (429, 500, 502, 503, 504):
                    raise
        raise last_error

    def worksheet(self, name: str):
        cache_key = f"ws:{name}"
        ws = self.cache.get(cache_key, ttl=300)
        if ws is not None:
            return ws
        ws = self._with_retry(self.sh.worksheet, name)
        self.cache.set(cache_key, ws)
        return ws

    def get_headers(self, sheet_name: str) -> list[str]:
        cache_key = f"headers:{sheet_name}"
        headers = self.cache.get(cache_key, ttl=300)
        if headers is not None:
            return headers
        ws = self.worksheet(sheet_name)
        headers = [str(x).strip() for x in self._with_retry(ws.row_values, 1)]
        self.cache.set(cache_key, headers)
        return headers

    def get_all_values(self, sheet_name: str, ttl: float = 5.0) -> list[list[str]]:
        cache_key = f"values:{sheet_name}"
        values = self.cache.get(cache_key, ttl=ttl)
        if values is not None:
            return values
        ws = self.worksheet(sheet_name)
        values = self._with_retry(ws.get_all_values)
        self.cache.set(cache_key, values)
        return values

    def get_all_records(self, sheet_name: str, ttl: float = 5.0) -> list[dict[str, Any]]:
        cache_key = f"records:{sheet_name}"
        cached = self.cache.get(cache_key, ttl=ttl)
        if cached is not None:
            return cached

        headers = self.get_headers(sheet_name)
        rows = self.get_all_values(sheet_name, ttl=ttl)

        items: list[dict[str, Any]] = []
        for row_index, row in enumerate(rows[1:], start=2):
            obj: dict[str, Any] = {}
            for i, h in enumerate(headers):
                obj[h] = row[i] if i < len(row) else ""
            obj["__row"] = row_index
            items.append(obj)

        self.cache.set(cache_key, items)
        return items

    def invalidate_sheet_cache(self, sheet_name: str):
        self.cache.delete_prefix(f"values:{sheet_name}")
        self.cache.delete_prefix(f"records:{sheet_name}")

        if sheet_name == "Settings":
            self.cache.delete_prefix("settings_map")
        elif sheet_name == "Users":
            self.cache.delete_prefix("users_map")
        elif sheet_name == "Agents":
            self.cache.delete_prefix("agents_active")
        elif sheet_name == "Leads":
            self.cache.delete_prefix("stats_summary")

    def find_one(self, sheet_name: str, column: str, value: Any) -> dict[str, Any] | None:
        target = str(value).strip()
        for row in self.get_all_records(sheet_name, ttl=5.0):
            if str(row.get(column, "")).strip() == target:
                return row
        return None

    def append_row_by_headers(self, sheet_name: str, row_data: dict[str, Any]) -> int:
        ws = self.worksheet(sheet_name)
        headers = self.get_headers(sheet_name)
        row = [row_data.get(h, "") for h in headers]
        self._with_retry(ws.append_row, row, value_input_option="USER_ENTERED")
        self.invalidate_sheet_cache(sheet_name)
        return len(self.get_all_values(sheet_name, ttl=0.1))

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
        row_index = int(row["__row"])

        new_row_values = [row.get(h, "") for h in headers]
        for key, value in fields.items():
            if key in headers:
                new_row_values[headers.index(key)] = value

        end_col = col_to_letter(len(headers))
        rng = f"A{row_index}:{end_col}{row_index}"
        self._with_retry(ws.update, rng, [new_row_values], value_input_option="USER_ENTERED")
        self.invalidate_sheet_cache(sheet_name)
        return True

    def get_settings_map(self) -> dict[str, str]:
        cache_key = "settings_map"
        cached = self.cache.get(cache_key, ttl=60)
        if cached is not None:
            return cached

        rows = self.get_all_records("Settings", ttl=10.0)
        result = {
            str(row.get("key", "")).strip(): str(row.get("value", "")).strip()
            for row in rows
            if str(row.get("key", "")).strip()
        }
        self.cache.set(cache_key, result)
        return result

    def get_setting(self, key: str) -> str:
        return self.get_settings_map().get(key, "")

    def get_users_map(self) -> dict[str, dict[str, Any]]:
        cache_key = "users_map"
        cached = self.cache.get(cache_key, ttl=20)
        if cached is not None:
            return cached

        rows = self.get_all_records("Users", ttl=10.0)
        result = {
            str(row.get("tg_id", "")).strip(): row
            for row in rows
            if str(row.get("tg_id", "")).strip()
        }
        self.cache.set(cache_key, result)
        return result

    def get_user_by_tg_id(self, tg_id: int | str) -> dict[str, Any] | None:
        return self.get_users_map().get(str(tg_id).strip())

    def upsert_user(
        self,
        tg_id: int,
        full_name: str,
        phone: str,
        username: str,
        role: str = "client",
        is_active: str = "TRUE",
    ) -> None:
        existing = self.get_user_by_tg_id(tg_id)

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

    def touch_user(self, tg_id: int) -> None:
        key = str(tg_id)
        now = time.time()
        last_touch = self._last_touch_by_user.get(key, 0)

        if now - last_touch < 60:
            return

        existing = self.get_user_by_tg_id(tg_id)
        if not existing:
            return

        ok = self.update_row_by_match(
            "Users",
            "tg_id",
            str(tg_id),
            {"last_seen_at": now_str()},
        )
        if ok:
            self._last_touch_by_user[key] = now

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
        cache_key = "agents_active"
        cached = self.cache.get(cache_key, ttl=20)
        if cached is not None:
            return cached

        rows = self.get_all_records("Agents", ttl=10.0)
        result = [
            row for row in rows
            if (
                str(row.get("is_active", "")).upper() == "TRUE"
                and str(row.get("can_take_leads", "")).upper() == "TRUE"
                and str(row.get("tg_id", "")).isdigit()
            )
        ]
        self.cache.set(cache_key, result)
        return result

    def next_lead_id(self) -> str:
        rows = self.get_all_records("Leads", ttl=10.0)
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

    def get_stats_summary(self) -> dict[str, int]:
        cache_key = "stats_summary"
        cached = self.cache.get(cache_key, ttl=30)
        if cached is not None:
            return cached

        leads = self.get_all_records("Leads", ttl=10.0)
        agents = self.get_all_records("Agents", ttl=10.0)

        today = now_str()[:10]
        month = now_str()[:7]

        daily = [x for x in leads if str(x.get("created_at", "")).startswith(today)]
        monthly = [x for x in leads if str(x.get("created_at", "")).startswith(month)]
        active_agents = [
            x for x in agents
            if str(x.get("is_active", "")).upper() == "TRUE"
        ]

        daily_done = [x for x in daily if x.get("lead_status") in {"done", "contract_signed"}]
        monthly_done = [x for x in monthly if x.get("lead_status") in {"done", "contract_signed"}]

        result = {
            "active_agents": len(active_agents),
            "daily": len(daily),
            "daily_done": len(daily_done),
            "monthly": len(monthly),
            "monthly_done": len(monthly_done),
        }
        self.cache.set(cache_key, result)
        return result


sheets = GoogleSheetsService()
