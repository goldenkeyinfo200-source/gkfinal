import logging
from contextlib import suppress

from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import settings
from services.sheets import sheets, now_str

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(
    token=settings.bot_token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())
app = FastAPI()

PURPOSE_OPTIONS = [
    "сотиш",
    "ижара",
    "ипотека",
    "сотиб олиш учун",
    "ижарага олиш учун",
]


class LeadForm(StatesGroup):
    full_name = State()
    phone = State()
    purpose = State()
    notes = State()


def normalize_username(username: str | None) -> str:
    if not username:
        return ""
    return f"@{username.lstrip('@')}"


def normalize_phone(phone: str) -> str:
    return "".join(ch for ch in phone if ch.isdigit() or ch == "+")


def detect_role(tg_id: int) -> str:
    if tg_id in settings.admins:
        return "admin"

    agent = sheets.find_one("Agents", "tg_id", str(tg_id))
    if agent and str(agent.get("is_active", "")).upper() == "TRUE":
        return str(agent.get("role", "agent")).strip() or "agent"

    user = sheets.find_one("Users", "tg_id", str(tg_id))
    if user:
        return str(user.get("role", "client")).strip() or "client"

    return "client"


def main_menu(role: str = "client") -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📝 Заявка қолдириш")]
    ]

    if role == "client":
        rows.append([KeyboardButton(text="🧑‍💼 Агент бўлиш")])

    if role in {"agent", "admin", "special_agent"}:
        rows.append([KeyboardButton(text="🏠 Объект қўшиш")])

    if role == "admin":
        rows.append([KeyboardButton(text="📊 Админ статистика")])

    rows.append([KeyboardButton(text=f"📞 Алоқа: {settings.contact_phone}")])

    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
    )


def phone_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📞 Телефон юбориш", request_contact=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def purpose_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=item)] for item in PURPOSE_OPTIONS],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def admin_approve_kb(tg_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Тасдиқлаш", callback_data=f"approve_agent:{tg_id}")
    kb.button(text="❌ Рад қилиш", callback_data=f"reject_agent:{tg_id}")
    kb.adjust(1)
    return kb.as_markup()


def lead_actions_kb(lead_id: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Олдим", callback_data=f"take:{lead_id}")
    kb.button(text="❌ Рад этдим", callback_data=f"reject:{lead_id}")
    kb.button(text="🏁 Бажарилди", callback_data=f"done:{lead_id}")
    kb.adjust(1)
    return kb.as_markup()


def locked_kb(agent_name: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=f"✅ Олинди: {agent_name}", callback_data="locked")
    return kb.as_markup()


def build_lead_text(lead: dict) -> str:
    return (
        "🆕 <b>Янги лид</b>\n\n"
        f"🆔 Lead ID: <b>{lead.get('lead_id', '')}</b>\n"
        f"👤 Исм: {lead.get('client_name', '')}\n"
        f"📞 Телефон: {lead.get('client_phone', '')}\n"
        f"🔗 Username: {lead.get('client_username', '') or '-'}\n"
        f"🎯 Мақсад: {lead.get('result', '')}\n"
        f"📝 Изоҳ: {lead.get('notes', '') or '-'}\n"
        f"📌 Статус: {lead.get('lead_status', '')}"
    )


def ensure_user_exists(user: types.User) -> str:
    role = detect_role(user.id)

    sheets.upsert_user(
        tg_id=user.id,
        full_name=user.full_name,
        phone="",
        username=normalize_username(user.username),
        role=role,
    )

    if user.id in settings.admins:
        existing_agent = sheets.find_one("Agents", "tg_id", str(user.id))
        payload = {
            "tg_id": str(user.id),
            "full_name": user.full_name,
            "phone": "",
            "username": normalize_username(user.username),
            "role": "admin",
            "is_active": "TRUE",
            "can_take_leads": "TRUE",
            "is_special_agent": "FALSE",
            "registered_at": now_str(),
            "notes": "auto-admin",
        }

        if existing_agent:
            sheets.update_row_by_match("Agents", "tg_id", str(user.id), payload)
        else:
            sheets.append_row_by_headers("Agents", payload)

        return "admin"

    return role


def request_agent_registration(user: types.User) -> str:
    existing = sheets.find_one("Agents", "tg_id", str(user.id))
    if existing:
        if str(existing.get("is_active", "")).upper() == "TRUE":
            return "Сиз аллақачон агентсиз."
        return "Сизнинг агент сўровингиз аллақачон юборилган."

    sheets.append_row_by_headers(
        "Agents",
        {
            "tg_id": str(user.id),
            "full_name": user.full_name,
            "phone": "",
            "username": normalize_username(user.username),
            "role": "agent",
            "is_active": "FALSE",
            "can_take_leads": "FALSE",
            "is_special_agent": "FALSE",
            "registered_at": now_str(),
            "notes": "pending",
        },
    )

    text = (
        "🧑‍💼 <b>Янги агент сўрови</b>\n\n"
        f"👤 {user.full_name}\n"
        f"🆔 <code>{user.id}</code>\n"
        f"🔗 {normalize_username(user.username) or '-'}"
    )

    for admin_id in settings.admins:
        with suppress(Exception):
            bot_loop_message = text
            # sent async outside sync block handled in caller
            pass

    return "Сўров юборилди. Админ тасдиғидан кейин агент бўласиз."


async def send_agent_request_to_admins(user: types.User):
    text = (
        "🧑‍💼 <b>Янги агент сўрови</b>\n\n"
        f"👤 {user.full_name}\n"
        f"🆔 <code>{user.id}</code>\n"
        f"🔗 {normalize_username(user.username) or '-'}"
    )
    for admin_id in settings.admins:
        with suppress(Exception):
            await bot.send_message(
                admin_id,
                text,
                reply_markup=admin_approve_kb(user.id),
            )


async def notify_lead_to_agents_and_admins(lead_id: str) -> None:
    lead = sheets.find_one("Leads", "lead_id", lead_id)
    if not lead:
        return

    text = build_lead_text(lead)
    recipients: set[int] = set(settings.admins)

    for agent in sheets.get_active_agents():
        tg_id = str(agent.get("tg_id", "")).strip()
        if tg_id.isdigit():
            recipients.add(int(tg_id))

    group_id = sheets.get_setting("AGENTS_GROUP_ID")
    if group_id and str(group_id).startswith("-100"):
        with suppress(Exception):
            await bot.send_message(
                int(group_id),
                text,
                reply_markup=lead_actions_kb(lead_id),
            )

    for chat_id in recipients:
        with suppress(Exception):
            await bot.send_message(
                chat_id,
                text,
                reply_markup=lead_actions_kb(lead_id),
            )


def get_stats_text() -> str:
    leads = sheets.get_all_records("Leads")
    agents = sheets.get_all_records("Agents")

    today = now_str()[:10]
    month = now_str()[:7]

    daily = [x for x in leads if str(x.get("created_at", "")).startswith(today)]
    monthly = [x for x in leads if str(x.get("created_at", "")).startswith(month)]
    active_agents = [
        x for x in agents
        if str(x.get("is_active", "")).upper() == "TRUE"
    ]

    daily_done = [x for x in daily if x.get("lead_status") == "done"]
    monthly_done = [x for x in monthly if x.get("lead_status") == "done"]

    return (
        "📊 <b>Админ статистика</b>\n\n"
        f"🧑‍💼 Актив агентлар: {len(active_agents)}\n\n"
        f"📅 Кунлик лидлар: {len(daily)}\n"
        f"🏁 Кунлик якунланган: {len(daily_done)}\n\n"
        f"🗓 Ойлик лидлар: {len(monthly)}\n"
        f"🏁 Ойлик якунланган: {len(monthly_done)}"
    )


@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "goldenkey-bot",
        "webhook_path": settings.webhook_path,
    }


@app.post("/{secret_path}")
async def telegram_webhook(secret_path: str, request: Request):
    expected = settings.webhook_path.lstrip("/")
    if secret_path != expected:
        raise HTTPException(status_code=403, detail="Forbidden")

    data = await request.json()
    update = types.Update(**data)
    await dp.feed_update(bot, update)
    return {"ok": True}


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await state.clear()

    role = ensure_user_exists(message.from_user)

    await message.answer(
        f"Ассалому алайкум, <b>{message.from_user.full_name}</b>!\n\n"
        f"{settings.company_name} ботга хуш келибсиз.",
        reply_markup=main_menu(role),
    )


@dp.message(Command("agent"))
async def agent_request_command(message: Message):
    role = ensure_user_exists(message.from_user)
    if role in {"admin", "agent", "special_agent"}:
        await message.answer("Сиз аллақачон агент ёки админсиз.", reply_markup=main_menu(role))
        return

    result = request_agent_registration(message.from_user)
    await message.answer(result, reply_markup=main_menu("client"))

    if "Сўров юборилди" in result:
        await send_agent_request_to_admins(message.from_user)


@dp.message(F.text == "🧑‍💼 Агент бўлиш")
async def agent_request_button(message: Message):
    role = ensure_user_exists(message.from_user)
    if role in {"admin", "agent", "special_agent"}:
        await message.answer("Сиз аллақачон агент ёки админсиз.", reply_markup=main_menu(role))
        return

    result = request_agent_registration(message.from_user)
    await message.answer(result, reply_markup=main_menu("client"))

    if "Сўров юборилди" in result:
        await send_agent_request_to_admins(message.from_user)


@dp.message(F.text == "📝 Заявка қолдириш")
async def request_handler(message: Message, state: FSMContext):
    ensure_user_exists(message.from_user)
    await state.set_state(LeadForm.full_name)
    await message.answer(
        "Исмингизни киритинг:",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(F.text == "🏠 Объект қўшиш")
async def add_property_handler(message: Message):
    role = ensure_user_exists(message.from_user)
    if role not in {"agent", "admin", "special_agent"}:
        await message.answer("Бу функция фақат агент ва админ учун.")
        return

    await message.answer("🏠 Объект қўшиш функцияси кейинги босқичда тўлиқ уланади.")


@dp.message(F.text == "📊 Админ статистика")
async def stats_handler(message: Message):
    role = ensure_user_exists(message.from_user)
    if role != "admin":
        await message.answer("Бу бўлим фақат админ учун.")
        return

    await message.answer(get_stats_text(), reply_markup=main_menu("admin"))


@dp.message(LeadForm.full_name)
async def full_name_handler(message: Message, state: FSMContext):
    await state.update_data(full_name=(message.text or "").strip())
    await state.set_state(LeadForm.phone)
    await message.answer(
        "Телефон рақамингизни юборинг ёки ёзинг:",
        reply_markup=phone_keyboard(),
    )


@dp.message(LeadForm.phone, F.contact)
async def phone_contact_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.contact.phone_number or "")
    await state.update_data(phone=phone)
    await state.set_state(LeadForm.purpose)
    await message.answer(
        "Мақсадни танланг:",
        reply_markup=purpose_keyboard(),
    )


@dp.message(LeadForm.phone)
async def phone_text_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text or "")
    await state.update_data(phone=phone)
    await state.set_state(LeadForm.purpose)
    await message.answer(
        "Мақсадни танланг:",
        reply_markup=purpose_keyboard(),
    )


@dp.message(LeadForm.purpose)
async def purpose_handler(message: Message, state: FSMContext):
    if message.text not in PURPOSE_OPTIONS:
        await message.answer(
            "Рўйхатдан биттасини танланг:",
            reply_markup=purpose_keyboard(),
        )
        return

    await state.update_data(purpose=message.text)
    await state.set_state(LeadForm.notes)
    await message.answer(
        "Қўшимча изоҳ ёзинг:",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await state.clear()

    role = ensure_user_exists(message.from_user)

    await message.answer(
        f"🏠 <b>{settings.company_name}</b>\n\n"
        f"Салом, {message.from_user.full_name}!\n"
        f"Менюдан танланг 👇",
        reply_markup=main_menu(role)
    )

    lead_id = sheets.create_lead(
        client_tg_id=message.from_user.id,
        client_name=full_name,
        client_phone=phone,
        client_username=username,
        purpose=purpose,
        notes=notes,
    )

    await state.clear()

    await message.answer(
        f"✅ Заявкангиз қабул қилинди.\nID: <b>{lead_id}</b>\nТез орада агент ёки админ боғланади.",
        reply_markup=main_menu(role),
    )

    await notify_lead_to_agents_and_admins(lead_id)


@dp.callback_query(F.data == "locked")
async def locked_handler(callback: CallbackQuery):
    await callback.answer("Бу лид олинган", show_alert=True)


@dp.callback_query(F.data.startswith("approve_agent:"))
async def approve_agent_handler(callback: CallbackQuery):
    if detect_role(callback.from_user.id) != "admin":
        await callback.answer("Фақат админ", show_alert=True)
        return

    tg_id = callback.data.split(":", 1)[1]
    existing = sheets.find_one("Agents", "tg_id", tg_id)
    user = sheets.find_one("Users", "tg_id", tg_id)

    payload = {
        "tg_id": tg_id,
        "full_name": (user or {}).get("full_name", existing.get("full_name", "") if existing else ""),
        "phone": (user or {}).get("phone", existing.get("phone", "") if existing else ""),
        "username": (user or {}).get("username", existing.get("username", "") if existing else ""),
        "role": "agent",
        "is_active": "TRUE",
        "can_take_leads": "TRUE",
        "is_special_agent": "FALSE",
        "registered_at": existing.get("registered_at", now_str()) if existing else now_str(),
        "notes": "approved",
    }

    if existing:
        sheets.update_row_by_match("Agents", "tg_id", tg_id, payload)
    else:
        sheets.append_row_by_headers("Agents", payload)

    sheets.update_row_by_match("Users", "tg_id", tg_id, {"role": "agent"})

    await callback.answer("Агент тасдиқланди")
    with suppress(Exception):
        await callback.message.edit_reply_markup(reply_markup=None)

    if tg_id.isdigit():
        with suppress(Exception):
            await bot.send_message(
                int(tg_id),
                "🎉 Сиз агент сифатида тасдиқландингиз.",
                reply_markup=main_menu("agent"),
            )


@dp.callback_query(F.data.startswith("reject_agent:"))
async def reject_agent_handler(callback: CallbackQuery):
    if detect_role(callback.from_user.id) != "admin":
        await callback.answer("Фақат админ", show_alert=True)
        return

    tg_id = callback.data.split(":", 1)[1]
    existing = sheets.find_one("Agents", "tg_id", tg_id)

    if existing:
        sheets.update_row_by_match(
            "Agents",
            "tg_id",
            tg_id,
            {
                "is_active": "FALSE",
                "can_take_leads": "FALSE",
                "notes": "rejected",
            },
        )

    await callback.answer("Сўров рад қилинди")
    with suppress(Exception):
        await callback.message.edit_reply_markup(reply_markup=None)

    if tg_id.isdigit():
        with suppress(Exception):
            await bot.send_message(
                int(tg_id),
                "Сизнинг агент сўровингиз рад қилинди.",
                reply_markup=main_menu("client"),
            )


@dp.callback_query(F.data.startswith("take:"))
async def take_handler(callback: CallbackQuery):
    lead_id = callback.data.split(":", 1)[1]
    lead = sheets.find_one("Leads", "lead_id", lead_id)

    if not lead:
        await callback.answer("Лид топилмади", show_alert=True)
        return

    role = detect_role(callback.from_user.id)
    if role not in {"agent", "admin", "special_agent"}:
        await callback.answer("Сизга рухсат йўқ", show_alert=True)
        return

    assigned_to = str(lead.get("assigned_to_tg_id", "")).strip()
    if assigned_to and assigned_to != str(callback.from_user.id):
        await callback.answer("Бу лидни бошқа ходим олган", show_alert=True)
        return

    sheets.update_row_by_match(
        "Leads",
        "lead_id",
        lead_id,
        {
            "lead_status": "taken",
            "assigned_to_tg_id": str(callback.from_user.id),
            "assigned_to_name": callback.from_user.full_name,
            "taken_at": now_str(),
        },
    )

    await callback.answer("Лид сизга бириктирилди")

    with suppress(Exception):
        await callback.message.edit_reply_markup(
            reply_markup=locked_kb(callback.from_user.first_name or callback.from_user.full_name)
        )

    client_tg_id = str(lead.get("client_tg_id", "")).strip()
    if client_tg_id.isdigit():
        with suppress(Exception):
            await bot.send_message(
                int(client_tg_id),
                f"✅ Сизнинг сўровингиз <b>{callback.from_user.full_name}</b> га бириктирилди.",
            )


@dp.callback_query(F.data.startswith("reject:"))
async def reject_handler(callback: CallbackQuery):
    lead_id = callback.data.split(":", 1)[1]
    lead = sheets.find_one("Leads", "lead_id", lead_id)

    if not lead:
        await callback.answer("Лид топилмади", show_alert=True)
        return

    assigned_to = str(lead.get("assigned_to_tg_id", "")).strip()
    is_admin = detect_role(callback.from_user.id) == "admin"

    if assigned_to and assigned_to != str(callback.from_user.id) and not is_admin:
        await callback.answer("Бу лид сизга тегишли эмас", show_alert=True)
        return

    sheets.update_row_by_match(
        "Leads",
        "lead_id",
        lead_id,
        {
            "lead_status": "new",
            "assigned_to_tg_id": "",
            "assigned_to_name": "",
            "taken_at": "",
            "notes": f"Rejected by {callback.from_user.full_name} at {now_str()}",
        },
    )

    await callback.answer("Лид қайта очилди")
    await notify_lead_to_agents_and_admins(lead_id)


@dp.callback_query(F.data.startswith("done:"))
async def done_handler(callback: CallbackQuery):
    lead_id = callback.data.split(":", 1)[1]
    lead = sheets.find_one("Leads", "lead_id", lead_id)

    if not lead:
        await callback.answer("Лид топилмади", show_alert=True)
        return

    assigned_to = str(lead.get("assigned_to_tg_id", "")).strip()
    is_admin = detect_role(callback.from_user.id) == "admin"

    if assigned_to != str(callback.from_user.id) and not is_admin:
        await callback.answer("Фақат лидни олган ходим якунлай олади", show_alert=True)
        return

    sheets.update_row_by_match(
        "Leads",
        "lead_id",
        lead_id,
        {
            "lead_status": "done",
            "finished_at": now_str(),
        },
    )

    await callback.answer("Лид якунланди")

    client_tg_id = str(lead.get("client_tg_id", "")).strip()
    if client_tg_id.isdigit():
        with suppress(Exception):
            await bot.send_message(
                int(client_tg_id),
                "🏁 Сизнинг мурожаатингиз якунланди. Раҳмат.",
            )


@app.on_event("startup")
async def on_startup():
    logger.info("App starting...")
    logger.info("Webhook path: %s", settings.webhook_path)
    logger.info("Webhook url: %s", settings.webhook_url)

    try:
        await bot.set_webhook(settings.webhook_url)
        logger.info("Webhook set successfully")
    except Exception as e:
        logger.exception("Webhook set error: %s", e)


@app.on_event("shutdown")
async def on_shutdown():
    with suppress(Exception):
        await bot.delete_webhook(drop_pending_updates=False)
    await bot.session.close()