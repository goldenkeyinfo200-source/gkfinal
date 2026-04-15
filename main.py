import asyncio
import logging
from contextlib import suppress

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from fastapi import FastAPI, Request, HTTPException

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


def main_menu(role: str = "client") -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text="📝 Заявка қолдириш")]]

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
            [KeyboardButton(text="📞 Телефон юбориш", request_contact=True)],
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


def lead_actions_kb(lead_id: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Олдим", callback_data=f"take:{lead_id}")
    kb.button(text="❌ Рад этдим", callback_data=f"reject:{lead_id}")
    kb.button(text="🏁 Бажарилди", callback_data=f"done:{lead_id}")
    kb.adjust(1)
    return kb.as_markup()


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


def normalize_username(username: str | None) -> str:
    if not username:
        return ""
    return f"@{username.lstrip('@')}"


def normalize_phone(phone: str) -> str:
    return "".join(ch for ch in phone if ch.isdigit() or ch == "+")


async def notify_lead_to_admins_and_agents(lead_id: str) -> None:
    lead = sheets.find_one("Leads", "lead_id", lead_id)
    if not lead:
        return

    text = (
        "🆕 <b>Янги лид</b>\n\n"
        f"🆔 Lead ID: <b>{lead_id}</b>\n"
        f"👤 Исм: {lead.get('client_name', '')}\n"
        f"📞 Телефон: {lead.get('client_phone', '')}\n"
        f"🎯 Мақсад: {lead.get('result', '')}\n"
        f"📝 Изоҳ: {lead.get('notes', '')}\n"
        f"📌 Статус: {lead.get('lead_status', '')}"
    )

    recipients: set[int] = set(settings.admins)
    for agent in sheets.get_active_agents():
        tg_id = str(agent.get("tg_id", "")).strip()
        if tg_id.isdigit():
            recipients.add(int(tg_id))

    for chat_id in recipients:
        try:
            sent = await bot.send_message(chat_id, text, reply_markup=lead_actions_kb(lead_id))
            if not lead.get("group_message_id"):
                sheets.update_row_by_match(
                    "Leads",
                    "lead_id",
                    lead_id,
                    {"group_message_id": str(sent.message_id)},
                )
        except Exception as e:
            logger.exception("Failed to send lead %s to %s: %s", lead_id, chat_id, e)


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()

    full_name = message.from_user.full_name
    username = normalize_username(message.from_user.username)

    sheets.upsert_user(
        tg_id=message.from_user.id,
        full_name=full_name,
        phone="",
        username=username,
        role=detect_role(message.from_user.id),
    )

    await message.answer(
        f"Ассалому алайкум, <b>{full_name}</b>!\n\n"
        f"{settings.company_name} ботга хуш келибсиз.",
        reply_markup=main_menu(detect_role(message.from_user.id)),
    )


@dp.message(F.text == "📝 Заявка қолдириш")
async def request_handler(message: Message, state: FSMContext) -> None:
    await state.set_state(LeadForm.full_name)
    await message.answer("Исмингизни киритинг:", reply_markup=ReplyKeyboardRemove())


@dp.message(LeadForm.full_name)
async def lead_full_name_handler(message: Message, state: FSMContext) -> None:
    await state.update_data(full_name=message.text.strip())
    await state.set_state(LeadForm.phone)
    await message.answer("Телефон рақамингизни юборинг ёки ёзинг:", reply_markup=phone_keyboard())


@dp.message(LeadForm.phone, F.contact)
async def lead_phone_contact_handler(message: Message, state: FSMContext) -> None:
    phone = normalize_phone(message.contact.phone_number or "")
    await state.update_data(phone=phone)
    await state.set_state(LeadForm.purpose)
    await message.answer("Мақсадни танланг:", reply_markup=purpose_keyboard())


@dp.message(LeadForm.phone)
async def lead_phone_text_handler(message: Message, state: FSMContext) -> None:
    phone = normalize_phone(message.text or "")
    await state.update_data(phone=phone)
    await state.set_state(LeadForm.purpose)
    await message.answer("Мақсадни танланг:", reply_markup=purpose_keyboard())


@dp.message(LeadForm.purpose)
async def lead_purpose_handler(message: Message, state: FSMContext) -> None:
    if message.text not in PURPOSE_OPTIONS:
        await message.answer("Рўйхатдан биттасини танланг:", reply_markup=purpose_keyboard())
        return

    await state.update_data(purpose=message.text.strip())
    await state.set_state(LeadForm.notes)
    await message.answer("Қўшимча изоҳ ёзинг:", reply_markup=ReplyKeyboardRemove())


@dp.message(LeadForm.notes)
async def lead_notes_handler(message: Message, state: FSMContext) -> None:
    data = await state.get_data()

    full_name = data.get("full_name", message.from_user.full_name)
    phone = data.get("phone", "")
    purpose = data.get("purpose", "")
    notes = (message.text or "").strip()
    username = normalize_username(message.from_user.username)

    sheets.upsert_user(
        tg_id=message.from_user.id,
        full_name=full_name,
        phone=phone,
        username=username,
        role=detect_role(message.from_user.id),
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
        reply_markup=main_menu(detect_role(message.from_user.id)),
    )

    await notify_lead_to_admins_and_agents(lead_id)


@dp.callback_query(F.data.startswith("take:"))
async def take_lead_handler(callback: CallbackQuery) -> None:
    lead_id = callback.data.split(":", 1)[1]
    lead = sheets.find_one("Leads", "lead_id", lead_id)

    if not lead:
        await callback.answer("Лид топилмади", show_alert=True)
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
    await callback.message.edit_reply_markup(
        reply_markup=lead_actions_kb(lead_id)
    )

    client_tg_id = str(lead.get("client_tg_id", "")).strip()
    if client_tg_id.isdigit():
        with suppress(Exception):
            await bot.send_message(
                int(client_tg_id),
                f"✅ Сизнинг сўровингиз <b>{callback.from_user.full_name}</b> га бириктирилди.",
            )


@dp.callback_query(F.data.startswith("reject:"))
async def reject_lead_handler(callback: CallbackQuery) -> None:
    lead_id = callback.data.split(":", 1)[1]
    lead = sheets.find_one("Leads", "lead_id", lead_id)

    if not lead:
        await callback.answer("Лид топилмади", show_alert=True)
        return

    assigned_to = str(lead.get("assigned_to_tg_id", "")).strip()
    if assigned_to and assigned_to != str(callback.from_user.id) and callback.from_user.id not in settings.admins:
        await callback.answer("Бу лид сизга тегишли эмас", show_alert=True)
        return

    sheets.update_row_by_match(
        "Leads",
        "lead_id",
        lead_id,
        {
            "lead_status": "rejected",
            "assigned_to_tg_id": "",
            "assigned_to_name": "",
            "taken_at": "",
            "notes": f"Rejected by {callback.from_user.full_name} at {now_str()}",
        },
    )

    await callback.answer("Лид рад этилди")
    await notify_lead_to_admins_and_agents(lead_id)


@dp.callback_query(F.data.startswith("done:"))
async def done_lead_handler(callback: CallbackQuery) -> None:
    lead_id = callback.data.split(":", 1)[1]
    lead = sheets.find_one("Leads", "lead_id", lead_id)

    if not lead:
        await callback.answer("Лид топилмади", show_alert=True)
        return

    assigned_to = str(lead.get("assigned_to_tg_id", "")).strip()
    if assigned_to != str(callback.from_user.id) and callback.from_user.id not in settings.admins:
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
async def on_startup() -> None:
    await bot.set_webhook(settings.webhook_url)
    logger.info("Webhook set: %s", settings.webhook_url)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    with suppress(Exception):
        await bot.delete_webhook(drop_pending_updates=False)
    await bot.session.close()


@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != settings.webhook_secret:
        raise HTTPException(status_code=403, detail="Forbidden")

    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}


@app.get("/")
async def healthcheck():
    return {"ok": True, "service": "goldenkey-bot"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)