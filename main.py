import logging
from contextlib import suppress

from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)

from config import settings
from services.sheets import sheets

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

    role = detect_role(message.from_user.id)
    full_name = message.from_user.full_name
    username = normalize_username(message.from_user.username)

    sheets.upsert_user(
        tg_id=message.from_user.id,
        full_name=full_name,
        phone="",
        username=username,
        role=role,
    )

    await message.answer(
        f"Ассалому алайкум, <b>{full_name}</b>!\n\n"
        f"{settings.company_name} ботга хуш келибсиз.",
        reply_markup=main_menu(role),
    )


@dp.message(F.text == "📝 Заявка қолдириш")
async def request_handler(message: Message, state: FSMContext):
    await state.set_state(LeadForm.full_name)
    await message.answer(
        "Исмингизни киритинг:",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(F.text == "🏠 Объект қўшиш")
async def add_property_handler(message: Message):
    role = detect_role(message.from_user.id)
    if role not in {"agent", "admin", "special_agent"}:
        await message.answer("Бу функция фақат агент ва админ учун.")
        return

    await message.answer("🏠 Объект қўшиш функцияси кейинги босқичда тўлиқ уланади.")


@dp.message(F.text == "📊 Админ статистика")
async def stats_handler(message: Message):
    role = detect_role(message.from_user.id)
    if role != "admin":
        await message.answer("Бу бўлим фақат админ учун.")
        return

    await message.answer("📊 Админ статистика функцияси кейинги босқичда тўлиқ уланади.")


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


@dp.message(LeadForm.notes)
async def notes_handler(message: Message, state: FSMContext):
    data = await state.get_data()

    full_name = data.get("full_name") or message.from_user.full_name
    phone = data.get("phone", "")
    purpose = data.get("purpose", "")
    notes = (message.text or "").strip()
    username = normalize_username(message.from_user.username)
    role = detect_role(message.from_user.id)

    sheets.upsert_user(
        tg_id=message.from_user.id,
        full_name=full_name,
        phone=phone,
        username=username,
        role="client" if role == "client" else role,
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