# Required packages:
#   aiogram
#   asyncpg
#   python-dotenv (optional)
#   pytz
#
# Required environment variables:
#   BOT_TOKEN
#   DATABASE_URL
#   ADMIN_IDS
#   PUBLIC_PAYOUT_CHANNEL_ID
#   PRIVATE_WITHDRAW_QUEUE_CHANNEL_ID
# Optional environment variables:
#   BOT_USERNAME
#   RENDER
#   LOG_LEVEL
#   PORT

import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Any, Optional
from urllib.parse import quote_plus

import asyncpg
import pytz
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ============================================================
# CONFIG
# ============================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
BOT_USERNAME = (os.getenv("BOT_USERNAME") or "").strip().lstrip("@") or None
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "10000"))

REFERRAL_REWARD = 3000
WITHDRAW_AMOUNT = 30000
YANGON_TZ = pytz.timezone("Asia/Yangon")

WELCOME_JOIN_TEXT = (
    "🌸 မင်္ဂလာပါ မြန်မာ့နှစ်သစ်ကြို သင်္ကြန်အချိန် သာယာလှပစေ 🌊\n\n"
    "💦 Bot အသုံးပြုနိုင်ရန် အောက်ပါ Channel များကို အရင် Join ပေးပါနော် 👇"
)
WELCOME_SUCCESS_TEXT = (
    "✅ အောင်မြင်ပါသည်။ သင်္ကြန်မုန့်ဖိုး Bot ကို စတင်အသုံးပြုနိုင်ပါပြီ 💦\n\n"
    "💦 သင်္ကြန်မုန့်ဖိုး Bot မှ ကြိုဆိုပါတယ် 🌸\n"
    "အောက်ပါ Menu များကို အသုံးပြုနိုင်ပါသည် 👇"
)
HELP_TEXT = (
    "🌸 အကူအညီ (Help & Information) 🌸\n\n"
    "1️⃣ Bot အသုံးပြုရန် သတ်မှတ်ထားသော Channel များကို မဖြစ်မနေ Join ရပါမည်။\n"
    "2️⃣ သူငယ်ချင်း ၁ ယောက်ကို ဖိတ်ခေါ်တိုင်း 3000 ကျပ် ရရှိပါမည်။\n"
    "3️⃣ အနည်းဆုံး 30,000 ကျပ် (၁၀ ယောက်) ပြည့်ပါက KPay, Wave Pay, Phone Bill တို့ဖြင့် ငွေထုတ်နိုင်ပါသည်။\n\n"
    "💦 ပျော်ရွှင်စရာ သင်္ကြန်လေး ဖြစ်ပါစေ!"
)

VALID_MEMBER_STATUSES = {
    ChatMemberStatus.MEMBER,
    ChatMemberStatus.ADMINISTRATOR,
    ChatMemberStatus.CREATOR,
}


def parse_admin_ids(raw: str) -> set[int]:
    result: set[int] = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            logging.warning("Invalid ADMIN_IDS entry ignored: %s", part)
    return result


ADMIN_IDS = parse_admin_ids(os.getenv("ADMIN_IDS", ""))
PUBLIC_PAYOUT_CHANNEL_ID = int(os.getenv("PUBLIC_PAYOUT_CHANNEL_ID", "0"))
PRIVATE_WITHDRAW_QUEUE_CHANNEL_ID = int(os.getenv("PRIVATE_WITHDRAW_QUEUE_CHANNEL_ID", "0"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")
if not ADMIN_IDS:
    raise RuntimeError("ADMIN_IDS is required")
if not PUBLIC_PAYOUT_CHANNEL_ID:
    raise RuntimeError("PUBLIC_PAYOUT_CHANNEL_ID is required")
if not PRIVATE_WITHDRAW_QUEUE_CHANNEL_ID:
    raise RuntimeError("PRIVATE_WITHDRAW_QUEUE_CHANNEL_ID is required")

# ============================================================
# LOGGING / GLOBALS
# ============================================================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("thingyan_referral_bot")

router = Router()
db_pool: Optional[asyncpg.Pool] = None
withdraw_locks: dict[int, asyncio.Lock] = {}


class WithdrawFSM(StatesGroup):
    choosing_method = State()
    waiting_name = State()
    waiting_number = State()
    confirming = State()


# ============================================================
# DATABASE
# ============================================================
def get_pool() -> asyncpg.Pool:
    if db_pool is None:
        raise RuntimeError("Database pool is not initialized")
    return db_pool


async def init_db() -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                invited_by BIGINT NULL,
                balance BIGINT NOT NULL DEFAULT 0,
                total_referrals INTEGER NOT NULL DEFAULT 0,
                is_join_verified BOOLEAN NOT NULL DEFAULT FALSE,
                referral_reward_given BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS required_channels (
                id SERIAL PRIMARY KEY,
                channel_id BIGINT UNIQUE NOT NULL,
                title TEXT,
                username TEXT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
        )
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_invited_by ON users(invited_by);")


async def inviter_exists(user_id: int) -> bool:
    return bool(
        await get_pool().fetchval("SELECT EXISTS(SELECT 1 FROM users WHERE user_id = $1)", user_id)
    )


async def upsert_user(tg_user: Any, invited_by: Optional[int] = None) -> None:
    valid_invited_by = None
    if invited_by and invited_by != tg_user.id and await inviter_exists(invited_by):
        valid_invited_by = invited_by

    await get_pool().execute(
        """
        INSERT INTO users (user_id, username, full_name, invited_by)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (user_id) DO UPDATE
        SET username = EXCLUDED.username,
            full_name = EXCLUDED.full_name
        """,
        tg_user.id,
        tg_user.username,
        tg_user.full_name,
        valid_invited_by,
    )


async def get_user(user_id: int) -> Optional[asyncpg.Record]:
    return await get_pool().fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)


async def set_join_verified(user_id: int, is_verified: bool) -> None:
    await get_pool().execute(
        "UPDATE users SET is_join_verified = $2 WHERE user_id = $1",
        user_id,
        is_verified,
    )


async def finalize_join_verification(user_id: int) -> tuple[bool, bool]:
    async with get_pool().acquire() as conn:
        async with conn.transaction():
            user = await conn.fetchrow(
                """
                SELECT user_id, invited_by, is_join_verified, referral_reward_given
                FROM users
                WHERE user_id = $1
                FOR UPDATE
                """,
                user_id,
            )
            if not user:
                return False, False

            joined_now = not user["is_join_verified"]
            if joined_now:
                await conn.execute("UPDATE users SET is_join_verified = TRUE WHERE user_id = $1", user_id)

            reward_given_now = False
            inviter_id = user["invited_by"]
            can_reward = (
                inviter_id
                and inviter_id != user_id
                and not user["referral_reward_given"]
                and (user["is_join_verified"] or joined_now)
            )
            if can_reward:
                inviter_row = await conn.fetchrow(
                    "SELECT user_id FROM users WHERE user_id = $1 FOR UPDATE",
                    inviter_id,
                )
                if inviter_row:
                    await conn.execute(
                        """
                        UPDATE users
                        SET balance = balance + $2,
                            total_referrals = total_referrals + 1
                        WHERE user_id = $1
                        """,
                        inviter_id,
                        REFERRAL_REWARD,
                    )
                    await conn.execute(
                        "UPDATE users SET referral_reward_given = TRUE WHERE user_id = $1",
                        user_id,
                    )
                    reward_given_now = True

            return joined_now, reward_given_now


async def get_required_channels() -> list[asyncpg.Record]:
    return await get_pool().fetch(
        "SELECT id, channel_id, title, username FROM required_channels ORDER BY id ASC"
    )


async def add_required_channel(channel_id: int, title: Optional[str], username: Optional[str]) -> None:
    await get_pool().execute(
        """
        INSERT INTO required_channels (channel_id, title, username)
        VALUES ($1, $2, $3)
        ON CONFLICT (channel_id) DO UPDATE
        SET title = EXCLUDED.title,
            username = EXCLUDED.username
        """,
        channel_id,
        title,
        username.lstrip("@") if username else None,
    )


async def remove_required_channel(channel_id: int) -> str:
    return await get_pool().execute(
        "DELETE FROM required_channels WHERE channel_id = $1",
        channel_id,
    )


async def deduct_balance_once(user_id: int, amount: int) -> Optional[int]:
    row = await get_pool().fetchrow(
        """
        UPDATE users
        SET balance = balance - $2
        WHERE user_id = $1 AND balance >= $2
        RETURNING balance
        """,
        user_id,
        amount,
    )
    return None if not row else int(row["balance"])


# ============================================================
# HELPERS
# ============================================================
MDV2_ESCAPE_RE = re.compile(r"([_\*\[\]\(\)~`>#+\-=|{}\.!])")


def format_ks(amount: int) -> str:
    return f"{amount:,}"


def escape_markdown_v2(text: Any) -> str:
    return MDV2_ESCAPE_RE.sub(r"\\\1", str(text or ""))


def mask_number(value: str) -> str:
    clean = re.sub(r"\s+", "", value or "")
    if len(clean) <= 2:
        return "*" * len(clean)
    if len(clean) <= 5:
        return f"{clean[0]}***{clean[-1]}"
    return f"{clean[:3]}****{clean[-2:]}"


def mask_name(name: str) -> str:
    stripped = (name or "").strip()
    if len(stripped) <= 1:
        return stripped or "User"
    if len(stripped) == 2:
        return f"{stripped[0]}*"
    return f"{stripped[0]}{'*' * max(2, len(stripped) - 2)}{stripped[-1]}"


def yangon_now_str() -> str:
    return datetime.now(YANGON_TZ).strftime("%Y-%m-%d %H:%M:%S")


def parse_start_referrer(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    parts = text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return None
    payload = parts[1].strip()
    if not re.fullmatch(r"-?\d+", payload):
        return None
    try:
        return int(payload)
    except ValueError:
        return None


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def channel_title_text(channel: asyncpg.Record) -> str:
    return channel["title"] or (f"@{channel['username']}" if channel["username"] else f"Channel {channel['channel_id']}")


def build_channel_url(username: Optional[str]) -> Optional[str]:
    return None if not username else f"https://t.me/{username.lstrip('@')}"


def build_referral_link(user_id: int) -> str:
    if not BOT_USERNAME:
        raise RuntimeError("BOT_USERNAME is not available")
    return f"https://t.me/{BOT_USERNAME}?start={user_id}"


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 ငွေစာရင်း"), KeyboardButton(text="👥 သူငယ်ချင်းဖိတ်မယ်")],
            [KeyboardButton(text="💸 ငွေထုတ်မယ်"), KeyboardButton(text="🌸 အကူအညီ")],
        ],
        resize_keyboard=True,
    )


def withdraw_method_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Wave Pay"), KeyboardButton(text="KPay")],
            [KeyboardButton(text="Phone Bill")],
            [KeyboardButton(text="❌ မလုပ်တော့ပါ")],
        ],
        resize_keyboard=True,
    )


def cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ မလုပ်တော့ပါ")]],
        resize_keyboard=True,
    )


def withdraw_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="✅ အတည်ပြုမည်", callback_data="withdraw_confirm"),
            InlineKeyboardButton(text="❌ မလုပ်တော့ပါ", callback_data="withdraw_cancel"),
        ]]
    )


def build_join_keyboard(channels: list[asyncpg.Record]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for channel in channels:
        title = channel_title_text(channel)
        url = build_channel_url(channel["username"])
        if url:
            rows.append([InlineKeyboardButton(text=f"🌸 Join {title}", url=url)])
        else:
            rows.append([InlineKeyboardButton(text=f"🌸 {title}", callback_data=f"channel_nourl:{channel['channel_id']}")])
    rows.append([InlineKeyboardButton(text="✅ Join ပြီးပါပြီ", callback_data="check_join")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_referral_share_keyboard(ref_link: str) -> InlineKeyboardMarkup:
    share_text = quote_plus("🌸 သင်္ကြန်မုန့်ဖိုး Bot ကို Join လုပ်ပြီး မုန့်ဖိုးရယူလိုက်ပါ 💦")
    share_url = f"https://t.me/share/url?url={quote_plus(ref_link)}&text={share_text}"
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🚀 ဖိတ်ခေါ်မည်", url=share_url)]]
    )


def get_withdraw_lock(user_id: int) -> asyncio.Lock:
    lock = withdraw_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        withdraw_locks[user_id] = lock
    return lock


def valid_name_input(value: str) -> bool:
    text = (value or "").strip()
    return 1 <= len(text) <= 100


def valid_number_input(value: str) -> bool:
    text = (value or "").strip()
    return 5 <= len(text) <= 50 and bool(re.fullmatch(r"[0-9A-Za-z+\-\s]+", text))


async def notify_admins(bot: Bot, text: str) -> None:
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception as exc:
            logger.warning("Failed to notify admin %s: %s", admin_id, exc)


async def verify_user_memberships(bot: Bot, user_id: int, channels: list[asyncpg.Record]) -> tuple[bool, list[asyncpg.Record]]:
    missing: list[asyncpg.Record] = []
    for channel in channels:
        try:
            member = await bot.get_chat_member(channel["channel_id"], user_id)
            if member.status not in VALID_MEMBER_STATUSES:
                missing.append(channel)
        except Exception as exc:
            logger.warning(
                "Membership check failed for user %s in channel %s: %s",
                user_id,
                channel["channel_id"],
                exc,
            )
            missing.append(channel)
    return len(missing) == 0, missing


async def ensure_user_access(bot: Bot, user_id: int) -> tuple[bool, list[asyncpg.Record]]:
    channels = await get_required_channels()
    if not channels:
        await finalize_join_verification(user_id)
        return True, channels

    all_joined, _missing = await verify_user_memberships(bot, user_id, channels)
    if all_joined:
        await finalize_join_verification(user_id)
        return True, channels

    await set_join_verified(user_id, False)
    return False, channels


async def show_join_required(target: Message | CallbackQuery, channels: Optional[list[asyncpg.Record]] = None) -> None:
    channels = channels or await get_required_channels()
    keyboard = build_join_keyboard(channels)
    if isinstance(target, CallbackQuery):
        await target.message.answer(WELCOME_JOIN_TEXT, reply_markup=keyboard)
    else:
        await target.answer(WELCOME_JOIN_TEXT, reply_markup=keyboard)


async def send_menu_welcome(message: Message) -> None:
    await message.answer(WELCOME_SUCCESS_TEXT, reply_markup=main_menu_keyboard())


async def send_balance(message: Message, user_id: int) -> None:
    user = await get_user(user_id)
    if not user:
        return
    text = (
        "🌸 သင့်ရဲ့ ငွေစာရင်း 🌸\n\n"
        f"💵 လက်ကျန်ငွေ: {format_ks(int(user['balance']))} ကျပ်\n"
        f"👥 ဖိတ်ခေါ်ထားသူ: {int(user['total_referrals'])} ယောက်\n\n"
        "💡 လစဉ် 10 ယောက်ဖိတ်ခေါ်ရင် 30,000 ကျပ် ပြည့်ပြီး ငွေထုတ်နိုင်ပါသည် 💦"
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


async def send_referral_info(message: Message, user_id: int) -> None:
    ref_link = build_referral_link(user_id)
    text = (
        "💦 သင့်ရဲ့ သူငယ်ချင်းဖိတ်မယ် 🌸\n\n"
        "သင့်ရဲ့ မိတ်ဆွေလင့် (Referral Link) မှတဆင့် သူငယ်ချင်းတွေ Join ပြီး "
        "အောင်မြင်စွာ စတင်အသုံးပြုပါက 1 ယောက်လျှင် 3000 ကျပ် ရရှိမှာ ဖြစ်ပါတယ် 💵\n\n"
        "🔗 သင့်ရဲ့ Referral Link:\n"
        f"{ref_link}\n\n"
        "အောက်ပါ 'ဖိတ်ခေါ်မည် 🚀' Button ကိုနှိပ်ပြီး သူငယ်ချင်းများကို Group များထံသို့ "
        "လွယ်ကူစွာ ဖိတ်ခေါ်နိုင်ပါပြီ 🎉"
    )
    await message.answer(text, reply_markup=build_referral_share_keyboard(ref_link))


async def send_public_payout_post(bot: Bot, name: str, method: str, number: str) -> None:
    post = (
        "💦 *Thingyan Lucky Withdrawals* 🌸\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 {escape_markdown_v2(mask_name(name))}\n"
        f"💵 Amount: *{escape_markdown_v2(format_ks(WITHDRAW_AMOUNT))} Ks*\n"
        f"🏦 Via: _{escape_markdown_v2(method)}_ ✅\n"
        f"📱 Account: {escape_markdown_v2(mask_number(number))}\n\n"
        "⚡ *Real\\-time payouts are ongoing\\.\\.\\.*\n"
        "👥 လူများစွာ နေ့စဉ် ထုတ်ယူနေကြပါပြီ\\!\n\n"
        "🎁 *Invite friends & claim your Thingyan money now\\!* 💦"
    )
    await bot.send_message(
        PUBLIC_PAYOUT_CHANNEL_ID,
        post,
        parse_mode=ParseMode.MARKDOWN_V2,
        disable_web_page_preview=True,
    )


async def send_private_queue_post(bot: Bot, tg_user: Any, name: str, method: str, number: str, new_balance: int) -> None:
    username_text = f"@{tg_user.username}" if tg_user.username else "မရှိပါ"
    post = (
        "📥 *New Withdraw Queue*\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 Name: {escape_markdown_v2(name)}\n"
        f"🆔 User ID: `{escape_markdown_v2(tg_user.id)}`\n"
        f"🔗 Username: {escape_markdown_v2(username_text)}\n"
        f"🏦 Method: {escape_markdown_v2(method)}\n"
        f"📱 Number: {escape_markdown_v2(number)}\n"
        f"💵 Amount: {escape_markdown_v2(format_ks(WITHDRAW_AMOUNT))} Ks\n"
        f"💰 Balance After Deduction: {escape_markdown_v2(format_ks(new_balance))} Ks\n"
        f"🕒 Time: {escape_markdown_v2(yangon_now_str())}\n\n"
        "#withdraw\\_queue"
    )
    await bot.send_message(
        PRIVATE_WITHDRAW_QUEUE_CHANNEL_ID,
        post,
        parse_mode=ParseMode.MARKDOWN_V2,
        disable_web_page_preview=True,
    )


async def cancel_withdraw_flow(message: Message, state: FSMContext, notice: Optional[str] = None) -> None:
    await state.clear()
    await message.answer(
        notice or "❌ ငွေထုတ်လုပ်ငန်းစဉ်ကို ပယ်ဖျက်လိုက်ပါပြီ။",
        reply_markup=main_menu_keyboard(),
    )


def build_http_response(
    body: str = "OK",
    status: str = "200 OK",
    content_type: str = "text/plain; charset=utf-8",
    head_only: bool = False,
) -> bytes:
    body_bytes = body.encode("utf-8")
    headers = [
        f"HTTP/1.1 {status}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body_bytes)}",
        "Connection: close",
        "",
        "",
    ]
    header_bytes = "\r\n".join(headers).encode("utf-8")
    return header_bytes if head_only else header_bytes + body_bytes


async def health_http_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        data = await asyncio.wait_for(reader.read(4096), timeout=5)
        request_text = data.decode("utf-8", errors="ignore")
        first_line = request_text.splitlines()[0] if request_text else ""
        parts = first_line.split()

        method = parts[0].upper() if len(parts) >= 1 else "GET"
        path = parts[1] if len(parts) >= 2 else "/"
        head_only = method == "HEAD"

        if path in {"/", "/health", "/ping"} and method in {"GET", "HEAD"}:
            payload = (
                "{"
                f"\"status\":\"ok\"," 
                f"\"service\":\"telegram-referral-bot\"," 
                f"\"time\":\"{yangon_now_str()}\""
                "}"
            )
            response = build_http_response(
                body=payload,
                status="200 OK",
                content_type="application/json; charset=utf-8",
                head_only=head_only,
            )
        else:
            response = build_http_response(
                body="Not Found",
                status="404 Not Found",
                head_only=head_only,
            )

        writer.write(response)
        await writer.drain()
    except Exception as exc:
        logger.warning("Health server request error: %s", exc)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_health_server() -> None:
    server = await asyncio.start_server(health_http_handler, HOST, PORT)
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    logger.info("Health server listening on %s", addresses)

    async with server:
        await server.serve_forever()


# ============================================================
# ADMIN HANDLERS
# ============================================================
@router.message(Command("addchannel"))
async def admin_add_channel(message: Message) -> None:
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("❌ ဒီ command ကို Admin များသာ အသုံးပြုနိုင်ပါသည်။")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("အသုံးပြုပုံ: /addchannel <channel_id>")
        return

    try:
        channel_id = int(parts[1].strip())
    except ValueError:
        await message.answer("❌ channel_id မှန်ကန်သော number ဖြစ်ရပါမည်။")
        return

    try:
        chat = await message.bot.get_chat(channel_id)
        title = getattr(chat, "title", None) or getattr(chat, "full_name", None)
        username = getattr(chat, "username", None)
        await add_required_channel(channel_id, title, username)
        response_text = (
            "✅ Required Channel ထည့်ပြီးပါပြီ။\n\n"
            f"ID: {channel_id}\n"
            f"Title: {title or 'Unknown'}\n"
            f"Username: @{username}"
        ) if username else (
            "✅ Required Channel ထည့်ပြီးပါပြီ။\n\n"
            f"ID: {channel_id}\n"
            f"Title: {title or 'Unknown'}\n"
            "Username: မရှိပါ"
        )
        await message.answer(response_text)
    except Exception as exc:
        logger.exception("Failed to add required channel")
        await message.answer(f"❌ Channel ထည့်မရပါ။\nError: {exc}")


@router.message(Command("removechannel"))
async def admin_remove_channel(message: Message) -> None:
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("❌ ဒီ command ကို Admin များသာ အသုံးပြုနိုင်ပါသည်။")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("အသုံးပြုပုံ: /removechannel <channel_id>")
        return

    try:
        channel_id = int(parts[1].strip())
    except ValueError:
        await message.answer("❌ channel_id မှန်ကန်သော number ဖြစ်ရပါမည်။")
        return

    result = await remove_required_channel(channel_id)
    if result.endswith("0"):
        await message.answer("❌ ဖျက်မည့် Channel ကို မတွေ့ပါ။")
    else:
        await message.answer(f"✅ Channel ဖျက်ပြီးပါပြီ။\nID: {channel_id}")


@router.message(Command("channels"))
async def admin_list_channels(message: Message) -> None:
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("❌ ဒီ command ကို Admin များသာ အသုံးပြုနိုင်ပါသည်။")
        return

    channels = await get_required_channels()
    if not channels:
        await message.answer("ℹ️ Required Channel မရှိသေးပါ။")
        return

    lines = ["📋 Required Channel များ\n"]
    for idx, channel in enumerate(channels, start=1):
        url = build_channel_url(channel["username"])
        lines.append(
            f"{idx}. {channel_title_text(channel)}\n"
            f"   ID: {channel['channel_id']}\n"
            f"   Link: {url or 'Public username မရှိပါ'}"
        )
    await message.answer("\n\n".join(lines))


# ============================================================
# START / JOIN HANDLERS
# ============================================================
@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    try:
        await state.clear()
        await upsert_user(message.from_user, invited_by=parse_start_referrer(message.text))
        allowed, channels = await ensure_user_access(message.bot, message.from_user.id)
        if not allowed:
            await show_join_required(message, channels)
            return
        await send_menu_welcome(message)
    except Exception as exc:
        logger.exception("Error in /start handler")
        await message.answer("❌ အမှားတစ်ခု ဖြစ်သွားပါသည်။ နောက်မှ ထပ်စမ်းကြည့်ပါ။")
        await notify_admins(message.bot, f"Start handler error for {message.from_user.id}: {exc}")


@router.callback_query(F.data == "check_join")
async def check_join_callback(callback: CallbackQuery) -> None:
    if not callback.from_user:
        await callback.answer()
        return
    try:
        allowed, channels = await ensure_user_access(callback.bot, callback.from_user.id)
        if not allowed:
            await callback.answer("❌ Channel အားလုံးကို Join လုပ်ပြီးမှ ဆက်လုပ်နိုင်ပါမည်။", show_alert=True)
            await show_join_required(callback, channels)
            return
        await callback.answer("✅ အောင်မြင်ပါသည်။")
        await callback.message.answer(WELCOME_SUCCESS_TEXT, reply_markup=main_menu_keyboard())
    except Exception as exc:
        logger.exception("Error while checking join status")
        await callback.answer("❌ Join စစ်ဆေးရာတွင် အမှားဖြစ်သွားပါသည်။", show_alert=True)
        await notify_admins(callback.bot, f"Join check error for {callback.from_user.id}: {exc}")


@router.callback_query(F.data.startswith("channel_nourl:"))
async def no_url_channel_callback(callback: CallbackQuery) -> None:
    await callback.answer(
        "ဒီ Channel မှာ public username မရှိသေးပါဘူး။ Admin ကို Link ပြင်ပေးရန် အကြောင်းကြားပါ။",
        show_alert=True,
    )


# ============================================================
# MENU HANDLERS
# ============================================================
@router.message(F.text == "💰 ငွေစာရင်း")
async def balance_menu_handler(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    if await state.get_state():
        await state.clear()
    allowed, channels = await ensure_user_access(message.bot, message.from_user.id)
    if not allowed:
        await show_join_required(message, channels)
        return
    await send_balance(message, message.from_user.id)


@router.message(F.text == "👥 သူငယ်ချင်းဖိတ်မယ်")
async def referral_menu_handler(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    if await state.get_state():
        await state.clear()
    allowed, channels = await ensure_user_access(message.bot, message.from_user.id)
    if not allowed:
        await show_join_required(message, channels)
        return
    await send_referral_info(message, message.from_user.id)


@router.message(F.text == "🌸 အကူအညီ")
async def help_menu_handler(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    if await state.get_state():
        await state.clear()
    allowed, channels = await ensure_user_access(message.bot, message.from_user.id)
    if not allowed:
        await show_join_required(message, channels)
        return
    await message.answer(HELP_TEXT, reply_markup=main_menu_keyboard())


@router.message(F.text == "💸 ငွေထုတ်မယ်")
async def withdraw_menu_handler(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    if await state.get_state():
        await state.clear()
    allowed, channels = await ensure_user_access(message.bot, message.from_user.id)
    if not allowed:
        await show_join_required(message, channels)
        return

    user = await get_user(message.from_user.id)
    if not user:
        await message.answer("❌ User data မတွေ့ပါ။ /start ပြန်နှိပ်ပြီး ထပ်စမ်းကြည့်ပါ။")
        return

    balance = int(user["balance"])
    if balance < WITHDRAW_AMOUNT:
        await message.answer(
            "❌ ငွေထုတ်ရန် မလုံလောက်ပါ။\n\n"
            "ငွေထုတ်ရန် အနည်းဆုံး 30,000 ကျပ် ရှိရပါမည်။ သူငယ်ချင်းများကို ဆက်လက်ဖိတ်ခေါ်ပါ 🌸💦\n\n"
            f"လက်ကျန်ငွေ: {format_ks(balance)} ကျပ်",
            reply_markup=main_menu_keyboard(),
        )
        return

    await state.set_state(WithdrawFSM.choosing_method)
    await message.answer(
        "💸 ငွေထုတ်ရန် ငွေလက်ခံမည့် စနစ်ကို ရွေးပေးပါ 👇",
        reply_markup=withdraw_method_keyboard(),
    )


# ============================================================
# WITHDRAW FSM
# ============================================================
@router.message(F.text == "❌ မလုပ်တော့ပါ")
async def cancel_handler(message: Message, state: FSMContext) -> None:
    if await state.get_state():
        await cancel_withdraw_flow(message, state)
    else:
        await message.answer("ℹ️ လက်ရှိ ပယ်ဖျက်ရန် လုပ်ငန်းစဉ်မရှိပါ။", reply_markup=main_menu_keyboard())


@router.message(WithdrawFSM.choosing_method)
async def withdraw_choose_method(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if text not in {"Wave Pay", "KPay", "Phone Bill"}:
        await message.answer("❌ စနစ်ကို Button မှ ရွေးပေးပါ။", reply_markup=withdraw_method_keyboard())
        return
    await state.update_data(method=text)
    await state.set_state(WithdrawFSM.waiting_name)
    await message.answer("👤 ငွေလက်ခံမည့် အမည်ကို ရိုက်ထည့်ပေးပါ။", reply_markup=cancel_keyboard())


@router.message(WithdrawFSM.waiting_name)
async def withdraw_waiting_name(message: Message, state: FSMContext) -> None:
    name = (message.text or "").strip()
    if not valid_name_input(name):
        await message.answer("❌ အမည်ကို မှန်ကန်စွာ ရိုက်ထည့်ပေးပါ။", reply_markup=cancel_keyboard())
        return
    await state.update_data(name=name)
    await state.set_state(WithdrawFSM.waiting_number)
    await message.answer(
        "📱 ငွေလက်ခံမည့် ဖုန်းနံပါတ် / Account Number ကို ရိုက်ထည့်ပေးပါ။",
        reply_markup=cancel_keyboard(),
    )


@router.message(WithdrawFSM.waiting_number)
async def withdraw_waiting_number(message: Message, state: FSMContext) -> None:
    number = (message.text or "").strip()
    if not valid_number_input(number):
        await message.answer(
            "❌ ဖုန်းနံပါတ် / Account Number ကို မှန်ကန်စွာ ရိုက်ထည့်ပေးပါ။",
            reply_markup=cancel_keyboard(),
        )
        return

    await state.update_data(number=number, processing=False)
    data = await state.get_data()
    await state.set_state(WithdrawFSM.confirming)
    confirm_text = (
        "🌸 ငွေထုတ်အတည်ပြုချက် 🌸\n\n"
        f"💳 စနစ်: {data['method']}\n"
        f"👤 အမည်: {data['name']}\n"
        f"📱 အကောင့်: {data['number']}\n"
        f"💵 ငွေပမာဏ: {format_ks(WITHDRAW_AMOUNT)} ကျပ်\n\n"
        "အတည်ပြုလိုပါက '✅ အတည်ပြုမည်' ကိုနှိပ်ပါ။"
    )
    await message.answer(confirm_text, reply_markup=withdraw_confirm_keyboard())


@router.callback_query(WithdrawFSM.confirming, F.data == "withdraw_cancel")
async def withdraw_cancel_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.answer("❌ ပယ်ဖျက်လိုက်ပါပြီ။")
    await callback.message.answer(
        "❌ ငွေထုတ်လုပ်ငန်းစဉ်ကို ပယ်ဖျက်လိုက်ပါပြီ။",
        reply_markup=main_menu_keyboard(),
    )


@router.callback_query(WithdrawFSM.confirming, F.data == "withdraw_confirm")
async def withdraw_confirm_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.from_user:
        await callback.answer()
        return

    user_id = callback.from_user.id
    lock = get_withdraw_lock(user_id)
    async with lock:
        try:
            data = await state.get_data()
            if not data:
                await callback.answer("❌ Session သက်တမ်းကုန်သွားပါပြီ။", show_alert=True)
                await state.clear()
                return
            if data.get("processing"):
                await callback.answer("⏳ လုပ်ဆောင်နေပါသည်။")
                return
            await state.update_data(processing=True)

            allowed, channels = await ensure_user_access(callback.bot, user_id)
            if not allowed:
                await state.clear()
                await callback.answer("❌ Channel Join အခြေအနေ မမှန်ကန်တော့ပါ။", show_alert=True)
                await show_join_required(callback, channels)
                return

            new_balance = await deduct_balance_once(user_id, WITHDRAW_AMOUNT)
            if new_balance is None:
                await state.clear()
                await callback.answer("❌ ငွေလက်ကျန် မလုံလောက်တော့ပါ။", show_alert=True)
                await callback.message.answer(
                    "❌ ငွေထုတ်ရန် မလုံလောက်ပါ။\n\n"
                    "ငွေထုတ်ရန် အနည်းဆုံး 30,000 ကျပ် ရှိရပါမည်။ သူငယ်ချင်းများကို ဆက်လက်ဖိတ်ခေါ်ပါ 🌸💦",
                    reply_markup=main_menu_keyboard(),
                )
                return

            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass

            await callback.answer("✅ အောင်မြင်ပါသည်။")
            await callback.message.answer(
                "✅ ငွေထုတ်တောင်းဆိုမှု အောင်မြင်စွာ လက်ခံပြီးပါပြီ 💦\n\n"
                "Admin မှ စစ်ဆေးပြီး ငွေလွှဲပေးပါမည်။\n"
                "ကျေးဇူးတင်ပါသည် 🌸",
                reply_markup=main_menu_keyboard(),
            )

            try:
                await send_public_payout_post(callback.bot, data["name"], data["method"], data["number"])
            except Exception as exc:
                logger.exception("Failed to send public payout post for user %s", user_id)
                await notify_admins(callback.bot, f"Public payout post failed for user {user_id}: {exc}")

            try:
                await send_private_queue_post(
                    callback.bot,
                    callback.from_user,
                    data["name"],
                    data["method"],
                    data["number"],
                    new_balance,
                )
            except Exception as exc:
                logger.exception("Failed to send private queue post for user %s", user_id)
                await notify_admins(callback.bot, f"Private withdraw queue post failed for user {user_id}: {exc}")

            await state.clear()
        except Exception as exc:
            logger.exception("Withdraw confirmation error for user %s", user_id)
            await state.clear()
            await callback.answer("❌ အမှားတစ်ခု ဖြစ်သွားပါသည်။ နောက်မှ ထပ်စမ်းကြည့်ပါ။", show_alert=True)
            await notify_admins(callback.bot, f"Withdraw confirm error for user {user_id}: {exc}")


# ============================================================
# FALLBACK
# ============================================================
@router.message()
async def fallback_handler(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    try:
        allowed, channels = await ensure_user_access(message.bot, message.from_user.id)
        if not allowed:
            await show_join_required(message, channels)
            return
        if await state.get_state():
            await message.answer("လုပ်ငန်းစဉ်ကို ဆက်လုပ်ပါ သို့မဟုတ် ❌ မလုပ်တော့ပါ ကိုနှိပ်ပါ။")
            return
        await message.answer("🌸 Menu မှ လုပ်ဆောင်ချက်တစ်ခုကို ရွေးပေးပါ 👇", reply_markup=main_menu_keyboard())
    except Exception as exc:
        logger.exception("Fallback handler error")
        await message.answer("❌ အမှားတစ်ခု ဖြစ်သွားပါသည်။ နောက်မှ ထပ်စမ်းကြည့်ပါ။")
        await notify_admins(message.bot, f"Fallback handler error for {message.from_user.id}: {exc}")


# ============================================================
# MAIN
# ============================================================
async def main() -> None:
    global db_pool, BOT_USERNAME

    logger.info("Starting Thingyan referral/withdraw bot")
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    await init_db()
    logger.info("Database connected and tables ensured")

    bot = Bot(BOT_TOKEN)
    me = await bot.get_me()
    if not BOT_USERNAME:
        BOT_USERNAME = (me.username or "").lstrip("@") or None
    logger.info("Bot connected as @%s", BOT_USERNAME)

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    polling_task = asyncio.create_task(
        dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    )
    health_task = asyncio.create_task(run_health_server())

    try:
        done, pending = await asyncio.wait(
            {polling_task, health_task},
            return_when=asyncio.FIRST_EXCEPTION,
        )
        for task in done:
            exc = task.exception()
            if exc:
                raise exc
    finally:
        for task in (polling_task, health_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(polling_task, health_task, return_exceptions=True)
        await bot.session.close()
        if db_pool is not None:
            await db_pool.close()
        logger.info("Bot stopped")


if __name__ == "__main__":
    asyncio.run(main())
