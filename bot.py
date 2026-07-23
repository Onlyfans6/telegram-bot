"""
Complete Telegram Bot — Referral System, Premium, Admin Panel, Analytics
Production-Optimized: Async HTTP, Env Vars, Retry Logic, Rate Limiting
Built for python-telegram-bot v21.11, Python 3.13, Pydroid 3 on Android
"""

import logging
import os
import sys
import datetime
import json
import asyncio
from typing import Optional, Dict, Any, List

import httpx
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    MenuButtonCommands
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    filters,
    Application
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden

# =============================================================================
# CONFIGURATION — LOAD FROM ENVIRONMENT VARIABLES
# =============================================================================

TELEGRAM_BOT_TOKEN = "8785130315:AAHKTBZSBzsRm9KO8eqfM3LNGEhq9vwtcRk"

CHANNEL_ID = "-1004335036924"

ADMIN_USER_IDS = [8739934872]

SUPABASE_URL = "https://fspqpgqxilhrbygnogqx.supabase.co"

SUPABASE_KEY = "sb_publishable_uabfnPvn4fPg4vDh8KK_EQ_g0WP_Sdj"

# Validate required environment variables
missing = []
if not TELEGRAM_BOT_TOKEN:
    missing.append("8785130315:AAHKTBZSBzsRm9KO8eqfM3LNGEhq9vwtcRk")
if not CHANNEL_ID:
    missing.append("-1004335036924")
if not SUPABASE_URL:
    missing.append("https://fspqpgqxilhrbygnogqx.supabase.co")
if not SUPABASE_KEY:
    missing.append("sb_publishable_uabfnPvn4fPg4vDh8KK_EQ_g0WP_Sdj")

if missing:
    print(f"FATAL: Missing environment variables: {', '.join(missing)}")
    print("Set them before running:")
    print("  export BOT_TOKEN=your_token")
    print("  export CHANNEL_ID=-1001234567890")
    print("  export SUPABASE_URL=https://your-project.supabase.co")
    print("  export SUPABASE_KEY=your_service_role_key")
    print("  export ADMIN_USER_IDS=123456789")
    sys.exit(1)

if not ADMIN_USER_IDS:
    print("WARNING: No admin user IDs configured.")
    
# Optional config with defaults
PREMIUM_PRICE = os.getenv("PREMIUM_PRICE", "$10.00")
PAYMENT_METHODS = os.getenv("PAYMENT_METHODS", "Crypto, PayPal, Bank Transfer")
SUBSCRIBE_LINK = os.getenv("SUBSCRIBE_LINK", "https://onlyfan.fun/?u=Lilmissteee")

STATE_PAYMENT_UPLOAD = 1

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# ASYNC SUPABASE CLIENT WITH RETRY LOGIC
# =============================================================================

class AsyncSupabaseClient:
    """
    Fully async Supabase REST client with automatic retry and exponential backoff.
    Uses httpx.AsyncClient for non-blocking I/O.
    """

    RETRYABLE_STATUS = {500, 502, 503, 504}
    MAX_RETRIES = 3
    BACKOFF_DELAYS = [0, 1, 2]  # seconds between retries
    TIMEOUT = 30.0

    def __init__(self, url: str, key: str):
        self.url = url.rstrip("/")
        self.key = key
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=httpx.Timeout(self.TIMEOUT, connect=10.0)
        )

    def _build_filter(self, col: str, val: Any) -> str:
        """Build filter string with operator support."""
        if isinstance(val, str) and any(val.startswith(op) for op in ("gte.", "lte.", "gt.", "lt.", "neq.", "like.", "ilike.")):
            return f"{col}={val}"
        return f"{col}=eq.{val}"

    def _log_error(self, method: str, table: str, status: int, body: str, url: str) -> None:
        """Comprehensive error logging for production debugging."""
        logger.error(
            "[SUPABASE ERROR] %s %s | Status: %d | Body: %s | URL: %s",
            method, table, status, body[:500], url
        )

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Execute request with retry logic and exponential backoff."""
        last_exception = None
        for attempt in range(self.MAX_RETRIES):
            try:
                response = await self.client.request(method, url, **kwargs)
                if response.status_code in self.RETRYABLE_STATUS and attempt < self.MAX_RETRIES - 1:
                    delay = self.BACKOFF_DELAYS[attempt]
                    logger.warning(
                        "[SUPABASE RETRY] %s %s | Status: %d | Attempt %d/%d | Waiting %ds",
                        method, url, response.status_code, attempt + 1, self.MAX_RETRIES, delay
                    )
                    await asyncio.sleep(delay)
                    continue
                return response
            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as e:
                last_exception = e
                if attempt < self.MAX_RETRIES - 1:
                    delay = self.BACKOFF_DELAYS[attempt]
                    logger.warning(
                        "[SUPABASE RETRY] %s %s | Error: %s | Attempt %d/%d | Waiting %ds",
                        method, url, str(e), attempt + 1, self.MAX_RETRIES, delay
                    )
                    await asyncio.sleep(delay)
                    continue
                raise
        raise last_exception

    async def select(self, table: str, columns: str = "*", filters: Dict[str, Any] = None,
                     order: str = None, limit: int = None, eq: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Async select with full operator support."""
        query_parts = [f"select={columns}"]
        if eq:
            for col, val in eq.items():
                query_parts.append(self._build_filter(col, val))
        if filters:
            for col, val in filters.items():
                query_parts.append(self._build_filter(col, val))
        if order:
            query_parts.append(f"order={order}")
        if limit:
            query_parts.append(f"limit={limit}")

        url = f"{self.url}/rest/v1/{table}?" + "&".join(query_parts)
        response = await self._request("GET", url)

        if response.status_code >= 400:
            self._log_error("SELECT", table, response.status_code, response.text, url)
        response.raise_for_status()
        return response.json() if response.text else []

    async def insert(self, table: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Async insert."""
        url = f"{self.url}/rest/v1/{table}"
        response = await self._request("POST", url, json=data)
        if response.status_code >= 400:
            self._log_error("INSERT", table, response.status_code, response.text, url)
            logger.error("[INSERT DATA] %s", json.dumps(data, default=str))
        response.raise_for_status()
        return response.json() if response.text else []

    async def update(self, table: str, data: Dict[str, Any], eq: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Async update."""
        query_parts = [f"{col}=eq.{val}" for col, val in eq.items()]
        url = f"{self.url}/rest/v1/{table}?" + "&".join(query_parts)
        response = await self._request("PATCH", url, json=data)
        if response.status_code >= 400:
            self._log_error("UPDATE", table, response.status_code, response.text, url)
            logger.error("[UPDATE DATA] %s", json.dumps(data, default=str))
        response.raise_for_status()
        return response.json() if response.text else []

    async def delete(self, table: str, eq: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Async delete."""
        query_parts = [f"{col}=eq.{val}" for col, val in eq.items()]
        url = f"{self.url}/rest/v1/{table}?" + "&".join(query_parts)
        response = await self._request("DELETE", url)
        if response.status_code >= 400:
            self._log_error("DELETE", table, response.status_code, response.text, url)
        response.raise_for_status()
        return response.json() if response.text else []

    async def count(self, table: str, filters: Dict[str, Any] = None) -> int:
        """Async count with Content-Range header."""
        headers = dict(self.headers)
        headers["Prefer"] = "count=exact"
        query_parts = []
        if filters:
            for col, val in filters.items():
                query_parts.append(self._build_filter(col, val))

        url = f"{self.url}/rest/v1/{table}"
        if query_parts:
            url += "?" + "&".join(query_parts)

        response = await self._request("GET", url, headers=headers)
        if response.status_code >= 400:
            self._log_error("COUNT", table, response.status_code, response.text, url)
        response.raise_for_status()

        content_range = response.headers.get("content-range", "")
        if content_range:
            parts = content_range.split("/")
            if len(parts) == 2:
                return int(parts[1])
        data = response.json() if response.text else []
        return len(data)

    async def close(self) -> None:
        """Close the async client."""
        await self.client.aclose()


# Global client instance
supabase: AsyncSupabaseClient

# =============================================================================
# HELPERS
# =============================================================================

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS

async def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    try:
        users = await supabase.select("users", eq={"telegram_id": user_id})
        return users[0] if users else None
    except Exception as e:
        logger.error("[get_user] %d: %s", user_id, e)
        return None

async def safe_answer_callback(query) -> bool:
    """Answer callback query, ignore expired ones."""
    try:
        await query.answer()
        return True
    except BadRequest as e:
        msg = str(e).lower()
        if any(x in msg for x in ("query is too old", "query id is invalid", "response timeout")):
            logger.warning("[callback] Expired query ignored")
            return False
        raise

async def safe_edit_message(query, text: str, parse_mode: str = ParseMode.HTML, reply_markup=None) -> None:
    """Edit message text safely."""
    try:
        current = query.message.text or query.message.caption or ""
        if current.strip() == text.strip():
            return
        await query.edit_message_text(text=text, parse_mode=parse_mode, reply_markup=reply_markup)
    except BadRequest as e:
        msg = str(e).lower()
        if "message is not modified" in msg:
            return
        if any(x in msg for x in ("query is too old", "response timeout", "message to edit not found")):
            logger.warning("[edit] Cannot edit expired/deleted message")
            return
        raise
    except Exception as e:
        logger.error("[edit] %s", e)
        raise

async def safe_edit_caption(query, caption: str, parse_mode: str = ParseMode.HTML, reply_markup=None) -> None:
    """Edit caption safely."""
    try:
        current = query.message.caption or ""
        if current.strip() == caption.strip():
            return
        await query.edit_message_caption(caption=caption, parse_mode=parse_mode, reply_markup=reply_markup)
    except BadRequest as e:
        msg = str(e).lower()
        if "message is not modified" in msg:
            return
        if any(x in msg for x in ("query is too old", "response timeout", "message to edit not found")):
            logger.warning("[edit_caption] Cannot edit expired/deleted message")
            return
        raise
    except Exception as e:
        logger.error("[edit_caption] %s", e)
        raise

def get_channel_link() -> str:
    if CHANNEL_ID.startswith("-100"):
        return f"https://t.me/c/{CHANNEL_ID.replace('-100', '')}"
    return "https://t.me/yourchannel"

async def check_channel_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id=int(CHANNEL_ID), user_id=user_id)
        return member.status in ("member", "administrator", "creator")
    except BadRequest as e:
        logger.error("[channel_check] %d: %s", user_id, e)
        return False
    except Exception as e:
        logger.error("[channel_check] %d: %s", user_id, e)
        return False

# =============================================================================
# KEYBOARDS
# =============================================================================

def main_menu_keyboard(is_admin_user: bool = False) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("👤 My Profile", callback_data="menu_profile")],
        [InlineKeyboardButton("🏆 Leaderboard", callback_data="menu_leaderboard")],
        [InlineKeyboardButton("💎 Upgrade to Premium", callback_data="menu_premium")],
        [InlineKeyboardButton("📤 Upload Payment Proof", callback_data="menu_upload")],
        [InlineKeyboardButton("🔞 Subscribe to My Profile", url=SUBSCRIBE_LINK)],
    ]
    if is_admin_user:
        keyboard.append([InlineKeyboardButton("🔧 Admin Dashboard", callback_data="menu_admin")])
    return InlineKeyboardMarkup(keyboard)

def admin_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Analytics", callback_data="admin_analytics")],
        [InlineKeyboardButton("📋 Pending Payments", callback_data="admin_pending")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton("⏰ Send Reminders", callback_data="admin_reminders")],
        [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="menu_back")]
    ])

def payment_approval_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"approve_{user_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"reject_{user_id}")
        ]
    ])
def get_channel_link() -> str:
         return                                                      "https://t.me/LilmissteeeExclusive"

def join_channel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Join Channel", url=get_channel_link())],
        [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")]
    ])

# =============================================================================
# USER MANAGEMENT
# =============================================================================

async def create_or_update_user(user_id: int, username: str, first_name: str, last_name: str, referral_id: Optional[str] = None) -> bool:
    """Create or update user with self-referral prevention and referrer validation."""
    try:
        existing = await get_user(user_id)
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        if existing:
            await supabase.update("users", {
                "first_name": first_name,
                "username": username,
            }, eq={"telegram_id": user_id})
            logger.info("[user_updated] telegram_id=%d", user_id)
            return True

        # Validate referral
        referred_by = None
        if referral_id and str(referral_id).isdigit():
            ref_id = int(referral_id)
            if ref_id != user_id:  # Self-referral prevention
                referrer = await get_user(ref_id)
                if referrer:
                    referred_by = ref_id

        await supabase.insert("users", {
            "telegram_id": user_id,
            "username": username,
            "first_name": first_name,
            "joined_at": now,
            "referred_by": referred_by,
            "referral_count": 0
        })
        logger.info("[user_created] telegram_id=%d, referred_by=%s", user_id, referred_by)

        # Update referrer count
        if referred_by:
            referrer = await get_user(referred_by)
            if referrer:
                new_count = (referrer.get("referral_count", 0) or 0) + 1
                await supabase.update("users", {
                    "referral_count": new_count
                }, eq={"telegram_id": referred_by})

                # Log referral
                try:
                    await supabase.insert("referrals", {
                        "referrer_id": referred_by,
                        "referred_id": user_id,
                        "created_at": now
                    })
                except Exception as e:
                    logger.warning("[referral_log] %s", e)

        return True
    except Exception as e:
        logger.error("[create_user] %d: %s", user_id, e)
        raise

# =============================================================================
# COMMAND HANDLERS
# =============================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start with referral system."""
    user = update.effective_user
    if not user:
        logger.error("[start] update.effective_user is None")
        return

    user_id = user.id
    referral_id = context.args[0] if context.args else None

    try:
        await create_or_update_user(
            user_id=user_id,
            username=user.username or "",
            first_name=user.first_name or "",
            last_name=user.last_name or "",
            referral_id=referral_id
        )
    except Exception as e:
        logger.error("[start] DB error for %d: %s", user_id, e)
        await update.message.reply_text(
            f"⚠️ <b>Database Error</b>\n\n<code>{str(e)[:300]}</code>\n\nPlease try again.",
            parse_mode=ParseMode.HTML
        )
        return

    db_user = await get_user(user_id)
    is_admin_user = is_admin(user_id)

    welcome = (
        f"👋 <b>Welcome, {user.first_name or 'User'}!</b>\n\n"
        f"🤖 Premium Bot with full referral system.\n\n"
        f"📌 <b>Your Referral Link:</b>\n"
        f"<code>https://t.me/{context.bot.username}?start={user_id}</code>\n\n"
        f"🔗 <b>Join our channel to unlock all features:</b>"
    )

    is_member = await check_channel_membership(user_id, context)
    if not is_member:
        await update.message.reply_text(
            welcome + "\n\n⚠️ <b>You must join the channel first!</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=join_channel_keyboard()
        )
    else:
        await update.message.reply_text(
            welcome,
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(is_admin_user=is_admin_user)
        )

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /profile."""
    user = update.effective_user
    if not user:
        return
    user_id = user.id

    db_user = await get_user(user_id)
    if not db_user:
        await update.message.reply_text("⚠️ <b>Profile not found.</b> Use /start first.", parse_mode=ParseMode.HTML)
        return

    if not await check_channel_membership(user_id, context):
        await update.message.reply_text(
            "⚠️ <b>Access Denied!</b>\n\nJoin the channel first using /start",
            parse_mode=ParseMode.HTML
        )
        return

    joined = db_user.get("joined_at", "N/A")
    if isinstance(joined, str):
        joined = joined[:10]

    referred_by = db_user.get("referred_by")
    referred_text = f"\n👤 <b>Referred By:</b> <code>{referred_by}</code>" if referred_by else ""

    text = (
        f"👤 <b>Your Profile</b>\n\n"
        f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
        f"👤 <b>Name:</b> {db_user.get('first_name', 'N/A')}\n"
        f"📛 <b>Username:</b> @{db_user.get('username', 'N/A')}\n"
        f"📅 <b>Joined:</b> {joined}\n"
        f"👥 <b>Referrals:</b> {db_user.get('referral_count', 0) or 0}"
        f"{referred_text}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /leaderboard."""
    user = update.effective_user
    if not user:
        return
    user_id = user.id

    if not await check_channel_membership(user_id, context):
        await update.message.reply_text(
            "⚠️ <b>Access Denied!</b>\n\nJoin the channel first using /start",
            parse_mode=ParseMode.HTML
        )
        return

    try:
        leaders = await supabase.select("users", columns="telegram_id,first_name,username,referral_count", order="referral_count.desc", limit=10)
        if not leaders:
            await update.message.reply_text("🏆 <b>No referrals yet!</b> Be the first!", parse_mode=ParseMode.HTML)
            return

        text = "🏆 <b>Top Referrers</b>\n\n"
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        for idx, leader in enumerate(leaders):
            name = leader.get("first_name") or leader.get("username") or f"User {leader['telegram_id']}"
            refs = leader.get("referral_count", 0) or 0
            medal = medals[idx] if idx < len(medals) else "➡️"
            text += f"{medal} <b>{name}</b> — {refs} referrals\n"
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error("[leaderboard] %s", e)
        await update.message.reply_text("⚠️ Error loading leaderboard.", parse_mode=ParseMode.HTML)

async def premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /premium."""
    user = update.effective_user
    if not user:
        return
    user_id = user.id

    if not await check_channel_membership(user_id, context):
        await update.message.reply_text(
            "⚠️ <b>Access Denied!</b>\n\nJoin the channel first using /start",
            parse_mode=ParseMode.HTML
        )
        return

    text = (
        f"💎 <b>Upgrade to Premium</b>\n\n"
        f"• ✅ Unlimited access\n"
        f"• 🚀 Priority support\n"
        f"• 📊 Advanced analytics\n"
        f"• 🎁 Exclusive content\n\n"
        f"💰 <b>Price:</b> {PREMIUM_PRICE}\n"
        f"💳 <b>Methods:</b> {PAYMENT_METHODS}\n\n"
        f"📤 <b>Send payment proof:</b> /upload"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start payment proof upload."""
    user = update.effective_user
    if not user:
        return ConversationHandler.END
    user_id = user.id

    if not await check_channel_membership(user_id, context):
        await update.message.reply_text(
            "⚠️ <b>Access Denied!</b>\n\nJoin the channel first using /start",
            parse_mode=ParseMode.HTML
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "📤 <b>Upload Payment Proof</b>\n\n"
        "Send a screenshot of your payment receipt.\n\n"
        "Type /cancel to abort.",
        parse_mode=ParseMode.HTML
    )
    return STATE_PAYMENT_UPLOAD

async def receive_payment_proof(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive payment proof photo."""
    user = update.effective_user
    if not user:
        return ConversationHandler.END
    user_id = user.id

    if not update.message.photo:
        await update.message.reply_text(
            "⚠️ Please send a <b>photo</b> as payment proof.\n\nType /cancel to abort.",
            parse_mode=ParseMode.HTML
        )
        return STATE_PAYMENT_UPLOAD

    photo = update.message.photo[-1]
    file_id = photo.file_id

    try:
        for admin_id in ADMIN_USER_IDS:
            try:
                await context.bot.send_photo(
                    chat_id=admin_id,
                    photo=file_id,
                    caption=(
                        f"🆕 <b>New Payment Proof</b>\n\n"
                        f"👤 <b>User:</b> {user.first_name or 'N/A'} (@{user.username or 'N/A'})\n"
                        f"🆔 <b>ID:</b> <code>{user_id}</code>\n\n"
                        f"Approve or reject below:"
                    ),
                    parse_mode=ParseMode.HTML,
                    reply_markup=payment_approval_keyboard(user_id)
                )
            except Exception as e:
                logger.error("[notify_admin] %d: %s", admin_id, e)

        await update.message.reply_text(
            "✅ <b>Payment proof received!</b>\n\n"
            "⏳ Under review. You'll be notified when approved.\n\n"
            "Use /menu to return.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error("[upload] %s", e)
        await update.message.reply_text("⚠️ Error uploading proof.", parse_mode=ParseMode.HTML)

    return ConversationHandler.END

async def handle_non_photo_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle non-photo messages during payment upload conversation."""
    await update.message.reply_text(
        "⚠️ Please send a payment <b>screenshot/photo</b> only.\n\n"
        "Text, videos, documents, stickers, and voice messages are not accepted.\n\n"
        "Type /cancel to abort.",
        parse_mode=ParseMode.HTML
    )
    return STATE_PAYMENT_UPLOAD

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "❌ <b>Cancelled.</b>\n\nUse /menu to return.",
        parse_mode=ParseMode.HTML
    )
    return ConversationHandler.END

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    await update.message.reply_text(
        "📋 <b>Main Menu</b>\n\nChoose an option:",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(is_admin_user=is_admin(user.id))
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "📖 <b>Bot Commands</b>\n\n"
        "/start — Start & get referral link\n"
        "/menu — Main menu\n"
        "/profile — Your profile\n"
        "/leaderboard — Top referrers\n"
        "/premium — Premium info\n"
        "/upload — Upload payment proof\n"
        "/help — This message\n\n"
        "💎 <b>Premium:</b> Unlimited access, priority support, exclusive content"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

# =============================================================================
# CALLBACK HANDLERS
# =============================================================================

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not await safe_answer_callback(query):
        return

    data = query.data
    user = update.effective_user
    if not user:
        return
    user_id = user.id
    is_admin_user = is_admin(user_id)

    # Channel check for non-admin actions
    if not data.startswith("admin_") and data not in ("menu_back", "check_join"):
        if not await check_channel_membership(user_id, context):
            await safe_edit_message(
                query,
                "⚠️ <b>Access Denied!</b>\n\nPlease join the channel first.",
                reply_markup=join_channel_keyboard()
            )
            return

    # Menu callbacks
    if data == "menu_profile":
        await show_profile_callback(query, user_id)
    elif data == "menu_leaderboard":
        await show_leaderboard_callback(query)
    elif data == "menu_premium":
        await show_premium_callback(query)
    elif data == "menu_upload":
        await safe_edit_message(
            query,
            "📤 <b>Upload Payment Proof</b>\n\nUse /upload to send your receipt.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📤 Upload Now", callback_data="trigger_upload")],
                [InlineKeyboardButton("🔙 Back", callback_data="menu_back")]
            ])
        )
    elif data == "trigger_upload":
        await safe_edit_message(query, "📤 Send your payment proof photo now...\n\nType /cancel to abort.")

    elif data in ("menu_back", "check_join"):
        if not await check_channel_membership(user_id, context):
            await safe_edit_message(
                query, "⚠️ <b>Please join the channel first!</b>", reply_markup=join_channel_keyboard()
            )
            return
        text = (
            f"👋 <b>Welcome back!</b>\n\n"
            f"📌 Your Referral Link:\n"
            f"<code>https://t.me/{context.bot.username}?start={user_id}</code>"
        )
        await safe_edit_message(query, text, reply_markup=main_menu_keyboard(is_admin_user=is_admin_user))

    # Admin callbacks
    elif data == "menu_admin":
        if not is_admin_user:
            await query.answer("⛔ Admin only!", show_alert=True)
            return
        await safe_edit_message(query, "🔧 <b>Admin Dashboard</b>\n\nSelect an option:", reply_markup=admin_menu_keyboard())

    elif data == "admin_analytics":
        await show_analytics(query)
    elif data == "admin_pending":
        await show_pending_payments(query)
    elif data == "admin_broadcast":
        await safe_edit_message(
            query,
            "📢 <b>Broadcast Message</b>\n\nSend the message to broadcast.\n\nType /cancel to abort."
        )
        context.user_data["broadcast_state"] = True
    elif data == "admin_reminders":
        await send_reminders(query, context)

    # Payment approval
    elif data.startswith("approve_"):
        target_id = int(data.split("_")[1])
        await approve_payment(query, target_id, context)
    elif data.startswith("reject_"):
        target_id = int(data.split("_")[1])
        await reject_payment(query, target_id, context)

async def show_profile_callback(query, user_id: int) -> None:
    db_user = await get_user(user_id)
    if not db_user:
        await safe_edit_message(query, "⚠️ Profile not found.")
        return

    joined = db_user.get("joined_at", "N/A")
    if isinstance(joined, str):
        joined = joined[:10]

    referred_by = db_user.get("referred_by")
    referred_text = f"\n👤 Referred By: <code>{referred_by}</code>" if referred_by else ""

    text = (
        f"👤 <b>Your Profile</b>\n\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"👤 Name: {db_user.get('first_name', 'N/A')}\n"
        f"📛 Username: @{db_user.get('username', 'N/A')}\n"
        f"📅 Joined: {joined}\n"
        f"👥 Referrals: {db_user.get('referral_count', 0) or 0}"
        f"{referred_text}"
    )
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back", callback_data="menu_back")]
    ]))

async def show_leaderboard_callback(query) -> None:
    try:
        leaders = await supabase.select("users", columns="telegram_id,first_name,username,referral_count", order="referral_count.desc", limit=10)
        if not leaders:
            await safe_edit_message(query, "🏆 No referrals yet! Be the first!", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="menu_back")]
            ]))
            return

        text = "🏆 <b>Top Referrers</b>\n\n"
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        for idx, leader in enumerate(leaders):
            name = leader.get("first_name") or leader.get("username") or f"User {leader['telegram_id']}"
            refs = leader.get("referral_count", 0) or 0
            medal = medals[idx] if idx < len(medals) else "➡️"
            text += f"{medal} <b>{name}</b> — {refs} referrals\n"
        await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="menu_back")]
        ]))
    except Exception as e:
        logger.error("[leaderboard_cb] %s", e)
        await safe_edit_message(query, "⚠️ Error loading leaderboard.")

async def show_premium_callback(query) -> None:
    text = (
        f"💎 <b>Upgrade to Premium</b>\n\n"
        f"• ✅ Unlimited access\n"
        f"• 🚀 Priority support\n"
        f"• 📊 Advanced analytics\n"
        f"• 🎁 Exclusive content\n\n"
        f"💰 <b>Price:</b> {PREMIUM_PRICE}\n"
        f"💳 <b>Methods:</b> {PAYMENT_METHODS}\n\n"
        f"📤 Use /upload to send payment proof"
    )
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Upload Proof", callback_data="trigger_upload")],
        [InlineKeyboardButton("🔙 Back", callback_data="menu_back")]
    ]))

async def show_analytics(query) -> None:
    try:
        total_users = await supabase.count("users")
        total_referrals = await supabase.count("referrals")

        today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        today_resp = await supabase.select("users", columns="*", filters={"joined_at": f"gte.{today}T00:00:00Z"})
        today_count = len(today_resp) if today_resp else 0

        week_ago = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        week_resp = await supabase.select("users", columns="*", filters={"joined_at": f"gte.{week_ago}T00:00:00Z"})
        week_count = len(week_resp) if week_resp else 0

        text = (
            f"📊 <b>Bot Analytics</b>\n\n"
            f"👥 Total Users: <b>{total_users}</b>\n"
            f"🔗 Total Referrals: <b>{total_referrals}</b>\n"
            f"📈 New Today: <b>{today_count}</b>\n"
            f"📅 New This Week: <b>{week_count}</b>\n\n"
            f"🕐 {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="admin_analytics")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_admin")]
        ]))
    except Exception as e:
        logger.error("[analytics] %s", e)
        await safe_edit_message(query, "⚠️ Error loading analytics.")

async def show_pending_payments(query) -> None:
    await safe_edit_message(
        query,
        "📋 <b>Pending Payments</b>\n\n"
        "Payment proofs are sent directly to admin chat.\n"
        "Check your private messages for approval requests.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="menu_admin")]
        ])
    )

async def approve_payment(query, target_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    admin_id = query.from_user.id if query.from_user else 0
    if not is_admin(admin_id):
        await query.answer("⛔ Admin only!", show_alert=True)
        return

    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                "🎉 <b>Payment Approved!</b>\n\n"
                "✅ Your premium subscription is now active!\n\n"
                "Thank you for your support! 💎"
            ),
            parse_mode=ParseMode.HTML
        )
        await safe_edit_caption(
            query,
            f"✅ <b>APPROVED</b>\n\nUser ID: <code>{target_id}</code>\nStatus: Premium Activated"
        )
        logger.info("[approved] admin=%d user=%d", admin_id, target_id)
    except Exception as e:
        logger.error("[approve] %s", e)
        await query.answer("⚠️ Error approving!", show_alert=True)

async def reject_payment(query, target_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    admin_id = query.from_user.id if query.from_user else 0
    if not is_admin(admin_id):
        await query.answer("⛔ Admin only!", show_alert=True)
        return

    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                "❌ <b>Payment Rejected</b>\n\n"
                "Your payment proof was not approved.\n"
                "Please try again with /upload or contact support."
            ),
            parse_mode=ParseMode.HTML
        )
        await safe_edit_caption(
            query,
            f"❌ <b>REJECTED</b>\n\nUser ID: <code>{target_id}</code>"
        )
        logger.info("[rejected] admin=%d user=%d", admin_id, target_id)
    except Exception as e:
        logger.error("[reject] %s", e)
        await query.answer("⚠️ Error rejecting!", show_alert=True)

async def send_reminders(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    admin_id = query.from_user.id if query.from_user else 0
    if not is_admin(admin_id):
        await query.answer("⛔ Admin only!", show_alert=True)
        return

    try:
        users = await supabase.select("users", columns="telegram_id")
        sent = 0
        failed = 0

        for user in users:
            try:
                await context.bot.send_message(
                    chat_id=user["telegram_id"],
                    text=(
                        "⏰ <b>Reminder</b>\n\n"
                        "Don't miss out on Premium features!\n"
                        f"Upgrade now for just {PREMIUM_PRICE}.\n\n"
                        "Use /premium to learn more 💎"
                    ),
                    parse_mode=ParseMode.HTML
                )
                sent += 1
                await asyncio.sleep(0.05)  # 20 msg/sec rate limit
            except Forbidden:
                logger.warning("[reminder] Blocked by %d", user["telegram_id"])
                failed += 1
            except Exception as e:
                logger.error("[reminder] %d: %s", user["telegram_id"], e)
                failed += 1

        await safe_edit_message(
            query,
            f"✅ <b>Reminders Sent!</b>\n\n"
            f"📤 Successful: {sent}\n"
            f"❌ Failed: {failed}\n"
            f"👥 Total: {len(users)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="menu_admin")]
            ])
        )
    except Exception as e:
        logger.error("[reminders] %s", e)
        await safe_edit_message(query, "⚠️ Error sending reminders.")

# =============================================================================
# BROADCAST
# =============================================================================

async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    if not context.user_data.get("broadcast_state"):
        return

    message_text = update.message.text
    context.user_data["broadcast_message"] = message_text
    context.user_data["broadcast_state"] = False

    await update.message.reply_text(
        f"📢 <b>Broadcast Preview</b>\n\n"
        f"{message_text}\n\n"
        f"Send <b>/confirm</b> to broadcast.\n"
        f"Send <b>/cancel</b> to abort.",
        parse_mode=ParseMode.HTML
    )

async def confirm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id if update.effective_user else 0
    if not is_admin(user_id):
        return

    message_text = context.user_data.get("broadcast_message")
    if not message_text:
        await update.message.reply_text("⚠️ No broadcast message. Start with /broadcast")
        return

    try:
        users = await supabase.select("users", columns="telegram_id")
        sent = 0
        failed = 0

        for user in users:
            try:
                await context.bot.send_message(
                    chat_id=user["telegram_id"],
                    text=f"📢 <b>Announcement</b>\n\n{message_text}",
                    parse_mode=ParseMode.HTML
                )
                sent += 1
                await asyncio.sleep(0.05)  # 20 msg/sec rate limit
            except Forbidden:
                logger.warning("[broadcast] Blocked by %d", user["telegram_id"])
                failed += 1
            except Exception as e:
                logger.error("[broadcast] %d: %s", user["telegram_id"], e)
                failed += 1

        await update.message.reply_text(
            f"✅ <b>Broadcast Complete!</b>\n\n"
            f"📤 Sent: {sent}\n"
            f"❌ Failed: {failed}\n"
            f"👥 Total: {len(users)}",
            parse_mode=ParseMode.HTML
        )
        context.user_data.pop("broadcast_message", None)
    except Exception as e:
        logger.error("[broadcast] %s", e)
        await update.message.reply_text("⚠️ Error during broadcast.", parse_mode=ParseMode.HTML)

# =============================================================================
# ERROR HANDLER
# =============================================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("[error] %s", context.error, exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ <b>An error occurred.</b> Please try again.",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

# =============================================================================
# MAIN
# =============================================================================

async def post_init(application: Application) -> None:
    commands = [
        BotCommand("start", "Start the bot"),
        BotCommand("menu", "Show main menu"),
        BotCommand("profile", "View your profile"),
        BotCommand("leaderboard", "Top referrers"),
        BotCommand("premium", "Premium info"),
        BotCommand("upload", "Upload payment proof"),
        BotCommand("help", "Show help")
    ]
    await application.bot.set_my_commands(commands)
    await application.bot.set_chat_menu_button(menu_button=MenuButtonCommands())

async def post_shutdown(application: Application) -> None:
    """Clean up resources on shutdown."""
    logger.info("[shutdown] Closing Supabase client...")
    await supabase.close()
    logger.info("[shutdown] Supabase client closed.")

def main() -> None:
    global supabase
    supabase = AsyncSupabaseClient(SUPABASE_URL, SUPABASE_KEY)

    logger.info("=" * 60)
    logger.info("BOT STARTING")
    logger.info("=" * 60)
    logger.info("Supabase URL: %s", SUPABASE_URL)
    logger.info("Channel ID: %s", CHANNEL_ID)
    logger.info("Admin IDs: %s", ADMIN_USER_IDS)

    application = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # Conversation handler with photo filter + non-photo handler
    upload_conv = ConversationHandler(
        entry_points=[CommandHandler("upload", upload_command)],
        states={
            STATE_PAYMENT_UPLOAD: [
                MessageHandler(filters.PHOTO, receive_payment_proof),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_non_photo_upload),
                MessageHandler(filters.VIDEO, handle_non_photo_upload),
                MessageHandler(filters.Document.ALL, handle_non_photo_upload),
                MessageHandler(filters.Sticker.ALL, handle_non_photo_upload),
                MessageHandler(filters.VOICE, handle_non_photo_upload),
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("menu", menu))
    application.add_handler(CommandHandler("profile", profile))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("premium", premium))
    application.add_handler(upload_conv)
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("confirm", confirm_broadcast))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_message))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_error_handler(error_handler)

    logger.info("Bot running. Press Ctrl+C to stop.")
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES
    )

if __name__ == "__main__":
    main()
