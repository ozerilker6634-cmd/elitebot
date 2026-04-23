"""
Elite Earners — Gateway Bot (Entry Bot)
========================================
Ana satış botunu şikayetlerden korumak için giriş kapısı.
Kullanıcılar önce bu bottan geçer, human verification sonrası ana bota yönlenir.

Özellikler:
- Matematik captcha (rastgele toplama/çarpma sorusu)
- Emoji captcha (doğru emojiyi seç)
- Rate limiting (brute-force koruması)
- Doğrulanmış kullanıcıları hatırlar (SQLite)
- Admin: kullanıcı istatistikleri, broadcast

Kurulum:
1. BotFather'dan yeni bot oluştur (bu Gateway Bot)
2. .env dosyasına GATEWAY_BOT_TOKEN ve MAIN_BOT_LINK ekle
3. python gateway_bot.py
"""

import os
import sys
import json
import random
import time
import logging
import sqlite3
import hashlib
import threading
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ============================================================
# CONFIG
# ============================================================
GATEWAY_TOKEN     = os.getenv("GATEWAY_BOT_TOKEN", "").strip()
MAIN_BOT_LINK     = os.getenv("MAIN_BOT_LINK", "https://t.me/EliteEarnersBot").strip()
MAIN_BOT_USERNAME = os.getenv("MAIN_BOT_USERNAME", "EliteEarnersBot").strip()
MAIN_CHANNEL_LINK = os.getenv("MAIN_CHANNEL_LINK", "https://t.me/Eliteearners66").strip()
STOCK_CHANNEL_LINK = os.getenv("STOCK_NEWS_CHANNEL_LINK", "https://t.me/EliteEarnersStockBotnews").strip()
PROOF_CHANNEL_LINK = os.getenv("PROOF_CHANNEL_LINK", "https://t.me/EliteEarnersProof").strip()
ADMIN_CHAT_ID     = int(os.getenv("GATEWAY_ADMIN_CHAT_ID", os.getenv("ADMIN_CHAT_ID", "0")).strip() or "0")

# Captcha ayarları
CAPTCHA_TIMEOUT_SECONDS = 120      # Captcha süresi
CAPTCHA_MAX_ATTEMPTS = 3           # Max yanlış deneme
RATE_LIMIT_SECONDS = 30            # Başarısız deneme sonrası bekleme
VERIFIED_EXPIRY_DAYS = 365         # Doğrulama geçerlilik süresi

# Captcha tipi: "math", "emoji", "mixed" (rastgele)
CAPTCHA_TYPE = "mixed"

# ============================================================
# LOGGING
# ============================================================
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gateway.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("gateway_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ============================================================
# DATABASE
# ============================================================
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gateway.db")

_db_initialized = False
_db_lock = threading.Lock()


def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    global _db_initialized
    if _db_initialized:
        return
    with _db_lock:
        if _db_initialized:
            return
        conn = get_db()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS verified_users (
                user_id      INTEGER PRIMARY KEY,
                username     TEXT,
                full_name    TEXT,
                verified_at  TEXT NOT NULL,
                captcha_type TEXT,
                attempts     INTEGER DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS captcha_attempts (
                user_id      INTEGER PRIMARY KEY,
                fail_count   INTEGER DEFAULT 0,
                last_attempt TEXT,
                blocked_until TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()
        conn.close()
        _db_initialized = True
        logger.info("Gateway database initialized")


def is_verified(user_id: int) -> bool:
    init_db()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT verified_at FROM verified_users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return False
    # Expiry check
    try:
        verified_at = datetime.strptime(row["verified_at"], "%Y-%m-%d %H:%M:%S")
        days_passed = (datetime.now() - verified_at).days
        return days_passed < VERIFIED_EXPIRY_DAYS
    except Exception:
        return True


def mark_verified(user_id: int, username: str, full_name: str, captcha_type: str) -> None:
    init_db()
    conn = get_db()
    conn.execute("""
        INSERT OR REPLACE INTO verified_users (user_id, username, full_name, verified_at, captcha_type)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, username, full_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), captcha_type))
    conn.commit()
    conn.close()


def get_fail_count(user_id: int) -> tuple:
    """(fail_count, blocked_until) döner."""
    init_db()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT fail_count, blocked_until FROM captcha_attempts WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return (0, None)
    return (row["fail_count"], row["blocked_until"])


def add_fail(user_id: int) -> int:
    """Başarısız deneme ekle, yeni fail_count döner."""
    init_db()
    conn = get_db()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO captcha_attempts (user_id, fail_count, last_attempt)
        VALUES (?, 1, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            fail_count = fail_count + 1,
            last_attempt = ?
    """, (user_id, now, now))

    # Max attempts aşıldıysa blokla
    cur = conn.cursor()
    cur.execute("SELECT fail_count FROM captcha_attempts WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    count = row["fail_count"] if row else 1

    if count >= CAPTCHA_MAX_ATTEMPTS:
        from datetime import timedelta
        blocked = (datetime.now() + timedelta(seconds=RATE_LIMIT_SECONDS * count)).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE captcha_attempts SET blocked_until = ? WHERE user_id = ?", (blocked, user_id))

    conn.commit()
    conn.close()
    return count


def clear_fails(user_id: int) -> None:
    init_db()
    conn = get_db()
    conn.execute("DELETE FROM captcha_attempts WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def is_blocked(user_id: int) -> bool:
    fails, blocked_until = get_fail_count(user_id)
    if not blocked_until:
        return False
    try:
        bt = datetime.strptime(blocked_until, "%Y-%m-%d %H:%M:%S")
        if datetime.now() < bt:
            return True
        # Blok süresi doldu — temizle
        clear_fails(user_id)
        return False
    except Exception:
        return False


def get_total_verified() -> int:
    init_db()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as c FROM verified_users")
    row = cur.fetchone()
    conn.close()
    return row["c"] if row else 0


def get_today_verified() -> int:
    init_db()
    conn = get_db()
    cur = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    cur.execute("SELECT COUNT(*) as c FROM verified_users WHERE verified_at LIKE ?", (f"{today}%",))
    row = cur.fetchone()
    conn.close()
    return row["c"] if row else 0


# ============================================================
# CAPTCHA GENERATION
# ============================================================

def generate_math_captcha() -> tuple:
    """Matematik sorusu → (soru_text, doğru_cevap, seçenekler)"""
    op = random.choice(["add", "multiply"])
    if op == "add":
        a, b = random.randint(10, 50), random.randint(10, 50)
        answer = a + b
        question = f"🔢 What is {a} + {b}?"
    else:
        a, b = random.randint(2, 12), random.randint(2, 12)
        answer = a * b
        question = f"🔢 What is {a} × {b}?"

    # Yanlış seçenekler oluştur
    wrong = set()
    while len(wrong) < 3:
        offset = random.randint(-10, 10)
        if offset == 0:
            continue
        w = answer + offset
        if w > 0 and w != answer:
            wrong.add(w)

    options = list(wrong) + [answer]
    random.shuffle(options)
    return (question, str(answer), [str(o) for o in options], "math")


def generate_emoji_captcha() -> tuple:
    """Emoji sorusu → doğru emojiyi seç."""
    emoji_groups = {
        "fruit":   ["🍎", "🍊", "🍋", "🍇", "🍓", "🍑", "🍒", "🥝", "🍌", "🍉"],
        "animal":  ["🐶", "🐱", "🐭", "🐰", "🦊", "🐻", "🐼", "🐨", "🦁", "🐸"],
        "vehicle": ["🚗", "🚕", "🚌", "🏎️", "🚀", "✈️", "🚁", "🛥️", "🚂", "🏍️"],
        "weather": ["☀️", "🌧️", "❄️", "⛈️", "🌈", "🌪️", "🌤️", "🌙", "⭐", "☁️"],
    }

    # Hedef kategori
    target_cat = random.choice(list(emoji_groups.keys()))
    target_emoji = random.choice(emoji_groups[target_cat])

    # Diğer kategorilerden yanlış seçenekler
    other_cats = [c for c in emoji_groups.keys() if c != target_cat]
    wrong_emojis = []
    for cat in random.sample(other_cats, min(3, len(other_cats))):
        wrong_emojis.append(random.choice(emoji_groups[cat]))

    cat_names = {
        "fruit": "a fruit", "animal": "an animal",
        "vehicle": "a vehicle", "weather": "a weather symbol",
    }
    question = f"🎯 Select {cat_names.get(target_cat, target_cat)}:"

    options = wrong_emojis[:3] + [target_emoji]
    random.shuffle(options)
    return (question, target_emoji, options, "emoji")


def generate_captcha() -> tuple:
    """Rastgele captcha tipi seç."""
    if CAPTCHA_TYPE == "math":
        return generate_math_captcha()
    elif CAPTCHA_TYPE == "emoji":
        return generate_emoji_captcha()
    else:  # mixed
        return random.choice([generate_math_captcha, generate_emoji_captcha])()


# ============================================================
# BILINGUAL TEXT — TR/EN
# ============================================================

TEXTS = {
    "welcome_back": {
        "en": "✅ Welcome back, {name}!\n\nYou are already verified.\nClick below to access the shop:",
        "tr": "✅ Tekrar hoş geldin, {name}!\n\nZaten doğrulanmışsın.\nMağazaya erişmek için aşağıya tıkla:",
    },
    "blocked": {
        "en": "⛔ Too many failed attempts.\n\nPlease try again later.\nBlocked until: {until}",
        "tr": "⛔ Çok fazla başarısız deneme.\n\nLütfen daha sonra tekrar deneyin.\nEngel süresi: {until}",
    },
    "welcome": {
        "en": (
            "🏆 Welcome to Elite Earners!\n\n"
            "We offer first-hand prepaid cards & store gift cards at discounted member rates.\n\n"
            "✓ Instant card delivery\n"
            "✓ Fresh stock, never relisted\n"
            "✓ LTC & USDC accepted\n"
            "✓ 24/7 automated service\n\n"
            "To access the shop, please complete a quick verification below.\n"
            "This helps us keep bots and spammers out."
        ),
        "tr": (
            "🏆 Elite Earners'a Hoş Geldiniz!\n\n"
            "İndirimli üye fiyatlarıyla prepaid kart ve mağaza hediye kartları sunuyoruz.\n\n"
            "✓ Anında kart teslimi\n"
            "✓ Taze stok, asla yeniden listelenmez\n"
            "✓ LTC & USDC kabul edilir\n"
            "✓ 7/24 otomatik hizmet\n\n"
            "Mağazaya erişmek için aşağıdaki doğrulamayı tamamlayın.\n"
            "Bu, bot ve spam gönderenleri engellememize yardımcı olur."
        ),
    },
    "verify_btn": {"en": "✅ Verify Now", "tr": "✅ Şimdi Doğrula"},
    "shop_btn": {"en": "🛒 Open Shop", "tr": "🛒 Mağazayı Aç"},
    "join_btn": {"en": "🚀 Join Now", "tr": "🚀 Şimdi Katıl"},
    "captcha_title": {
        "en": "🔐 Human Verification\n\n{question}\n\nSelect the correct answer:",
        "tr": "🔐 İnsan Doğrulaması\n\n{question}\n\nDoğru cevabı seçin:",
    },
    "captcha_new": {"en": "🔄 New Question", "tr": "🔄 Yeni Soru"},
    "captcha_timeout": {
        "en": "⏱️ Time's up! Captcha expired.\n\nClick below to try again.",
        "tr": "⏱️ Süre doldu! Captcha süresi bitti.\n\nTekrar denemek için aşağıya tıklayın.",
    },
    "captcha_retry": {"en": "🔄 Try Again", "tr": "🔄 Tekrar Dene"},
    "verified": {
        "en": "✅ Verification Complete!\n\nWelcome, {name}! You are now verified.\n\nClick below to access the shop and start buying cards at the best rates.\n\n👥 Verified Members: {total}",
        "tr": "✅ Doğrulama Tamamlandı!\n\nHoş geldin, {name}! Artık doğrulanmış durumdasın.\n\nEn iyi fiyatlarla kart almaya başlamak için aşağıya tıkla.\n\n👥 Doğrulanmış Üyeler: {total}",
    },
    "wrong_blocked": {
        "en": "❌ Wrong answer!\n\n⛔ Too many failed attempts.\nPlease wait {seconds} seconds and try /start again.",
        "tr": "❌ Yanlış cevap!\n\n⛔ Çok fazla başarısız deneme.\nLütfen {seconds} saniye bekleyip /start yazın.",
    },
    "wrong_retry": {
        "en": "❌ Wrong answer!\n\nAttempts remaining: {remaining}\nClick below to try again.",
        "tr": "❌ Yanlış cevap!\n\nKalan deneme: {remaining}\nTekrar denemek için aşağıya tıklayın.",
    },
}


def _get_lang(user) -> str:
    """Kullanıcının Telegram dil ayarından TR/EN belirle."""
    lang = getattr(user, "language_code", "") or ""
    return "tr" if lang.lower().startswith("tr") else "en"


def _t(key: str, user, **kwargs) -> str:
    """Çeviri getir — kullanıcı diline göre."""
    lang = _get_lang(user)
    template = TEXTS.get(key, {}).get(lang, TEXTS.get(key, {}).get("en", key))
    try:
        return template.format(**kwargs) if kwargs else template
    except Exception:
        return template


# ============================================================
# BOT HANDLERS
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    user_id = user.id

    # Zaten doğrulanmış mı?
    if is_verified(user_id):
        await update.message.reply_text(
            _t("welcome_back", user, name=user.first_name),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(_t("shop_btn", user), url=MAIN_BOT_LINK)],
                [InlineKeyboardButton("💬 Chat Channel", url=MAIN_CHANNEL_LINK),
                 InlineKeyboardButton("📢 Stock Channel", url=STOCK_CHANNEL_LINK)],
                [InlineKeyboardButton("📋 Proof Channel", url=PROOF_CHANNEL_LINK)],
            ]),
        )
        return

    # Bloklu mu?
    if is_blocked(user_id):
        fails, blocked_until = get_fail_count(user_id)
        await update.message.reply_text(
            _t("blocked", user, until=blocked_until),
        )
        return

    # Hoşgeldin mesajı + captcha başlat
    await update.message.reply_text(
        _t("welcome", user),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(_t("verify_btn", user), callback_data="start_captcha")],
            [InlineKeyboardButton("💬 Chat Channel", url=MAIN_CHANNEL_LINK),
             InlineKeyboardButton("📢 Stock Channel", url=STOCK_CHANNEL_LINK)],
            [InlineKeyboardButton("📋 Proof Channel", url=PROOF_CHANNEL_LINK)],
        ]),
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass

    user = query.from_user
    user_id = user.id

    # ── Start Captcha ──
    if query.data == "start_captcha":
        if is_verified(user_id):
            await query.message.reply_text(
                _t("welcome_back", user, name=user.first_name),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(_t("shop_btn", user), url=MAIN_BOT_LINK)],
                ]),
            )
            return

        if is_blocked(user_id):
            await query.answer("⛔", show_alert=True)
            return

        # Captcha oluştur
        question, answer, options, cap_type = generate_captcha()

        # Context'e kaydet
        context.user_data["captcha_answer"] = answer
        context.user_data["captcha_type"] = cap_type
        context.user_data["captcha_time"] = time.time()

        # Butonlar
        rows = []
        row = []
        for opt in options:
            row.append(InlineKeyboardButton(opt, callback_data=f"captcha:{opt}"))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton(_t("captcha_new", user), callback_data="start_captcha")])

        captcha_text = _t("captcha_title", user, question=question)
        try:
            await query.edit_message_text(captcha_text, reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            await query.message.reply_text(captcha_text, reply_markup=InlineKeyboardMarkup(rows))
        return

    # ── Captcha Answer ──
    if query.data.startswith("captcha:"):
        selected = query.data.split(":", 1)[1]
        correct = context.user_data.get("captcha_answer", "")
        cap_type = context.user_data.get("captcha_type", "")
        cap_time = context.user_data.get("captcha_time", 0)

        # Timeout check
        if time.time() - cap_time > CAPTCHA_TIMEOUT_SECONDS:
            await query.edit_message_text(
                _t("captcha_timeout", user),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(_t("captcha_retry", user), callback_data="start_captcha")],
                ]),
            )
            return

        if selected == correct:
            # ✅ BAŞARILI
            mark_verified(
                user_id,
                f"@{user.username}" if user.username else "",
                user.full_name,
                cap_type,
            )
            clear_fails(user_id)

            # Temizle
            context.user_data.pop("captcha_answer", None)
            context.user_data.pop("captcha_type", None)
            context.user_data.pop("captcha_time", None)

            total = get_total_verified()

            await query.edit_message_text(
                _t("verified", user, name=user.first_name, total=total),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(_t("shop_btn", user), url=MAIN_BOT_LINK)],
                    [InlineKeyboardButton("💬 Chat Channel", url=MAIN_CHANNEL_LINK),
                     InlineKeyboardButton("📢 Stock Channel", url=STOCK_CHANNEL_LINK)],
                    [InlineKeyboardButton("📋 Proof Channel", url=PROOF_CHANNEL_LINK)],
                ]),
            )

            logger.info(f"[VERIFIED] {user.full_name} ({user_id}) via {cap_type}")

            # Admin bildirim
            if ADMIN_CHAT_ID:
                try:
                    await context.bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=(
                            f"🆕 New Verified User\n\n"
                            f"Name: {user.full_name}\n"
                            f"Username: @{user.username or 'none'}\n"
                            f"ID: {user_id}\n"
                            f"Language: {_get_lang(user)}\n"
                            f"Captcha: {cap_type}\n"
                            f"Total verified: {total}"
                        ),
                    )
                except Exception:
                    pass
        else:
            # ❌ YANLIŞ
            fail_count = add_fail(user_id)
            remaining = CAPTCHA_MAX_ATTEMPTS - fail_count

            if remaining <= 0:
                await query.edit_message_text(
                    _t("wrong_blocked", user, seconds=RATE_LIMIT_SECONDS * fail_count),
                )
                logger.warning(f"[BLOCKED] {user.full_name} ({user_id}) — {fail_count} fails")
            else:
                await query.edit_message_text(
                    _t("wrong_retry", user, remaining=remaining),
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(_t("captcha_retry", user), callback_data="start_captcha")],
                    ]),
                )
        return


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /stats — gateway istatistikleri."""
    if update.effective_chat.id != ADMIN_CHAT_ID:
        return
    total = get_total_verified()
    today = get_today_verified()
    await update.message.reply_text(
        f"📊 Gateway Stats\n\n"
        f"Total Verified: {total}\n"
        f"Today: {today}\n"
        f"Captcha Type: {CAPTCHA_TYPE}\n"
        f"Max Attempts: {CAPTCHA_MAX_ATTEMPTS}\n"
        f"Timeout: {CAPTCHA_TIMEOUT_SECONDS}s"
    )


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /broadcast mesaj — tüm doğrulanmış kullanıcılara mesaj gönder."""
    if update.effective_chat.id != ADMIN_CHAT_ID:
        return
    text = (update.message.text or "").replace("/broadcast", "", 1).strip()
    if not text:
        await update.message.reply_text("Usage: /broadcast Your message here")
        return

    init_db()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM verified_users")
    rows = cur.fetchall()
    conn.close()

    sent = 0
    failed = 0
    for row in rows:
        try:
            await context.bot.send_message(chat_id=row["user_id"], text=text)
            sent += 1
        except Exception:
            failed += 1

    await update.message.reply_text(f"Broadcast complete.\nSent: {sent}\nFailed: {failed}")


async def error_handler(update, context) -> None:
    logger.error(f"Gateway error: {context.error}")


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    if not GATEWAY_TOKEN:
        logger.critical("GATEWAY_BOT_TOKEN not set!")
        return

    logger.info("Starting Gateway Bot...")
    logger.info(f"Main bot link: {MAIN_BOT_LINK}")

    app = Application.builder().token(GATEWAY_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_error_handler(error_handler)

    logger.info("Gateway Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
