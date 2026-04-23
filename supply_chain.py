"""
Elite Earners — Supply Chain Agent
====================================
Rakip botların stok kanallarını dinler, kartları parse eder,
external_cards tablosuna yazar. Ana bot bu tabloyu okuyarak
kartları kendi listinginde gösterir.

Müşteri sipariş verdiğinde bu script rakip bottan otomatik
satın alıp kart bilgisini extract eder.

Kurulum:
1. my.telegram.org'dan API ID + Hash al
2. .env dosyasına ekle
3. python supply_chain.py (ilk çalıştırmada telefon doğrulaması ister)

Gereksinimler:
    pip install telethon python-dotenv
"""

import os
import sys
import re
import json
import time
import asyncio
import sqlite3
import logging
import hashlib
import threading
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

# ============================================================
# CONFIG
# ============================================================

TELEGRAM_API_ID   = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
TELEGRAM_PHONE    = os.getenv("TELEGRAM_PHONE", "").strip()  # +905xx...
SESSION_NAME      = os.getenv("SC_SESSION_NAME", "supply_chain_session")

# Ana bot DB — paylaşılan SQLite dosyası
MAIN_DB_FILE = os.getenv("SQLITE_DB_FILE",
    os.path.join(os.getenv("BOT_DATA_DIR", "."), "elite_bot.db"))

# Kendi stok kanalımız — external kartlar buraya da postlanır
_raw_stock_id = os.getenv("STOCK_NEWS_CHANNEL_ID",
    os.getenv("CHANNEL_ID", "0")).strip() or "0"
OUR_STOCK_CHANNEL_ID = int(_raw_stock_id)
POST_TO_OWN_CHANNEL = True  # Kendi kanalımıza post at

# Competitor bot configs
COMPETITORS = {
    "guru": {
        "bot_username": "GuruPrepaidBot",
        "stock_channel": os.getenv("GURU_STOCK_CHANNEL", ""),  # kanal ID veya invite link
        "stock_channel_link": "https://t.me/+rBw-IEIkP-44YWU0",
        "currency": "USD",
        "rate_type": "fixed_price",  # Fiyatı doğrudan veriyor
    },
    "xstock": {
        "bot_username": "XprepaidsExchangeBot",
        "stock_channel": os.getenv("XSTOCK_CHANNEL", ""),
        "stock_channel_link": "https://t.me/+_aBahO_NBONlM2Zk",
        "currency": "USDT",
        "rate_type": "rate_percent",  # Rate yüzdesi veriyor
    },
    "planet": {
        "bot_username": "GCCPLANETBOT",
        "stock_channel": os.getenv("PLANET_STOCK_CHANNEL", ""),
        "stock_channel_link": "",
        "currency": "USD",
        "rate_type": "rate_percent",
    },
    "sharks": {
        "bot_username": "Sharksprepaidbot",
        "stock_channel": os.getenv("SHARKS_STOCK_CHANNEL", ""),
        "stock_channel_link": "",
        "currency": "USD",
        "rate_type": "rate_percent",
    },
}

# Kamuflaj fiyatlandırma — rakip fiyatının üzerine görünmez yüzde
# Kart bakiyesine değil, rakibin SATIŞ FİYATINA yüzde eklenir
CAMO_MARGINS = [
    (0,    10,   0.07),   # $0-10   → rakip fiyatı + %7
    (10,   50,   0.05),   # $10-50  → rakip fiyatı + %5
    (50,   100,  0.04),   # $50-100 → rakip fiyatı + %4
    (100,  500,  0.03),   # $100-500 → rakip fiyatı + %3
    (500,  99999, 0.025), # $500+   → rakip fiyatı + %2.5
]

# Kaç dakika sonra external kart expire olsun
EXTERNAL_CARD_TTL_MINUTES = 45

# Aktif stok tarama aralığı (dakika)
ACTIVE_SCAN_INTERVAL_MINUTES = 5

# ============================================================
# LOGGING
# ============================================================

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "supply_chain.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("supply_chain")

# ============================================================
# DATABASE — external_cards tablosu (ana bot ile paylaşılır)
# ============================================================

_db_initialized = False
_db_lock = threading.Lock()


def get_db():
    conn = sqlite3.connect(MAIN_DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_external_cards_table():
    global _db_initialized
    if _db_initialized:
        return
    with _db_lock:
        if _db_initialized:
            return
        conn = get_db()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS external_cards (
                ext_id          TEXT PRIMARY KEY,
                source          TEXT NOT NULL,
                source_bot      TEXT NOT NULL,
                bin_number      TEXT,
                balance         REAL NOT NULL,
                currency        TEXT DEFAULT 'USD',
                cost_price      REAL NOT NULL,
                sell_price      REAL NOT NULL,
                profit          REAL NOT NULL,
                rate_percent    REAL,
                provider        TEXT,
                registered      INTEGER DEFAULT -1,
                used_paypal     INTEGER DEFAULT 0,
                used_google     INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'available',
                channel_msg_id  INTEGER,
                added_at        TEXT NOT NULL,
                expires_at      TEXT NOT NULL,
                purchased_at    TEXT,
                purchased_by    INTEGER,
                raw_message     TEXT,
                channel_posted  INTEGER DEFAULT 0,
                parsing_notes   TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ext_status ON external_cards(status, source)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ext_balance ON external_cards(status, balance)")
        # Migrations for existing databases
        for col, typ in [
            ("channel_posted", "INTEGER DEFAULT 0"),
            ("parsing_notes", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE external_cards ADD COLUMN {col} {typ}")
            except Exception:
                pass  # Column already exists
        conn.commit()
        conn.close()
        _db_initialized = True
        logger.info("external_cards table ready")


def get_camo_rate(balance: float) -> float:
    """Bakiye aralığına göre kamuflaj yüzdesini döndür."""
    for low, high, rate in CAMO_MARGINS:
        if low <= balance < high:
            return rate
    return 0.025  # Varsayılan %2.5


def calculate_sell_price(cost_price: float, balance: float) -> tuple:
    """Kamuflaj fiyatlandırma: rakip fiyatının üzerine görünmez yüzde.
    sell_price = cost_price × (1 + camo_rate)
    Minimum kâr: $0.02 (çok küçük kartlar için)"""
    camo_rate = get_camo_rate(balance)
    sell_price = round(cost_price * (1 + camo_rate), 2)
    profit = round(sell_price - cost_price, 2)

    # Minimum kâr koruması
    if profit < 0.02:
        profit = 0.02
        sell_price = round(cost_price + 0.02, 2)

    return (sell_price, profit)


def save_external_card(card_data: dict) -> bool:
    """Yeni external kart kaydet. Yeni/reactivated ise True, zaten aktif ise False."""
    init_external_cards_table()

    raw_key = f"{card_data['source']}:{card_data.get('bin', '')}:{card_data['balance']}:{card_data.get('channel_msg_id', '')}"
    ext_id = hashlib.md5(raw_key.encode()).hexdigest()[:16]

    now = datetime.now()
    expires = now + timedelta(minutes=EXTERNAL_CARD_TTL_MINUTES)

    sell_price, profit = calculate_sell_price(card_data["cost_price"], card_data["balance"])

    # Registration: True→1, False→0, None→-1 (unknown)
    reg_val = card_data.get("registered")
    if reg_val is True:
        reg_int = 1
    elif reg_val is False:
        reg_int = 0
    else:
        reg_int = -1  # Unknown

    parsing_notes = card_data.get("parsing_notes", "")

    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("SELECT ext_id, status, registered FROM external_cards WHERE ext_id = ?", (ext_id,))
        existing = cur.fetchone()

        if existing and existing["status"] in ("available", "reserved", "purchased"):
            # Active card — update, not new
            # Only overwrite registered if we have new confirmed info (not unknown)
            if reg_int != -1:
                conn.execute("""
                    UPDATE external_cards SET
                        sell_price = ?, profit = ?, cost_price = ?,
                        registered = ?,
                        expires_at = ?,
                        parsing_notes = CASE WHEN ? != '' THEN ? ELSE parsing_notes END
                    WHERE ext_id = ?
                """, (sell_price, profit, card_data["cost_price"],
                      reg_int, expires.strftime("%Y-%m-%d %H:%M:%S"),
                      parsing_notes, parsing_notes, ext_id))
            else:
                # Unknown — preserve existing registered
                conn.execute("""
                    UPDATE external_cards SET
                        sell_price = ?, profit = ?, cost_price = ?,
                        expires_at = ?
                    WHERE ext_id = ?
                """, (sell_price, profit, card_data["cost_price"],
                      expires.strftime("%Y-%m-%d %H:%M:%S"), ext_id))
            conn.commit()
            conn.close()
            return False

        elif existing:
            # Expired/failed → reactivate
            conn.execute("""
                UPDATE external_cards SET
                    status = 'available', sell_price = ?, profit = ?, cost_price = ?,
                    registered = ?,
                    expires_at = ?, added_at = ?,
                    parsing_notes = ?
                WHERE ext_id = ?
            """, (sell_price, profit, card_data["cost_price"],
                  reg_int, expires.strftime("%Y-%m-%d %H:%M:%S"),
                  now.strftime("%Y-%m-%d %H:%M:%S"),
                  parsing_notes, ext_id))
            conn.commit()
            conn.close()
            logger.info(f"[STOCK] Reactivated: {card_data['source']} | BIN:{card_data.get('bin','')} | "
                        f"Bal:${card_data['balance']:.2f} | Cost:${card_data['cost_price']:.2f} → "
                        f"Sell:${sell_price:.2f} (+${profit:.2f}) | Reg:{reg_val}")
            return True

        else:
            # Completely new card
            conn.execute("""
                INSERT INTO external_cards
                (ext_id, source, source_bot, bin_number, balance, currency, cost_price,
                 sell_price, profit, rate_percent, provider, registered, used_paypal,
                 used_google, status, channel_msg_id, added_at, expires_at, raw_message, parsing_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'available', ?, ?, ?, ?, ?)
            """, (
                ext_id,
                card_data["source"],
                card_data["source_bot"],
                card_data.get("bin", ""),
                card_data["balance"],
                card_data.get("currency", "USD"),
                card_data["cost_price"],
                sell_price,
                profit,
                card_data.get("rate_percent"),
                card_data.get("provider", ""),
                reg_int,
                1 if card_data.get("used_paypal") else 0,
                1 if card_data.get("used_google") else 0,
                card_data.get("channel_msg_id"),
                now.strftime("%Y-%m-%d %H:%M:%S"),
                expires.strftime("%Y-%m-%d %H:%M:%S"),
                card_data.get("raw_message", "")[:500],
                parsing_notes[:200] if parsing_notes else "",
            ))
            conn.commit()
            conn.close()
            logger.info(f"[STOCK] New: {card_data['source']} | BIN:{card_data.get('bin','')} | "
                        f"Bal:${card_data['balance']:.2f} | Cost:${card_data['cost_price']:.2f} → "
                        f"Sell:${sell_price:.2f} (+${profit:.2f}) | Reg:{reg_val}")
            return True

    except Exception as e:
        logger.error(f"save_external_card error: {e}")
        return False


def get_available_external_cards(provider: str = None, min_balance: float = 0,
                                  max_balance: float = 99999) -> list:
    """Ana bot tarafından çağrılır — listing için kullanılabilir kartları döndürür."""
    init_external_cards_table()
    conn = get_db()
    cur = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    query = """
        SELECT * FROM external_cards
        WHERE status = 'available' AND expires_at > ?
        AND balance >= ? AND balance <= ?
    """
    params = [now, min_balance, max_balance]

    if provider:
        query += " AND provider LIKE ?"
        params.append(f"%{provider}%")

    query += " ORDER BY balance DESC"
    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def reserve_external_card(ext_id: str, user_id: int) -> bool:
    """External kartı rezerve et (satın alma başladığında)."""
    init_external_cards_table()
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.cursor()
        cur.execute("SELECT status FROM external_cards WHERE ext_id = ?", (ext_id,))
        row = cur.fetchone()
        if not row or row["status"] != "available":
            conn.execute("ROLLBACK")
            conn.close()
            return False
        conn.execute("""
            UPDATE external_cards SET status = 'reserved', purchased_by = ?
            WHERE ext_id = ?
        """, (user_id, ext_id))
        conn.execute("COMMIT")
        conn.close()
        return True
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
            conn.close()
        except Exception:
            pass
        logger.error(f"reserve_external_card error: {e}")
        return False


def mark_external_purchased(ext_id: str) -> None:
    init_external_cards_table()
    conn = get_db()
    conn.execute("""
        UPDATE external_cards SET status = 'purchased',
        purchased_at = ? WHERE ext_id = ?
    """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ext_id))
    conn.commit()
    conn.close()


def mark_external_failed(ext_id: str) -> None:
    init_external_cards_table()
    conn = get_db()
    conn.execute("UPDATE external_cards SET status = 'failed' WHERE ext_id = ?", (ext_id,))
    conn.commit()
    conn.close()


def cleanup_expired_cards() -> int:
    """Süresi dolmuş kartları temizle."""
    init_external_cards_table()
    conn = get_db()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.cursor()
    cur.execute("DELETE FROM external_cards WHERE expires_at < ? AND status = 'available'", (now,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    if deleted:
        logger.info(f"[CLEANUP] Removed {deleted} expired external cards")
    return deleted


# ============================================================
# REGISTRATION STATUS PARSING
# ============================================================
# Registration icons (various bots use different symbols)
ICON_REGISTERED = {
    "🔒", "🔐", "®", "Ⓡ", "✓", "✔", "📎",  # 📎 = Guru's registered indicator
}
ICON_UNREGISTERED = {
    "✅", "🔓", "❌", "🔄", "🆕",  # Unregistered / fresh card indicators
}


def extract_registration_from_text(text: str) -> tuple:
    """Extract registration status from text (message or button).
    Returns: (registered: True/False/None, confidence: 'high'/'medium'/'low', icon_found: str)
    """
    if not text:
        return (None, "low", "")

    # 1. Emoji scan (highest confidence)
    for char in text:
        if char in ICON_REGISTERED:
            return (True, "high", char)
        if char in ICON_UNREGISTERED:
            return (False, "high", char)

    text_lower = text.lower()

    # 2. Explicit "Registered: <value>" patterns (most reliable)
    m_kv = re.search(r'registered\s*[:\-=]\s*(true|yes|1|false|no|0)', text_lower)
    if m_kv:
        val = m_kv.group(1)
        if val in ("true", "yes", "1"):
            return (True, "high", f"kv:{val}")
        else:
            return (False, "high", f"kv:{val}")

    # 3. "Unregistered" appears before "Registered" check (order matters!)
    if "unregistered" in text_lower or "not registered" in text_lower:
        return (False, "high", "keyword:unreg")
    if "new card" in text_lower or "fresh" in text_lower or "nrg" in text_lower:
        return (False, "medium", "keyword:new")

    # 4. Plain "registered" keyword (low confidence - could be ambiguous)
    if "registered" in text_lower or "reg" in text_lower:
        return (True, "medium", "keyword:reg")

    return (None, "low", "")


def extract_registration_from_buttons(buttons) -> tuple:
    """Scan Telegram inline buttons for registration indicators.
    Buttons format: [[Button, Button], [Button, Button]]
    Returns: (registered, confidence, source_text)
    """
    if not buttons:
        return (None, "low", "")

    best_result = (None, "low", "")

    try:
        for row in buttons:
            for btn in row:
                btn_text = getattr(btn, "text", "") or ""
                if not btn_text:
                    continue
                reg, conf, icon = extract_registration_from_text(btn_text)
                if reg is not None and conf == "high":
                    return (reg, conf, btn_text[:30])
                if reg is not None and best_result[0] is None:
                    best_result = (reg, conf, btn_text[:30])
    except Exception:
        pass

    return best_result


# ============================================================
# MESSAGE PARSERS — Her rakip bot için ayrı parser
# ============================================================

def parse_guru_stock(text: str, msg_id: int = None, buttons=None) -> dict | None:
    """
    Guru Prepaid stock kanalı formatı:
    🏦 BIN: 435880
    💰 Balance: USD$19.25
    💲 Price: $6.74
    🏪 Type: giftcardmall
    """
    if "New Card Added" not in text and "BIN:" not in text:
        return None

    data = {"source": "guru", "source_bot": "GuruPrepaidBot", "channel_msg_id": msg_id}

    # BIN — 6 haneye normalize et
    m = re.search(r'BIN:\s*(\d{4,8})', text)
    if m:
        raw_bin = m.group(1)
        if len(raw_bin) < 6:
            return None  # Yetersiz BIN, bu kart atla
        data["bin"] = raw_bin[:6]

    # Balance
    m = re.search(r'Balance:\s*(?:USD|CAD|AUD|EUR|GBP)?\$?([\d,.]+)', text)
    if m:
        data["balance"] = float(m.group(1).replace(",", ""))
    else:
        return None

    # Currency
    m_cur = re.search(r'Balance:\s*(USD|CAD|AUD|EUR|GBP)', text)
    data["currency"] = m_cur.group(1) if m_cur else "USD"

    # Price (cost)
    m = re.search(r'Price:\s*\$?([\d,.]+)', text)
    if m:
        data["cost_price"] = float(m.group(1).replace(",", ""))
    else:
        return None

    # Type (provider)
    m = re.search(r'Type:\s*(\S+)', text)
    if m:
        data["provider"] = m.group(1).strip()

    # Registration — Guru mantığı: 📎 varsa registered, yoksa unregistered
    reg_text, conf_text, icon_text = extract_registration_from_text(text)
    if reg_text is True:
        data["registered"] = True
        data["parsing_notes"] = f"text:high:{icon_text}"
    else:
        # Guru unregistered için ikon kullanmaz → ikon yok = unregistered
        data["registered"] = False
        data["parsing_notes"] = "guru:no_icon=unreg"

    # Buton kontrolü (override için)
    if buttons:
        reg_btn, conf_btn, src_btn = extract_registration_from_buttons(buttons)
        if reg_btn is True:
            data["registered"] = True
            data["parsing_notes"] = f"btn:{conf_btn}:{src_btn[:20]}"

    data["raw_message"] = text
    return data


def parse_xstock_channel(text: str, msg_id: int = None, buttons=None) -> dict | None:
    """
    X Stock kanalı formatı:
    • Card BIN: 403446xx
    • Balance: USD $50.00  (veya CAD $8.03)
    • Price: 19.50 USDT
    • Rate: 39.0%
    • Used PayPal: No
    • Used Google: No
    • Registered: True
    """
    if "New Listing Added" not in text and "Card BIN:" not in text:
        return None

    data = {"source": "xstock", "source_bot": "XprepaidsExchangeBot", "channel_msg_id": msg_id}

    # BIN — 6 haneye normalize et
    m = re.search(r'(?:Card\s*)?BIN:\s*(\d{4,8})', text)
    if m:
        raw_bin = m.group(1)
        if len(raw_bin) < 6:
            return None
        data["bin"] = raw_bin[:6]

    # Balance
    m = re.search(r'Balance:\s*(?:USD|CAD|AUD|EUR|GBP)?\s*\$?\s*([\d,.]+)', text)
    if m:
        data["balance"] = float(m.group(1).replace(",", ""))
    else:
        return None

    # Currency
    m_cur = re.search(r'Balance:\s*(USD|CAD|AUD|EUR|GBP)', text)
    data["currency"] = m_cur.group(1) if m_cur else "USD"

    # Price
    m = re.search(r'Price:\s*\$?\s*([\d,.]+)\s*(?:USDT|USD)?', text)
    if m:
        data["cost_price"] = float(m.group(1).replace(",", ""))
    else:
        return None

    # Rate
    m = re.search(r'Rate:\s*([\d.]+)%', text)
    if m:
        data["rate_percent"] = float(m.group(1))

    # Flags
    m_pp = re.search(r'Used PayPal:\s*(\w+)', text)
    data["used_paypal"] = bool(m_pp and m_pp.group(1).lower() in ("yes", "true"))

    m_gg = re.search(r'Used Google:\s*(\w+)', text)
    data["used_google"] = bool(m_gg and m_gg.group(1).lower() in ("yes", "true"))

    # X Stock cards are ALL registered by default (X Stock policy)
    data["registered"] = True
    data["parsing_notes"] = "xstock:default_registered"

    data["raw_message"] = text
    return data


def parse_planet_stock(text: str, msg_id: int = None, buttons=None) -> dict | None:
    """Planet stock kanalı parser — format belirlendiğinde doldurulacak."""
    return None


def parse_sharks_stock(text: str, msg_id: int = None, buttons=None) -> dict | None:
    """Sharks stock kanalı parser — format belirlendiğinde doldurulacak."""
    return None


# Source → parser mapping
CHANNEL_PARSERS = {
    "guru": parse_guru_stock,
    "xstock": parse_xstock_channel,
    "planet": parse_planet_stock,
    "sharks": parse_sharks_stock,
}


# ============================================================
# DELIVERY MESSAGE PARSERS — Satın alma sonrası kart bilgisi çıkarma
# ============================================================

def parse_guru_delivery(text: str) -> dict | None:
    """
    Guru teslim formatı:
    🎊 Purchase Successful!
    💳 4912779436810392:04:34:632:US$1.03
    🏪 Card Type: giftcardmall
    Registered: True
    ...
    """
    data = {}

    # Full card line: NUMBER:MM:YY:CVV:BALANCE
    m = re.search(r'(\d{15,19}):(\d{2}):(\d{2,4}):(\d{3,4})(?::(?:US)?\$?([\d.]+))?', text)
    if m:
        data["card_number"] = m.group(1)
        data["exp_month"] = m.group(2)
        data["exp_year"] = m.group(3) if len(m.group(3)) == 4 else "20" + m.group(3)
        data["cvv"] = m.group(4)
        if m.group(5):
            data["balance"] = float(m.group(5))
    else:
        return None

    # Card type
    m = re.search(r'Card Type:\s*(\w+)', text)
    if m:
        data["provider"] = m.group(1)

    # Registered
    m = re.search(r'Registered:\s*(\w+)', text)
    data["registered"] = m and m.group(1).lower() == "true" if m else False

    # Initial balance
    m = re.search(r'Initial Balance:\s*\$?([\d.]+)', text)
    if m:
        data["initial_balance"] = float(m.group(1))

    return data


def parse_xstock_delivery(text: str) -> dict | None:
    """
    X Stock teslim formatı (tahmini — Guru'ya benzer olacak):
    Card Info:
    4358808740825886:11:33:673
    Balance: $1.83 USD
    """
    data = {}

    # Card line: NUMBER:MM:YY:CVV
    m = re.search(r'(\d{15,19}):(\d{2}):(\d{2,4}):(\d{3,4})', text)
    if m:
        data["card_number"] = m.group(1)
        data["exp_month"] = m.group(2)
        data["exp_year"] = m.group(3) if len(m.group(3)) == 4 else "20" + m.group(3)
        data["cvv"] = m.group(4)
    else:
        return None

    # Balance
    m = re.search(r'Balance:\s*\$?([\d.]+)', text)
    if m:
        data["balance"] = float(m.group(1))

    return data


# Source → delivery parser
DELIVERY_PARSERS = {
    "guru": parse_guru_delivery,
    "xstock": parse_xstock_delivery,
}


# ============================================================
# TELETHON CLIENT — Kanal dinleyici + Bot etkileşimi
# ============================================================

async def run_supply_chain():
    """Ana Telethon loop — kanalları dinle, kartları DB'ye yaz."""
    try:
        from telethon import TelegramClient, events
        from telethon.tl.types import Channel
    except ImportError:
        logger.critical("Telethon not installed! Run: pip install telethon")
        return

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        logger.critical("TELEGRAM_API_ID and TELEGRAM_API_HASH required! Get from my.telegram.org")
        return

    init_external_cards_table()

    client = TelegramClient(
        SESSION_NAME,
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH,
        connection_retries=5,       # Bağlantı koparsa 5 kez dene
        retry_delay=5,              # Denemeler arası 5 sn
        auto_reconnect=True,        # Otomatik reconnect
        request_retries=3,          # API isteği başarısızsa 3 kez dene
        flood_sleep_threshold=60,   # Flood'ta 60sn'ye kadar otomatik bekle
    )
    await client.start(phone=TELEGRAM_PHONE)

    me = await client.get_me()
    logger.info(f"Logged in as: {me.first_name} ({me.id})")
    logger.info(f"Own stock channel ID: {OUR_STOCK_CHANNEL_ID} (post={POST_TO_OWN_CHANNEL})")

    # ── Kanalları bul / katıl ──
    monitored_channels = {}  # channel_id → source_name

    # Bilinen kanal ID'leri (ilk bağlantıdan sonra .env'e eklenebilir)
    KNOWN_CHANNEL_IDS = {
        "guru": int(os.getenv("GURU_CHANNEL_ID", "0")),
        "xstock": int(os.getenv("XSTOCK_CHANNEL_ID", "0")),
        "planet": int(os.getenv("PLANET_CHANNEL_ID", "0")),
        "sharks": int(os.getenv("SHARKS_CHANNEL_ID", "0")),
    }

    for source, config in COMPETITORS.items():
        channel_ref = config.get("stock_channel") or config.get("stock_channel_link")
        known_id = KNOWN_CHANNEL_IDS.get(source, 0)

        if not channel_ref and not known_id:
            logger.warning(f"[{source}] No stock channel configured, skipping")
            continue

        try:
            entity = None

            # Yöntem 1: Bilinen kanal ID'si varsa direkt kullan
            if known_id:
                try:
                    from telethon.tl.functions.channels import GetChannelsRequest
                    from telethon.tl.types import InputChannel
                    entity = await client.get_entity(known_id)
                    logger.info(f"[{source}] Found by known ID: {known_id}")
                except Exception as e:
                    logger.debug(f"[{source}] Known ID lookup failed: {e}")

            # Yöntem 2: Invite link ile katıl/bul
            if not entity and channel_ref and (channel_ref.startswith("https://t.me/+") or channel_ref.startswith("https://t.me/joinchat/")):
                from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
                invite_hash = channel_ref.split("+")[-1] if "+" in channel_ref else channel_ref.split("/")[-1]

                try:
                    # Önce katılmayı dene
                    result = await client(ImportChatInviteRequest(invite_hash))
                    entity = result.chats[0]
                except Exception as e:
                    err = str(e)
                    if "already" in err.lower() or "USER_ALREADY_PARTICIPANT" in err:
                        # Zaten üye — CheckChatInvite ile bilgi al
                        try:
                            invite_info = await client(CheckChatInviteRequest(invite_hash))
                            if hasattr(invite_info, 'chat') and invite_info.chat:
                                entity = invite_info.chat
                                logger.info(f"[{source}] Found via CheckChatInvite")
                        except Exception:
                            pass

                        # Hala bulamadıysa tüm dialog'ları tara
                        if not entity:
                            logger.info(f"[{source}] Searching dialogs...")
                            async for dialog in client.iter_dialogs():
                                name = (dialog.name or "").lower()
                                # Geniş arama: "stock" içeren kanal
                                if hasattr(dialog.entity, 'broadcast') and dialog.entity.broadcast:
                                    if ("stock" in name and "update" in name) or source.lower() in name:
                                        # Eşleşme adayı — daha önce eşleşmemiş olmalı
                                        cid = dialog.entity.id
                                        if cid not in monitored_channels:
                                            entity = dialog.entity
                                            logger.info(f"[{source}] Found via dialog search: '{dialog.name}' (ID: {cid})")
                                            break
                    elif "FLOOD" in err.upper():
                        import re as _re
                        wait = int(_re.search(r'(\d+)', err).group(1)) if _re.search(r'(\d+)', err) else 5
                        logger.info(f"[{source}] Flood wait {wait}s...")
                        await asyncio.sleep(wait + 1)
                    else:
                        logger.error(f"[{source}] Failed to join: {e}")

            # Yöntem 3: Username ile
            if not entity and channel_ref and not channel_ref.startswith("https://t.me/+"):
                try:
                    entity = await client.get_entity(channel_ref)
                except Exception as e:
                    logger.debug(f"[{source}] Username lookup failed: {e}")

            if entity:
                eid = entity.id
                monitored_channels[eid] = source
                title = getattr(entity, 'title', str(eid))
                logger.info(f"[{source}] ✅ Monitoring: {title} (ID: {eid})")
                # İlk bağlantıda ID'yi logla — .env'e eklenebilir
                logger.info(f"[{source}] 💡 Add to .env: {source.upper()}_CHANNEL_ID={eid}")
            else:
                logger.error(f"[{source}] ❌ Could not find channel!")

        except Exception as e:
            logger.error(f"[{source}] Channel setup failed: {e}")

    if not monitored_channels:
        logger.warning("No channels to monitor! Check your .env config.")
        logger.info("Waiting for manual configuration... Press Ctrl+C to stop.")
        await client.run_until_disconnected()
        return

    # ── Yeni mesaj handler ──
    @client.on(events.NewMessage(chats=list(monitored_channels.keys())))
    async def on_stock_update(event):
        """Stok kanalından yeni mesaj geldiğinde çalışır."""
        channel_id = event.chat_id
        source = monitored_channels.get(channel_id)
        if not source:
            return

        text = event.message.message or ""
        buttons = event.message.buttons if event.message else None
        if not text.strip():
            return

        logger.debug(f"[{source}] New message (id:{event.message.id}): {text[:80]}...")
        await _process_stock_message(source, text, event.message.id, client,
                                      post_to_channel=True, buttons=buttons)

    async def _process_stock_message(source: str, text: str, msg_id: int,
                                      tg_client=None, post_to_channel: bool = False,
                                      buttons=None) -> bool:
        """Stok mesajını parse et, kaydet, kendi kanalımıza postla."""
        parser = CHANNEL_PARSERS.get(source)
        if not parser:
            return False

        try:
            card_data = parser(text, msg_id=msg_id, buttons=buttons)
            if card_data:
                if card_data.get("balance", 0) < 1.0:
                    return False

                is_new = save_external_card(card_data)
                sell_price, profit = calculate_sell_price(
                    card_data["cost_price"], card_data["balance"]
                )

                if is_new:
                    cur = card_data.get("currency", "USD")
                    logger.info(
                        f"[{source.upper()}] 📥 BIN:{card_data.get('bin','')} "
                        f"{cur} ${card_data['balance']:.2f} | "
                        f"Cost:${card_data['cost_price']:.2f} → "
                        f"Sell:${sell_price:.2f} (+${profit:.2f})"
                    )

                # Kendi stok kanalımıza postla — yeni veya reactivated kartlar
                if is_new and post_to_channel and POST_TO_OWN_CHANNEL and OUR_STOCK_CHANNEL_ID and tg_client:
                    try:
                        await _post_to_own_stock_channel(tg_client, card_data, sell_price)
                    except Exception as e:
                        logger.error(f"[OWN_CHANNEL] Post error: {e}")

                return is_new
            return False
        except Exception as e:
            logger.error(f"[{source}] Parse error: {e}")
            return False

    async def _post_to_own_stock_channel(tg_client, card_data: dict, sell_price: float):
        """Kendi stok kanalımıza yeni kart bildirimi gönder."""
        balance = card_data.get("balance", 0)
        currency = card_data.get("currency", "USD")
        bin_num = card_data.get("bin", "xxxxxx")
        provider = card_data.get("provider", "Unknown")
        registered = card_data.get("registered", False)

        reg_icon = "🔒" if registered else "✅"

        post_text = (
            f"🆕 Stock Updates\n"
            f"New Card Added\n\n"
            f"🏦 BIN: {bin_num}\n"
            f"💰 Balance: {currency}${balance:.2f}\n"
            f"💲 Price: ${sell_price:.2f}\n"
            f"🏪 Type: {provider}\n"
            f"📋 Status: {reg_icon} {'Registered' if registered else 'Not Registered'}"
        )

        logger.info(f"[OWN_CHANNEL] Posting to channel ID: {OUR_STOCK_CHANNEL_ID}")
        try:
            await tg_client.send_message(OUR_STOCK_CHANNEL_ID, post_text)
            logger.info(f"[OWN_CHANNEL] ✅ Posted: BIN:{bin_num} ${balance:.2f}")
        except Exception as e:
            logger.error(f"[OWN_CHANNEL] ❌ Failed with ID {OUR_STOCK_CHANNEL_ID}: {e}")
            # Telethon bazen -100 prefix'siz ID ister
            alt_id = OUR_STOCK_CHANNEL_ID
            if str(OUR_STOCK_CHANNEL_ID).startswith("-100"):
                alt_id = int(str(OUR_STOCK_CHANNEL_ID)[4:])
            elif OUR_STOCK_CHANNEL_ID > 0:
                alt_id = int(f"-100{OUR_STOCK_CHANNEL_ID}")
            if alt_id != OUR_STOCK_CHANNEL_ID:
                try:
                    await tg_client.send_message(alt_id, post_text)
                    logger.info(f"[OWN_CHANNEL] ✅ Posted with alt ID {alt_id}")
                except Exception as e2:
                    logger.error(f"[OWN_CHANNEL] ❌ Alt ID also failed: {e2}")

    # ── Başlangıçta son mesajları oku (catch-up) ──
    CATCHUP_LIMIT = 50  # Her kanaldan son 50 mesajı oku
    for ch_id, source in monitored_channels.items():
        try:
            logger.info(f"[{source}] Reading last {CATCHUP_LIMIT} messages (catch-up)...")
            count = 0
            posted = 0
            async for msg in client.iter_messages(ch_id, limit=CATCHUP_LIMIT):
                text = msg.message or ""
                buttons = msg.buttons if msg else None
                if text.strip():
                    result = await _process_stock_message(source, text, msg.id, client,
                                                            post_to_channel=True, buttons=buttons)
                    if result:
                        count += 1
                        posted += 1
                        await asyncio.sleep(0.3)
            logger.info(f"[{source}] Catch-up complete: {count} cards imported, {posted} posted to channel")
        except Exception as e:
            logger.error(f"[{source}] Catch-up failed: {e}")

    # ── Aktif Stok Tarama — Rakip bot listinglerini periyodik oku ──
    # Her bot için listing buton adı ve listing parse bilgisi
    BOT_LISTING_CONFIG = {
        "guru": {
            "username": "GuruPrepaidBot",
            "listing_btn": ["View Listing", "View Listings"],
            "next_btn": ["Next ➡️", "Next →", "Next ▶", "Next"],
            "first_btn": ["⏪ First", "First Page", "First", "⏮️"],
            "back_btn": ["Back", "◀️ Back", "⬅️ Back", "Main Menu"],
            "clear_filters_btn": ["Clear Filters", "Clear All", "Reset Filters", "✨ Clear Filters"],
            "category_btns": [],
            "max_pages": 30,
        },
        "xstock": {
            "username": "XprepaidsExchangeBot",
            "listing_btn": ["Listing", "📋 Listing", "Listings", "📋 Listings"],
            "next_btn": ["Next ➡️", "Next →", "Next ▶", "▶️", "Next"],
            "first_btn": ["⏪ First", "First Page", "First", "⏮️"],
            "back_btn": ["Back", "◀️ Back", "⬅️ Back", "Main Menu", "🏠 Main Menu", "Home"],
            "clear_filters_btn": ["Clear Filters", "Clear All", "Reset Filters", "✨ Clear Filters", "Reset"],
            "category_btns": [],
            "max_pages": 50,  # X Stock'ta çok kart var
        },
    }

    def _parse_listing_text(source: str, text: str) -> list:
        """Bot listing mesajından kartları parse et → [{bin, balance, currency, ...}]"""
        cards = []

        # Telegram markdown formatlamasını temizle (backtick, bold, italic)
        clean_text = text.replace("`", "").replace("**", "").replace("__", "").replace("*", "")

        lines = clean_text.split("\n")
        debug_count = 0
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Satır numarayla başlamalı: "1." veya "11."
            if not re.match(r'^\d+\.', line):
                continue

            # İlk 3 satırın tam halini logla (registration debug)
            if debug_count < 3:
                logger.info(f"[PARSE:{source}] Line sample: {repr(line[:250])}")
                debug_count += 1

            card = {}

            if source == "guru":
                # Format: "1. 53393 | CA$778.69    $198.94"
                # veya:   "5. 420495xx:US$500.00: at 35.00%"
                m = re.match(r'\d+\.\s*(\d{4,8})\w*[\s|:]+(?:(?:USD|US|CAD|CA|AUD|AU|EUR?)\$?\s*)([\d,.]+)', line)
                if m:
                    raw_bin = m.group(1)
                    # BIN'i 6 haneye normalize et (5 hane ise sıfırla padle veya atla)
                    if len(raw_bin) >= 6:
                        card["bin"] = raw_bin[:6]
                    else:
                        # 5 haneli BIN — yetersiz, atla
                        continue
                    card["balance"] = float(m.group(2).replace(",", ""))
                # Price (sağdaki $xx.xx)
                m_price = re.search(r'\$([\d,.]+)\s*$', line)
                if m_price:
                    card["cost_price"] = float(m_price.group(1).replace(",", ""))
                # Rate
                m_rate = re.search(r'at\s*([\d.]+)%', line)
                if m_rate and "cost_price" not in card:
                    rate = float(m_rate.group(1)) / 100
                    card["cost_price"] = round(card.get("balance", 0) * rate, 2)
                # Currency
                if "CA$" in line or "CA" in line:
                    card["currency"] = "CAD"
                elif "AU$" in line or "AU" in line:
                    card["currency"] = "AUD"
                else:
                    card["currency"] = "USD"
                # Registration — 3-state: registered/unregistered/unknown
                # Guru: 📎 = registered, hiç bir şey yok = unregistered
                if "📎" in line or "🔒" in line or "🔐" in line:
                    card["registered"] = True
                else:
                    # Guru unregistered için ikon kullanmaz → ikon yoksa unregistered
                    card["registered"] = False
                # Used Google/PayPal indicators (Guru uses italic 𝑮 𝑷)
                card["used_google"] = "𝑮" in line or "🅶" in line or "🟢" in line
                card["used_paypal"] = "𝑷" in line or "🅿" in line

            elif source == "xstock":
                m = re.match(r'\d+\.\s*(\d{4,8})\w*[\s:]+(?:USD|US|CAD|AUD|EUR|GBP)?\s*\$?\s*([\d,.]+)', line)
                if m:
                    raw_bin = m.group(1)
                    if len(raw_bin) >= 6:
                        card["bin"] = raw_bin[:6]
                    else:
                        continue
                    card["balance"] = float(m.group(2).replace(",", ""))
                # Rate
                m_rate = re.search(r'at\s*([\d.]+)%', line)
                if m_rate and card.get("balance"):
                    rate = float(m_rate.group(1)) / 100
                    card["cost_price"] = round(card["balance"] * rate, 2)
                # X Stock cards are ALL registered by default (X Stock satıcı politikası)
                # 🔄 = re-listed (yine de registered)
                # 🅶/🅿 = sadece kullanım göstergesi, registration etkilenmez
                card["registered"] = True
                # Used indicators
                card["used_google"] = "🅶" in line or "G " in line or "🟢" in line
                card["used_paypal"] = "🅿" in line or "P " in line
                # Currency
                if "CAD" in line:
                    card["currency"] = "CAD"
                elif "AUD" in line:
                    card["currency"] = "AUD"
                else:
                    card["currency"] = "USD"

            if card.get("bin") and card.get("balance"):
                card["source"] = source
                card["provider"] = ""  # Listing'den provider bilgisi genelde yok
                cards.append(card)

        return cards

    def _find_button(msg, btn_texts: list):
        """Mesajdaki inline butonlardan birini bul."""
        if not msg or not msg.buttons:
            return None
        for row in msg.buttons:
            for btn in row:
                for txt in btn_texts:
                    if txt.lower() in (btn.text or "").lower():
                        return btn
        return None

    async def _read_listing_pages(tg_client, source: str, username: str, config: dict, label: str = "main") -> list:
        """Bir listing kategorisinin tüm sayfalarını oku.
        Loop koruması: aynı kart seti tekrar gelirse dur (Telegram edit ediyor olabilir)."""
        cards = []
        seen_card_hashes = set()
        consecutive_same = 0  # Üst üste aynı içerik sayacı

        try:
            msgs = await tg_client.get_messages(username, limit=1)
            if not msgs:
                return []
            msg = msgs[0]
            text = msg.text or ""

            button_texts = []
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if btn and hasattr(btn, 'text') and btn.text:
                            button_texts.append(btn.text)
            text_with_buttons = text + "\n" + "\n".join(button_texts) if button_texts else text

            page_cards = _parse_listing_text(source, text_with_buttons)
            page_hash = frozenset((c.get("bin", ""), round(float(c.get("balance", 0)), 2)) for c in page_cards)
            seen_card_hashes.add(page_hash)
            cards.extend(page_cards)
            logger.info(f"[SCAN] {source}/{label}: Page 1 — {len(page_cards)} cards (msg_id={msg.id})")

            for page_num in range(2, config.get("max_pages", 10) + 1):
                next_btn = _find_button(msg, config["next_btn"])
                if not next_btn:
                    logger.info(f"[SCAN] {source}/{label}: No Next button → end of pages")
                    break
                try:
                    await next_btn.click()
                except Exception as e:
                    logger.debug(f"[SCAN] {source}/{label}: Next click failed: {e}")
                    break
                await asyncio.sleep(2.5)  # Edit gecikmesi için biraz daha bekle

                msgs = await tg_client.get_messages(username, limit=1)
                if not msgs:
                    break
                msg = msgs[0]

                text = msg.text or ""
                button_texts = []
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            if btn and hasattr(btn, 'text') and btn.text:
                                button_texts.append(btn.text)
                text_with_buttons = text + "\n" + "\n".join(button_texts) if button_texts else text

                page_cards = _parse_listing_text(source, text_with_buttons)
                if not page_cards:
                    logger.info(f"[SCAN] {source}/{label}: Empty page → end")
                    break

                # İçerik loop tespiti — aynı kart seti 2 KEZ ÜST ÜSTE gelirse dur
                page_hash = frozenset((c.get("bin", ""), round(float(c.get("balance", 0)), 2)) for c in page_cards)
                if page_hash in seen_card_hashes:
                    consecutive_same += 1
                    logger.warning(f"[SCAN] {source}/{label}: Page {page_num} duplicate content (consecutive={consecutive_same})")
                    if consecutive_same >= 2:
                        logger.warning(f"[SCAN] {source}/{label}: Stopped — too many duplicate pages")
                        break
                    # Yine de dene — belki bir sonraki sayfa farklı
                    continue
                else:
                    consecutive_same = 0
                    seen_card_hashes.add(page_hash)

                cards.extend(page_cards)
                logger.info(f"[SCAN] {source}/{label}: Page {page_num} — {len(page_cards)} cards (msg_id={msg.id})")

            return cards
        except Exception as e:
            logger.error(f"[SCAN] {source}/{label}: Read pages failed: {e}")
            return cards

    async def _navigate_to_listing_menu(tg_client, source: str, username: str, config: dict):
        """Bot'a /start gönder ve Listing menüsüne git. Returns msg or None.
        1. Clear Filters varsa tıkla (filtre kalıntılarını temizle)
        2. First Page varsa tıkla (sayfa 1'den başla)"""
        try:
            await tg_client.send_message(username, "/start")
            await asyncio.sleep(3)
            msgs = await tg_client.get_messages(username, limit=1)
            if not msgs:
                return None
            msg = msgs[0]
            listing_btn = _find_button(msg, config["listing_btn"])
            if not listing_btn:
                return None
            await listing_btn.click()
            await asyncio.sleep(3)
            msgs = await tg_client.get_messages(username, limit=1)
            if not msgs:
                return None
            msg = msgs[0]

            # 1. Clear Filters varsa tıkla
            clear_btn = _find_button(msg, config.get("clear_filters_btn", []))
            if clear_btn:
                logger.info(f"[SCAN] {source}: Clicking Clear Filters")
                try:
                    await clear_btn.click()
                    await asyncio.sleep(2)
                    msgs = await tg_client.get_messages(username, limit=1)
                    if msgs:
                        msg = msgs[0]
                except Exception as e:
                    logger.debug(f"[SCAN] {source}: Clear Filters failed: {e}")

            # 2. First Page varsa tıkla — listing'in başından başla
            first_btn = _find_button(msg, config.get("first_btn", []))
            if first_btn:
                logger.info(f"[SCAN] {source}: Clicking First Page")
                try:
                    await first_btn.click()
                    await asyncio.sleep(2)
                    msgs = await tg_client.get_messages(username, limit=1)
                    if msgs:
                        msg = msgs[0]
                except Exception as e:
                    logger.debug(f"[SCAN] {source}: First Page failed: {e}")

            return msg
        except Exception as e:
            logger.error(f"[SCAN] {source}: Navigate failed: {e}")
            return None

    async def _scan_bot_listing(tg_client, source: str, config: dict) -> list:
        """Bir rakip botun tüm listingini oku — kategorili veya tek liste."""
        username = config["username"]
        all_cards = []

        try:
            logger.info(f"[SCAN] {source}: Starting listing scan of @{username}...")

            msg = await _navigate_to_listing_menu(tg_client, source, username, config)
            if not msg:
                logger.warning(f"[SCAN] {source}: Could not reach listing menu")
                return []

            text = msg.text or ""
            logger.info(f"[SCAN] {source}: Listing menu text (first 500): {text[:500]}")

            # Mevcut mesajdaki tüm butonları al
            menu_buttons = []
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if btn and hasattr(btn, 'text') and btn.text:
                            menu_buttons.append(btn.text)
            logger.info(f"[SCAN] {source}: Menu buttons ({len(menu_buttons)}): {menu_buttons[:15]}")

            # Kategori butonlarını tespit et
            category_btns_config = config.get("category_btns", [])
            category_buttons_found = []

            if category_btns_config and msg.buttons:
                # Navigation/control kelimeleri (kategori değil)
                nav_words = ["next", "back", "main menu", "home", "first page", "last page",
                             "previous", "prev", "▶️", "◀️", "⬅️", "➡️", "🏠", "purchase", "buy"]

                for row in msg.buttons:
                    for btn in row:
                        if not btn or not hasattr(btn, 'text') or not btn.text:
                            continue
                        btn_text = btn.text.strip()
                        btn_lower = btn_text.lower()

                        # Navigation butonu mu?
                        is_nav = any(nav in btn_lower for nav in nav_words)
                        if is_nav:
                            continue

                        # Listing button kendisi mi?
                        is_self = any(lb.lower() == btn_lower for lb in config["listing_btn"])
                        if is_self:
                            continue

                        # Kategori adı eşleşmesi
                        for cat in category_btns_config:
                            if cat.lower() in btn_lower:
                                if btn_text not in category_buttons_found:
                                    category_buttons_found.append(btn_text)
                                break

            if category_buttons_found:
                logger.info(f"[SCAN] {source}: Found {len(category_buttons_found)} categories: {category_buttons_found}")

                for cat_name in category_buttons_found:
                    try:
                        # Listing menüsüne tekrar dön
                        msg = await _navigate_to_listing_menu(tg_client, source, username, config)
                        if not msg:
                            continue

                        # Kategoriye tıkla
                        cat_btn = _find_button(msg, [cat_name])
                        if not cat_btn:
                            logger.warning(f"[SCAN] {source}: Category '{cat_name}' button missing")
                            continue

                        await cat_btn.click()
                        await asyncio.sleep(3)

                        # Bu kategorinin sayfalarını oku
                        cat_cards = await _read_listing_pages(tg_client, source, username, config, cat_name)
                        all_cards.extend(cat_cards)
                        logger.info(f"[SCAN] {source}: Category '{cat_name}' total: {len(cat_cards)} cards")
                    except Exception as e:
                        logger.error(f"[SCAN] {source}: Category '{cat_name}' failed: {e}")
                        continue
            else:
                # Kategori yok → Direkt sayfaları oku (Guru tarzı)
                logger.info(f"[SCAN] {source}: No category menu detected, reading pages directly")
                main_cards = await _read_listing_pages(tg_client, source, username, config, "main")
                all_cards.extend(main_cards)

            # BIN+balance bazında dedup
            seen_keys = set()
            deduped = []
            for c in all_cards:
                key = (c.get("bin", ""), round(float(c.get("balance", 0)), 2))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                deduped.append(c)

            if len(deduped) != len(all_cards):
                logger.info(f"[SCAN] {source}: Deduplicated {len(all_cards)} → {len(deduped)} cards")

            logger.info(f"[SCAN] {source}: Total {len(deduped)} unique cards")
            reg_count = sum(1 for c in deduped if c.get("registered") is True)
            unreg_count = sum(1 for c in deduped if c.get("registered") is False)
            unknown_count = len(deduped) - reg_count - unreg_count
            logger.info(f"[SCAN] {source}: Registration: {reg_count} registered, {unreg_count} unregistered, {unknown_count} unknown")
            return deduped

        except Exception as e:
            logger.error(f"[SCAN] {source}: Scan failed: {e}", exc_info=True)
            return all_cards

    async def _sync_external_stock(source: str, listing_cards: list):
        """DB'deki external kartları rakibin gerçek listesiyle senkronize et.
        1. Listing'de olmayan kartları expire et
        2. Registration durumunu güncelle (listing'de 🔒 varsa registered, yoksa unregistered)"""
        if not listing_cards:
            logger.warning(f"[SYNC] {source}: No listing cards to sync")
            return

        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT ext_id, bin_number, balance, registered, used_google, used_paypal FROM external_cards WHERE source = ? AND status = 'available'",
                    (source,))
        db_cards = {}
        for row in cur.fetchall():
            key = (str(row["bin_number"]).strip(), round(float(row["balance"]), 2))
            db_cards[key] = {
                "ext_id": row["ext_id"],
                "registered": row["registered"],
                "used_google": row["used_google"],
                "used_paypal": row["used_paypal"],
            }
        conn.close()

        listing_map = {}
        for card in listing_cards:
            key = (str(card.get("bin", "")).strip(), round(float(card.get("balance", 0)), 2))
            listing_map[key] = card

        logger.info(f"[SYNC] {source}: DB has {len(db_cards)} cards, listing has {len(listing_map)} cards")

        # 1. DB'de var ama listing'de yok → stale
        stale_count = 0
        for db_key, db_info in db_cards.items():
            if db_key not in listing_map:
                conn = get_db()
                conn.execute("UPDATE external_cards SET status = 'expired' WHERE ext_id = ?", (db_info["ext_id"],))
                conn.commit()
                conn.close()
                stale_count += 1

        # 2. Registration güncelle — listing mantığı:
        # listing'de 🔒 ikonu = registered
        # listing'de ikon yok veya ✅ = unregistered (kesin bilgi, çünkü artık kartı gerçekten gördük)
        reg_to_1 = 0
        reg_to_0 = 0
        matched = 0
        for db_key, db_info in db_cards.items():
            if db_key in listing_map:
                matched += 1
                listing_card = listing_map[db_key]
                listing_reg = listing_card.get("registered")
                db_reg = db_info["registered"]

                # Artık listing'de gördüğümüz tüm kartlar için kesin karar ver
                if listing_reg is True:
                    target_value = 1
                else:
                    # None veya False → unregistered kabul et (listing'de ikon yok = kayıtsız)
                    target_value = 0

                if db_reg != target_value:
                    conn = get_db()
                    conn.execute("UPDATE external_cards SET registered = ? WHERE ext_id = ?",
                                 (target_value, db_info["ext_id"]))
                    conn.commit()
                    conn.close()
                    if target_value == 1:
                        reg_to_1 += 1
                    else:
                        reg_to_0 += 1

                # Google/PayPal usage update
                listing_google = listing_card.get("used_google", False)
                listing_paypal = listing_card.get("used_paypal", False)
                if listing_google and not db_info["used_google"]:
                    conn = get_db()
                    conn.execute("UPDATE external_cards SET used_google = 1 WHERE ext_id = ?", (db_info["ext_id"],))
                    conn.commit()
                    conn.close()
                if listing_paypal and not db_info["used_paypal"]:
                    conn = get_db()
                    conn.execute("UPDATE external_cards SET used_paypal = 1 WHERE ext_id = ?", (db_info["ext_id"],))
                    conn.commit()
                    conn.close()

        logger.info(f"[SYNC] {source}: Matched {matched} cards")
        if stale_count:
            logger.info(f"[SYNC] {source}: Expired {stale_count} stale cards")
        if reg_to_1 or reg_to_0:
            logger.info(f"[SYNC] {source}: Updated {reg_to_1} → 🔒 Registered, {reg_to_0} → ✅ Unregistered")

    async def active_scan_loop():
        """Periyodik aktif stok tarama — her 15 dakikada rakip botları tara."""
        await asyncio.sleep(30)  # İlk taramaya 30 saniye bekle (registration hızlı güncellensin)
        while True:
            for source, config in BOT_LISTING_CONFIG.items():
                try:
                    listing_cards = await _scan_bot_listing(client, source, config)
                    if listing_cards:
                        await _sync_external_stock(source, listing_cards)

                        # Yeni kartları da DB'ye ekle ve kanala postla
                        new_count = 0
                        for card in listing_cards:
                            if card.get("balance", 0) < 1.0:
                                continue
                            if "cost_price" not in card:
                                card["cost_price"] = round(card["balance"] * 0.40, 2)
                            card["source_bot"] = config["username"]
                            card["channel_msg_id"] = 0
                            card["raw_message"] = ""
                            saved = save_external_card(card)
                            if saved:
                                new_count += 1
                                # Yeni kartı kanalına postla
                                if POST_TO_OWN_CHANNEL and OUR_STOCK_CHANNEL_ID:
                                    try:
                                        sell_price, _ = calculate_sell_price(card["cost_price"], card["balance"])
                                        await _post_to_own_stock_channel(client, card, sell_price)
                                        await asyncio.sleep(0.3)
                                    except Exception:
                                        pass
                        if new_count:
                            logger.info(f"[SCAN] {source}: Added {new_count} new cards from listing")

                    # Botlar arası bekleme (flood koruması)
                    await asyncio.sleep(30)
                except Exception as e:
                    logger.error(f"[SCAN] {source}: Error: {e}")
                    await asyncio.sleep(10)

            logger.info(f"[SCAN] Next scan in {ACTIVE_SCAN_INTERVAL_MINUTES} minutes")
            await asyncio.sleep(ACTIVE_SCAN_INTERVAL_MINUTES * 60)

    asyncio.create_task(active_scan_loop())

    # ── Periyodik temizlik ──
    async def cleanup_loop():
        while True:
            await asyncio.sleep(3600)  # Her saat
            try:
                cleanup_expired_cards()
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    asyncio.create_task(cleanup_loop())

    # ── Stats göster ──
    async def stats_loop():
        while True:
            await asyncio.sleep(300)  # Her 5 dakika
            try:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT source, COUNT(*) as cnt FROM external_cards WHERE status='available' GROUP BY source")
                rows = cur.fetchall()
                conn.close()
                parts = [f"{r['source']}:{r['cnt']}" for r in rows]
                total = sum(r['cnt'] for r in rows)
                if total:
                    logger.info(f"[STOCK] Available: {total} cards ({', '.join(parts)})")
            except Exception:
                pass

    asyncio.create_task(stats_loop())
    asyncio.create_task(fulfill_pending_orders_loop(client))

    logger.info(f"Supply Chain Agent running — monitoring {len(monitored_channels)} channels")
    logger.info("Order fulfillment loop active (checks ext_pending orders every 30s)")
    logger.info("Press Ctrl+C to stop")

    await client.run_until_disconnected()


# ============================================================
# AUTO-PURCHASE — Rakip bottan otomatik satın alma
# ============================================================

async def auto_purchase_from_competitor(tg_client, ext_id: str) -> dict:
    """Rakip bottan kart satın al ve hata handling yap.
    Returns:
        {"status": "success", "card_data": {...}}
        {"status": "retry", "retry_after_sec": 75, "detail": "checker_cooldown"}
        {"status": "card_dead", "detail": "..."}
        {"status": "card_changed", "detail": "registration_changed", ...}
        {"status": "balance_changed", "old_bal": 20.02, "new_bal": 10.03}
        {"status": "error", "detail": "..."}
    """
    init_external_cards_table()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM external_cards WHERE ext_id = ?", (ext_id,))
    card = cur.fetchone()
    conn.close()

    if not card:
        return {"status": "error", "detail": "card_not_found"}

    source = card["source"]
    source_bot = card["source_bot"]
    target_bin = card["bin_number"] or ""
    target_balance = float(card["balance"])

    logger.info(f"[PURCHASE] {source}: Buying BIN:{target_bin} ${target_balance:.2f}")

    try:
        # 1. /start gönder
        await tg_client.send_message(source_bot, "/start")
        await asyncio.sleep(3)

        msgs = await tg_client.get_messages(source_bot, limit=1)
        if not msgs:
            return {"status": "error", "detail": "no_start_response"}
        msg = msgs[0]

        # 2. Listing butonuna tıkla
        listing_btn_names = ["View Listing", "View Listings", "Listing", "📋 Listing", "Listings"]
        listing_btn = None
        if msg.buttons:
            for row in msg.buttons:
                for btn in row:
                    if btn and hasattr(btn, 'text') and btn.text:
                        if any(name.lower() in btn.text.lower() for name in listing_btn_names):
                            listing_btn = btn
                            break
                if listing_btn:
                    break

        if not listing_btn:
            return {"status": "error", "detail": "listing_btn_not_found"}

        await listing_btn.click()
        await asyncio.sleep(3)

        # 3. Clear filters + First page (temiz başlangıç)
        for pass_num in range(2):  # 2 deneme: clear, then first
            msgs = await tg_client.get_messages(source_bot, limit=1)
            if not msgs:
                break
            msg = msgs[0]
            for target_name in ["Clear Filters", "⏪ First", "First Page"]:
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            if btn and hasattr(btn, 'text') and btn.text:
                                if target_name.lower() in btn.text.lower():
                                    try:
                                        await btn.click()
                                        await asyncio.sleep(2)
                                    except Exception:
                                        pass
                                    break

        # 4. Doğru kartı bul — sayfaları gez, matching satırı tıkla
        msgs = await tg_client.get_messages(source_bot, limit=1)
        if not msgs:
            return {"status": "error", "detail": "no_listing_after_filters"}
        msg = msgs[0]

        purchase_btn = None
        max_pages = 30
        seen_page_hashes = set()

        for page_num in range(max_pages):
            text = msg.text or ""
            button_texts = []
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if btn and hasattr(btn, 'text') and btn.text:
                            button_texts.append(btn.text)

            # Bu sayfada kart var mı?
            page_cards_on_this_page = _parse_listing_text(source, text)

            # Matching kartı ara (BIN + balance)
            target_idx = None
            for idx, pc in enumerate(page_cards_on_this_page):
                pc_bin = str(pc.get("bin", ""))[:6]
                pc_bal = round(float(pc.get("balance", 0)), 2)
                if pc_bin == target_bin[:6] and abs(pc_bal - target_balance) < 0.01:
                    target_idx = idx
                    break

            if target_idx is not None:
                # Bu sayfadaki target_idx. karta karşılık gelen Purchase/kart butonunu bul
                # Genelde satır numarasıyla eşleşen buton var
                target_line_num_prefix = f"{target_idx + 1}."  # "1.", "2.", etc — relative
                # Ama listing boyunca satır numaraları değişebilir. Text'ten line gerçek numarayı bul
                card_line = None
                for line in text.split("\n"):
                    stripped = line.strip()
                    if not stripped or not re.match(r'^\d+\.', stripped):
                        continue
                    line_bin_match = re.search(r'(\d{5,8})', stripped)
                    line_bal_match = re.search(r'\$\s*([\d,.]+)', stripped)
                    if line_bin_match and line_bal_match:
                        line_bin = line_bin_match.group(1)[:6]
                        line_bal = float(line_bal_match.group(1).replace(",", ""))
                        if line_bin == target_bin[:6] and abs(line_bal - target_balance) < 0.01:
                            # Satır numarası alalım
                            num_match = re.match(r'^(\d+)\.', stripped)
                            if num_match:
                                card_line = num_match.group(1)
                                break

                # Butonları tara — "{number}." ile başlayan veya o kartla eşleşen
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            if not btn or not hasattr(btn, 'text') or not btn.text:
                                continue
                            btn_text = btn.text
                            # Önce tam satır eşleşmesi (örn "1. 435880xx $25")
                            if card_line and btn_text.strip().startswith(f"{card_line}."):
                                # BIN ve balance da eşleşiyorsa
                                if target_bin[:6] in btn_text and f"{target_balance:.2f}".rstrip("0").rstrip(".") in btn_text.replace("$", "").replace(",", ""):
                                    purchase_btn = btn
                                    break
                                # Ya da sadece BIN eşleşmesi
                                if target_bin[:6] in btn_text:
                                    purchase_btn = btn
                                    break
                        if purchase_btn:
                            break
                if purchase_btn:
                    break

            # Kart bu sayfada yok → sonraki sayfaya geç
            next_btn = None
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if btn and hasattr(btn, 'text') and btn.text:
                            if "next" in btn.text.lower() or "➡️" in btn.text:
                                next_btn = btn
                                break
                    if next_btn:
                        break
            if not next_btn:
                break  # Son sayfa

            # Loop koruması
            page_hash = frozenset((c.get("bin", ""), round(float(c.get("balance", 0)), 2))
                                   for c in page_cards_on_this_page)
            if page_hash in seen_page_hashes:
                break  # Aynı sayfa tekrar geldi, loop
            seen_page_hashes.add(page_hash)

            try:
                await next_btn.click()
            except Exception:
                break
            await asyncio.sleep(2)

            msgs = await tg_client.get_messages(source_bot, limit=1)
            if not msgs:
                break
            msg = msgs[0]

        if not purchase_btn:
            return {"status": "error", "detail": "target_card_not_in_listing"}

        # 5. Kart butonuna tıkla
        try:
            await purchase_btn.click()
        except Exception as e:
            return {"status": "error", "detail": f"card_click_failed: {e}"}
        await asyncio.sleep(3)

        # 6. Confirm/Purchase butonunu bul
        msgs = await tg_client.get_messages(source_bot, limit=1)
        if not msgs:
            return {"status": "error", "detail": "no_card_detail_response"}
        msg = msgs[0]

        confirm_names = ["Purchase", "🛒 Purchase", "Confirm", "✅ Confirm", "Buy",
                         "Confirm Purchase", "✅ Confirm Purchase"]
        confirm_btn = None
        if msg.buttons:
            for row in msg.buttons:
                for btn in row:
                    if btn and hasattr(btn, 'text') and btn.text:
                        if any(name.lower() == btn.text.lower().strip() for name in confirm_names):
                            confirm_btn = btn
                            break
                        # Kısmi eşleşme (daha toleranslı)
                        if any(name.lower() in btn.text.lower() for name in ["purchase", "confirm", "buy"]):
                            if "back" not in btn.text.lower() and "cancel" not in btn.text.lower():
                                confirm_btn = btn
                                break
                if confirm_btn:
                    break

        if not confirm_btn:
            return {"status": "error", "detail": "confirm_btn_not_found"}

        # 7. Satın alma öncesi kart info check — text'te error var mı?
        card_info_text = msg.text or ""
        error_check = classify_purchase_error_sc(card_info_text)
        if error_check["category"] != "unknown" and error_check["category"] != "none":
            return {"status": error_check["category"], "detail": error_check["detail"],
                    "retry_after_sec": error_check.get("retry_after_sec")}

        # 8. Confirm bas
        try:
            await confirm_btn.click()
        except Exception as e:
            return {"status": "error", "detail": f"confirm_click_failed: {e}"}

        # 9. Teslim mesajını bekle (up to 30s, birkaç mesaj gelebilir)
        await asyncio.sleep(3)
        start_wait = time.time()
        last_check_id = msg.id
        delivered_card = None
        last_error_msg = ""

        while time.time() - start_wait < 30:
            new_msgs = await tg_client.get_messages(source_bot, limit=5)
            for nm in new_msgs:
                if nm.id <= last_check_id:
                    continue
                nm_text = nm.message or nm.text or ""
                if not nm_text:
                    continue

                # Error mı?
                err = classify_purchase_error_sc(nm_text)
                if err["category"] != "unknown" and err["category"] != "none":
                    return {"status": err["category"], "detail": err["detail"],
                            "retry_after_sec": err.get("retry_after_sec"),
                            "raw": nm_text[:300]}

                # Card details mı? (regex ile kart no tespit et)
                card_match = re.search(r'\b(\d{13,19})\b', nm_text)
                exp_match = re.search(r'(\d{1,2})[/\-](\d{2,4})', nm_text)
                cvv_match = re.search(r'CVV[:\s]*(\d{3,4})', nm_text, re.IGNORECASE)
                if card_match and exp_match:
                    card_num = card_match.group(1)
                    exp_m = exp_match.group(1).zfill(2)
                    exp_y = exp_match.group(2)
                    if len(exp_y) == 2:
                        exp_y = "20" + exp_y
                    cvv = cvv_match.group(1) if cvv_match else ""
                    delivered_card = {
                        "card_number": card_num,
                        "exp_month": exp_m,
                        "exp_year": exp_y,
                        "cvv": cvv,
                        "balance": target_balance,
                        "raw_message": nm_text[:500],
                    }
                    break
                last_error_msg = nm_text[:200]

            if delivered_card:
                break
            await asyncio.sleep(2)

        if delivered_card:
            logger.info(f"[PURCHASE] ✅ Got card {delivered_card['card_number'][:6]}xx from {source}")
            return {"status": "success", "card_data": delivered_card}
        else:
            return {"status": "error", "detail": f"timeout_waiting_delivery: {last_error_msg[:150]}"}

    except Exception as e:
        logger.error(f"[PURCHASE] Exception: {e}", exc_info=True)
        return {"status": "error", "detail": str(e)[:200]}


def classify_purchase_error_sc(error_text: str) -> dict:
    """Supply chain tarafında rakip bot hata mesajı sınıflandırma.
    (bot_v2.py'daki classify_purchase_error ile aynı mantık, bu tarafta local.)"""
    if not error_text:
        return {"category": "none", "detail": "", "retry_after_sec": None}

    text_lower = error_text.lower()

    if "just checked" in text_lower and ("wait" in text_lower or "second" in text_lower):
        return {"category": "retry_wait", "detail": "checker_cooldown", "retry_after_sec": 75}

    if "card is dead" in text_lower or "invalid card" in text_lower:
        return {"category": "card_dead", "detail": "dead_or_invalid", "retry_after_sec": None}

    if "balance" in text_lower and ("→" in error_text or "changed" in text_lower):
        return {"category": "balance_changed", "detail": "balance_mismatch", "retry_after_sec": None}

    if "balance is too low" in text_lower:
        return {"category": "balance_changed", "detail": "balance_too_low", "retry_after_sec": None}

    if "card info changed" in text_lower or "purchase aborted" in text_lower:
        detail = "info_changed"
        if "registered" in text_lower:
            detail = "registration_changed"
        elif "google" in text_lower:
            detail = "google_used"
        elif "paypal" in text_lower:
            detail = "paypal_used"
        return {"category": "card_changed", "detail": detail, "retry_after_sec": None}

    if "choose another card" in text_lower or "try another card" in text_lower:
        return {"category": "card_changed", "detail": "unspecified_change", "retry_after_sec": None}

    return {"category": "unknown", "detail": "", "retry_after_sec": None}


# ─── Pending Order Fulfillment Loop ───
# bot_v2.py siparişleri "ext_pending" status ile bırakır.
# Bu loop o siparişleri alıp otomatik satın almayı dener.

async def fulfill_pending_orders_loop(tg_client):
    """Her 30 saniyede bir ext_pending siparişleri tara ve satın almayı dene."""
    ORDERS_FILE = os.path.join(os.getenv("BOT_DATA_DIR", "."), "orders.json")
    MAX_ATTEMPTS = 3

    await asyncio.sleep(60)  # İlk çalışmaya bekle, bot tam başlasın

    while True:
        try:
            import json as _json
            if not os.path.exists(ORDERS_FILE):
                await asyncio.sleep(30)
                continue

            try:
                with open(ORDERS_FILE, "r", encoding="utf-8") as f:
                    orders = _json.load(f)
            except Exception:
                await asyncio.sleep(30)
                continue

            for order_id, order in list(orders.items()):
                if order.get("status") != "ext_pending":
                    continue

                attempts = order.get("attempt_count", 0)
                if attempts >= MAX_ATTEMPTS:
                    continue  # Bu siparişi bot_v2 stuck handler işleyecek

                # Son denemeden 60 sn geçmediyse bekle
                last_attempt = order.get("last_attempt_at", "")
                if last_attempt:
                    try:
                        last_time = datetime.strptime(last_attempt, "%Y-%m-%d %H:%M:%S")
                        if (datetime.now() - last_time).total_seconds() < 60:
                            continue
                    except Exception:
                        pass

                ext_id = order.get("ext_id")
                if not ext_id:
                    continue

                logger.info(f"[FULFILL] Attempting purchase for order {order_id} (attempt {attempts+1})")

                # Deneme sayısını arttır
                order["attempt_count"] = attempts + 1
                order["last_attempt_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                orders[order_id] = order
                with open(ORDERS_FILE, "w", encoding="utf-8") as f:
                    _json.dump(orders, f, indent=2, ensure_ascii=False)

                # Satın al
                result = await auto_purchase_from_competitor(tg_client, ext_id)
                status = result.get("status")

                if status == "success":
                    card_data = result["card_data"]
                    # Siparişi delivered olarak işaretle
                    order["status"] = "delivered"
                    order["delivered_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    order["card_number"] = card_data["card_number"]
                    order["exp_month"] = card_data["exp_month"]
                    order["exp_year"] = card_data["exp_year"]
                    order["cvv"] = card_data["cvv"]
                    orders[order_id] = order
                    with open(ORDERS_FILE, "w", encoding="utf-8") as f:
                        _json.dump(orders, f, indent=2, ensure_ascii=False)
                    logger.info(f"[FULFILL] ✅ {order_id} delivered")

                elif status == "retry_wait":
                    # Bir sonraki döngüde tekrar dene, sayı kalır
                    wait_sec = result.get("retry_after_sec", 75)
                    logger.info(f"[FULFILL] {order_id} retry after {wait_sec}s: {result.get('detail')}")
                    order["attempt_count"] = attempts  # Sayıyı geri al (retry sayılmıyor)
                    order["failure_reason"] = f"retry: {result.get('detail', '')}"
                    orders[order_id] = order
                    with open(ORDERS_FILE, "w", encoding="utf-8") as f:
                        _json.dump(orders, f, indent=2, ensure_ascii=False)

                else:
                    # Kalıcı hata (card_dead, card_changed, balance_changed, error)
                    logger.warning(f"[FULFILL] {order_id} failed: {status} / {result.get('detail')}")
                    order["failure_reason"] = f"{status}: {result.get('detail', '')}"[:200]
                    orders[order_id] = order
                    with open(ORDERS_FILE, "w", encoding="utf-8") as f:
                        _json.dump(orders, f, indent=2, ensure_ascii=False)

                    # Başarısız kartı DB'de işaretle (tekrar satışa girmesin)
                    try:
                        conn = get_db()
                        conn.execute("UPDATE external_cards SET status = 'failed' WHERE ext_id = ?", (ext_id,))
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass

                # Deneme arası 5 sn bekle
                await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"fulfill_pending_orders_loop error: {e}")

        await asyncio.sleep(30)


# ============================================================
# HELPER — Ana bot'tan çağrılacak fonksiyonlar
# ============================================================

def get_external_stock_summary() -> dict:
    """Admin için stok özeti."""
    init_external_cards_table()
    conn = get_db()
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    summary = {}
    cur.execute("""
        SELECT source, COUNT(*) as cnt, SUM(balance) as total_bal,
               AVG(balance) as avg_bal, MIN(sell_price) as min_price,
               MAX(sell_price) as max_price
        FROM external_cards
        WHERE status = 'available' AND expires_at > ?
        GROUP BY source
    """, (now,))
    for row in cur.fetchall():
        summary[row["source"]] = {
            "count": row["cnt"],
            "total_balance": round(row["total_bal"] or 0, 2),
            "avg_balance": round(row["avg_bal"] or 0, 2),
            "min_price": round(row["min_price"] or 0, 2),
            "max_price": round(row["max_price"] or 0, 2),
        }
    conn.close()
    return summary


# ============================================================
# MAIN
# ============================================================

def main():
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        print("ERROR: Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env")
        print("Get them from https://my.telegram.org")
        return

    print(f"""
╔══════════════════════════════════════════════════╗
║     Elite Earners — Supply Chain Agent           ║
║                                                  ║
║  Monitoring competitor stock channels            ║
║  Auto-importing cards with profit margins        ║
║                                                  ║
║  Competitors: {', '.join(c for c in COMPETITORS)}
║  DB: {MAIN_DB_FILE}
╚══════════════════════════════════════════════════╝
    """)

    # Auto-restart loop — session errors or crashes trigger full reconnect
    restart_count = 0
    max_restarts = 20
    restart_cooldown = 10  # ilk cooldown

    while restart_count < max_restarts:
        try:
            asyncio.run(run_supply_chain())
            # Normal exit
            logger.info("Supply chain exited normally")
            break
        except KeyboardInterrupt:
            logger.info("Stopped by user (Ctrl+C)")
            break
        except Exception as e:
            restart_count += 1
            err_str = str(e)
            logger.error(f"Supply chain crashed (restart {restart_count}/{max_restarts}): {err_str}")

            # Session/security errors → fresh reconnect gerekli
            if any(keyword in err_str.lower() for keyword in
                   ["security error", "too many messages", "auth_key", "session"]):
                logger.warning(f"Session issue detected — waiting {restart_cooldown}s before reconnect")
                time.sleep(restart_cooldown)
                # Escalating cooldown (max 5 min)
                restart_cooldown = min(restart_cooldown * 2, 300)
            else:
                # Diğer hatalar kısa cooldown
                time.sleep(30)
                restart_cooldown = 10  # sıfırla

    if restart_count >= max_restarts:
        logger.critical(f"Too many restarts ({max_restarts}), giving up")


if __name__ == "__main__":
    main()
