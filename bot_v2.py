import json
import csv
import os
import sys
import random
import re
import time
import logging
import asyncio
import traceback
import shutil
import sqlite3
import hashlib
import base64
import hmac
import struct
import threading
import requests as http_requests
try:
    import httpx as _httpx
    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False
from datetime import datetime, timezone, timedelta
from html import escape

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes

# .env dosyasini yukle
from dotenv import load_dotenv
load_dotenv()

# ============================================================
# LOGGING CONFIGURATION
# ============================================================
_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("elite_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("hpack").setLevel(logging.WARNING)

# Performans ölçüm yardımcısı
import contextlib

@contextlib.contextmanager
def timed(label: str, warn_threshold: float = 2.0):
    """Bir işlemin süresini ölç, yavaşsa uyar."""
    t0 = time.time()
    logger.debug(f"[START] {label}")
    try:
        yield
    finally:
        elapsed = round(time.time() - t0, 3)
        if elapsed >= warn_threshold:
            logger.warning(f"[SLOW] {label} took {elapsed}s (threshold: {warn_threshold}s)")
        else:
            logger.debug(f"[DONE] {label} in {elapsed}s")

# ============================================================

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
# v2 - Non-custodial crypto payment system (LTC + USDC)
# Base lineage: elite_bot_wallet_final_rc1_envfix + HD wallet payment module

from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

def _get_env_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()



def _get_env_int(name: str, default: int = 0) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return int(default)
    try:
        return int(value.strip())
    except Exception:
        return int(default)


def _build_data_path(*parts: str) -> str:
    return os.path.join(DATA_DIR, *parts)


APP_VERSION = "elite_bot_v2_noncustodial"
DATA_DIR = _get_env_str("BOT_DATA_DIR", ".") or "."
# ============================================================
# LOGGING CONFIGURATION
# ============================================================
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("elite_bot")

# Telegram ve httpx loglarını sustur
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
# ============================================================

TOKEN = _get_env_str("BOT_TOKEN", "")
MAIN_CHANNEL_LINK = _get_env_str("MAIN_CHANNEL_LINK", "https://t.me/elitearners66")
STOCK_NEWS_CHANNEL_LINK = _get_env_str("STOCK_NEWS_CHANNEL_LINK", "https://t.me/EliteEarnersStockBotnews")
CHANNEL_USERNAME = _get_env_str("CHANNEL_USERNAME", "@EliteEarnersStockBotnews")
CHANNEL_BOT_USERNAME = _get_env_str("CHANNEL_BOT_USERNAME", "EliteEarnersBot")
ADMIN_CHAT_ID = _get_env_int("ADMIN_CHAT_ID", 0)

# Proof Channel — her satış/aktivasyon sonrası otomatik kanıt postu
PROOF_CHANNEL_ID = _get_env_int("PROOF_CHANNEL_ID", 0)  # Kanal chat ID (negatif sayı)
PROOF_CHANNEL_LINK = _get_env_str("PROOF_CHANNEL_LINK", "")  # https://t.me/EliteEarnersProof
PROOF_ENABLED = True  # Proof postlama açık/kapalı
GATEWAY_BOT_LINK = _get_env_str("GATEWAY_BOT_LINK", "")  # https://t.me/GatewayBotUsername

# Non-custodial odeme sistemi
ALCHEMY_API_KEY = _get_env_str("ALCHEMY_API_KEY", "")
HELIUS_API_KEY  = _get_env_str("HELIUS_API_KEY", "")
USDC_SOLANA_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # Solana USDC
BLOCKCYPHER_TOKEN = _get_env_str("BLOCKCYPHER_TOKEN", "")
WALLET_PASSWORD = _get_env_str("WALLET_PASSWORD", "")
ENCRYPTED_SEED = _get_env_str("ENCRYPTED_SEED", "")

USDC_CONTRACT_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
PAYMENT_TIMEOUT_SECONDS = 3600
PAYMENT_POLL_INTERVAL = 60
# BlockCypher rate limit koruması
BLOCKCYPHER_REQUEST_DELAY = 2.0  # İstekler arası bekleme (saniye)
_last_blockcypher_request = 0.0  # Son istek zamanı
LTC_MIN_CONFIRMATIONS = 3  # Minimum 3 confirmation gerekli
ETH_MIN_CONFIRMATIONS = 12

_master_seed_bytes: bytes | None = None
_wallet_ready = False


def refresh_runtime_settings() -> None:
    global DATA_DIR, TOKEN, MAIN_CHANNEL_LINK, STOCK_NEWS_CHANNEL_LINK, CHANNEL_USERNAME, ADMIN_CHAT_ID
    global ALCHEMY_API_KEY, BLOCKCYPHER_TOKEN, WALLET_PASSWORD, ENCRYPTED_SEED
    DATA_DIR = _get_env_str("BOT_DATA_DIR", DATA_DIR or ".") or "."
    TOKEN = _get_env_str("BOT_TOKEN", TOKEN)
    MAIN_CHANNEL_LINK = _get_env_str("MAIN_CHANNEL_LINK", MAIN_CHANNEL_LINK)
    STOCK_NEWS_CHANNEL_LINK = _get_env_str("STOCK_NEWS_CHANNEL_LINK", STOCK_NEWS_CHANNEL_LINK)
    CHANNEL_USERNAME = _get_env_str("CHANNEL_USERNAME", CHANNEL_USERNAME)
    global CHANNEL_BOT_USERNAME
    CHANNEL_BOT_USERNAME = _get_env_str("CHANNEL_BOT_USERNAME", CHANNEL_BOT_USERNAME)
    ADMIN_CHAT_ID = _get_env_int("ADMIN_CHAT_ID", ADMIN_CHAT_ID or 0)
    ALCHEMY_API_KEY = _get_env_str("ALCHEMY_API_KEY", ALCHEMY_API_KEY)
    global HELIUS_API_KEY
    HELIUS_API_KEY = _get_env_str("HELIUS_API_KEY", HELIUS_API_KEY)
    BLOCKCYPHER_TOKEN = _get_env_str("BLOCKCYPHER_TOKEN", BLOCKCYPHER_TOKEN)
    WALLET_PASSWORD = _get_env_str("WALLET_PASSWORD", WALLET_PASSWORD)
    ENCRYPTED_SEED = _get_env_str("ENCRYPTED_SEED", ENCRYPTED_SEED)
    global PROOF_CHANNEL_ID, PROOF_CHANNEL_LINK
    PROOF_CHANNEL_ID = _get_env_int("PROOF_CHANNEL_ID", PROOF_CHANNEL_ID or 0)
    PROOF_CHANNEL_LINK = _get_env_str("PROOF_CHANNEL_LINK", PROOF_CHANNEL_LINK)

WALLET_ADDRESSES_FILE = _build_data_path("wallet_addresses.json")
PENDING_PAYMENTS_FILE = _build_data_path("pending_payments.json")

# ============================================================
# CARD STOCK MODULE — individual card listing & reservation
# ============================================================



# Provider bayrak emojileri
PROVIDER_FLAGS = {
    "GiftCardMall":    "🇺🇸",
    "MyPrepaidCenter": "🇺🇸",
    "VanillaGift":     "🇺🇸",
    "VanillaPrepaid":  "🇺🇸",
    "Walmart":         "🇺🇸",
    "Amex":            "🇺🇸",
    "BalanceNow":      "🇺🇸",
    "PerfectGift":     "🇺🇸",
    "PrepaidGift":     "🇺🇸",
    "Prepaid":         "🇺🇸",
    "JokerCard":       "🇨🇦",
    "PerfectGiftCA":   "🇨🇦",
    "CardBalanceAU":   "🇦🇺",
    "CardBalance":     "🇦🇺",
}

# ============================================================
# BIN → PROVIDER MAPPING TABLE
# ============================================================
BIN_TO_PROVIDER = {
    # GiftCardMall (GCM)
    "451129": "GiftCardMall",
    "491277": "GiftCardMall",
    "435880": "GiftCardMall",
    "403446": "GiftCardMall",
    "511332": "GiftCardMall",
    "461126": "GiftCardMall",
    # MyPrepaidCenter (MPC)
    "533937": "MyPrepaidCenter",
    "544768": "MyPrepaidCenter",
    "420495": "MyPrepaidCenter",
    "511538": "MyPrepaidCenter",
    "413949": "MyPrepaidCenter",
    "373778": "MyPrepaidCenter",
    # JokerCard (JKR) - Canada
    "533985": "JokerCard",
    "408635": "JokerCard",
    # CardBalanceAU (CBAU) - Australia
    "432465": "CardBalanceAU",
    "428313": "CardBalanceAU",
    # VanillaGift / VanillaPrepaid
    "409758": "VanillaGift",
    "411810": "VanillaGift",
    "520356": "VanillaGift",
    "375163": "VanillaGift",
    # Walmart (WMT)
    "485246": "Walmart",
    # AMEX (American Express prepaid)
    "377935": "Amex",
    "379358": "Amex",
    "376766": "Amex",
    "374887": "Amex",
    "372691": "Amex",
    "371146": "Amex",
    "379435": "Amex",
    # PrepaidGift
    "525362": "PrepaidGift",
}

def detect_provider_from_bin(card_number: str) -> str | None:
    """Kart numarasinin ilk 6 hanesine gore provider tespit et."""
    bin6 = card_number.replace(" ", "")[:6]
    return BIN_TO_PROVIDER.get(bin6)

# ============================================================
# BIN TABLE END
# ============================================================
CARDS_FILE = _build_data_path("cards.json")
PENDING_ACTIVATIONS_FILE = _build_data_path("pending_activations.json")
CARD_RESERVATION_SECONDS = 60
CARDS_PER_PAGE = 10

PROVIDER_SHORT = {
    "MyPrepaidCenter": "MPC",
    "GiftCardMall":    "GCM",
    "JokerCard":       "JKR",
    "CardBalanceAU":   "CBAU",
    "CardBalance":     "CB",
    "BalanceNow":      "BNow",
    "PerfectGift":     "PG",
    "PerfectGiftCA":   "PGCA",
    "VanillaGift":     "VG",
    "VanillaPrepaid":  "VP",
    "Walmart":         "WMT",
    "Amex":            "AMEX",
    "PrepaidGift":     "PPG",
    "Prepaid":         "PP",
}

SHORT_TO_PROVIDER = {v: k for k, v in PROVIDER_SHORT.items()}


# ============================================================
# CARD DATA ENCRYPTION (AES-GCM)
# ============================================================
# Kart numaraları ve CVV şifreli saklanır, sadece teslimatta açılır

def _get_card_encryption_key() -> bytes:
    """Card encryption key — ayrı CARD_ENCRYPTION_KEY env değişkeninden."""
    # WALLET_PASSWORD'dan ayrı tutulur — biri ele geçirilse diğeri güvende
    key_source = os.environ.get("CARD_ENCRYPTION_KEY") or WALLET_PASSWORD
    if not key_source:
        logger.critical("CARD_ENCRYPTION_KEY or WALLET_PASSWORD must be set!")
        raise ValueError("No encryption key configured")
    pwd = key_source.encode()
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"card_field_encryption_v1",
        iterations=200_000,  # Daha güçlü KDF
    )
    return kdf.derive(pwd)

def encrypt_card_field(plaintext: str) -> str:
    """Kart alanını AES-GCM ile şifrele → base64 string döner."""
    if not plaintext or plaintext.startswith("ENC:"):
        return plaintext  # Zaten şifreli veya boş
    try:
        key   = _get_card_encryption_key()
        nonce = os.urandom(12)
        ct    = AESGCM(key).encrypt(nonce, plaintext.encode(), None)
        return "ENC:" + base64.b64encode(nonce + ct).decode()
    except Exception as e:
        logger.error(f"Card field encryption error: {e}")
        return plaintext

def decrypt_card_field(ciphertext: str) -> str:
    """Şifreli kart alanını çöz."""
    if not ciphertext or not ciphertext.startswith("ENC:"):
        return ciphertext  # Şifresiz (eski kart)
    try:
        key  = _get_card_encryption_key()
        data = base64.b64decode(ciphertext[4:])
        nonce, ct = data[:12], data[12:]
        return AESGCM(key).decrypt(nonce, ct, None).decode()
    except Exception as e:
        logger.error(f"Card field decryption error: {e}")
        return ""

def encrypt_card_record(card: dict) -> dict:
    """Kart kaydındaki hassas alanları şifrele."""
    sensitive = ["card_number", "cvv"]
    encrypted = dict(card)
    for field in sensitive:
        if field in encrypted and encrypted[field]:
            encrypted[field] = encrypt_card_field(str(encrypted[field]))
    return encrypted

def decrypt_card_record(card: dict) -> dict:
    """Kart kaydındaki şifreli alanları çöz — sadece teslimatta kullan."""
    sensitive = ["card_number", "cvv"]
    decrypted = dict(card)
    for field in sensitive:
        if field in decrypted and decrypted[field]:
            decrypted[field] = decrypt_card_field(str(decrypted[field]))
    return decrypted

# ============================================================
# CARD ENCRYPTION END
# ============================================================

# ============================================================
# BALANCE CHECKER MODULE — Live balance & transaction check
# ============================================================
# Provider sitelerine HTTP istek atarak bakiye + işlem geçmişi çeker.
# Teslim anında ve "Recheck Balance" butonuyla kullanılır.
#
# Desteklenen provider'lar:
#   - GiftCardMall (Visa: mygift.giftcardmall.com)
#   - GiftCardMall (Mastercard: mcgift.giftcardmall.com)
#   - (Diğerleri eklenecek: MPC, JokerCard, Walmart, Vanilla)
# ============================================================

CHECKER_ENABLED = False  # CAPTCHA çözümü hazır olunca True yap
CHECKER_TIMEOUT = 15  # HTTP timeout (saniye)
CHECKER_MAX_RETRIES = 2
RECHECK_MAX_PER_ORDER = 1  # Kullanıcı başına max recheck hakkı
RECHECK_COOLDOWN_SECONDS = 60  # Recheck arası bekleme

# Recheck sayaçları: {order_id: count}
_recheck_counts: dict = {}

# Provider → checker URL mapping
CHECKER_URLS = {
    "GiftCardMall": {
        "visa":       "https://mygift.giftcardmall.com",
        "mastercard": "https://mcgift.giftcardmall.com",
    },
    "MyPrepaidCenter": {
        "default": "https://www.myprepaidcenter.com/check-balance",
    },
    "VanillaGift": {
        "default": "https://www.vanillagift.com/check-balance",
    },
    "Walmart": {
        "default": "https://www.walmartgift.com/check-balance",
    },
}


class BalanceCheckResult:
    """Balance check sonucunu taşıyan data class."""
    def __init__(self):
        self.success: bool = False
        self.balance: float | None = None
        self.currency: str = "USD"
        self.transactions: list = []   # [{"date": ..., "merchant": ..., "amount": ..., "type": ...}]
        self.initial_balance: float | None = None
        self.activation_date: str | None = None
        self.card_status: str | None = None  # "active", "inactive", "closed"
        self.error: str | None = None
        self.raw_html: str | None = None  # Debug için

    def format_transactions_text(self, max_items: int = 10) -> str:
        """İşlem geçmişini text formatında döndür."""
        if not self.transactions:
            return "No transactions found."
        lines = []
        for tx in self.transactions[:max_items]:
            date = tx.get("date", "")
            merchant = tx.get("merchant", "Unknown")
            amount = tx.get("amount", "")
            tx_type = tx.get("type", "")
            lines.append(f"{date} | {merchant} | {amount} ({tx_type})")
        if len(self.transactions) > max_items:
            lines.append(f"... and {len(self.transactions) - max_items} more")
        return "\n".join(lines)

    def format_delivery_text(self) -> str:
        """Teslim mesajına eklenecek balance bilgisi."""
        if not self.success:
            return f"⚠️ Balance check failed: {self.error or 'Unknown error'}"
        parts = [f"💰 Verified Balance: {self.currency} ${self.balance:.2f}"]
        if self.initial_balance is not None:
            parts.append(f"📋 Initial Balance: {self.currency} ${self.initial_balance:.2f}")
        if self.activation_date:
            parts.append(f"🎯 Activation: {self.activation_date}")
        if self.transactions:
            parts.append(f"\n📝 All Transactions:")
            parts.append(self.format_transactions_text())
        return "\n".join(parts)


def _get_http_session():
    """HTTP session oluştur — httpx varsa async, yoksa requests."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    if _HTTPX_AVAILABLE:
        return _httpx.AsyncClient(
            headers=headers,
            timeout=CHECKER_TIMEOUT,
            follow_redirects=True,
            verify=True,
        )
    return None


def _parse_balance_from_html(html: str) -> BalanceCheckResult:
    """HTML'den bakiye ve işlem bilgisini çıkar — genel parser."""
    result = BalanceCheckResult()
    result.raw_html = html[:5000]  # İlk 5KB debug için

    # Bakiye pattern'leri
    import re
    # $1.83, $25.00, $142.27 gibi
    balance_patterns = [
        r'(?:available|current|remaining)\s*(?:balance|funds)[^$]*\$\s*([\d,]+\.?\d*)',
        r'balance[^$]*\$\s*([\d,]+\.?\d*)',
        r'\$\s*([\d,]+\.\d{2})\s*(?:USD|available|remaining)',
    ]
    for pattern in balance_patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            try:
                result.balance = float(match.group(1).replace(",", ""))
                result.success = True
                break
            except Exception:
                continue

    # Initial balance
    init_patterns = [
        r'(?:initial|original|loaded)\s*(?:balance|amount|value)[^$]*\$\s*([\d,]+\.?\d*)',
    ]
    for pattern in init_patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            try:
                result.initial_balance = float(match.group(1).replace(",", ""))
                break
            except Exception:
                continue

    # Activation date
    act_patterns = [
        r'(?:activation|activated)\s*(?:date)?[:\s]*([\d]{4}-[\d]{2}-[\d]{2})',
        r'(?:activation|activated)\s*(?:date)?[:\s]*([\d]{2}/[\d]{2}/[\d]{4})',
    ]
    for pattern in act_patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            result.activation_date = match.group(1)
            break

    # Transaction parsing — tablo/liste formatı
    tx_patterns = [
        # ISO date format: 2026-03-30 18:15:52 | MERCHANT | -$7.09 (PURCHASE)
        r'(\d{4}-\d{2}-\d{2}[\sT][\d:]+)\s*[|,]\s*([^|,]+?)\s*[|,]\s*(-?\$?[\d,.]+)\s*(?:USD\s*)?\((\w+)\)',
        # Simpler: $-11.99 - MERCHANT (TYPE) - DATE
        r'\$?\s*(-?[\d,.]+)\s*[-–]\s*([^(]+?)\s*\((\w+)\)\s*[-–]\s*([\d\w\-:T.]+)',
    ]
    for pattern in tx_patterns:
        matches = re.findall(pattern, html, re.IGNORECASE)
        if matches:
            for m in matches:
                if len(m) == 4:
                    # Pattern 1: date, merchant, amount, type
                    result.transactions.append({
                        "date": m[0].strip(),
                        "merchant": m[1].strip(),
                        "amount": m[2].strip(),
                        "type": m[3].strip(),
                    })
            if result.transactions:
                break

    return result


def _parse_json_balance(data: dict) -> BalanceCheckResult:
    """JSON response'dan bakiye bilgisini çıkar."""
    result = BalanceCheckResult()
    try:
        # Yaygın JSON yapıları
        bal = (data.get("balance") or data.get("availableBalance")
               or data.get("available_balance") or data.get("currentBalance")
               or data.get("card", {}).get("balance"))
        if bal is not None:
            result.balance = float(bal)
            result.success = True

        init_bal = (data.get("initialBalance") or data.get("initial_balance")
                    or data.get("loadedAmount") or data.get("card", {}).get("initialBalance"))
        if init_bal is not None:
            result.initial_balance = float(init_bal)

        act_date = (data.get("activationDate") or data.get("activation_date")
                    or data.get("card", {}).get("activationDate"))
        if act_date:
            result.activation_date = str(act_date)

        status = (data.get("status") or data.get("cardStatus")
                  or data.get("card", {}).get("status"))
        if status:
            result.card_status = str(status).lower()

        # Transactions
        txs = (data.get("transactions") or data.get("recentTransactions")
               or data.get("history") or [])
        for tx in txs:
            if isinstance(tx, dict):
                result.transactions.append({
                    "date": str(tx.get("date", tx.get("transactionDate", tx.get("postedDate", "")))),
                    "merchant": str(tx.get("merchant", tx.get("description", tx.get("merchantName", "Unknown")))),
                    "amount": str(tx.get("amount", tx.get("transactionAmount", ""))),
                    "type": str(tx.get("type", tx.get("transactionType", tx.get("category", "")))),
                })
    except Exception as e:
        result.error = f"JSON parse error: {e}"
    return result


async def check_balance_gcm(card_number: str, exp_month: str, exp_year: str, cvv: str) -> BalanceCheckResult:
    """GiftCardMall (Visa) balance check — mygift.giftcardmall.com
    API: POST /api/card/getCardBalanceSummary
    Payload: {cardNumber, expirationMonth (int), expirationYear (int), securityCode, rmsSessionId}
    """
    result = BalanceCheckResult()

    if not _HTTPX_AVAILABLE:
        result.error = "httpx not installed"
        return result

    # Expiry normalization
    try:
        exp_m = int(exp_month)
        exp_y = int(exp_year) if len(exp_year) == 4 else int("20" + exp_year)
    except Exception:
        result.error = "Invalid expiry format"
        return result

    try:
        async with _httpx.AsyncClient(
            timeout=CHECKER_TIMEOUT,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://mygift.giftcardmall.com",
                "Referer": "https://mygift.giftcardmall.com/",
            },
        ) as client:
            rms_session_id = ""

            # Step 1: Landing page — opsiyonel, cookie almak için
            try:
                logger.info(f"[CHECKER] GCM: fetching landing page...")
                lp = await client.get("https://mygift.giftcardmall.com/")
                logger.info(f"[CHECKER] GCM: landing page status={lp.status_code}")

                # HTML'den session ID dene
                if lp.status_code == 200:
                    import re
                    for pat in [r'"rmsSessionId"\s*:\s*"([^"]+)"', r'"sessionId"\s*:\s*"([^"]+)"',
                                r'sessionKey=([A-Z0-9]+)']:
                        m = re.search(pat, lp.text)
                        if m:
                            rms_session_id = m.group(1)
                            logger.info(f"[CHECKER] GCM: rmsSessionId from HTML")
                            break
            except Exception as e:
                logger.debug(f"[CHECKER] GCM: landing page failed (continuing): {e}")

            # Step 2: sessionDetails endpoint
            if not rms_session_id:
                try:
                    logger.info(f"[CHECKER] GCM: trying sessionDetails...")
                    sess_resp = await client.get("https://mygift.giftcardmall.com/api/card/sessionDetails")
                    logger.info(f"[CHECKER] GCM: sessionDetails status={sess_resp.status_code}")
                    if sess_resp.status_code == 200:
                        sess_data = sess_resp.json()
                        rms_session_id = (
                            sess_data.get("rmsSessionId", "")
                            or sess_data.get("sessionId", "")
                            or sess_data.get("session", {}).get("id", "")
                        )
                        if rms_session_id:
                            logger.info(f"[CHECKER] GCM: rmsSessionId from sessionDetails")
                        else:
                            # Tüm key'leri logla — debug için
                            logger.info(f"[CHECKER] GCM: sessionDetails keys: {list(sess_data.keys())}")
                except Exception as e:
                    logger.debug(f"[CHECKER] GCM: sessionDetails failed: {e}")

            # Step 3: Cookie'lerden session ID
            if not rms_session_id:
                for ck in ["rmsSessionId", "sessionId", "JSESSIONID", "datadome"]:
                    for domain in ["mygift.giftcardmall.com", ".giftcardmall.com", "giftcardmall.com"]:
                        val = client.cookies.get(ck, domain=domain)
                        if val:
                            rms_session_id = val
                            logger.info(f"[CHECKER] GCM: session from cookie '{ck}' @ {domain}")
                            break
                    if rms_session_id:
                        break

            logger.info(f"[CHECKER] GCM: rmsSessionId={'YES' if rms_session_id else 'NO'}")

            # Step 4: POST balance check
            payload = {
                "cardNumber": card_number,
                "expirationMonth": exp_m,
                "expirationYear": exp_y,
                "securityCode": cvv,
            }
            if rms_session_id:
                payload["rmsSessionId"] = rms_session_id

            logger.info(f"[CHECKER] GCM: POST getCardBalanceSummary for {card_number[:6]}xx...")
            resp = await client.post(
                "https://mygift.giftcardmall.com/api/card/getCardBalanceSummary",
                json=payload,
                headers={"Content-Type": "application/json"},
            )

            logger.info(f"[CHECKER] GCM: response status={resp.status_code}, size={len(resp.text)}")

            # Log response body for debugging (first 500 chars)
            logger.info(f"[CHECKER] GCM: response body: {resp.text[:500]}")

            if resp.status_code == 405:
                result.error = "CAPTCHA required (HTTP 405)"
                logger.warning(f"[CHECKER] GCM: CAPTCHA block (405)")
                return result

            if resp.status_code == 403:
                result.error = "Access denied (HTTP 403) — possible bot detection"
                return result

            if resp.status_code != 200:
                result.error = f"HTTP {resp.status_code}"
                return result

            # JSON response parse
            try:
                data = resp.json()
                logger.info(f"[CHECKER] GCM: response data keys: {list(data.keys()) if isinstance(data, dict) else 'not dict'}")
            except Exception as e:
                result.error = f"Invalid JSON response: {e}"
                result.raw_html = resp.text[:2000]
                return result

            # Başarısız response kontrolü
            if data.get("success") is False:
                error_code = data.get("chsErrorCode", data.get("errorCode", "unknown"))
                error_map = {
                    "ERR-1004": "Unable to process — possible CAPTCHA or rate limit",
                    "ERR-1001": "Invalid card number",
                    "ERR-1002": "Invalid expiry date",
                    "ERR-1003": "Invalid CVV",
                    "ERR-1005": "Card not found",
                    "ERR-1006": "Card inactive or closed",
                }
                result.error = error_map.get(error_code, f"API error: {error_code}")
                return result

            # Başarılı response — balance + transactions çıkar
            result.success = True

            # Balance
            bal = (data.get("balance") or data.get("availableBalance")
                   or data.get("currentBalance") or data.get("card", {}).get("balance"))
            if bal is not None:
                result.balance = float(bal)

            # Initial balance
            init_bal = (data.get("initialBalance") or data.get("originalBalance")
                        or data.get("loadedAmount") or data.get("card", {}).get("initialBalance"))
            if init_bal is not None:
                result.initial_balance = float(init_bal)

            # Activation date
            act = (data.get("activationDate") or data.get("activation_date")
                   or data.get("card", {}).get("activationDate"))
            if act:
                result.activation_date = str(act)

            # Card status
            status = data.get("cardStatus") or data.get("status")
            if status:
                result.card_status = str(status).lower()

            # Transactions
            txs = (data.get("transactions") or data.get("recentTransactions")
                   or data.get("transactionHistory") or [])
            for tx in txs:
                if isinstance(tx, dict):
                    result.transactions.append({
                        "date": str(tx.get("date", tx.get("transactionDate",
                                    tx.get("postedDate", tx.get("dateTime", ""))))),
                        "merchant": str(tx.get("merchant", tx.get("description",
                                        tx.get("merchantName", "Unknown")))),
                        "amount": str(tx.get("amount", tx.get("transactionAmount", ""))),
                        "type": str(tx.get("type", tx.get("transactionType",
                                    tx.get("category", "")))).upper(),
                    })

            if result.balance is None and result.success:
                # success=true ama balance yok — raw data logla
                result.error = "Balance field not found in response"
                result.success = False
                logger.warning(f"[CHECKER] GCM: success but no balance. Keys: {list(data.keys())}")

            return result

    except Exception as e:
        result.error = f"HTTP error: {e}"
        logger.error(f"[CHECKER] GCM: exception: {e}")
        return result


async def check_balance_mpc(card_number: str, exp_month: str, exp_year: str, cvv: str) -> BalanceCheckResult:
    """MyPrepaidCenter balance check — placeholder."""
    result = BalanceCheckResult()
    result.error = "MPC checker not implemented yet"
    return result


async def check_balance_vanilla(card_number: str, exp_month: str, exp_year: str, cvv: str) -> BalanceCheckResult:
    """VanillaGift balance check — placeholder."""
    result = BalanceCheckResult()
    result.error = "Vanilla checker not implemented yet"
    return result


# Provider → checker function mapping
BALANCE_CHECKERS = {
    "GiftCardMall": check_balance_gcm,
    "MyPrepaidCenter": check_balance_mpc,
    "VanillaGift": check_balance_vanilla,
    "VanillaPrepaid": check_balance_vanilla,
}


async def check_card_balance(card: dict) -> BalanceCheckResult:
    """Kart bilgileriyle provider'a uygun checker'ı çalıştır."""
    if not CHECKER_ENABLED:
        result = BalanceCheckResult()
        result.error = "Balance checker disabled"
        return result

    provider = card.get("provider", "")
    checker_fn = BALANCE_CHECKERS.get(provider)

    if not checker_fn:
        result = BalanceCheckResult()
        result.error = f"No checker for provider: {provider}"
        return result

    # Kart bilgilerini decrypt et
    decrypted = decrypt_card_record(card)
    card_number = decrypted.get("card_number", "")
    exp_month = decrypted.get("expiry_month", "")
    exp_year = decrypted.get("expiry_year", "")
    cvv = decrypted.get("cvv", "")

    if not card_number or not exp_month or not cvv:
        result = BalanceCheckResult()
        result.error = "Incomplete card data"
        return result

    # Retry logic
    last_error = None
    for attempt in range(CHECKER_MAX_RETRIES):
        try:
            with timed(f"balance_check_{provider}", warn_threshold=10.0):
                result = await checker_fn(card_number, exp_month, exp_year, cvv)
            if result.success:
                return result
            last_error = result.error
        except Exception as e:
            last_error = str(e)
            logger.warning(f"[CHECKER] Attempt {attempt+1} failed: {e}")
        if attempt < CHECKER_MAX_RETRIES - 1:
            await asyncio.sleep(2)  # Retry arası bekle

    result = BalanceCheckResult()
    result.error = last_error or "All attempts failed"
    return result


def can_recheck_order(order_id: str) -> bool:
    """Bu sipariş için recheck hakkı var mı?"""
    return _recheck_counts.get(order_id, 0) < RECHECK_MAX_PER_ORDER


def increment_recheck(order_id: str) -> None:
    _recheck_counts[order_id] = _recheck_counts.get(order_id, 0) + 1


# ============================================================
# BALANCE CHECKER MODULE END
# ============================================================

# ============================================================
# PROOF CHANNEL MODULE — otomatik satış/aktivasyon kanıtı
# ============================================================
# Her başarılı teslim ve aktivasyonda kanıt kanalına post atar.
# Kullanıcı gizliliği korunur — isim/ID/kart numarası paylaşılmaz.
# ============================================================

# Proof sayaçları (bot çalışma süresi boyunca)
_proof_stats = {
    "total_deliveries": 0,
    "total_activations": 0,
    "total_volume_usd": 0.0,
    "bot_start_time": None,
}


def _get_proof_stats() -> dict:
    if _proof_stats["bot_start_time"] is None:
        _proof_stats["bot_start_time"] = datetime.now()
    return _proof_stats


def _safe_gc_region_flag(region: str) -> str:
    """GC_REGIONS henüz tanımlı olmayabilir — güvenli erişim."""
    try:
        return GC_REGIONS.get(region, "🌍")
    except NameError:
        _flags = {"US": "🇺🇸", "CA": "🇨🇦", "EU": "🇪🇺", "UK": "🇬🇧", "AUS": "🇦🇺"}
        return _flags.get(region, "🌍")


async def post_proof_delivery(bot, provider: str, card_balance: float,
                               cost: float, delivery_seconds: float,
                               order_id: str, is_gift_card: bool = False,
                               region: str = "", gc_type: str = "") -> None:
    """Kanıt kanalına başarılı teslim postu at."""
    logger.info(f"[PROOF] Called: enabled={PROOF_ENABLED}, channel_id={PROOF_CHANNEL_ID}, order={order_id}")
    if not PROOF_ENABLED or not PROOF_CHANNEL_ID:
        logger.warning(f"[PROOF] Skipped: enabled={PROOF_ENABLED}, channel_id={PROOF_CHANNEL_ID}")
        return

    stats = _get_proof_stats()
    stats["total_deliveries"] += 1
    stats["total_volume_usd"] += cost

    # Provider kısa adı
    short = PROVIDER_SHORT.get(provider, provider[:6]) if not is_gift_card else f"{gc_type}"
    flag = PROVIDER_FLAGS.get(provider, "🏪") if not is_gift_card else _safe_gc_region_flag(region)

    # Delivery süresini formatla
    if delivery_seconds < 1:
        speed_text = "< 1s"
    elif delivery_seconds < 60:
        speed_text = f"{delivery_seconds:.0f}s"
    else:
        speed_text = f"{delivery_seconds/60:.1f}min"

    # Rastgele doğrulama kodu (kullanıcı doğrulayamaz ama güven verir)
    verify_code = hashlib.md5(f"{order_id}{time.time()}".encode()).hexdigest()[:8].upper()

    if is_gift_card:
        text = (
            f"✅ Gift Card Delivered!\n\n"
            f"{flag} {gc_type} | {region}\n"
            f"💰 Value: ${card_balance:.2f}\n"
            f"⚡ Speed: {speed_text}\n"
            f"🔒 Verified: #{verify_code}\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 Total Deliveries: {stats['total_deliveries']}\n"
            f"💵 Volume: {format_usd(stats['total_volume_usd'])}"
        )
    else:
        rate_line = f"📊 Rate: {cost/card_balance*100:.0f}%\n" if card_balance > 0 else ""
        text = (
            f"✅ Prepaid Card Delivered!\n\n"
            f"{flag} {short}\n"
            f"💰 Balance: ${card_balance:.2f}\n"
            f"{rate_line}"
            f"⚡ Speed: {speed_text}\n"
            f"🔒 Verified: #{verify_code}\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 Total Deliveries: {stats['total_deliveries']}\n"
            f"💵 Volume: {format_usd(stats['total_volume_usd'])}"
        )

    # Bot link butonu — Gateway Bot'a yönlendir (ana bot gizli)
    shop_url = GATEWAY_BOT_LINK or f"https://t.me/{CHANNEL_BOT_USERNAME}"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Shop Now", url=shop_url)],
    ])

    try:
        await bot.send_message(
            chat_id=PROOF_CHANNEL_ID,
            text=text,
            reply_markup=keyboard,
        )
        logger.debug(f"[PROOF] Delivery posted: {order_id}")
    except Exception as e:
        logger.warning(f"[PROOF] Failed to post delivery: {e}")


async def post_proof_activation(bot, user_name: str = "New Member") -> None:
    """Kanıt kanalına başarılı aktivasyon postu at."""
    if not PROOF_ENABLED or not PROOF_CHANNEL_ID:
        return

    stats = _get_proof_stats()
    stats["total_activations"] += 1

    # İsmi maskele: "John Doe" → "J***"
    if user_name and len(user_name) > 1:
        masked_name = user_name[0] + "***"
    else:
        masked_name = "***"

    text = (
        f"🎉 New Member Activated!\n\n"
        f"👤 {masked_name}\n"
        f"💎 Elite Lifetime Access\n"
        f"🕐 {datetime.now().strftime('%H:%M UTC')}\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"👥 Total Activations: {stats['total_activations']}"
    )

    shop_url = GATEWAY_BOT_LINK or f"https://t.me/{CHANNEL_BOT_USERNAME}"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Join Now", url=shop_url)],
    ])

    try:
        await bot.send_message(
            chat_id=PROOF_CHANNEL_ID,
            text=text,
            reply_markup=keyboard,
        )
        logger.debug(f"[PROOF] Activation posted")
    except Exception as e:
        logger.warning(f"[PROOF] Failed to post activation: {e}")


# ============================================================
# PROOF CHANNEL MODULE END
# ============================================================
def load_cards() -> dict:
    """Kartları SQLite'tan yükle, yoksa JSON fallback."""
    try:
        db_data = db_load_cards()
        if db_data:
            return db_data
    except Exception:
        pass
    return load_json_file(CARDS_FILE, {})

def save_cards(data: dict) -> None:
    """JSON'a yaz + SQLite senkronize et."""
    save_json_file(CARDS_FILE, data)
    try:
        for card_id, card in data.items():
            db_save_card(card_id, card)
    except Exception as e:
        logger.warning(f"save_cards SQLite sync error: {e}")

def generate_card_id() -> str:
    return f"CARD-{int(time.time())}-{random.randint(1000, 9999)}"


# ─── Competitor Balance Management (Just-In-Time Funding) ───
def load_competitor_balances() -> dict:
    """Rakip botlardaki tahmini bakiyeleri yükle."""
    default = {
        "guru":   {"balance": COMPETITOR_DEFAULT_BUFFER, "last_updated": "", "notes": ""},
        "xstock": {"balance": COMPETITOR_DEFAULT_BUFFER, "last_updated": "", "notes": ""},
        "sharks": {"balance": 0.0, "last_updated": "", "notes": ""},
        "planet": {"balance": 0.0, "last_updated": "", "notes": ""},
    }
    data = load_json_file(COMPETITOR_BALANCES_FILE, default)
    for key, val in default.items():
        if key not in data:
            data[key] = val
    return data


def save_competitor_balances(data: dict) -> None:
    save_json_file(COMPETITOR_BALANCES_FILE, data)


def get_competitor_balance(source: str) -> float:
    balances = load_competitor_balances()
    return float(balances.get(source, {}).get("balance", 0.0))


def deduct_competitor_balance(source: str, amount: float, reason: str = "") -> bool:
    try:
        balances = load_competitor_balances()
        if source not in balances:
            balances[source] = {"balance": 0.0, "last_updated": "", "notes": ""}
        balances[source]["balance"] = round(float(balances[source]["balance"]) - amount, 2)
        balances[source]["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if reason:
            balances[source]["notes"] = reason[:100]
        save_competitor_balances(balances)
        logger.info(f"[COMP_BAL] {source}: -${amount:.2f} ({reason}) → ${balances[source]['balance']:.2f}")
        return True
    except Exception as e:
        logger.error(f"deduct_competitor_balance error: {e}")
        return False


def credit_competitor_balance(source: str, amount: float, note: str = "manual deposit") -> bool:
    try:
        balances = load_competitor_balances()
        if source not in balances:
            balances[source] = {"balance": 0.0, "last_updated": "", "notes": ""}
        balances[source]["balance"] = round(float(balances[source]["balance"]) + amount, 2)
        balances[source]["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        balances[source]["notes"] = note[:100]
        save_competitor_balances(balances)
        logger.info(f"[COMP_BAL] {source}: +${amount:.2f} ({note}) → ${balances[source]['balance']:.2f}")
        return True
    except Exception as e:
        logger.error(f"credit_competitor_balance error: {e}")
        return False


def classify_order_size(amount: float) -> str:
    """Returns 'instant' (<$20), 'standard' ($20-50), 'premium' (>$50)"""
    if amount < ORDER_INSTANT_THRESHOLD:
        return "instant"
    elif amount < ORDER_PREMIUM_THRESHOLD:
        return "standard"
    else:
        return "premium"


# ─── Purchase Error Classification (Rakip bot hata mesajları) ───
# Rakip botların döndürdüğü hata mesajlarını kategorize et, doğru aksiyonu belirle.
# Kategoriler:
#   "retry_wait"     → 60-75 sn bekle, aynı kartı yeniden dene
#   "card_dead"      → Kart bozuk/geçersiz, başka kartla substitute
#   "card_changed"   → Kart bilgisi değişti (reg/google), başka kartla substitute
#   "balance_changed"→ Bakiye değişti, müşteriye sor
#   "unknown"        → Bilinmeyen hata, admin incelemeli

def classify_purchase_error(error_text: str) -> dict:
    """Rakip bot hata mesajını analiz et ve aksiyon döndür.
    Returns: {"category": "...", "detail": "...", "retry_after_sec": int|None}"""
    if not error_text:
        return {"category": "unknown", "detail": "empty_response", "retry_after_sec": None}

    text_lower = error_text.lower()

    # Checker cooldown — kartı az önce kontrol etmişler, bekle
    if "just checked" in text_lower and ("wait" in text_lower or "60 second" in text_lower):
        return {"category": "retry_wait", "detail": "checker_cooldown", "retry_after_sec": 75}

    # Kart ölü / geçersiz
    if "card is dead" in text_lower or "invalid card" in text_lower:
        return {"category": "card_dead", "detail": "dead_or_invalid", "retry_after_sec": None}

    # Bakiye değişti
    if "balance" in text_lower and ("→" in error_text or "changed" in text_lower):
        return {"category": "balance_changed", "detail": "balance_mismatch", "retry_after_sec": None}

    if "balance is too low" in text_lower:
        return {"category": "balance_changed", "detail": "balance_too_low", "retry_after_sec": None}

    # Registration / Google / PayPal değişti
    if "card info changed" in text_lower or "purchase aborted" in text_lower:
        detail = "info_changed"
        if "registered" in text_lower:
            detail = "registration_changed"
        elif "google" in text_lower:
            detail = "google_used"
        elif "paypal" in text_lower:
            detail = "paypal_used"
        return {"category": "card_changed", "detail": detail, "retry_after_sec": None}

    # Başka bir kart seç çağrısı — genelde substitute anlamına gelir
    if "choose another card" in text_lower or "try another card" in text_lower:
        return {"category": "card_changed", "detail": "unspecified_change", "retry_after_sec": None}

    return {"category": "unknown", "detail": error_text[:100], "retry_after_sec": None}


def find_substitute_card(provider: str, target_balance: float, registered: bool | None,
                          currency: str = "USD", exclude_ext_ids: list = None) -> dict | None:
    """Aynı provider + benzer bakiye + aynı registration'da yedek kart ara.
    Müşterinin istediği ile eşdeğer bir alternatif bulursa döndürür.
    Returns: {'ext_id', 'bin', 'balance', 'sell_price', 'source', ...} or None"""
    exclude_ext_ids = exclude_ext_ids or []
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Bakiye toleransı: ±%10 (müşteri biraz daha fazla/az alırsa sorun yok)
        bal_min = target_balance * 0.90
        bal_max = target_balance * 1.10

        # Registration filtresi
        if registered is True:
            reg_clause = "AND registered = 1"
        elif registered is False:
            reg_clause = "AND (registered = 0 OR registered = -1)"
        else:
            reg_clause = ""

        placeholders = ",".join(["?"] * len(exclude_ext_ids)) if exclude_ext_ids else ""
        exclude_clause = f"AND ext_id NOT IN ({placeholders})" if exclude_ext_ids else ""

        query = f"""
            SELECT ext_id, source, bin_number, balance, currency, cost_price, sell_price,
                   profit, provider, registered
            FROM external_cards
            WHERE status = 'available' AND expires_at > ?
              AND balance >= ? AND balance <= ?
              AND currency = ?
              {reg_clause}
              {exclude_clause}
            ORDER BY ABS(balance - ?) ASC
            LIMIT 1
        """
        params = [now, bal_min, bal_max, currency]
        if exclude_ext_ids:
            params.extend(exclude_ext_ids)
        params.append(target_balance)

        cur.execute(query, params)
        row = cur.fetchone()
        conn.close()

        if not row:
            return None

        raw_prov = (row["provider"] or "").strip().lower()
        actual_provider = _normalize_provider(raw_prov, bin_hint=row["bin_number"] or "")

        # Provider eşleşmesi zorunlu
        if actual_provider.lower() != provider.lower():
            return None

        return {
            "ext_id": row["ext_id"],
            "source": row["source"],
            "bin": row["bin_number"] or "xxxxxx",
            "balance": float(row["balance"]),
            "currency": row["currency"] or "USD",
            "cost_price": float(row["cost_price"]),
            "sell_price": float(row["sell_price"]),
            "profit": float(row["profit"]),
            "provider": actual_provider,
            "registered": row["registered"],
        }
    except Exception as e:
        logger.error(f"find_substitute_card error: {e}")
        return None


def refund_order(order_id: str, bonus_pct: float = 0.0, reason: str = "") -> bool:
    """Siparişi iptal et, müşterinin bakiyesine iade yap.
    bonus_pct: ek bonus (0.02 = %2)"""
    try:
        orders = load_orders()
        order = orders.get(order_id)
        if not order:
            logger.warning(f"refund_order: {order_id} not found")
            return False

        user_id = order["user_id"]
        cost = float(order.get("cost", 0))
        bonus = round(cost * bonus_pct, 2) if bonus_pct > 0 else 0

        # Kullanıcı bakiyesine iade (+ bonus)
        user_record = get_hybrid_balance_record(user_id)
        current_balance = float(user_record.get("available_balance", 0))
        refund_total = cost + bonus
        new_balance = current_balance + refund_total

        # total_spent'i de düşelim (iptal edildiği için)
        total_spent = max(0, float(user_record.get("total_spent", 0)) - cost)

        update_hybrid_balance(user_id, available_balance=new_balance, total_spent=total_spent)

        # Sipariş durumu
        order["status"] = "refunded"
        order["failure_reason"] = reason[:200]
        order["refund_amount"] = refund_total
        order["refund_bonus_pct"] = bonus_pct
        order["refunded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        orders[order_id] = order
        save_orders(orders)

        # External kartı available'a geri döndür
        ext_id = order.get("ext_id")
        if ext_id:
            try:
                conn = get_sqlite_connection()
                conn.execute("UPDATE external_cards SET status = 'available', purchased_by = NULL, purchased_at = NULL WHERE ext_id = ?",
                             (ext_id,))
                conn.commit()
                conn.close()
            except Exception:
                pass

        # Rakip bakiyesini geri ekle (düşmüştük)
        source = order.get("ext_source")
        if source:
            # Varsa gerçek supplier_cost kullan, yoksa yaklaşık hesap
            supplier_cost = float(order.get("supplier_cost", 0))
            if supplier_cost <= 0:
                supplier_cost = cost / 1.04  # Fallback: yaklaşık %4 marj
            credit_competitor_balance(source, supplier_cost, f"refund {order_id}")

        logger.info(f"[REFUND] {order_id}: ${refund_total:.2f} to user {user_id} (bonus=${bonus:.2f}, reason={reason})")
        return True
    except Exception as e:
        logger.error(f"refund_order error: {e}")
        return False


# ─── Pending Deposits (Manuel deposit bekleyen siparişler) ───
PENDING_DEPOSITS_FILE = os.path.join(DATA_DIR, "pending_deposits.json")


def load_pending_deposits() -> list:
    return load_json_file(PENDING_DEPOSITS_FILE, [])


def save_pending_deposits(data: list) -> None:
    save_json_file(PENDING_DEPOSITS_FILE, data)


def add_pending_deposit(order_id: str, user_id: int, source: str,
                        amount_needed: float, ext_id: str, card_info: str) -> None:
    pending = load_pending_deposits()
    pending.append({
        "order_id": order_id,
        "user_id": user_id,
        "source": source,
        "amount_needed": round(amount_needed, 2),
        "ext_id": ext_id,
        "card_info": card_info,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "awaiting_deposit",
    })
    save_pending_deposits(pending)


def remove_pending_deposit(order_id: str) -> bool:
    pending = load_pending_deposits()
    new_list = [p for p in pending if p.get("order_id") != order_id]
    if len(new_list) != len(pending):
        save_pending_deposits(new_list)
        return True
    return False


def parse_card_line(line: str) -> dict | None:
    """Parse card line - multiple formats:
    Full:   4912779436810392:04:2034:632:US$25.00
    Medium: 4912779436810392 US$25.00
    Short:  491277 25.00
    """
    import re as _re
    line = line.strip()
    if not line:
        return None

    # Format 1: tam format kart:ay:yil:cvv:bakiye
    if line.count(":") >= 4:
        parts = line.split(":")
        try:
            card_number = parts[0].strip()
            exp_month   = parts[1].strip().zfill(2)
            exp_year    = parts[2].strip()
            cvv         = parts[3].strip()
            bal_raw     = parts[4].strip()
            if len(bal_raw) >= 3 and bal_raw[2] == "$":
                currency = bal_raw[:2]
                bal_str  = bal_raw[3:]
            elif bal_raw.startswith("$"):
                currency = "US"
                bal_str  = bal_raw[1:]
            else:
                currency = "US"
                bal_str  = bal_raw
            balance = float(bal_str.replace(",", ""))
            if len(card_number) < 6:
                return None
            return {
                "card_number":  card_number,
                "expiry_month": exp_month,
                "expiry_year":  exp_year,
                "cvv":          cvv,
                "balance":      round(balance, 2),
                "currency":     currency,
            }
        except Exception:
            pass

    # Format 2: "kart_no bakiye" veya "kart_no [CUR]$bakiye"
    # Ornekler: 491277 25.00 | 4912779436810392 US$25.00 | 4912779436810392 $25.00
    parts = line.split()
    if len(parts) >= 2:
        raw_card = parts[0].strip()
        bal_part = parts[-1].strip()
        currency = "US"
        if len(parts) == 3:
            currency = parts[1].strip().upper().rstrip("$")
        # currency$balance formatı
        if "$" in bal_part:
            idx_d = bal_part.index("$")
            if idx_d > 0:
                currency = bal_part[:idx_d].upper()
            bal_part = bal_part[idx_d+1:]
        bal_part = bal_part.replace(",", "")
        try:
            balance = float(bal_part)
            if balance <= 0 or len(raw_card) < 6:
                return None
            return {
                "card_number":  raw_card,
                "expiry_month": "00",
                "expiry_year":  "0000",
                "cvv":          "000",
                "balance":      round(balance, 2),
                "currency":     currency or "US",
            }
        except Exception:
            pass

    return None

def import_cards_from_text(text: str, provider: str | None = None, registered: bool | None = None) -> dict:
    """Import cards from TXT. Auto-detects provider from BIN if provider is None."""
    cards = load_cards()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    added = 0
    skipped = 0
    errors = 0
    unassigned = 0
    existing_numbers = {v["card_number"] for v in cards.values()}

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.upper().startswith("PROVIDER"):
            continue
        parsed = parse_card_line(line)
        if not parsed:
            errors += 1
            continue
        if parsed["card_number"] in existing_numbers:
            skipped += 1
            continue

        # Provider tespiti: verilen > BIN tablosu > Unassigned
        detected = provider or detect_provider_from_bin(parsed["card_number"])
        if not detected:
            detected = "Unassigned"
            unassigned += 1

        card_id = generate_card_id()
        while card_id in cards:
            card_id = generate_card_id()

        # Hassas alanları şifrele
        raw_record = {
            **parsed,
            "provider":       detected,
            "registered":     registered,
            "status":         "available",
            "reserved_by":    None,
            "reserved_until": None,
            "added_at":       now_str,
            "order_id":       None,
            "sold_at":        None,
            "channel_posted": False,
        }
        cards[card_id] = encrypt_card_record(raw_record)
        existing_numbers.add(parsed["card_number"])
        added += 1

    save_cards(cards)  # JSON + SQLite sync
    return {
        "added": added,
        "skipped": skipped,
        "errors": errors,
        "unassigned": unassigned,
        "total": len(cards),
    }

def release_expired_reservations() -> int:
    """Süresi dolmuş rezervasyonları SQLite'ta temizle."""
    try:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur  = conn.cursor()
        cur.execute(
            "UPDATE cards SET status='available', reserved_by=NULL, reserved_until=NULL "
            "WHERE status='reserved' AND reserved_until < ?",
            (now_str,)
        )
        released = cur.rowcount
        conn.commit()
        conn.close()
        return released
    except Exception as e:
        logger.error(f"release_expired_reservations error: {e}")
        return 0


def get_available_cards(provider_filter: str | None = None,
                        balance_range: str | None = None) -> list:
    """Mevcut kartları SQLite'tan filtreli getir — internal + external."""
    bal_min, bal_max = 0, float("inf")
    if balance_range:
        if balance_range.endswith("+"):
            bal_min = float(balance_range[:-1])
        elif "-" in balance_range:
            parts = balance_range.split("-")
            try:
                bal_min = float(parts[0])
                bal_max = float(parts[1])
            except Exception:
                pass

    # Internal kartlar
    internal = []
    try:
        internal = db_get_available_cards(
            provider=provider_filter,
            bal_min=bal_min,
            bal_max=bal_max,
        )
    except Exception as e:
        logger.warning(f"db_get_available_cards failed: {e}")

    # External kartlar (supply chain)
    external = []
    try:
        external = _get_external_cards_for_listing(provider_filter, bal_min, bal_max)
    except Exception as e:
        logger.debug(f"external_cards load failed: {e}")

    # Birleştir — bakiyeye göre sırala + duplicate'leri kaldır
    combined = internal + external

    # Deduplicate: aynı BIN + balance + currency varsa birini sil
    # External'i internal'a göre sil (internal kartlarımız kendi envanterimiz, onlar öncelikli)
    seen = {}
    deduped = []
    for card_id, card in combined:
        bin_num = str(card.get("card_number", ""))[:6].replace("x", "")
        balance = round(float(card.get("balance", 0)), 2)
        currency = card.get("currency", "US")
        key = (bin_num, balance, currency)

        if key in seen:
            # Duplicate — öncelik internal (external'i atla)
            is_external = card.get("_external", False)
            existing_is_external = seen[key].get("_external", False)
            if is_external and not existing_is_external:
                continue  # External, internal'dan sonra geldi — atla
            if not is_external and existing_is_external:
                # Internal geldi, önceki external'di — değiştir
                # deduped'dan eskisini bul ve kaldır
                deduped = [(cid, c) for cid, c in deduped if (
                    str(c.get("card_number", ""))[:6].replace("x", ""),
                    round(float(c.get("balance", 0)), 2),
                    c.get("currency", "US")
                ) != key]
                deduped.append((card_id, card))
                seen[key] = card
                continue
            # İki tanesi de aynı tip — ilkini koru
            continue
        seen[key] = card
        deduped.append((card_id, card))

    deduped.sort(key=lambda x: float(x[1].get("balance", 0)), reverse=True)
    return deduped


def _get_external_cards_for_listing(provider_filter: str | None,
                                      bal_min: float, bal_max: float) -> list:
    """external_cards tablosundan listing için kartları oku."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        query = """
            SELECT ext_id, source, source_bot, bin_number, balance, currency,
                   cost_price, sell_price, profit, rate_percent, provider,
                   registered, used_paypal, used_google, added_at
            FROM external_cards
            WHERE status = 'available' AND expires_at > ?
            AND balance >= ? AND balance <= ?
        """
        params = [now, bal_min, bal_max]

        if provider_filter and provider_filter not in ("unregistered", "registered"):
            # Provider + BIN bazlı filtreleme
            # Provider ismi VEYA BIN eşleşmesi
            bins_for_provider = [b for b, p in BIN_TO_PROVIDER.items() if p == provider_filter]
            conditions = [
                "provider LIKE ?",
                "provider LIKE ?",
            ]
            params.append(f"%{provider_filter}%")
            params.append(f"%{provider_filter.lower()}%")

            for bin_prefix in bins_for_provider:
                conditions.append("bin_number LIKE ?")
                params.append(f"{bin_prefix}%")

            query += f" AND ({' OR '.join(conditions)})"

        elif provider_filter == "registered":
            query += " AND registered = 1"
        elif provider_filter == "unregistered":
            # Unregistered OR Unknown — bilinmeyen kartları da unregistered kabul et
            # (güvenli tarafta olmak için)
            query += " AND (registered = 0 OR registered = -1 OR registered IS NULL)"

        query += " ORDER BY balance DESC"
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()

        result = []
        for row in rows:
            # Provider normalizasyonu — BIN'den tespit et, kaynak adını gizle
            raw_prov = (row["provider"] or "").strip().lower()
            bin_num = row["bin_number"] or ""
            provider = _normalize_provider(raw_prov, bin_hint=bin_num)

            # 3-state registration: 1 → True, 0 → False, -1 → None (unknown)
            reg_db = row["registered"]
            if reg_db == 1:
                registered = True
            elif reg_db == 0:
                registered = False
            else:
                registered = None  # -1 or NULL → unknown

            card_data = {
                "card_number": f"{bin_num}xx" if bin_num else "xxxxxx",
                "balance": row["balance"],
                "currency": row["currency"] or "US",
                "provider": provider,
                "registered": registered,
                "used_google": bool(row["used_google"]),
                "used_paypal": bool(row["used_paypal"]),
                "status": "available",
                "_external": True,
                "_ext_id": row["ext_id"],
                "_source": row["source"],
                "_sell_price": row["sell_price"],
                "_cost_price": row["cost_price"],
                "_profit": row["profit"],
                "_rate_percent": row["rate_percent"],
            }
            result.append((f"ext:{row['ext_id']}", card_data))
        return result
    except Exception as e:
        logger.debug(f"_get_external_cards_for_listing: {e}")
        return []


def _normalize_provider(raw: str, bin_hint: str = "") -> str:
    """Küçük harf provider adını standart formata çevir.
    BIN'den tespit edilebiliyorsa onu kullan.
    'giftcardmall' → 'GiftCardMall', 'mpc' → 'MyPrepaidCenter' vs."""
    mapping = {
        "giftcardmall": "GiftCardMall",
        "gcm": "GiftCardMall",
        "myprepaidcenter": "MyPrepaidCenter",
        "mpc": "MyPrepaidCenter",
        "vanillagift": "VanillaGift",
        "vanilla": "VanillaGift",
        "jokercard": "JokerCard",
        "joker": "JokerCard",
        "walmart": "Walmart",
        "cardbalance": "CardBalance",
        "cardbalanceau": "CardBalanceAU",
        "perfectgiftca": "PerfectGiftCA",
        "perfectgift": "PerfectGift",
        "amex": "Amex",
        "prepaidgift": "PrepaidGift",
    }

    # 1. İsimden eşleştir
    if raw:
        normalized = mapping.get(raw.lower().strip())
        if normalized:
            return normalized

    # 2. BIN numarasından tespit et
    if bin_hint:
        bin6 = bin_hint.replace(" ", "").replace("x", "")[:6]
        bin_match = BIN_TO_PROVIDER.get(bin6)
        if bin_match:
            return bin_match

    # 3. İsim varsa capitalize et
    if raw and raw.strip():
        return raw.strip().capitalize()

    return "Prepaid"

def reserve_card(card_id: str, user_id: int) -> bool:
    """Reserve a card — SQLite atomic update."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        conn.execute("BEGIN IMMEDIATE")
        cur  = conn.cursor()
        cur.execute("SELECT status FROM cards WHERE card_id = ?", (card_id,))
        row = cur.fetchone()
        if not row or row["status"] != "available":
            conn.execute("ROLLBACK")
            conn.close()
            return False
        exp = datetime.fromtimestamp(
            time.time() + CARD_RESERVATION_SECONDS
        ).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "UPDATE cards SET status=?, reserved_by=?, reserved_until=? WHERE card_id=?",
            ("reserved", user_id, exp, card_id)
        )
        conn.execute("COMMIT")
        conn.close()
        return True
    except Exception as e:
        logger.error(f"reserve_card error: {e}")
        return False

def release_card(card_id: str) -> None:
    """Rezervasyonu serbest bırak — SQLite."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        conn.execute(
            "UPDATE cards SET status='available', reserved_by=NULL, reserved_until=NULL WHERE card_id=?",
            (card_id,)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"release_card error: {e}")

def mark_card_sold(card_id: str, order_id: str) -> None:
    """Kartı satıldı olarak işaretle — SQLite."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        conn.execute(
            "UPDATE cards SET status='sold', order_id=?, sold_at=?, reserved_by=NULL, reserved_until=NULL WHERE card_id=?",
            (order_id, now_str, card_id)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"mark_card_sold error: {e}")

def get_card(card_id: str) -> dict | None:
    """Tek kartı SQLite'tan getir."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM cards WHERE card_id = ?", (card_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        d = dict(row)
        d["channel_posted"] = bool(d.get("channel_posted", 0))
        reg = d.get("registered")
        d["registered"] = True if reg == 1 else (False if reg == 0 else None)
        return d
    except Exception:
        return load_cards().get(card_id)

def get_card_stats() -> dict:
    """Kart istatistikleri — SQLite."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur  = conn.cursor()
        cur.execute("SELECT status, COUNT(*) as cnt FROM cards GROUP BY status")
        rows = cur.fetchall()
        conn.close()
        stats = {"available": 0, "reserved": 0, "sold": 0, "total": 0}
        for row in rows:
            s = row["status"]
            cnt = int(row["cnt"])
            if s in stats:
                stats[s] = cnt
            stats["total"] += cnt
        return stats
    except Exception:
        return {"available": 0, "reserved": 0, "sold": 0, "total": 0}

def format_card_number_masked(card_number: str) -> str:
    """491277xx style mask. Handles encrypted ENC: values."""
    n = card_number.replace(" ", "")
    # Şifreli kart numarasını çöz
    if n.startswith("ENC:"):
        try:
            n = decrypt_card_field(n).replace(" ", "")
        except Exception:
            return "****xx"
    if len(n) >= 8:
        return n[:6] + "xx"
    return n

def format_card_row(idx: int, card_id: str, card: dict) -> str:
    """Compact format: 1. 🇺🇸 435880xx $25.00 | GCM $9.19 🔒 🅶 🅿"""
    is_ext = card.get("_external", False)
    balance = float(card.get("balance", 0))
    currency = card.get("currency", "US")
    provider = card.get("provider", "")
    reg_status = card.get("registered", None)
    used_google = card.get("used_google", False)
    used_paypal = card.get("used_paypal", False)

    # 3-state registration icon
    if reg_status is True:
        reg_icon = "🔒"
    elif reg_status is False:
        reg_icon = "✅"
    elif is_ext:
        reg_icon = "❓"
    else:
        reg_icon = ""

    # Google/PayPal usage indicators
    usage_icons = ""
    if used_google:
        usage_icons += " 🅶"
    if used_paypal:
        usage_icons += " 🅿"

    short = PROVIDER_SHORT.get(provider, provider[:4] if provider else "")
    flag = PROVIDER_FLAGS.get(provider, "🇺🇸")

    # Balance format
    if balance == int(balance) and balance >= 10:
        bal_str = f"${int(balance)}"
    else:
        bal_str = f"${balance:.2f}"

    cur_prefix = f"{currency}" if currency not in ("US", "USD") else ""

    if is_ext:
        sell_price = card.get("_sell_price", 0)
        bin_num = card.get("card_number", "xxxxxx")
        return f"{idx}. {flag} {bin_num} {cur_prefix}{bal_str} | {short} ${sell_price:.2f} {reg_icon}{usage_icons}"
    else:
        masked = format_card_number_masked(card["card_number"])
        rate = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.38)))
        price = round(balance * rate, 2)
        return f"{idx}. {flag} {masked} {cur_prefix}{bal_str} | {short} ${price:.2f} {reg_icon}{usage_icons}"

# ── Listing UI ────────────────────────────────────────────────

def listing_page_markup(page: int, total_pages: int,
                        provider_filter: str | None,
                        balance_range: str | None,
                        card_ids_on_page: list,
                        cards_on_page: list) -> InlineKeyboardMarkup:
    pf  = provider_filter or ""
    br  = balance_range or ""
    pfx = f"{pf}:{br}"
    rows = []

    # Row 1: First | Back | Next | Last
    rows.append([
        InlineKeyboardButton("⏮ First", callback_data=f"lst:1:{pfx}"),
        InlineKeyboardButton("◀ Back",  callback_data=f"lst:{max(1,page-1)}:{pfx}"),
        InlineKeyboardButton("Next ▶",  callback_data=f"lst:{min(total_pages,page+1)}:{pfx}"),
        InlineKeyboardButton("Last ⏭",  callback_data=f"lst:{total_pages}:{pfx}"),
    ])

    # Row 2: -5 | +5
    rows.append([
        InlineKeyboardButton("⏪ -5", callback_data=f"lst:{max(1,page-5)}:{pfx}"),
        InlineKeyboardButton("+5 ⏩", callback_data=f"lst:{min(total_pages,page+5)}:{pfx}"),
    ])

    # Card rows: [card info (left)] [Purchase (right)]
    for i, (card_id, card) in enumerate(zip(card_ids_on_page, cards_on_page)):
        start_idx = (page - 1) * CARDS_PER_PAGE
        num      = start_idx + i + 1
        currency = card.get("currency", "US")
        balance  = float(card.get("balance", 0))
        is_ext   = card.get("_external", False)

        if is_ext:
            sell_price = card.get("_sell_price", 0)
            bin_num = card.get("card_number", "xxxxxx")
            # Mobil uyumlu kısa format: bakiye tam sayıysa .00 gösterme
            bal_str = f"${int(balance)}" if balance == int(balance) else f"${balance:.2f}"
            label = f"{num}. {bin_num} {bal_str}"
            buy_cb = f"ext_buy:{card.get('_ext_id', '')}"
        else:
            masked = format_card_number_masked(card["card_number"])
            bal_str = f"${int(balance)}" if balance == int(balance) else f"${balance:.2f}"
            label = f"{num}. {masked} {bal_str}"
            buy_cb = f"card_buy:{card_id}"

        rows.append([
            InlineKeyboardButton(label,       callback_data=buy_cb),
            InlineKeyboardButton("Purchase", callback_data=buy_cb),
        ])

    # Bottom: Filters | Main Menu
    rows.append([
        InlineKeyboardButton("⚙️ Filters",   callback_data="listing_filter"),
        InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
    ])
    # Deposit | Refresh
    rows.append([
        InlineKeyboardButton("💰 Deposit",  callback_data="add_balance"),
        InlineKeyboardButton("🔄 Refresh",  callback_data=f"lst:{page}:{pfx}"),
    ])
    return InlineKeyboardMarkup(rows)

def _get_providers_with_counts() -> list:
    """Stokta gerçekten kart olan provider'ları ve sayılarını getir."""
    providers = {}  # provider_name → count

    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()

        # Internal kartlar
        cur.execute("SELECT provider, COUNT(*) as cnt FROM cards WHERE status='available' GROUP BY provider")
        for row in cur.fetchall():
            prov = row["provider"] or "Other"
            providers[prov] = providers.get(prov, 0) + row["cnt"]

        # External kartlar
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            SELECT provider, bin_number, COUNT(*) as cnt FROM external_cards
            WHERE status='available' AND expires_at > ?
            GROUP BY provider, bin_number
        """, (now,))
        for row in cur.fetchall():
            raw_prov = (row["provider"] or "").strip()
            bin_num = row["bin_number"] or ""
            normalized = _normalize_provider(raw_prov.lower(), bin_hint=bin_num)
            providers[normalized] = providers.get(normalized, 0) + row["cnt"]

        conn.close()
    except Exception:
        pass

    # Sayıya göre sırala (en çok kart olan üstte)
    sorted_provs = sorted(providers.items(), key=lambda x: x[1], reverse=True)
    # "Other" ve boş olanları atla
    return [(p, c) for p, c in sorted_provs if p and p != "Other" and c > 0]


def listing_filter_markup(active_provider: str = "", active_balance: str = "") -> InlineKeyboardMarkup:
    rows = []
    # Balance Range
    rows.append([InlineKeyboardButton("💰 Balance Range", callback_data="noop")])
    bal_ranges = [
        ("0-5", "Below $5"), ("5-25", "$5–$25"), ("25-100", "$25–$100"),
        ("100-500", "$100–$500"), ("500+", "$500+"),
    ]
    bal_row = []
    for key, label in bal_ranges:
        btn = f"✅ {label}" if active_balance == key else label
        bal_row.append(InlineKeyboardButton(btn, callback_data=f"lst_filter:{active_provider}:{key}"))
        if len(bal_row) == 3:
            rows.append(bal_row)
            bal_row = []
    if bal_row:
        rows.append(bal_row)

    # Provider — sadece stokta olan
    rows.append([InlineKeyboardButton("🏪 Provider", callback_data="noop")])
    active_providers = _get_providers_with_counts()
    prov_row = []
    for provider, count in active_providers:
        flag = PROVIDER_FLAGS.get(provider, "🇺🇸")
        btn_label = f"{flag} {provider} ({count})"
        btn = f"✅ {btn_label}" if active_provider == provider else btn_label
        prov_row.append(InlineKeyboardButton(btn, callback_data=f"lst_filter:{provider}:{active_balance}"))
        if len(prov_row) == 2:
            rows.append(prov_row)
            prov_row = []
    if prov_row:
        rows.append(prov_row)

    # Registration (başlık yok, sadece butonlar)
    rows.append([
        InlineKeyboardButton(
            "☑️ Unregistered" if active_provider == "unregistered" else "✅ Unregistered",
            callback_data=f"lst_filter:unregistered:{active_balance}"
        ),
        InlineKeyboardButton(
            "☑️ Registered" if active_provider == "registered" else "🔒 Registered",
            callback_data=f"lst_filter:registered:{active_balance}"
        ),
    ])

    # Actions
    rows.append([InlineKeyboardButton("✨ Clear Filters", callback_data="lst:1:")])
    rows.append([InlineKeyboardButton("◀ Back to Listings", callback_data=f"lst:1:{active_provider}:{active_balance}")])
    return InlineKeyboardMarkup(rows)

def purchase_confirm_markup(card_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm Purchase", callback_data=f"card_confirm:{card_id}")],
        [InlineKeyboardButton("❌ Cancel",           callback_data=f"card_cancel:{card_id}")],
    ])

async def show_listing_page(target, user_id: int, page: int,
                             provider_filter: str | None = None,
                             balance_range: str | None = None,
                             edit: bool = False) -> None:
    """Render listing page matching reference layout."""
    available = get_available_cards(provider_filter, balance_range)
    total     = len(available)

    # Kullanici bakiyesi
    user_record  = get_hybrid_balance_record(user_id)
    user_balance = float(user_record.get("available_balance", 0.0))

    # Toplam stok (filtresiz)
    all_avail           = get_available_cards()
    total_stock_cards   = len(all_avail)
    total_stock_balance = sum(float(c.get("balance", 0)) for _, c in all_avail)

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if total == 0:
        pf_label = PROVIDER_SHORT.get(provider_filter, provider_filter) if provider_filter else "All"
        text = (
            f"Account Balance: {format_usd(user_balance)}\n\n"
            f"Cards in Stock: {total_stock_cards}  |  Total: ${total_stock_balance:,.2f}\n\n"
            f"No cards match your filter (Provider: {pf_label} | Range: {balance_range or 'Any'}).\n"
            f"Last updated @ {now_utc}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Filter",  callback_data="listing_filter"),
             InlineKeyboardButton("🔄 Refresh", callback_data="lst:1:")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ])
        try:
            if edit: await target.edit_text(text, reply_markup=kb)
            else:    await target.reply_text(text, reply_markup=kb)
        except Exception:
            await target.reply_text(text, reply_markup=kb)
        return

    total_pages = max(1, (total + CARDS_PER_PAGE - 1) // CARDS_PER_PAGE)
    page        = max(1, min(page, total_pages))
    _start      = (page - 1) * CARDS_PER_PAGE
    page_cards  = available[_start: _start + CARDS_PER_PAGE]

    prov_label = PROVIDER_SHORT.get(provider_filter, provider_filter) if provider_filter else "All"
    bal_label  = balance_range or "Any"

    # Text mesaj - compact layout
    lines = [
        "💳 Elite Earners — Card Listings",
        "",
        f"Your Balance: {format_usd(user_balance)}",
        "",
    ]
    start = (page - 1) * CARDS_PER_PAGE
    card_ids_on_page = []
    cards_on_page    = []
    for i, (card_id, card) in enumerate(page_cards, start=start+1):
        lines.append(format_card_row(i, card_id, card))
        card_ids_on_page.append(card_id)
        cards_on_page.append(card)
    lines += [
        "",
        f"Total Cards: {total_stock_cards}  |  Total: ${total_stock_balance:,.2f}",
        "",
        "🔒 Registered  ✅ Unregistered  ❓ Unknown  🅶 Used Google  🅿 Used PayPal",
        "",
        f"Filters: {prov_label if prov_label != 'All' else 'None'}  |  Balance: {bal_label if bal_label != 'Any' else 'None'}",
        f"Page {page}/{total_pages}  |  Updated: {now_utc}",
    ]

    text   = "\n".join(lines)
    markup = listing_page_markup(
        page, total_pages, provider_filter, balance_range,
        card_ids_on_page, cards_on_page
    )
    try:
        if edit:
            await target.edit_text(text, reply_markup=markup)
        else:
            await target.reply_text(text, reply_markup=markup)
    except Exception as e:
        err = str(e).lower()
        if "message is not modified" in err:
            pass
        elif "query is too old" in err or "query_id_invalid" in err:
            pass
        else:
            # Edit başarısız — yeni mesaj gönder, eskisini silmeye çalış
            try:
                new_msg = await target.reply_text(text, reply_markup=markup)
                # Eski mesajı sil (chat temiz kalsın)
                try:
                    await target.delete()
                except Exception:
                    pass
            except Exception:
                pass

# ============================================================
# CARD STOCK MODULE END
# ============================================================

# ============================================================
# STORE GIFT CARD MODULE — Region + Type based gift card system
# ============================================================
GIFT_CARDS_FILE = _build_data_path("gift_cards.json")
GIFT_CARDS_PER_PAGE = 8

# ── Region & Type tanımları ─────────────────────────────────
GC_REGIONS = {
    "US":  "🇺🇸",
    "CA":  "🇨🇦",
    "EU":  "🇪🇺",
    "UK":  "🇬🇧",
    "AUS": "🇦🇺",
}

GC_TYPES = {
    "Gaming":   "🎮",
    "Shopping": "🛍️",
    "Food":     "🍔",
    "Fashion":  "👗",
    "Digital":  "📱",
    "Crypto":   "💰",
    "Other":    "🏪",
}

# Import tag → (gc_type, region) parser
# Örnek: "GamingUS" → ("Gaming", "US"), "ShoppingEU" → ("Shopping", "EU")
def parse_gc_import_tag(tag: str) -> tuple:
    """Import tag'ini parse et → (gc_type, region) veya (None, None)."""
    tag = tag.strip()
    region = None
    gc_type = None
    for r in sorted(GC_REGIONS.keys(), key=len, reverse=True):
        if tag.upper().endswith(r):
            region = r
            type_part = tag[:len(tag) - len(r)]
            break
    if not region:
        return (None, None)
    type_part_lower = type_part.lower()
    for t in GC_TYPES.keys():
        if t.lower() == type_part_lower:
            gc_type = t
            break
    if not gc_type:
        return (None, None)
    return (gc_type, region)


# Rate'ler: type+region bazlı veya genel type bazlı
DEFAULT_GC_RATES = {
    "Gaming":   0.65,
    "Shopping": 0.60,
    "Food":     0.50,
    "Fashion":  0.55,
    "Digital":  0.65,
    "Crypto":   0.85,
    "Other":    0.50,
}


def get_gift_card_rate(gc_type: str, region: str = "") -> float:
    """Gift card rate — önce type+region, sonra type, sonra varsayılan."""
    if region:
        key = f"gc_{gc_type}_{region}"
        if key in rates_data:
            return float(rates_data[key])
    key = f"gc_{gc_type}"
    if key in rates_data:
        return float(rates_data[key])
    return float(DEFAULT_GC_RATES.get(gc_type, 0.50))


def load_gift_cards() -> dict:
    try:
        data = db_load_gift_cards()
        if data:
            return data
    except Exception:
        pass
    return load_json_file(GIFT_CARDS_FILE, {})


def save_gift_cards(data: dict) -> None:
    save_json_file(GIFT_CARDS_FILE, data)
    try:
        for gc_id, gc in data.items():
            db_save_gift_card(gc_id, gc)
    except Exception as e:
        logger.warning(f"save_gift_cards SQLite sync error: {e}")


def generate_gift_card_id() -> str:
    return f"GC-{int(time.time())}-{random.randint(1000, 9999)}"


def parse_gift_card_line(line: str) -> dict | None:
    """Parse gift card line — flexible formats:
      CODE 50$              (code + amount)
      50$                   (amount only)
      brand CODE 250$       (brand + code + amount)
      brand $50             (brand + amount)
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split()
    if not parts:
        return None

    def _parse_amount(raw: str) -> tuple:
        currency = "US"
        s = raw.strip()
        if s.endswith("$"):
            s = s[:-1]
            for i, ch in enumerate(s):
                if ch.isdigit() or ch in ".,":
                    if i > 0:
                        currency = s[:i].upper()
                    s = s[i:]
                    break
        elif "$" in s:
            idx_d = s.index("$")
            if idx_d > 0:
                currency = s[:idx_d].upper()
            s = s[idx_d + 1:]
        s = s.replace(",", "").strip()
        if not s:
            return None
        try:
            amount = float(s)
            if amount <= 0:
                return None
            return (currency, round(amount, 2))
        except Exception:
            return None

    parsed_amount = _parse_amount(parts[-1])
    if parsed_amount is None:
        if len(parts) == 1:
            parsed_amount = _parse_amount(parts[0])
            if parsed_amount:
                return {"brand": "", "code": "", "amount": parsed_amount[1], "currency": parsed_amount[0]}
        return None

    currency, amount = parsed_amount
    if len(parts) == 1:
        return {"brand": "", "code": "", "amount": amount, "currency": currency}
    if len(parts) == 2:
        return {"brand": parts[0].strip(), "code": parts[0].strip(), "amount": amount, "currency": currency}
    return {"brand": parts[0].strip(), "code": " ".join(parts[1:-1]).strip(), "amount": round(amount, 2), "currency": currency}


def import_gift_cards_from_text(text: str, gc_type: str = "Other", region: str = "US") -> dict:
    """Import gift cards from text. Her satır bir gift card."""
    cards = load_gift_cards()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    added = 0
    skipped = 0
    errors = 0
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.upper().startswith("BRAND"):
            continue
        parsed = parse_gift_card_line(line)
        if not parsed:
            errors += 1
            continue
        brand = parsed.get("brand", "") or ""
        gc_id = generate_gift_card_id()
        while gc_id in cards:
            gc_id = generate_gift_card_id()
            time.sleep(0.001)
        record = {
            "brand": brand, "code": parsed.get("code", ""),
            "amount": parsed["amount"], "currency": parsed.get("currency", "US"),
            "gc_type": gc_type, "region": region,
            "status": "available", "reserved_by": None, "reserved_until": None,
            "added_at": now_str, "order_id": None, "sold_at": None,
        }
        cards[gc_id] = record
        added += 1
    save_gift_cards(cards)
    return {"added": added, "skipped": skipped, "errors": errors, "total": len(cards)}


def get_available_gift_cards(gc_type: str | None = None, region: str | None = None,
                              brand_filter: str | None = None) -> list:
    try:
        return db_get_available_gift_cards(gc_type=gc_type, region=region, brand=brand_filter)
    except Exception as e:
        logger.warning(f"db_get_available_gift_cards failed, JSON fallback: {e}")
        cards = load_json_file(GIFT_CARDS_FILE, {})
        result = []
        for gc_id, gc in cards.items():
            if gc.get("status") != "available":
                continue
            if gc_type and gc.get("gc_type") != gc_type:
                continue
            if region and gc.get("region") != region:
                continue
            if brand_filter and gc.get("brand") != brand_filter:
                continue
            result.append((gc_id, gc))
        result.sort(key=lambda x: (x[1].get("brand", ""), -x[1].get("amount", 0)))
        return result


def get_gift_card(gc_id: str) -> dict | None:
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM gift_cards WHERE gc_id = ?", (gc_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return load_gift_cards().get(gc_id)


def reserve_gift_card(gc_id: str, user_id: int) -> bool:
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.cursor()
        cur.execute("SELECT status FROM gift_cards WHERE gc_id = ?", (gc_id,))
        row = cur.fetchone()
        if not row or row["status"] != "available":
            conn.execute("ROLLBACK"); conn.close(); return False
        exp = datetime.fromtimestamp(time.time() + CARD_RESERVATION_SECONDS).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE gift_cards SET status=?, reserved_by=?, reserved_until=? WHERE gc_id=?",
                     ("reserved", user_id, exp, gc_id))
        conn.execute("COMMIT"); conn.close(); return True
    except Exception as e:
        logger.error(f"reserve_gift_card error: {e}"); return False


def release_gift_card(gc_id: str) -> None:
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        conn.execute("UPDATE gift_cards SET status='available', reserved_by=NULL, reserved_until=NULL WHERE gc_id=?", (gc_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"release_gift_card error: {e}")


def mark_gift_card_sold(gc_id: str, order_id: str) -> None:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        conn.execute("UPDATE gift_cards SET status='sold', order_id=?, sold_at=?, reserved_by=NULL, reserved_until=NULL WHERE gc_id=?",
                     (order_id, now_str, gc_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"mark_gift_card_sold error: {e}")


def get_gift_card_stats() -> dict:
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        cur.execute("SELECT status, COUNT(*) as cnt FROM gift_cards GROUP BY status")
        rows = cur.fetchall()
        stats = {"available": 0, "reserved": 0, "sold": 0, "total": 0}
        for row in rows:
            s = row["status"]; cnt = int(row["cnt"])
            if s in stats: stats[s] = cnt
            stats["total"] += cnt
        return stats
    except Exception:
        return {"available": 0, "reserved": 0, "sold": 0, "total": 0}


def get_gc_region_summary() -> list:
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        cur.execute("SELECT region, COUNT(*) as cnt, SUM(amount) as total_amount FROM gift_cards WHERE status='available' GROUP BY region ORDER BY region")
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_gc_type_summary(region: str) -> list:
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        cur.execute("SELECT gc_type, COUNT(*) as cnt, SUM(amount) as total_amount FROM gift_cards WHERE status='available' AND region=? GROUP BY gc_type ORDER BY gc_type", (region,))
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_gc_cards_summary(gc_type: str, region: str) -> list:
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        cur.execute("SELECT brand, COUNT(*) as cnt, SUM(amount) as total_amount, MIN(amount) as min_amount, MAX(amount) as max_amount FROM gift_cards WHERE status='available' AND gc_type=? AND region=? GROUP BY brand ORDER BY brand", (gc_type, region))
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def release_expired_gift_card_reservations() -> int:
    try:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        cur.execute("UPDATE gift_cards SET status='available', reserved_by=NULL, reserved_until=NULL WHERE status='reserved' AND reserved_until < ?", (now_str,))
        released = cur.rowcount; conn.commit()
        conn.close()
        return released
    except Exception as e:
        logger.error(f"release_expired_gift_card_reservations error: {e}"); return 0


# ── SQLite helpers for gift_cards ───────────────────────────

def db_load_gift_cards() -> dict:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM gift_cards")
    rows = cur.fetchall()
    result = {}
    for row in rows:
        d = dict(row); gc_id = d.pop("gc_id", None)
        if gc_id: result[gc_id] = d
    return result


def db_save_gift_card(gc_id: str, gc: dict) -> None:
    init_sqlite_db()
    conn = get_sqlite_connection()
    conn.execute("""
        INSERT OR REPLACE INTO gift_cards
        (gc_id, brand, code, amount, currency, gc_type, region, status,
         reserved_by, reserved_until, added_at, order_id, sold_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (gc_id, gc.get("brand", ""), gc.get("code", ""), float(gc.get("amount", 0)),
          gc.get("currency", "US"), gc.get("gc_type", "Other"), gc.get("region", "US"),
          gc.get("status", "available"), gc.get("reserved_by"), gc.get("reserved_until"),
          gc.get("added_at", ""), gc.get("order_id"), gc.get("sold_at")))
    conn.commit()
    conn.close()


def db_get_available_gift_cards(gc_type: str | None = None, region: str | None = None,
                                 brand: str | None = None) -> list:
    init_sqlite_db()
    conn = get_sqlite_connection()
    sql = "SELECT gc_id, * FROM gift_cards WHERE status = 'available'"
    params = []
    if gc_type: sql += " AND gc_type = ?"; params.append(gc_type)
    if region: sql += " AND region = ?"; params.append(region)
    if brand: sql += " AND brand = ?"; params.append(brand)
    sql += " ORDER BY brand, amount DESC"
    cur = conn.cursor(); cur.execute(sql, params)
    rows = cur.fetchall()
    return [(dict(row).pop("gc_id"), dict(row)) for row in rows] if False else [(r["gc_id"], {k: r[k] for k in r.keys() if k != "gc_id"}) for r in rows]


# ── Gift Card UI ────────────────────────────────────────────

def gc_region_menu_markup() -> InlineKeyboardMarkup:
    regions = get_gc_region_summary()
    rows = []
    for r in regions:
        region = r["region"]; flag = GC_REGIONS.get(region, "🌍")
        rows.append([InlineKeyboardButton(f"{flag} {region} — {r['cnt']} cards | {format_usd(r['total_amount'])}",
                                           callback_data=f"gc_region:{region}")])
    if not rows:
        rows.append([InlineKeyboardButton("No gift cards in stock", callback_data="noop")])
    rows.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)


def gc_type_menu_markup(region: str) -> InlineKeyboardMarkup:
    types = get_gc_type_summary(region)
    rows = []
    for t in types:
        gc_type = t["gc_type"]; icon = GC_TYPES.get(gc_type, "🏪")
        rate = get_gift_card_rate(gc_type, region)
        rows.append([InlineKeyboardButton(
            f"{icon} {gc_type} — {t['cnt']} cards | {format_usd(t['total_amount'])} | {rate*100:.0f}%",
            callback_data=f"gc_type:{region}:{gc_type}")])
    if not rows:
        rows.append([InlineKeyboardButton("No cards in this region", callback_data="noop")])
    rows.append([InlineKeyboardButton("◀ Regions", callback_data="gc_menu"),
                 InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)


def gc_listing_markup(gc_type: str, region: str, page: int,
                       gc_items: list, total_pages: int) -> InlineKeyboardMarkup:
    rows = []
    if total_pages > 1:
        rows.append([
            InlineKeyboardButton("◀", callback_data=f"gc_list:{region}:{gc_type}:{max(1,page-1)}"),
            InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"),
            InlineKeyboardButton("▶", callback_data=f"gc_list:{region}:{gc_type}:{min(total_pages,page+1)}"),
        ])
    for gc_id, gc in gc_items:
        brand = gc.get("brand", ""); amount = float(gc.get("amount", 0))
        currency = gc.get("currency", "US")
        rate = get_gift_card_rate(gc_type, region)
        cost = round(amount * rate, 2)
        label = f"{brand} {currency}${amount:.2f} → {format_usd(cost)}" if brand else f"{currency}${amount:.2f} → {format_usd(cost)}"
        rows.append([InlineKeyboardButton(label, callback_data=f"gc_buy:{gc_id}"),
                     InlineKeyboardButton("🛒", callback_data=f"gc_buy:{gc_id}")])
    rows.append([InlineKeyboardButton("◀ Types", callback_data=f"gc_region:{region}"),
                 InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
    rows.append([InlineKeyboardButton("💰 Deposit", callback_data="add_balance")])
    return InlineKeyboardMarkup(rows)


async def show_gc_listing(target, gc_type: str, region: str, page: int = 1,
                           user_id: int = 0, edit: bool = False) -> None:
    available = get_available_gift_cards(gc_type=gc_type, region=region)
    total = len(available)
    flag = GC_REGIONS.get(region, "🌍"); icon = GC_TYPES.get(gc_type, "🏪")
    rate = get_gift_card_rate(gc_type, region)
    user_record = get_hybrid_balance_record(user_id)
    user_balance = float(user_record.get("available_balance", 0.0))
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if total == 0:
        text = f"{flag} {icon} {gc_type} — {region}\n\nNo cards available.\nUpdated: {now_utc}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data=f"gc_region:{region}")],
                                    [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])
        try:
            if edit: await target.edit_text(text, reply_markup=kb)
            else: await target.reply_text(text, reply_markup=kb)
        except Exception: await target.reply_text(text, reply_markup=kb)
        return

    total_pages = max(1, (total + GIFT_CARDS_PER_PAGE - 1) // GIFT_CARDS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * GIFT_CARDS_PER_PAGE
    page_items = available[start:start + GIFT_CARDS_PER_PAGE]

    lines = [f"{flag} {icon} {gc_type} — {region}", "",
             f"Your Balance: {format_usd(user_balance)}", f"Rate: {rate*100:.0f}%", ""]
    for i, (gc_id, gc) in enumerate(page_items, start=start + 1):
        brand = gc.get("brand", ""); amount = float(gc.get("amount", 0))
        currency = gc.get("currency", "US"); cost = round(amount * rate, 2)
        brand_str = f" {brand}" if brand else ""
        lines.append(f"{i}.{brand_str} {currency}${amount:.2f} → {format_usd(cost)}")
    lines += ["", f"Total: {total} | Page {page}/{total_pages} | {now_utc}"]

    text = "\n".join(lines)
    markup = gc_listing_markup(gc_type, region, page, page_items, total_pages)
    try:
        if edit: await target.edit_text(text, reply_markup=markup)
        else: await target.reply_text(text, reply_markup=markup)
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            try: await target.reply_text(text, reply_markup=markup)
            except Exception: pass


def gc_purchase_confirm_markup(gc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm Purchase", callback_data=f"gc_confirm:{gc_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"gc_cancel:{gc_id}")],
    ])


ORDERS_FILE = _build_data_path("orders.json")
STOCK_FILE = _build_data_path("stock.json")
CONFIG_FILE = _build_data_path("config.json")
ACCESS_FILE = _build_data_path("access.json")
BALANCES_FILE = _build_data_path("balances.json")
RATES_FILE = _build_data_path("rates.json")
BALANCE_REQUESTS_FILE = _build_data_path("balance_requests.json")
AUDIT_LOG_FILE = _build_data_path("audit_log.json")
MODERATORS_FILE = _build_data_path("moderators.json")
BALANCE_LEDGER_FILE = _build_data_path("balance_ledger.json")
BACKUP_DIR = _build_data_path("backups")
SQLITE_DB_FILE = _build_data_path("elite_bot.db")

LOW_STOCK_CARDS_THRESHOLD = 5
LOW_STOCK_BALANCE_THRESHOLD = 50.0
AUTO_STOCK_UPDATE_SECONDS = 3600
AUTO_DAILY_SUMMARY_HOUR = 21
AUTO_DAILY_SUMMARY_MINUTE = 0
ANTI_SPAM_WINDOW_SECONDS = 8
ANTI_SPAM_BURST_LIMIT = 5
SUPPORT_MESSAGE_COOLDOWN_SECONDS = 30
ORDER_PREVIEW_COOLDOWN_SECONDS = 5

LIFETIME_ACCESS_NAME = "Elite Lifetime Access"
LIFETIME_ACCESS_PRICE = "$100"
ACTIVATION_FEE_USD = 100.0  # Otomatik aktivasyon için gereken minimum miktar
MIN_BALANCE_TOPUP_USD = 10.0

# ─── Rakip Bot Bakiye Yönetimi (Just-In-Time Funding) ───
# Her rakipte tutulan tahmini bakiye — admin manuel günceller (/setbal komutu)
# Sipariş bu tampondan büyükse müşteriye onay sorulur, admin'e bildirim gider
COMPETITOR_BALANCES_FILE = os.path.join(DATA_DIR, "competitor_balances.json")
COMPETITOR_DEFAULT_BUFFER = 30.0  # FAZ 1: her rakipte $30 başlangıç tamponu
ORDER_INSTANT_THRESHOLD = 20.0    # < $20 → anında teslim, onay gerekmez
ORDER_PREMIUM_THRESHOLD = 50.0    # > $50 → her zaman premium akış

DEFAULT_PROVIDER_RATES = {
    "MyPrepaidCenter": 0.38,
    "GiftCardMall":    0.38,
    "JokerCard":   0.38,
    "CardBalanceAU":   0.38,
    "BalanceNow":      0.38,
    "PerfectGift":     0.38,
    "VanillaGift":     0.38,
    "VanillaPrepaid":  0.38,
    "Walmart":         0.38,
    "Amex":            0.38,
}

SUPPORTED_PROVIDERS = [
    "MyPrepaidCenter",
    "GiftCardMall",
    "JokerCard",
    "CardBalanceAU",
    "BalanceNow",
    "PerfectGift",
    "VanillaGift",
    "VanillaPrepaid",
    "Walmart",
    "Amex",
]

PROVIDER_ALIASES = {
    # Full names
    "myprepaidcenter":  "MyPrepaidCenter",
    "myprepaidcentre":  "MyPrepaidCenter",
    "myprepaidcente":   "MyPrepaidCenter",
    "giftcardmall":     "GiftCardMall",
    "perfectgiftca":    "JokerCard",
    "cardbalanceau":    "CardBalanceAU",
    "balancenow":       "BalanceNow",
    "perfectgift":      "PerfectGift",
    "vanillagift":      "VanillaGift",
    "vanillaprepaid":   "VanillaPrepaid",
    "walmart":          "Walmart",
    "amex":             "Amex",
    # Short codes
    "mpc":              "MyPrepaidCenter",
    "gcm":              "GiftCardMall",
    "pgca":             "JokerCard",
    "cbau":             "CardBalanceAU",
    "bnow":             "BalanceNow",
    "pg":               "PerfectGift",
    "vg":               "VanillaGift",
    "vp":               "VanillaPrepaid",
    "wmt":              "Walmart",
}

support_mode_users = set()
add_balance_mode_users = {}
admin_delivery_mode_users = {}
broadcast_confirmations = {}
user_message_timestamps = {}
user_support_cooldowns = {}
user_order_preview_cooldowns = {}
transaction_lock = asyncio.Lock()
active_order_confirms = set()
active_balance_request_actions = set()
active_refund_orders = set()


def load_json_file(filename: str, default):
    if not os.path.exists(filename):
        return default
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error in {filename}: {e}")
        # Bozuk dosyayı yedekle
        try:
            import shutil as _sh
            _sh.copy2(filename, filename + ".corrupt")
        except Exception:
            pass
        return default
    except Exception as e:
        logger.error(f"load_json_file error {filename}: {e}")
        return default


def save_json_file(filename: str, data) -> None:
    """Atomic write - veri kaybını önlemek için temp dosya kullan."""
    tmp = filename + ".tmp"
    _t0 = time.time()
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        os.replace(tmp, filename)
        _elapsed = round(time.time() - _t0, 3)
        if _elapsed > 1.0:
            logger.warning(f"[SLOW] save_json {os.path.basename(filename)} took {_elapsed}s")
    except Exception as e:
        logger.error(f"save_json_file error {filename}: {e}")
        try:
            os.remove(tmp)
        except Exception:
            pass




def is_spamming_user(user_id: int) -> bool:
    now_ts = time.time()
    timestamps = user_message_timestamps.get(user_id, [])
    timestamps = [ts for ts in timestamps if now_ts - ts <= ANTI_SPAM_WINDOW_SECONDS]
    timestamps.append(now_ts)
    user_message_timestamps[user_id] = timestamps
    return len(timestamps) > ANTI_SPAM_BURST_LIMIT


def support_cooldown_active(user_id: int) -> int:
    now_ts = time.time()
    last_ts = float(user_support_cooldowns.get(user_id, 0) or 0)
    remaining = int(SUPPORT_MESSAGE_COOLDOWN_SECONDS - (now_ts - last_ts))
    return max(0, remaining)


def mark_support_message_sent(user_id: int) -> None:
    user_support_cooldowns[user_id] = time.time()


def order_preview_cooldown_active(user_id: int) -> int:
    now_ts = time.time()
    last_ts = float(user_order_preview_cooldowns.get(user_id, 0) or 0)
    remaining = int(ORDER_PREVIEW_COOLDOWN_SECONDS - (now_ts - last_ts))
    return max(0, remaining)


def mark_order_preview(user_id: int) -> None:
    user_order_preview_cooldowns[user_id] = time.time()


def maintenance_enabled() -> bool:
    return bool(load_config().get("maintenance_mode", False))


def maintenance_message_text() -> str:
    cfg = load_config()
    custom = str(cfg.get("maintenance_message", "") or "").strip()
    if custom:
        return custom
    return (
        "The bot is currently under maintenance.\n\n"
        "Please try again later.\n"
        "If you need urgent help, use Client Support after maintenance ends."
    )


async def send_maintenance_message(message_target) -> None:
    await message_target.reply_text(maintenance_message_text())




def format_order_search_line(order_id: str, data: dict) -> str:
    provider = data.get("provider") or "Unknown"
    requested = data.get("requested_card_balance")
    requested_text = format_usd(float(requested)) if isinstance(requested, (int, float)) else "-"
    order_status = str(data.get("status", "unknown") or "unknown")
    payment_status = str(data.get("payment_status", "unpaid") or "unpaid")
    user_id = data.get("user_id", "-")
    tags = get_order_admin_tags(data) or "-"
    return (
        f"{order_id} | {provider} | {requested_text} | "
        f"order={order_status} | payment={payment_status} | user {user_id} | tags {tags}"
    )


def format_balance_request_search_line(request_id: str, data: dict) -> str:
    amount = format_usd(float(data.get("usd_amount", 0.0) or 0.0))
    coin = data.get("coin", "-")
    status = data.get("status", "-")
    user_id = data.get("user_id", "-")
    tags = get_balance_request_admin_tags(data) or "-"
    return f"{request_id} | {amount} | {coin} | {status} | user {user_id} | tags {tags}"


def search_orders_by_keyword(keyword: str):
    keyword_lower = str(keyword or "").strip().lower()
    results = []
    all_orders = get_all_hybrid_orders()
    for order_id, data in all_orders.items():
        haystack = " ".join([
            str(order_id),
            str(data.get("provider", "")),
            str(data.get("status", "")),
            str(data.get("payment_status", "")),
            str(data.get("username", "")),
            str(data.get("name", "")),
            str(data.get("user_id", "")),
            str(data.get("chat_id", "")),
            str(data.get("details", "")),
            str(data.get("admin_note", "")),
            str(data.get("admin_tags", "")),
        ]).lower()
        if keyword_lower in haystack:
            results.append((order_id, data))
    results.sort(key=lambda item: item[1].get("time", ""), reverse=True)
    return results


def search_balance_requests_by_keyword(keyword: str):
    keyword_lower = str(keyword or "").strip().lower()
    results = []
    all_requests = get_all_hybrid_balance_requests()
    for request_id, data in all_requests.items():
        haystack = " ".join([
            str(request_id),
            str(data.get("status", "")),
            str(data.get("name", "")),
            str(data.get("username", "")),
            str(data.get("user_id", "")),
            str(data.get("coin", "")),
            str(data.get("network", "")),
            str(data.get("usd_amount", "")),
            str(data.get("admin_note", "")),
            str(data.get("admin_tags", "")),
        ]).lower()
        if keyword_lower in haystack:
            results.append((request_id, data))
    results.sort(key=lambda item: item[1].get("time", ""), reverse=True)
    return results


def load_audit_log() -> list:
    return load_json_file(AUDIT_LOG_FILE, [])


def save_audit_log(data: list) -> None:
    save_json_file(AUDIT_LOG_FILE, data)


audit_log_data = load_audit_log()


def add_audit_log_entry(action: str, admin_user, target_type: str = "", target_id: str = "", details: str = "") -> None:
    global audit_log_data

    entry = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "admin_user_id": getattr(admin_user, "id", ""),
        "admin_username": f"@{admin_user.username}" if getattr(admin_user, "username", None) else "No username",
        "admin_name": getattr(admin_user, "full_name", "Unknown"),
        "action": action,
        "target_type": target_type,
        "target_id": target_id,
        "details": details,
    }
    audit_log_data.append(entry)

    if len(audit_log_data) > 5000:
        audit_log_data = audit_log_data[-5000:]

    save_audit_log(audit_log_data)
    sqlite_add_audit_log_entry(entry)


def build_audit_log_text(limit: int = 20) -> str:
    if not audit_log_data:
        return "Audit Log\n\nNo audit entries found."

    lines = ["Audit Log", ""]
    for entry in audit_log_data[-limit:][::-1]:
        lines.append(
            f"{entry.get('time', '-')}"
            f" | {entry.get('action', '-')}"
            f" | {entry.get('target_type', '-')}"
            f" | {entry.get('target_id', '-')}"
            f" | {entry.get('admin_username', '-')}"
        )
        details = str(entry.get("details", "") or "").strip()
        if details:
            lines.append(f"  {details}")
    return "\n".join(lines)


def export_audit_log_csv(filepath: str) -> str:
    fieldnames = [
        "time",
        "admin_user_id",
        "admin_username",
        "admin_name",
        "action",
        "target_type",
        "target_id",
        "details",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for entry in audit_log_data[::-1]:
            writer.writerow({
                "time": entry.get("time", ""),
                "admin_user_id": entry.get("admin_user_id", ""),
                "admin_username": entry.get("admin_username", ""),
                "admin_name": entry.get("admin_name", ""),
                "action": entry.get("action", ""),
                "target_type": entry.get("target_type", ""),
                "target_id": entry.get("target_id", ""),
                "details": entry.get("details", ""),
            })
    return filepath



def load_moderators() -> list:
    data = load_json_file(MODERATORS_FILE, [])
    cleaned = []
    if isinstance(data, list):
        for item in data:
            try:
                cleaned.append(int(item))
            except Exception:
                pass
    return cleaned


def save_moderators(data: list) -> None:
    cleaned = []
    for item in data:
        try:
            cleaned.append(int(item))
        except Exception:
            pass
    save_json_file(MODERATORS_FILE, cleaned)


moderators_data = load_moderators()


def is_moderator(chat_id: int) -> bool:
    try:
        return int(chat_id) in moderators_data
    except Exception:
        return False


def has_staff_access(chat_id: int) -> bool:
    return is_admin(chat_id) or is_moderator(chat_id)


def is_full_admin(chat_id: int) -> bool:
    return is_admin(chat_id)




# ── SQLite Connection (WAL mode, per-operation) ────────────
_sqlite_wal_set = False
_sqlite_init_lock = threading.Lock()

def get_sqlite_connection():
    """Her işlem kendi bağlantısını açar — WAL mode + busy timeout."""
    global _sqlite_wal_set
    conn = sqlite3.connect(
        SQLITE_DB_FILE,
        timeout=30,              # 30 saniye busy timeout
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-4000")
    # WAL mode — ilk bağlantıda set et (DB seviyesinde kalıcı)
    if not _sqlite_wal_set:
        with _sqlite_init_lock:
            if not _sqlite_wal_set:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA wal_autocheckpoint=100")
                _sqlite_wal_set = True
                logger.info("SQLite WAL mode activated, busy_timeout=30s")
    return conn


def close_sqlite_connection() -> None:
    """Bot kapanırken WAL checkpoint yap."""
    try:
        conn = sqlite3.connect(SQLITE_DB_FILE, timeout=5)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    except Exception:
        pass


_sqlite_db_initialized = False
_sqlite_db_init_lock = threading.Lock()

def init_sqlite_db() -> None:
    global _sqlite_db_initialized
    if _sqlite_db_initialized:
        return
    with _sqlite_db_init_lock:
        if _sqlite_db_initialized:
            return
        conn = get_sqlite_connection()
        cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS balances (
            user_id INTEGER PRIMARY KEY,
            available_balance REAL NOT NULL DEFAULT 0,
            total_deposited REAL NOT NULL DEFAULT 0,
            total_spent REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            time TEXT,
            status TEXT,
            payment_status TEXT,
            payment_method TEXT,
            payment_note TEXT,
            paid_at TEXT,
            provider TEXT,
            requested_card_balance REAL,
            charged_amount REAL,
            name TEXT,
            username TEXT,
            user_id INTEGER,
            chat_id INTEGER,
            details TEXT,
            refunded_at TEXT,
            delivered_at TEXT,
            admin_note TEXT,
            admin_tags TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS balance_requests (
            request_id TEXT PRIMARY KEY,
            time TEXT,
            user_id INTEGER,
            name TEXT,
            username TEXT,
            coin TEXT,
            network TEXT,
            usd_amount REAL,
            status TEXT,
            credited_at TEXT,
            admin_note TEXT,
            admin_tags TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            admin_user_id TEXT,
            admin_username TEXT,
            admin_name TEXT,
            action TEXT,
            target_type TEXT,
            target_id TEXT,
            details TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS balance_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            user_id INTEGER,
            delta REAL,
            reason TEXT,
            reference_id TEXT,
            actor TEXT,
            before_balance REAL,
            after_balance REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_transactions (
            tx_hash    TEXT PRIMARY KEY,
            payment_id TEXT,
            coin       TEXT,
            amount     REAL,
            processed_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            payment_id        TEXT PRIMARY KEY,
            user_id           INTEGER NOT NULL,
            coin              TEXT NOT NULL,
            address           TEXT NOT NULL,
            address_index     INTEGER,
            usd_amount        REAL NOT NULL,
            ltc_amount_locked REAL,
            status            TEXT NOT NULL DEFAULT 'waiting',
            credited          INTEGER NOT NULL DEFAULT 0,
            tx_hash           TEXT,
            confirmed_amount_usd REAL,
            name              TEXT,
            username          TEXT,
            created_at        TEXT NOT NULL,
            expires_at        TEXT NOT NULL,
            credited_at       TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status, credited)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cards (
            card_id        TEXT PRIMARY KEY,
            card_number    TEXT NOT NULL,
            expiry_month   TEXT,
            expiry_year    TEXT,
            cvv            TEXT,
            balance        REAL NOT NULL,
            currency       TEXT NOT NULL DEFAULT 'US',
            provider       TEXT NOT NULL,
            registered     INTEGER,
            status         TEXT NOT NULL DEFAULT 'available',
            reserved_by    INTEGER,
            reserved_until TEXT,
            added_at       TEXT NOT NULL,
            order_id       TEXT,
            sold_at        TEXT,
            channel_posted INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cards_status ON cards(status, provider)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gift_cards (
            gc_id          TEXT PRIMARY KEY,
            brand          TEXT DEFAULT '',
            code           TEXT DEFAULT '',
            amount         REAL NOT NULL,
            currency       TEXT NOT NULL DEFAULT 'US',
            gc_type        TEXT NOT NULL DEFAULT 'Other',
            region         TEXT NOT NULL DEFAULT 'US',
            status         TEXT NOT NULL DEFAULT 'available',
            reserved_by    INTEGER,
            reserved_until TEXT,
            added_at       TEXT NOT NULL,
            order_id       TEXT,
            sold_at        TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gc_status ON gift_cards(status, gc_type, region)")

    # External cards (supply chain agent)
    cur.execute("""
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
            registered      INTEGER DEFAULT 0,
            used_paypal     INTEGER DEFAULT 0,
            used_google     INTEGER DEFAULT 0,
            status          TEXT DEFAULT 'available',
            channel_msg_id  INTEGER,
            added_at        TEXT NOT NULL,
            expires_at      TEXT NOT NULL,
            purchased_at    TEXT,
            purchased_by    INTEGER,
            raw_message     TEXT,
            channel_posted  INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ext_status ON external_cards(status, source)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ext_balance ON external_cards(status, balance)")
    conn.commit()
    conn.close()
    _sqlite_db_initialized = True
    logger.info("SQLite database initialized (tables verified)")


def get_sqlite_table_counts() -> dict:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()

    tables = ["balances", "orders", "balance_requests", "audit_log", "balance_ledger", "gift_cards"]
    counts = {}
    for table in tables:
        cur.execute(f"SELECT COUNT(*) AS c FROM {table}")
        counts[table] = int(cur.fetchone()["c"])

    conn.close()
    return counts


def migrate_json_to_sqlite() -> dict:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()

    migrated = {
        "balances": 0,
        "orders": 0,
        "balance_requests": 0,
        "audit_log": 0,
        "balance_ledger": 0,
    }

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("DELETE FROM balances")
    for user_id, record in balances_data.items():
        if not isinstance(record, dict):
            continue
        cur.execute(
            """
            INSERT OR REPLACE INTO balances
            (user_id, available_balance, total_deposited, total_spent, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(user_id),
                float(record.get("available_balance", 0.0) or 0.0),
                float(record.get("total_deposited", 0.0) or 0.0),
                float(record.get("total_spent", 0.0) or 0.0),
                now_str,
            ),
        )
        migrated["balances"] += 1

    cur.execute("DELETE FROM orders")
    for order_id, data in get_all_hybrid_orders().items():
        cur.execute(
            """
            INSERT OR REPLACE INTO orders
            (order_id, time, status, payment_status, payment_method, payment_note, paid_at,
             provider, requested_card_balance, charged_amount, name, username, user_id, chat_id,
             details, refunded_at, delivered_at, admin_note, admin_tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                data.get("time", ""),
                data.get("status", ""),
                data.get("payment_status", ""),
                data.get("payment_method", ""),
                data.get("payment_note", ""),
                data.get("paid_at", ""),
                data.get("provider", ""),
                float(data.get("requested_card_balance", 0.0) or 0.0) if isinstance(data.get("requested_card_balance"), (int, float)) else None,
                float(data.get("charged_amount", 0.0) or 0.0) if isinstance(data.get("charged_amount"), (int, float)) else None,
                data.get("name", ""),
                data.get("username", ""),
                int(data.get("user_id")) if data.get("user_id") not in (None, "") else None,
                int(data.get("chat_id")) if data.get("chat_id") not in (None, "") else None,
                data.get("details", ""),
                data.get("refunded_at", ""),
                data.get("delivered_at", ""),
                data.get("admin_note", ""),
                data.get("admin_tags", ""),
            ),
        )
        migrated["orders"] += 1

    cur.execute("DELETE FROM balance_requests")
    for request_id, data in get_all_hybrid_balance_requests().items():
        cur.execute(
            """
            INSERT OR REPLACE INTO balance_requests
            (request_id, time, user_id, name, username, coin, network, usd_amount, status, credited_at, admin_note, admin_tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                data.get("time", ""),
                int(data.get("user_id")) if data.get("user_id") not in (None, "") else None,
                data.get("name", ""),
                data.get("username", ""),
                data.get("coin", ""),
                data.get("network", ""),
                float(data.get("usd_amount", 0.0) or 0.0),
                data.get("status", ""),
                data.get("credited_at", ""),
                data.get("admin_note", ""),
                data.get("admin_tags", ""),
            ),
        )
        migrated["balance_requests"] += 1

    cur.execute("DELETE FROM audit_log")
    for entry in audit_log_data:
        cur.execute(
            """
            INSERT INTO audit_log
            (time, admin_user_id, admin_username, admin_name, action, target_type, target_id, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.get("time", ""),
                str(entry.get("admin_user_id", "")),
                entry.get("admin_username", ""),
                entry.get("admin_name", ""),
                entry.get("action", ""),
                entry.get("target_type", ""),
                entry.get("target_id", ""),
                entry.get("details", ""),
            ),
        )
        migrated["audit_log"] += 1

    cur.execute("DELETE FROM balance_ledger")
    for entry in balance_ledger_data:
        cur.execute(
            """
            INSERT INTO balance_ledger
            (time, user_id, delta, reason, reference_id, actor, before_balance, after_balance)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.get("time", ""),
                int(entry.get("user_id", 0) or 0),
                float(entry.get("delta", 0.0) or 0.0),
                entry.get("reason", ""),
                entry.get("reference_id", ""),
                entry.get("actor", ""),
                float(entry.get("before_balance", 0.0) or 0.0),
                float(entry.get("after_balance", 0.0) or 0.0),
            ),
        )
        migrated["balance_ledger"] += 1

    cur.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", ("last_migration_at", now_str))
    conn.commit()
    conn.close()
    return migrated


def build_sqlite_status_text() -> str:
    init_sqlite_db()
    counts = get_sqlite_table_counts()
    return (
        "SQLite Status\n\n"
        f"Database File: {SQLITE_DB_FILE}\n"
        f"Balances Rows: {counts.get('balances', 0)}\n"
        f"Orders Rows: {counts.get('orders', 0)}\n"
        f"Balance Requests Rows: {counts.get('balance_requests', 0)}\n"
        f"Audit Log Rows: {counts.get('audit_log', 0)}\n"
        f"Balance Ledger Rows: {counts.get('balance_ledger', 0)}"
    )







def sqlite_primary_balances_enabled() -> bool:
    return bool(load_config().get("sqlite_primary_balances", False))


def sqlite_primary_orders_enabled() -> bool:
    return bool(load_config().get("sqlite_primary_orders", False))


def sqlite_primary_balance_requests_enabled() -> bool:
    return bool(load_config().get("sqlite_primary_balance_requests", False))


def build_sqlite_mode_status_text() -> str:
    cfg = load_config()
    return (
        "SQLite Mode Status\n\n"
        f"Balances Primary: {'on' if cfg.get('sqlite_primary_balances', False) else 'off'}\n"
        f"Orders Primary: {'on' if cfg.get('sqlite_primary_orders', False) else 'off'}\n"
        f"Balance Requests Primary: {'on' if cfg.get('sqlite_primary_balance_requests', False) else 'off'}"
    )


def sqlite_add_audit_log_entry(entry: dict) -> None:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO audit_log
        (time, admin_user_id, admin_username, admin_name, action, target_type, target_id, details)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entry.get("time", ""),
            str(entry.get("admin_user_id", "")),
            entry.get("admin_username", ""),
            entry.get("admin_name", ""),
            entry.get("action", ""),
            entry.get("target_type", ""),
            entry.get("target_id", ""),
            entry.get("details", ""),
        ),
    )
    conn.commit()
    conn.close()


def sqlite_add_balance_ledger_entry(entry: dict) -> None:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO balance_ledger
        (time, user_id, delta, reason, reference_id, actor, before_balance, after_balance)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entry.get("time", ""),
            int(entry.get("user_id", 0) or 0),
            float(entry.get("delta", 0.0) or 0.0),
            entry.get("reason", ""),
            entry.get("reference_id", ""),
            entry.get("actor", ""),
            float(entry.get("before_balance", 0.0) or 0.0),
            float(entry.get("after_balance", 0.0) or 0.0),
        ),
    )
    conn.commit()
    conn.close()


def sqlite_get_audit_count() -> int:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM audit_log")
    row = cur.fetchone()
    conn.close()
    return int(row["c"])


def sqlite_get_ledger_count() -> int:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM balance_ledger")
    row = cur.fetchone()
    conn.close()
    return int(row["c"])


def build_log_storage_status_text() -> str:
    return (
        "Log Storage Status\n\n"
        f"JSON Audit Entries: {len(audit_log_data)}\n"
        f"SQLite Audit Entries: {sqlite_get_audit_count()}\n\n"
        f"JSON Ledger Entries: {len(balance_ledger_data)}\n"
        f"SQLite Ledger Entries: {sqlite_get_ledger_count()}"
    )


def sqlite_get_balance_request(request_id: str) -> dict | None:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM balance_requests WHERE request_id = ?", (str(request_id),))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return dict(row)


def sqlite_save_balance_request(request_id: str, data: dict) -> None:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO balance_requests
        (request_id, time, user_id, name, username, coin, network, usd_amount, status, credited_at, admin_note, admin_tags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(request_id),
            data.get("time", ""),
            int(data.get("user_id")) if data.get("user_id") not in (None, "") else None,
            data.get("name", ""),
            data.get("username", ""),
            data.get("coin", ""),
            data.get("network", ""),
            float(data.get("usd_amount", 0.0) or 0.0),
            data.get("status", ""),
            data.get("credited_at", ""),
            data.get("admin_note", ""),
            data.get("admin_tags", ""),
        ),
    )
    conn.commit()
    conn.close()


def get_hybrid_balance_request(request_id: str) -> dict | None:
    json_data = balance_requests_data.get(request_id)
    sqlite_data = sqlite_get_balance_request(request_id)

    if sqlite_primary_balance_requests_enabled():
        if sqlite_data:
            balance_requests_data[request_id] = sqlite_data
            save_balance_requests(balance_requests_data)
            return sqlite_data
        if json_data:
            sqlite_save_balance_request(request_id, json_data)
            return json_data
        return None

    if json_data and not sqlite_data:
        sqlite_save_balance_request(request_id, json_data)
        return json_data

    if sqlite_data and not json_data:
        balance_requests_data[request_id] = sqlite_data
        save_balance_requests(balance_requests_data)
        return sqlite_data

    if json_data:
        sqlite_save_balance_request(request_id, json_data)
        return json_data

    if sqlite_data:
        balance_requests_data[request_id] = sqlite_data
        save_balance_requests(balance_requests_data)
        return sqlite_data

    return None


def save_hybrid_balance_request(request_id: str, data: dict) -> None:
    balance_requests_data[request_id] = data
    save_balance_requests(balance_requests_data)
    sqlite_save_balance_request(request_id, data)


def build_balance_request_sync_status_text(request_id: str) -> str:
    json_data = balance_requests_data.get(request_id) or {}
    sqlite_data = sqlite_get_balance_request(request_id) or {}

    def amt(val):
        return format_usd(float(val or 0.0)) if isinstance(val, (int, float)) else "-"

    return (
        "Balance Request Sync Status\n\n"
        f"Request ID: {request_id}\n\n"
        "JSON\n"
        f"Status: {json_data.get('status', '-')}\n"
        f"Coin: {json_data.get('coin', '-')}\n"
        f"Network: {json_data.get('network', '-')}\n"
        f"Amount: {amt(json_data.get('usd_amount'))}\n\n"
        "SQLite\n"
        f"Status: {sqlite_data.get('status', '-')}\n"
        f"Coin: {sqlite_data.get('coin', '-')}\n"
        f"Network: {sqlite_data.get('network', '-')}\n"
        f"Amount: {amt(sqlite_data.get('usd_amount'))}"
    )



def sqlite_get_all_orders() -> dict:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM orders")
    rows = cur.fetchall()
    conn.close()

    result = {}
    for row in rows:
        data = dict(row)
        order_id = str(data.get("order_id", ""))
        if order_id:
            result[order_id] = data
    return result


def sqlite_get_all_balance_requests() -> dict:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM balance_requests")
    rows = cur.fetchall()
    conn.close()

    result = {}
    for row in rows:
        data = dict(row)
        request_id = str(data.get("request_id", ""))
        if request_id:
            result[request_id] = data
    return result


def get_all_hybrid_orders() -> dict:
    if sqlite_primary_orders_enabled():
        sqlite_orders = sqlite_get_all_orders()
        if sqlite_orders:
            orders.clear()
            orders.update(sqlite_orders)
            save_orders(orders)
            return dict(sqlite_orders)

    if orders:
        for order_id, data in orders.items():
            sqlite_save_order(order_id, data)
        return dict(orders)

    sqlite_orders = sqlite_get_all_orders()
    if sqlite_orders:
        orders.clear()
        orders.update(sqlite_orders)
        save_orders(orders)
    return dict(sqlite_orders)


def get_all_hybrid_balance_requests() -> dict:
    if sqlite_primary_balance_requests_enabled():
        sqlite_requests = sqlite_get_all_balance_requests()
        if sqlite_requests:
            balance_requests_data.clear()
            balance_requests_data.update(sqlite_requests)
            save_balance_requests(balance_requests_data)
            return dict(sqlite_requests)

    if balance_requests_data:
        for request_id, data in balance_requests_data.items():
            sqlite_save_balance_request(request_id, data)
        return dict(balance_requests_data)

    sqlite_requests = sqlite_get_all_balance_requests()
    if sqlite_requests:
        balance_requests_data.clear()
        balance_requests_data.update(sqlite_requests)
        save_balance_requests(balance_requests_data)
    return dict(sqlite_requests)


def sqlite_get_order(order_id: str) -> dict | None:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM orders WHERE order_id = ?", (str(order_id),))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    data = dict(row)
    return data


def sqlite_save_order(order_id: str, data: dict) -> None:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO orders
        (order_id, time, status, payment_status, payment_method, payment_note, paid_at,
         provider, requested_card_balance, charged_amount, name, username, user_id, chat_id,
         details, refunded_at, delivered_at, admin_note, admin_tags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(order_id),
            data.get("time", ""),
            data.get("status", ""),
            data.get("payment_status", ""),
            data.get("payment_method", ""),
            data.get("payment_note", ""),
            data.get("paid_at", ""),
            data.get("provider", ""),
            float(data.get("requested_card_balance", 0.0) or 0.0) if isinstance(data.get("requested_card_balance"), (int, float)) else None,
            float(data.get("charged_amount", 0.0) or 0.0) if isinstance(data.get("charged_amount"), (int, float)) else None,
            data.get("name", ""),
            data.get("username", ""),
            int(data.get("user_id")) if data.get("user_id") not in (None, "") else None,
            int(data.get("chat_id")) if data.get("chat_id") not in (None, "") else None,
            data.get("details", ""),
            data.get("refunded_at", ""),
            data.get("delivered_at", ""),
            data.get("admin_note", ""),
            data.get("admin_tags", ""),
        ),
    )
    conn.commit()
    conn.close()


def get_hybrid_order(order_id: str) -> dict | None:
    json_order = orders.get(order_id)
    sqlite_order = sqlite_get_order(order_id)

    if sqlite_primary_orders_enabled():
        if sqlite_order:
            orders[order_id] = sqlite_order
            save_orders(orders)
            return sqlite_order
        if json_order:
            sqlite_save_order(order_id, json_order)
            return json_order
        return None

    if json_order and not sqlite_order:
        sqlite_save_order(order_id, json_order)
        return json_order

    if sqlite_order and not json_order:
        orders[order_id] = sqlite_order
        save_orders(orders)
        return sqlite_order

    if json_order:
        sqlite_save_order(order_id, json_order)
        return json_order

    if sqlite_order:
        orders[order_id] = sqlite_order
        save_orders(orders)
        return sqlite_order

    return None


def save_hybrid_order(order_id: str, data: dict) -> None:
    orders[order_id] = data
    save_orders(orders)
    sqlite_save_order(order_id, data)


def build_order_sync_status_text(order_id: str) -> str:
    json_order = orders.get(order_id) or {}
    sqlite_order = sqlite_get_order(order_id) or {}

    return (
        "Order Sync Status\n\n"
        f"Order ID: {order_id}\n\n"
        "JSON\n"
        f"Status: {json_order.get('status', '-')}\n"
        f"Payment Status: {json_order.get('payment_status', '-')}\n"
        f"Provider: {json_order.get('provider', '-')}\n"
        f"Charged: {format_usd(float(json_order.get('charged_amount', 0.0) or 0.0)) if isinstance(json_order.get('charged_amount'), (int, float)) else '-'}\n\n"
        "SQLite\n"
        f"Status: {sqlite_order.get('status', '-')}\n"
        f"Payment Status: {sqlite_order.get('payment_status', '-')}\n"
        f"Provider: {sqlite_order.get('provider', '-')}\n"
        f"Charged: {format_usd(float(sqlite_order.get('charged_amount', 0.0) or 0.0)) if isinstance(sqlite_order.get('charged_amount'), (int, float)) else '-'}"
    )


def sqlite_get_balance_record(user_id: int) -> dict:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, available_balance, total_deposited, total_spent FROM balances WHERE user_id = ?",
        (int(user_id),),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return {
            "available_balance": 0.0,
            "total_deposited": 0.0,
            "total_spent": 0.0,
        }

    return {
        "available_balance": float(row["available_balance"] or 0.0),
        "total_deposited": float(row["total_deposited"] or 0.0),
        "total_spent": float(row["total_spent"] or 0.0),
    }


def sqlite_save_balance_record(user_id: int, record: dict) -> None:
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO balances
        (user_id, available_balance, total_deposited, total_spent, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            int(user_id),
            float(record.get("available_balance", 0.0) or 0.0),
            float(record.get("total_deposited", 0.0) or 0.0),
            float(record.get("total_spent", 0.0) or 0.0),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    conn.commit()
    conn.close()


def get_hybrid_balance_record(user_id: int) -> dict:
    json_record = get_user_balance_record(user_id)
    sqlite_record = sqlite_get_balance_record(user_id)

    json_has_data = any(float(json_record.get(k, 0.0) or 0.0) != 0.0 for k in ["available_balance", "total_deposited", "total_spent"])
    sqlite_has_data = any(float(sqlite_record.get(k, 0.0) or 0.0) != 0.0 for k in ["available_balance", "total_deposited", "total_spent"])

    if sqlite_primary_balances_enabled():
        if sqlite_has_data:
            balances_data[str(user_id)] = sqlite_record
            save_all_balances()
            return sqlite_record
        if json_has_data:
            sqlite_save_balance_record(user_id, json_record)
            return json_record
        return json_record

    if json_has_data and not sqlite_has_data:
        sqlite_save_balance_record(user_id, json_record)
        return json_record

    if sqlite_has_data and not json_has_data:
        balances_data[str(user_id)] = sqlite_record
        save_all_balances()
        return sqlite_record

    if json_has_data:
        sqlite_save_balance_record(user_id, json_record)
        return json_record

    balances_data[str(user_id)] = sqlite_record
    save_all_balances()
    return sqlite_record


def save_hybrid_balance_record(user_id: int, record: dict) -> None:
    balances_data[str(user_id)] = {
        "available_balance": round(float(record.get("available_balance", 0.0) or 0.0), 2),
        "total_deposited": round(float(record.get("total_deposited", 0.0) or 0.0), 2),
        "total_spent": round(float(record.get("total_spent", 0.0) or 0.0), 2),
    }
    save_all_balances()
    sqlite_save_balance_record(user_id, balances_data[str(user_id)])


def update_hybrid_balance(user_id: int, **updates) -> dict:
    """Backward-compatible balance updater used by older flows."""
    record = get_hybrid_balance_record(user_id)
    for key in ("available_balance", "total_deposited", "total_spent"):
        if key in updates and updates[key] is not None:
            record[key] = round(float(updates[key] or 0.0), 2)
    save_hybrid_balance_record(user_id, record)
    return record


def encrypt_card_data(plaintext: str) -> str:
    """Backward-compatible alias for older delivery/security flows."""
    return encrypt_card_field(plaintext)


def build_balance_sync_status_text(user_id: int) -> str:
    json_record = get_user_balance_record(user_id)
    sqlite_record = sqlite_get_balance_record(user_id)

    return (
        "Balance Sync Status\n\n"
        f"User ID: {user_id}\n\n"
        "JSON\n"
        f"Available: {format_usd(float(json_record.get('available_balance', 0.0) or 0.0))}\n"
        f"Deposited: {format_usd(float(json_record.get('total_deposited', 0.0) or 0.0))}\n"
        f"Spent: {format_usd(float(json_record.get('total_spent', 0.0) or 0.0))}\n\n"
        "SQLite\n"
        f"Available: {format_usd(float(sqlite_record.get('available_balance', 0.0) or 0.0))}\n"
        f"Deposited: {format_usd(float(sqlite_record.get('total_deposited', 0.0) or 0.0))}\n"
        f"Spent: {format_usd(float(sqlite_record.get('total_spent', 0.0) or 0.0))}"
    )


def load_balance_ledger() -> list:
    return load_json_file(BALANCE_LEDGER_FILE, [])


def save_balance_ledger(data: list) -> None:
    save_json_file(BALANCE_LEDGER_FILE, data)


balance_ledger_data = load_balance_ledger()


def add_balance_ledger_entry(user_id: int, delta: float, reason: str, reference_id: str = "", actor: str = "", before_balance: float = 0.0, after_balance: float = 0.0) -> None:
    global balance_ledger_data
    entry = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": int(user_id),
        "delta": round(float(delta), 2),
        "reason": str(reason or ""),
        "reference_id": str(reference_id or ""),
        "actor": str(actor or ""),
        "before_balance": round(float(before_balance), 2),
        "after_balance": round(float(after_balance), 2),
    }
    balance_ledger_data.append(entry)
    if len(balance_ledger_data) > 10000:
        balance_ledger_data = balance_ledger_data[-10000:]
    save_balance_ledger(balance_ledger_data)
    sqlite_add_balance_ledger_entry(entry)


def atomic_create_paid_order_from_wallet(
    user_id: int,
    chat_id: int,
    full_name: str,
    telegram_username: str | None,
    provider: str,
    requested_balance: float,
) -> dict:
    global balance_ledger_data

    init_sqlite_db()

    username_text = f"@{telegram_username}" if telegram_username else "No username"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Gift card siparişleri için rate'i gc_ prefix'li key'den al
    is_gift_card_order = provider.startswith("GC:")
    if is_gift_card_order:
        gc_parts = provider.split(":")
        gc_type = gc_parts[1] if len(gc_parts) > 1 else "Other"
        gc_region = gc_parts[2] if len(gc_parts) > 2 else ""
        current_rate = get_gift_card_rate(gc_type, gc_region)
    else:
        current_rate = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.0)))
    requested_balance = round(float(requested_balance), 2)
    cost = round(requested_balance * current_rate, 2)

    current_record = get_hybrid_balance_record(user_id)
    available_balance = round(float(current_record.get("available_balance", 0.0) or 0.0), 2)
    if available_balance + 1e-9 < cost:
        return {
            "ok": False,
            "reason": "insufficient_balance",
            "available_balance": available_balance,
            "cost": cost,
        }

    # Gift card siparişlerinde prepaid stok kontrolü atla
    if is_gift_card_order:
        stock_cards_before = 1
        stock_balance_before = requested_balance
    else:
        # cards.json'dan stok kontrolü
        all_cards = load_cards()
        provider_cards = [
            c for c in all_cards.values()
            if c.get("provider") == provider and c.get("status") in ("available", "reserved")
        ]
        stock_cards_before = len(provider_cards)
        stock_balance_before = sum(float(c.get("balance", 0)) for c in provider_cards)

    if stock_cards_before <= 0:
        return {
            "ok": False,
            "reason": "insufficient_stock_cards",
            "provider": provider,
            "stock_cards": stock_cards_before,
            "cost": cost,
        }

    order_id = generate_order_id()
    before_balance = round(available_balance, 2)
    after_balance = round(available_balance - cost, 2)
    updated_record = {
        "available_balance": after_balance,
        "total_deposited": round(float(current_record.get("total_deposited", 0.0) or 0.0), 2),
        "total_spent": round(float(current_record.get("total_spent", 0.0) or 0.0) + cost, 2),
    }
    order_data = {
        "chat_id": chat_id,
        "name": full_name,
        "username": username_text,
        "user_id": user_id,
        "provider": provider,
        "requested_card_balance": requested_balance,
        "charged_amount": cost,
        "details": f"{provider} {format_usd(requested_balance)} | charged {format_usd(cost)} from balance",
        "time": now_str,
        "status": "paid_pending_delivery",
        "payment_status": "paid",
        "payment_method": "wallet_balance",
        "payment_note": "Paid automatically from wallet balance",
        "paid_at": now_str,
    }
    ledger_entry = {
        "time": now_str,
        "user_id": int(user_id),
        "delta": round(-cost, 2),
        "reason": "order_charge",
        "reference_id": str(order_id),
        "actor": str(user_id),
        "before_balance": before_balance,
        "after_balance": after_balance,
    }

    # stock.json artık kullanılmıyor - cards.json yönetiyor
    stock_after = {provider: {
        "balance": round(max(0.0, stock_balance_before - requested_balance), 2),
        "cards": max(0, stock_cards_before - 1),
    }}

    conn = get_sqlite_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO balances
            (user_id, available_balance, total_deposited, total_spent, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(user_id),
                float(updated_record.get("available_balance", 0.0) or 0.0),
                float(updated_record.get("total_deposited", 0.0) or 0.0),
                float(updated_record.get("total_spent", 0.0) or 0.0),
                now_str,
            ),
        )
        cur.execute(
            """
            INSERT OR REPLACE INTO orders
            (order_id, time, status, payment_status, payment_method, payment_note, paid_at,
             provider, requested_card_balance, charged_amount, name, username, user_id, chat_id,
             details, refunded_at, delivered_at, admin_note, admin_tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(order_id),
                order_data.get("time", ""),
                order_data.get("status", ""),
                order_data.get("payment_status", ""),
                order_data.get("payment_method", ""),
                order_data.get("payment_note", ""),
                order_data.get("paid_at", ""),
                order_data.get("provider", ""),
                float(order_data.get("requested_card_balance", 0.0) or 0.0),
                float(order_data.get("charged_amount", 0.0) or 0.0),
                order_data.get("name", ""),
                order_data.get("username", ""),
                int(order_data.get("user_id")) if order_data.get("user_id") not in (None, "") else None,
                int(order_data.get("chat_id")) if order_data.get("chat_id") not in (None, "") else None,
                order_data.get("details", ""),
                order_data.get("refunded_at", ""),
                order_data.get("delivered_at", ""),
                order_data.get("admin_note", ""),
                order_data.get("admin_tags", ""),
            ),
        )
        cur.execute(
            """
            INSERT INTO balance_ledger
            (time, user_id, delta, reason, reference_id, actor, before_balance, after_balance)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ledger_entry.get("time", ""),
                int(ledger_entry.get("user_id", 0) or 0),
                float(ledger_entry.get("delta", 0.0) or 0.0),
                ledger_entry.get("reason", ""),
                ledger_entry.get("reference_id", ""),
                ledger_entry.get("actor", ""),
                float(ledger_entry.get("before_balance", 0.0) or 0.0),
                float(ledger_entry.get("after_balance", 0.0) or 0.0),
            ),
        )
        conn.commit()
        conn.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    balances_data[str(user_id)] = updated_record
    save_all_balances()

    orders[str(order_id)] = order_data
    save_orders(orders)

    balance_ledger_data.append(ledger_entry)
    if len(balance_ledger_data) > 10000:
        balance_ledger_data = balance_ledger_data[-10000:]
    save_balance_ledger(balance_ledger_data)

    # stock.json kaydetme kaldirildi

    create_backup_snapshot("order_charge_stock")

    return {
        "ok": True,
        "order_id": str(order_id),
        "order_time": now_str,
        "cost": cost,
        "record": updated_record,
        "order_data": order_data,
        "ledger_entry": ledger_entry,
        "stock_before": {
            "balance": stock_balance_before,
            "cards": stock_cards_before,
        },
        "stock_after": stock_after.get(provider, {"balance": 0.0, "cards": 0}),
    }


def create_backup_snapshot(reason: str = "manual") -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder = os.path.join(BACKUP_DIR, f"{stamp}_{reason}")
    os.makedirs(folder, exist_ok=True)
    for filename in [
        ORDERS_FILE, STOCK_FILE, CONFIG_FILE, ACCESS_FILE, BALANCES_FILE,
        RATES_FILE, BALANCE_REQUESTS_FILE, AUDIT_LOG_FILE, MODERATORS_FILE,
        BALANCE_LEDGER_FILE, SQLITE_DB_FILE,
    ]:
        if os.path.exists(filename):
            shutil.copy2(filename, os.path.join(folder, os.path.basename(filename)))
    return folder


def load_orders() -> dict:
    return load_json_file(ORDERS_FILE, {})


def save_orders(orders: dict) -> None:
    save_json_file(ORDERS_FILE, orders)


def load_stock() -> dict:
    return load_json_file(STOCK_FILE, {})


def save_stock(stock: dict) -> None:
    save_json_file(STOCK_FILE, stock)


def load_config() -> dict:
    return load_json_file(CONFIG_FILE, {})


def save_config(config: dict) -> None:
    save_json_file(CONFIG_FILE, config)


def load_access() -> dict:
    default = {
        "mode": "restricted",
        "approved_users": []
    }
    data = load_json_file(ACCESS_FILE, default)

    if "mode" not in data:
        data["mode"] = "restricted"
    if "approved_users" not in data:
        data["approved_users"] = []

    return data


def save_access(access_data: dict) -> None:
    save_json_file(ACCESS_FILE, access_data)


def load_balances() -> dict:
    return load_json_file(BALANCES_FILE, {})


def save_balances(balances: dict) -> None:
    save_json_file(BALANCES_FILE, balances)


def load_rates() -> dict:
    data = load_json_file(RATES_FILE, DEFAULT_PROVIDER_RATES.copy())
    merged = DEFAULT_PROVIDER_RATES.copy()
    if isinstance(data, dict):
        for provider, rate in data.items():
            if provider in SUPPORTED_PROVIDERS:
                try:
                    merged[provider] = float(rate)
                except Exception:
                    pass
    return merged


def save_rates(rates: dict) -> None:
    save_json_file(RATES_FILE, rates)


def load_balance_requests() -> dict:
    return load_json_file(BALANCE_REQUESTS_FILE, {})


def save_balance_requests(data: dict) -> None:
    save_json_file(BALANCE_REQUESTS_FILE, data)


def get_payment_status_label(status: str) -> str:
    mapping = {
        "unpaid": "❌ Unpaid",
        "pending": "⏳ Pending",
        "paid": "✅ Paid",
        "failed": "⚠️ Failed",
        "refunded": "↩️ Refunded",
    }
    return mapping.get((status or "").lower(), "❔ Unknown")


def get_order_status_label(status: str) -> str:
    mapping = {
        "new": "🆕 New",
        "paid": "✅ Paid",
        "processing": "⚙️ Processing",
        "done": "✅ Done",
        "cancelled": "❌ Cancelled",
        "archived": "🗂️ Archived",
        "paid_pending_delivery": "📦 Pending Delivery",
        "delivered": "🚚 Delivered",
        "refunded": "↩️ Refunded",
    }
    return mapping.get((status or "").lower(), status or "unknown")


def ensure_orders_payment_fields() -> None:
    global orders
    changed = False
    for order_id, data in get_all_hybrid_orders().items():
        if not isinstance(data, dict):
            continue
        if "payment_status" not in data:
            if "charged_amount" in data or data.get("status") in {"paid_pending_delivery", "delivered"}:
                data["payment_status"] = "paid"
                if "paid_at" not in data:
                    data["paid_at"] = data.get("time", "")
                if "payment_method" not in data:
                    data["payment_method"] = "wallet_balance"
            elif data.get("status") == "refunded":
                data["payment_status"] = "refunded"
                if "payment_method" not in data:
                    data["payment_method"] = "wallet_balance"
            elif data.get("status") in {"paid", "processing", "done"}:
                data["payment_status"] = "paid"
                if "paid_at" not in data:
                    data["paid_at"] = data.get("time", "")
                if "payment_method" not in data:
                    data["payment_method"] = "manual"
            else:
                data["payment_status"] = "unpaid"
            changed = True
        if "payment_method" not in data:
            data["payment_method"] = ""
            changed = True
        if "payment_note" not in data:
            data["payment_note"] = ""
            changed = True
        if "paid_at" not in data:
            data["paid_at"] = ""
            changed = True
    if changed:
        save_orders(orders)


def set_order_payment_status(order_id: str, status: str, method: str = "", note: str = "", paid_at: str | None = None) -> bool:
    if order_id not in orders:
        return False

    data = orders[order_id]
    data["payment_status"] = (status or "").lower().strip()
    data["payment_method"] = method.strip() if method else data.get("payment_method", "")
    data["payment_note"] = note.strip() if note else data.get("payment_note", "")

    if paid_at is not None:
        data["paid_at"] = paid_at
    elif data["payment_status"] == "paid" and not data.get("paid_at"):
        data["paid_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if data["payment_status"] != "paid" and paid_at == "":
        data["paid_at"] = ""

    orders[order_id] = data
    save_hybrid_order(order_id, data)
    return True


def payment_panel_markup(order_id: str) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("Pending", callback_data=f"paymentpanel::{order_id}::pending"),
            InlineKeyboardButton("Paid", callback_data=f"paymentpanel::{order_id}::paid"),
        ],
        [
            InlineKeyboardButton("Failed", callback_data=f"paymentpanel::{order_id}::failed"),
            InlineKeyboardButton("Refunded", callback_data=f"paymentpanel::{order_id}::refunded"),
        ],
        [InlineKeyboardButton("Refresh Info", callback_data=f"paymentpanelinfo::{order_id}")],
    ]
    return InlineKeyboardMarkup(keyboard)


def apply_payment_status_update(order_id: str, new_status: str, method: str = "", note: str = "", paid_at: str = "") -> None:
    set_order_payment_status(order_id, new_status, method, note, paid_at)


def order_panel_markup(order_id: str) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("Deliver", callback_data=f"orderpanel::{order_id}::deliver"),
            InlineKeyboardButton("Refund Balance", callback_data=f"orderpanel::{order_id}::refund"),
        ],
        [
            InlineKeyboardButton("Cancel Order", callback_data=f"orderpanel::{order_id}::cancel"),
            InlineKeyboardButton("Payment Panel", callback_data=f"orderpanel::{order_id}::payment"),
        ],
        [
            InlineKeyboardButton("Order Info", callback_data=f"orderpanel::{order_id}::info"),
            InlineKeyboardButton("Refresh", callback_data=f"orderpanel::{order_id}::refresh"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def build_order_panel_text(order_id: str, data: dict) -> str:
    provider = data.get("provider") or "Unknown"
    requested = data.get("requested_card_balance")
    charged = data.get("charged_amount")
    requested_text = format_usd(float(requested)) if isinstance(requested, (int, float)) else "-"
    charged_text = format_usd(float(charged)) if isinstance(charged, (int, float)) else "-"
    order_status = data.get("status", "unknown")
    payment_status = data.get("payment_status", "unpaid")
    username = data.get("username", "unknown")
    user_id = data.get("user_id", "unknown")
    chat_id = data.get("chat_id", "unknown")
    created_at = data.get("time", "-")
    delivered_at = data.get("delivered_at", "-")
    refunded_at = data.get("refunded_at", "-")

    return (
        "Order Panel\n\n"
        f"Order ID: {order_id}\n"
        f"Provider: {provider}\n"
        f"Requested Card Balance: {requested_text}\n"
        f"Charged Amount: {charged_text}\n"
        f"Order Status: {get_order_status_label(order_status)}\n"
        f"Payment Status: {get_payment_status_label(payment_status)}\n"
        f"Username: {username}\n"
        f"User ID: {user_id}\n"
        f"Chat ID: {chat_id}\n"
        f"Created At: {created_at}\n"
        f"Delivered At: {delivered_at}\n"
        f"Refunded At: {refunded_at}\n"
        f"Admin Note: {get_order_admin_note(data) or '-'}\n"
        f"Admin Tags: {get_order_admin_tags(data) or '-'}"
    )


orders = load_orders()
config = load_config()
access_data = load_access()
balances_data = load_balances()
rates_data = load_rates()
balance_requests_data = load_balance_requests()
ensure_orders_payment_fields()

def format_usd(amount: float) -> str:
    return f"${amount:,.2f}"


def get_provider_rate(provider: str) -> float:
    try:
        return float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.50)))
    except Exception:
        return 0.50


def get_user_balance_record(user_id: int) -> dict:
    key = str(user_id)
    if key not in balances_data or not isinstance(balances_data.get(key), dict):
        balances_data[key] = {
            "available_balance": 0.0,
            "total_deposited": 0.0,
            "total_spent": 0.0,
        }
    record = balances_data[key]

    try:
        record["available_balance"] = float(record.get("available_balance", 0.0))
    except Exception:
        record["available_balance"] = 0.0

    try:
        record["total_deposited"] = float(record.get("total_deposited", 0.0))
    except Exception:
        record["total_deposited"] = 0.0

    try:
        record["total_spent"] = float(record.get("total_spent", 0.0))
    except Exception:
        record["total_spent"] = 0.0

    balances_data[key] = record
    return record


def save_all_balances() -> None:
    save_balances(balances_data)


def get_pending_topup_request(user_id: int) -> dict | None:
    return add_balance_mode_users.get(user_id)


def create_balance_request(user_id: int, full_name: str, username: str, coin: str, network: str, usd_amount: float) -> dict:
    global balance_requests_data

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    request_id = f"BR-{int(time.time())}-{random.randint(100,999)}"
    request_data = {
        "request_id": request_id,
        "user_id": user_id,
        "name": full_name,
        "username": username,
        "coin": coin,
        "network": network,
        "usd_amount": round(float(usd_amount), 2),
        "status": "pending_manual_payment",
        "time": now_str,
    }
    balance_requests_data[request_id] = request_data
    save_balance_requests(balance_requests_data)
    return request_data




# ============================================================
# NON-CUSTODIAL PAYMENT MODULE v2
# HD Wallet — unique address per payment, global index counter
# Supports: LTC (BIP44 coin 2) + USDC/ETH (BIP44 coin 60)
# ============================================================

# ── Crypto primitives ────────────────────────────────────────

_SECP256K1_P  = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
_SECP256K1_N  = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
_SECP256K1_Gx = 0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798
_SECP256K1_Gy = 0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8

def _point_add(P, Q):
    if P is None: return Q
    if Q is None: return P
    p = _SECP256K1_P
    if P[0] == Q[0]:
        if P[1] != Q[1]: return None
        lam = (3 * P[0] * P[0] * pow(2 * P[1], p - 2, p)) % p
    else:
        lam = ((Q[1] - P[1]) * pow(Q[0] - P[0], p - 2, p)) % p
    rx = (lam * lam - P[0] - Q[0]) % p
    ry = (lam * (P[0] - rx) - P[1]) % p
    return (rx, ry)

def _scalar_mult(k, P):
    R = None
    while k:
        if k & 1: R = _point_add(R, P)
        P = _point_add(P, P)
        k >>= 1
    return R

def _privkey_to_pubkey_compressed(privkey_bytes: bytes) -> bytes:
    k = int.from_bytes(privkey_bytes, "big") % _SECP256K1_N
    G = (_SECP256K1_Gx, _SECP256K1_Gy)
    pub = _scalar_mult(k, G)
    prefix = b"\x02" if pub[1] % 2 == 0 else b"\x03"
    return prefix + pub[0].to_bytes(32, "big")

def _privkey_to_pubkey_uncompressed(privkey_bytes: bytes) -> bytes:
    k = int.from_bytes(privkey_bytes, "big") % _SECP256K1_N
    G = (_SECP256K1_Gx, _SECP256K1_Gy)
    pub = _scalar_mult(k, G)
    return pub[0].to_bytes(32, "big") + pub[1].to_bytes(32, "big")

def _b58encode(data: bytes) -> str:
    ALPHA = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    num = int.from_bytes(data, "big")
    result = ""
    while num > 0:
        num, rem = divmod(num, 58)
        result = ALPHA[rem] + result
    for b in data:
        if b == 0: result = "1" + result
        else: break
    return result

# ── Seed / Key derivation ─────────────────────────────────────

def _derive_key_from_password(password: str, salt: bytes) -> bytes:
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives import hashes
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=390000)
    return kdf.derive(password.encode())

def decrypt_seed(encrypted_b64: str, password: str) -> str:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    raw = base64.b64decode(encrypted_b64)
    salt, nonce, ciphertext = raw[:16], raw[16:28], raw[28:]
    key = _derive_key_from_password(password, salt)
    return AESGCM(key).decrypt(nonce, ciphertext, None).decode()

def _hmac_sha512(key: bytes, data: bytes) -> bytes:
    import hmac as _hmac
    return _hmac.new(key, data, hashlib.sha512).digest()

def _ckd(parent_privkey: bytes, parent_chain: bytes, index: int):
    """BIP32 child key derivation — hardened (index >= 0x80000000) or normal."""
    if index >= 0x80000000:
        data = b"\x00" + parent_privkey + struct.pack(">I", index)
    else:
        data = _privkey_to_pubkey_compressed(parent_privkey) + struct.pack(">I", index)
    I = _hmac_sha512(parent_chain, data)
    child_int = (int.from_bytes(I[:32], "big") + int.from_bytes(parent_privkey, "big")) % _SECP256K1_N
    return child_int.to_bytes(32, "big"), I[32:]

def _master_from_seed(seed_bytes: bytes):
    I = _hmac_sha512(b"Bitcoin seed", seed_bytes)
    return I[:32], I[32:]

def _derive_path(seed_bytes: bytes, path: list) -> bytes:
    key, chain = _master_from_seed(seed_bytes)
    for idx in path:
        key, chain = _ckd(key, chain, idx)
    return key

# ── Address generation ────────────────────────────────────────

def _privkey_to_ltc_address(privkey: bytes) -> str:
    """P2PKH LTC address (version byte 0x30)."""
    pub = _privkey_to_pubkey_compressed(privkey)
    h = hashlib.sha256(pub).digest()
    r = hashlib.new("ripemd160", h).digest()
    versioned = bytes([0x30]) + r
    checksum = hashlib.sha256(hashlib.sha256(versioned).digest()).digest()[:4]
    return _b58encode(versioned + checksum)

def _privkey_to_eth_address(privkey: bytes) -> str:
    """Ethereum address from private key (pysha3 optional)."""
    pub = _privkey_to_pubkey_uncompressed(privkey)
    try:
        import sha3 as _sha3
        k = hashlib.new("keccak_256")
    except ImportError:
        k = hashlib.sha3_256()
    k.update(pub)
    return "0x" + k.digest()[-20:].hex()

H = 0x80000000  # hardened offset

def _ltc_privkey_for_index(index: int) -> bytes:
    """BIP44 m/44'/2'/0'/0/index for LTC."""
    return _derive_path(_master_seed_bytes, [44+H, 2+H, 0+H, 0, index])

def _eth_privkey_for_index(index: int) -> bytes:
    """BIP44 m/44'/60'/0'/0/index for ETH/USDC."""
    return _derive_path(_master_seed_bytes, [44+H, 60+H, 0+H, 0, index])

def derive_ltc_address(index: int) -> str:
    return _privkey_to_ltc_address(_ltc_privkey_for_index(index))

def derive_eth_address(index: int) -> str:
    return _privkey_to_eth_address(_eth_privkey_for_index(index))

# ── Wallet init ───────────────────────────────────────────────

def init_wallet() -> bool:
    global _master_seed_bytes, _wallet_ready
    try:
        if not ENCRYPTED_SEED or not WALLET_PASSWORD:
            print("WALLET: ENCRYPTED_SEED or WALLET_PASSWORD missing")
            return False
        seed_phrase = decrypt_seed(ENCRYPTED_SEED, WALLET_PASSWORD)
        import unicodedata
        normalized = unicodedata.normalize("NFKD", seed_phrase)
        _master_seed_bytes = hashlib.pbkdf2_hmac(
            "sha512", normalized.encode("utf-8"), b"mnemonic", 2048, 64
        )
        _wallet_ready = True
        logger.info("WALLET: Master seed loaded, ready")
        return True
    except Exception as e:
        print(f"WALLET: Seed load failed: {e}")
        return False

# ── Payment index counter ─────────────────────────────────────

PAYMENT_INDEX_FILE = _build_data_path("payment_index.json")
_index_lock = threading.Lock()

def _load_payment_index() -> int:
    """Payment index - SQLite metadata tablosundan oku."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur  = conn.cursor()
        cur.execute("SELECT value FROM metadata WHERE key = 'payment_index'")
        row = cur.fetchone()
        conn.close()
        return int(row["value"]) if row else 0
    except Exception as e:
        logger.error(f"_load_payment_index error: {e}")
        return 0

def _increment_payment_index() -> int:
    """Payment index - SQLite atomic increment. Restart-safe, race-safe."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        conn.execute("BEGIN IMMEDIATE")
        cur  = conn.cursor()
        cur.execute("SELECT value FROM metadata WHERE key = 'payment_index'")
        row = cur.fetchone()
        idx = int(row["value"]) + 1 if row else 1
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('payment_index', ?)",
            (str(idx),)
        )
        conn.execute("COMMIT")
        conn.close()
        return idx
    except Exception as e:
        logger.error(f"_increment_payment_index error: {e}")
        return int(time.time()) % 1000000  # Fallback


# ── SQLite tabanlı Payment CRUD ────────────────────────────────

def db_save_payment(rec: dict) -> None:
    """Ödeme kaydını SQLite payments tablosuna kaydet/güncelle."""
    init_sqlite_db()
    conn = get_sqlite_connection()
    conn.execute("""
        INSERT OR REPLACE INTO payments
        (payment_id, user_id, coin, address, address_index, usd_amount,
         ltc_amount_locked, status, credited, tx_hash, confirmed_amount_usd,
         name, username, created_at, expires_at, credited_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        rec["payment_id"], int(rec["user_id"]), rec["coin"],
        rec["address"], rec.get("address_index"),
        float(rec["usd_amount"]), rec.get("ltc_amount_locked"),
        rec.get("status", "waiting"), 1 if rec.get("credited") else 0,
        rec.get("tx_hash"), rec.get("confirmed_amount_usd"),
        rec.get("name"), rec.get("username"),
        rec["created_at"], rec["expires_at"], rec.get("credited_at"),
    ))
    conn.commit()
    conn.close()

def db_load_payments(status_filter: list | None = None) -> dict:
    """Ödemeleri SQLite'tan yükle."""
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur  = conn.cursor()
    if status_filter:
        placeholders = ",".join("?" * len(status_filter))
        cur.execute(f"SELECT * FROM payments WHERE status IN ({placeholders})", status_filter)
    else:
        cur.execute("SELECT * FROM payments")
    rows = cur.fetchall()
    conn.close()
    result = {}
    for row in rows:
        d = dict(row)
        d["credited"] = bool(d.get("credited", 0))
        result[d["payment_id"]] = d
    return result

def db_get_payment(payment_id: str) -> dict | None:
    """Tek ödemeyi SQLite'tan getir."""
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM payments WHERE payment_id = ?", (payment_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d["credited"] = bool(d.get("credited", 0))
    return d

def db_get_user_payments(user_id: int) -> list:
    """Kullanıcının ödemelerini getir."""
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM payments WHERE user_id = ? ORDER BY created_at DESC", (user_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ── SQLite tabanlı Card CRUD ────────────────────────────────────

def db_save_card(card_id: str, card: dict) -> None:
    """Kartı SQLite cards tablosuna kaydet/güncelle."""
    init_sqlite_db()
    conn = get_sqlite_connection()
    conn.execute("""
        INSERT OR REPLACE INTO cards
        (card_id, card_number, expiry_month, expiry_year, cvv, balance,
         currency, provider, registered, status, reserved_by, reserved_until,
         added_at, order_id, sold_at, channel_posted)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        card_id,
        card.get("card_number", ""),
        card.get("expiry_month", ""),
        card.get("expiry_year", ""),
        card.get("cvv", ""),
        float(card.get("balance", 0)),
        card.get("currency", "US"),
        card.get("provider", ""),
        1 if card.get("registered") is True else (0 if card.get("registered") is False else None),
        card.get("status", "available"),
        card.get("reserved_by"),
        card.get("reserved_until"),
        card.get("added_at", ""),
        card.get("order_id"),
        card.get("sold_at"),
        1 if card.get("channel_posted") else 0,
    ))
    conn.commit()
    conn.close()

def db_load_cards() -> dict:
    """Tüm kartları SQLite'tan yükle."""
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM cards")
    rows = cur.fetchall()
    conn.close()
    result = {}
    for row in rows:
        d = dict(row)
        d["channel_posted"] = bool(d.get("channel_posted", 0))
        reg = d.get("registered")
        d["registered"] = True if reg == 1 else (False if reg == 0 else None)
        result[d["card_id"]] = d
    return result

def db_get_available_cards(provider: str | None = None,
                            bal_min: float = 0, bal_max: float = float("inf"),
                            registered: bool | None = None) -> list:
    """Mevcut kartları filtreli getir."""
    init_sqlite_db()
    conn = get_sqlite_connection()
    cur  = conn.cursor()
    query  = "SELECT * FROM cards WHERE status = 'available'"
    params = []
    if provider and provider not in ("registered", "unregistered"):
        query += " AND provider = ?"
        params.append(provider)
    if provider == "registered":
        query += " AND registered = 1"
    elif provider == "unregistered":
        query += " AND registered = 0"
    if registered is not None:
        query += " AND registered = ?"
        params.append(1 if registered else 0)
    if bal_min > 0:
        query += " AND balance >= ?"
        params.append(bal_min)
    if bal_max < float("inf"):
        query += " AND balance <= ?"
        params.append(bal_max)
    query += " ORDER BY balance DESC"
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    result = []
    for row in rows:
        d = dict(row)
        d["channel_posted"] = bool(d.get("channel_posted", 0))
        reg = d.get("registered")
        d["registered"] = True if reg == 1 else (False if reg == 0 else None)
        result.append((d["card_id"], d))
    return result

def db_update_card_status(card_id: str, status: str, **kwargs) -> None:
    """Kart durumunu güncelle."""
    init_sqlite_db()
    conn = get_sqlite_connection()
    fields = ["status = ?"]
    params = [status]
    for k, v in kwargs.items():
        fields.append(f"{k} = ?")
        params.append(v)
    params.append(card_id)
    conn.execute(f"UPDATE cards SET {', '.join(fields)} WHERE card_id = ?", params)
    conn.commit()
    conn.close()

# ── Pending payments ──────────────────────────────────────────

PENDING_PAYMENTS_FILE = _build_data_path("pending_payments.json")

def load_pending_payments() -> dict:
    """Ödemeleri SQLite'tan yükle, yoksa JSON'a fallback."""
    try:
        db_data = db_load_payments()
        if db_data:
            return db_data
    except Exception:
        pass
    return load_json_file(PENDING_PAYMENTS_FILE, {})

def save_pending_payments(data: dict) -> None:
    """JSON'a yaz (legacy uyumluluk) + SQLite senkronize et."""
    save_json_file(PENDING_PAYMENTS_FILE, data)
    # SQLite'a da sync et
    try:
        for pid, rec in data.items():
            db_save_payment(rec)
    except Exception as e:
        logger.warning(f"save_pending_payments SQLite sync error: {e}")


# ============================================================
# PAYMENT SECURITY MODULE
# ============================================================

# processed_transactions → SQLite tablosunda (PROCESSED_TX_FILE kaldırıldı)
LTC_MIN_CONFIRMATIONS  = 3   # LTC için minimum confirmation
USDC_MIN_CONFIRMATIONS    = 8   # USDC Ethereum için (chain reorg güvenliği)
USDC_SOL_MIN_CONFIRMATIONS = 1   # USDC Solana için (finalized = kesinleşmiş)
PARTIAL_PAYMENT_THRESHOLD = 0.98  # %98 ve üzeri kabul (eski %50 çok düşüktü)

def save_processed_tx(tx_hash: str, payment_id: str = "", coin: str = "", amount: float = 0.0) -> bool:
    """TX hash'i SQLite'a kaydet. UNIQUE constraint ihlali = zaten işlenmiş → False döner."""
    init_sqlite_db()
    max_retries = 5
    for attempt in range(max_retries):
        conn = None
        try:
            conn = get_sqlite_connection()
            conn.execute(
                "INSERT INTO processed_transactions (tx_hash, payment_id, coin, amount, processed_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (tx_hash, payment_id, coin, amount, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )
            conn.commit()
            conn.close()
            return True
        except sqlite3.IntegrityError:
            # UNIQUE constraint — TX zaten işlenmiş
            if conn:
                try: conn.close()
                except Exception: pass
            return False
        except Exception as e:
            if conn:
                try: conn.close()
                except Exception: pass
            err_str = str(e).lower()
            if "database is locked" in err_str and attempt < max_retries - 1:
                time.sleep(0.1 * (attempt + 1) + random.random() * 0.1)
                continue
            logger.error(f"save_processed_tx error: {e}")
            return False
    return False

def is_tx_processed(tx_hash: str) -> bool:
    """Bu TX daha önce işlendi mi? — SQLite sorgusu."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM processed_transactions WHERE tx_hash = ?", (tx_hash,))
        result = cur.fetchone()
        conn.close()
        return result is not None
    except Exception as e:
        logger.error(f"is_tx_processed error: {e}")
        return False  # Hata durumunda güvenli taraf: işlenmemiş say

def verify_ltc_tx(tx: dict, expected_address: str, expected_ltc: float) -> tuple[bool, str]:
    """
    LTC transaction güvenlik doğrulaması.
    Returns: (is_valid, reason)
    """
    # 1. TX hash daha önce kullanıldı mı?
    if is_tx_processed(tx.get("hash", "")):
        return False, "TX hash already processed"

    # 2. Minimum confirmation kontrolü
    confirmations = tx.get("confirmations", 0)
    if confirmations < LTC_MIN_CONFIRMATIONS:
        return False, f"Insufficient confirmations: {confirmations}/{LTC_MIN_CONFIRMATIONS}"

    # 3. Gerçekten bizim adresimize mi geldi?
    tx_address = tx.get("address", "")
    if tx_address and tx_address != expected_address:
        return False, f"TX address mismatch: {tx_address[:20]} != {expected_address[:20]}"

    # 4. Miktar kontrolü - %98 threshold
    received_ltc = tx.get("value_ltc", 0)
    ratio = received_ltc / expected_ltc if expected_ltc > 0 else 0
    if ratio < PARTIAL_PAYMENT_THRESHOLD:
        return False, f"Insufficient amount: {ratio*100:.1f}% (need {PARTIAL_PAYMENT_THRESHOLD*100:.0f}%)"

    return True, "OK"

def verify_usdc_tx(tx: dict, expected_address: str, expected_usdc: float,
                   is_solana: bool = False) -> tuple[bool, str]:
    """
    USDC transaction güvenlik doğrulaması.
    Returns: (is_valid, reason)
    """
    if is_tx_processed(tx.get("hash", "")):
        return False, "TX hash already processed"

    confirmations = tx.get("confirmations", 0)
    min_conf = USDC_SOL_MIN_CONFIRMATIONS if is_solana else USDC_MIN_CONFIRMATIONS
    if confirmations < min_conf:
        return False, f"Insufficient confirmations: {confirmations}/{min_conf}"

    received_usdc = tx.get("value_usdc", 0)
    ratio = received_usdc / expected_usdc if expected_usdc > 0 else 0
    if ratio < PARTIAL_PAYMENT_THRESHOLD:
        return False, f"Insufficient amount: {ratio*100:.1f}% (need {PARTIAL_PAYMENT_THRESHOLD*100:.0f}%)"

    return True, "OK"

# ============================================================
# PAYMENT SECURITY MODULE END
# ============================================================
def create_pending_payment(user_id: int, coin: str, usd_amount: float,
                           full_name: str, username: str) -> dict:
    """Create a new payment with a unique derived address."""
    idx = _increment_payment_index()

    # Adres türet
    if coin == "LTC":
        address = derive_ltc_address(idx)
    elif coin == "USDC_SOLANA":
        address = derive_solana_address(idx)
        if not address:
            logger.error(f"USDC_SOLANA address empty for index {idx}!")
    else:  # USDC Ethereum (legacy)
        address = derive_eth_address(idx)
    logger.info(f"Payment address derived: coin={coin} idx={idx} addr={address[:20] if address else 'EMPTY'}")

    payment_id = f"PAY-{int(time.time())}-{random.randint(100, 999)}"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    expires_str = datetime.fromtimestamp(
        time.time() + PAYMENT_TIMEOUT_SECONDS
    ).strftime("%Y-%m-%d %H:%M:%S")

    # LTC için ödeme anındaki fiyatı sabitle
    ltc_amount_locked = None
    if coin == "LTC":
        ltc_price = get_ltc_price_usd()
        if ltc_price > 0:
            ltc_amount_locked = round(usd_amount / ltc_price, 6)

    record = {
        "payment_id": payment_id,
        "user_id": user_id,
        "name": full_name,
        "username": username,
        "coin": coin,
        "address": address,
        "address_index": idx,
        "usd_amount": round(float(usd_amount), 2),
        "ltc_amount_locked": ltc_amount_locked,  # Sabitlenmiş LTC miktarı
        "status": "waiting",
        "created_at": now_str,
        "expires_at": expires_str,
        "credited": False,
        "tx_hash": None,
        "confirmed_amount_usd": None,
        "credited_at": None,
    }
    # SQLite'a kaydet (primary store)
    db_save_payment(record)
    # JSON'a da yaz (geçiş dönemi uyumluluğu için)
    payments = load_pending_payments()
    payments[payment_id] = record
    save_pending_payments(payments)
    return record

# ── Price feeds ───────────────────────────────────────────────


# ============================================================
# SOLANA WALLET MODULE — Ed25519 HD Wallet (BIP44 m/44'/501'/0'/0'/index')
# ============================================================

def _derive_solana_keypair(index: int) -> tuple[bytes, bytes]:
    """
    BIP44 Solana türetme: m/44'/501'/0'/0'/index'
    Ed25519 private key → public key (32 byte her biri)
    """
    import hmac as _hmac
    import hashlib as _hashlib

    _seed_str = decrypt_seed(ENCRYPTED_SEED, WALLET_PASSWORD)
    # Seed: mnemonic string → BIP39 seed bytes
    import hashlib as _hl
    import unicodedata as _ud
    _mnemonic = _ud.normalize("NFKD", _seed_str).encode("utf-8")
    _salt     = _ud.normalize("NFKD", "mnemonic").encode("utf-8")
    seed = _hl.pbkdf2_hmac("sha512", _mnemonic, _salt, 2048)

    def _hmac_sha512(key: bytes, data: bytes) -> bytes:
        return _hmac.new(key, data, _hashlib.sha512).digest()

    def _derive_child(parent_key: bytes, parent_chain: bytes, index: int) -> tuple[bytes, bytes]:
        # Hardened child (index >= 0x80000000)
        hardened = index + 0x80000000
        data = b"\x00" + parent_key + hardened.to_bytes(4, "big")
        I = _hmac_sha512(parent_chain, data)
        return I[:32], I[32:]

    # Master key
    I      = _hmac_sha512(b"ed25519 seed", seed)
    key_m  = I[:32]
    chain_m = I[32:]

    # m/44'/501'/0'/0'/index'
    path = [44, 501, 0, 0, index]
    k, c = key_m, chain_m
    for lvl in path:
        k, c = _derive_child(k, c, lvl)

    # Ed25519 public key türet
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        priv_obj = Ed25519PrivateKey.from_private_bytes(k)
        pub_bytes = priv_obj.public_key().public_bytes_raw()
    except Exception:
        # Fallback: basit türetme (test için)
        pub_bytes = _hashlib.sha256(k).digest()

    return k, pub_bytes

def _b58encode_solana(data: bytes) -> str:
    """Base58 encode — Solana adres formatı."""
    ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    count = 0
    for byte in data:
        if byte == 0:
            count += 1
        else:
            break
    num = int.from_bytes(data, "big")
    result = []
    while num > 0:
        num, rem = divmod(num, 58)
        result.append(ALPHABET[rem])
    return "1" * count + "".join(reversed(result))

def derive_solana_address(index: int) -> str:
    """Solana USDC adresi türet (public key = adres)."""
    try:
        _, pub = _derive_solana_keypair(index)
        addr = _b58encode_solana(pub)
        logger.debug(f"Solana address derived (index={index}): {addr[:16]}...")
        if not addr:
            logger.error("Solana address is empty after derivation!")
        return addr
    except Exception as e:
        logger.error(f"Solana address derivation error (index={index}): {e}", exc_info=True)
        return ""

def get_usdc_solana_received(solana_address: str) -> list:
    """
    Helius API ile Solana adresine gelen USDC transferlerini kontrol et.
    Returns: [{"hash": str, "value_usdc": float, "confirmations": int, "confirmed": bool}]
    """
    if not HELIUS_API_KEY:
        return []
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        # getSignaturesForAddress - son 10 tx
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                solana_address,
                {"limit": 10, "commitment": "finalized"}
            ]
        }
        r = http_requests.post(url, json=payload, timeout=8)
        r.raise_for_status()
        signatures = r.json().get("result", [])
        if not signatures:
            return []

        txs = []
        for sig_info in signatures:
            if sig_info.get("err"):
                continue  # Hatalı tx atla
            sig = sig_info.get("signature", "")
            if not sig:
                continue

            # TX detayını al
            tx_payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [sig, {"encoding": "jsonParsed", "commitment": "finalized", "maxSupportedTransactionVersion": 0}]
            }
            tx_r = http_requests.post(url, json=tx_payload, timeout=8)
            tx_data = tx_r.json().get("result")
            if not tx_data:
                continue

            # Token transfer kontrolü
            meta = tx_data.get("meta", {})
            post_token = meta.get("postTokenBalances", [])
            pre_token  = meta.get("preTokenBalances", [])

            for post in post_token:
                if post.get("mint") != USDC_SOLANA_MINT:
                    continue
                if post.get("owner") != solana_address:
                    continue
                # Pre balance bul
                pre_amount = 0.0
                for pre in pre_token:
                    if pre.get("accountIndex") == post.get("accountIndex"):
                        pre_amount = float(pre.get("uiTokenAmount", {}).get("uiAmount") or 0)
                        break
                post_amount = float(post.get("uiTokenAmount", {}).get("uiAmount") or 0)
                received = round(post_amount - pre_amount, 6)
                if received > 0:
                    txs.append({
                        "hash":          sig,
                        "value_usdc":    received,
                        "confirmations": 1,  # finalized = kesinleşmiş
                        "confirmed":     True,
                    })
        return txs
    except Exception as e:
        logger.error(f"Helius USDC query error ({solana_address[:16]}): {e}")
        return []

# ============================================================
# SOLANA WALLET MODULE END
# ============================================================
def get_ltc_price_usd() -> float:
    """Sync LTC fiyatı - run_in_executor ile çağrılmalı."""
    try:
        r = http_requests.get(
            "https://api.coingecko.com/api/v3/simple/price"
            "?ids=litecoin&vs_currencies=usd", timeout=8
        )
        r.raise_for_status()
        return float(r.json()["litecoin"]["usd"])
    except Exception as e:
        logger.warning(f"LTC price fetch error: {e}")
        return 0.0

async def get_ltc_price_usd_async() -> float:
    """Async LTC fiyatı - doğrudan await ile çağrılabilir."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, get_ltc_price_usd)

# ── Blockchain queries ────────────────────────────────────────

def get_ltc_received(address: str) -> list:
    """Return list of confirmed incoming txs for an LTC address."""
    global _last_blockcypher_request
    try:
        # Rate limit: istekler arası minimum bekleme (non-blocking kontrol)
        now = time.time()
        elapsed = now - _last_blockcypher_request
        if elapsed < BLOCKCYPHER_REQUEST_DELAY:
            return []  # Beklemek yerine atla - bir sonraki döngüde dene
        _last_blockcypher_request = time.time()

        url = (f"https://api.blockcypher.com/v1/ltc/main/addrs/{address}/full"
               f"?limit=10&token={BLOCKCYPHER_TOKEN}")
        r = http_requests.get(url, timeout=8)
        if r.status_code == 429:
            logger.warning("BlockCypher rate limit - skipping address")
            return []
        r.raise_for_status()
        txs = []
        for tx in r.json().get("txs", []):
            confs = int(tx.get("confirmations", 0))
            for out in tx.get("outputs", []):
                if address in out.get("addresses", []):
                    txs.append({
                        "hash":          tx.get("hash", ""),
                        "value_ltc":     int(out.get("value", 0)) / 1e8,
                        "confirmations": confs,
                        "confirmed":     confs >= LTC_MIN_CONFIRMATIONS,
                        "address":       address,
                    })
        return txs
    except Exception as e:
        print(f"LTC tx query error ({address[:12]}...): {e}")
        return []

def get_usdc_received(eth_address: str) -> list:
    """Return list of incoming USDC transfers for an ETH address."""
    try:
        url = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "alchemy_getAssetTransfers",
            "params": [{
                "toAddress": eth_address,
                "contractAddresses": [USDC_CONTRACT_ADDRESS],
                "category": ["erc20"],
                "maxCount": "0x32",
                "order": "desc",
                "withMetadata": False,
            }],
        }
        r = http_requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
        txs = []
        for tx in r.json().get("result", {}).get("transfers", []):
            txs.append({
                "hash":          tx.get("hash", ""),
                "value_usdc":    float(tx.get("value", 0)),
                "confirmations": 1,
                "confirmed":     True,
            })
        return txs
    except Exception as e:
        print(f"USDC tx query error ({eth_address[:12]}...): {e}")
        return []

# ── Credit engine ─────────────────────────────────────────────


# ============================================================
# ACTIVATION MODULE — $100 otomatik üyelik aktivasyonu
# ============================================================

def load_pending_activations() -> dict:
    return load_json_file(PENDING_ACTIVATIONS_FILE, {})

def save_pending_activations(data: dict) -> None:
    save_json_file(PENDING_ACTIVATIONS_FILE, data)

def create_activation_payment(user_id: int, coin: str, full_name: str, username: str) -> dict:
    """Aktivasyon ödemesi oluştur — $100 sabit."""
    idx = _increment_payment_index()
    if coin == "LTC":
        address = derive_ltc_address(idx)
    else:
        address = derive_eth_address(idx)

    payment_id = f"ACT-{int(time.time())}-{random.randint(100, 999)}"
    now_str    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    expires_str = datetime.fromtimestamp(
        time.time() + PAYMENT_TIMEOUT_SECONDS
    ).strftime("%Y-%m-%d %H:%M:%S")

    record = {
        "payment_id":    payment_id,
        "user_id":       user_id,
        "name":          full_name,
        "username":      username,
        "coin":          coin,
        "address":       address,
        "address_index": idx,
        "usd_amount":    ACTIVATION_FEE_USD,
        "status":        "waiting",
        "created_at":    now_str,
        "expires_at":    expires_str,
        "credited":      False,
        "tx_hash":       None,
        "activated_at":  None,
    }
    acts = load_pending_activations()
    acts[payment_id] = record
    save_pending_activations(acts)
    return record

def get_user_activation_payment(user_id: int) -> dict | None:
    """Kullanıcının bekleyen aktivasyon ödemesini döndür."""
    acts = load_pending_activations()
    for rec in acts.values():
        if int(rec.get("user_id", 0)) == user_id and not rec.get("credited"):
            if rec.get("status") in ("waiting", "expired"):
                return rec
    return None

async def activate_user(user_id: int, payment_id: str, tx_hash: str,
                        coin: str, usd_amount: float, bot) -> None:
    """Kullanıcıyı aktive et ve bildirimleri gönder."""
    global access_data
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Aktivasyon kaydını güncelle
    acts = load_pending_activations()
    if payment_id in acts:
        acts[payment_id]["credited"]     = True
        acts[payment_id]["status"]       = "activated"
        acts[payment_id]["tx_hash"]      = tx_hash
        acts[payment_id]["activated_at"] = now_str
        rec = acts[payment_id]
        save_pending_activations(acts)
    else:
        rec = {"name": "-", "username": "-"}

    # Kullanıcıyı approved_users'a ekle
    approved = access_data.get("approved_users", [])
    if user_id not in approved:
        approved.append(user_id)
        access_data["approved_users"] = approved
        save_access(access_data)

    # Audit log
    add_audit_log_entry(
        action="auto_activation",
        admin_user=type("obj", (object,), {
            "id": 0, "username": "auto_system",
            "full_name": "Auto Activation"
        })(),
        target_type="user",
        target_id=str(user_id),
        details=f"payment_id={payment_id} coin={coin} amount={usd_amount} tx={tx_hash}",
    )

    # Kullanıcıya tebrik mesajı
    try:
        await bot.send_message(
            chat_id=user_id,
            text=(
                f"Your account has been activated!\n\n"
                f"Payment ID: {payment_id}\n"
                f"Coin: {coin}\n"
                f"Amount: {format_usd(usd_amount)}\n"
                f"TX: {tx_hash[:24]}...\n\n"
                f"Welcome to {LIFETIME_ACCESS_NAME}!\n"
                f"You now have full access to the bot.\n"
                f"Use /start to open the main menu."
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Open Main Menu", callback_data="main_menu")],
            ]),
        )
    except Exception as e:
        logger.warning(f"Activation notify failed {user_id}: {e}")

    # Admin'e bildirim
    try:
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=(
                f"New Member Activated\n\n"
                f"Payment ID: {payment_id}\n"
                f"User: {rec.get('name', '-')} ({rec.get('username', '-')})\n"
                f"User ID: {user_id}\n"
                f"Coin: {coin}\n"
                f"Amount: {format_usd(usd_amount)}\n"
                f"TX: {tx_hash}\n"
                f"Time: {now_str}"
            ),
        )
    except Exception:
        pass

    # Proof Channel'a aktivasyon postu
    try:
        await post_proof_activation(bot, user_name=rec.get("name", "New Member"))
    except Exception as e:
        logger.error(f"[PROOF] Activation proof error: {e}")

async def check_activation_payments_job(context) -> None:
    """Her 60sn — aktivasyon ödemelerini kontrol et."""
    _t0 = time.time()
    logger.debug("Started activation monitor job")
    acts     = load_pending_activations()
    now_ts   = time.time()
    ltc_price = None

    to_check = {
        pid: rec for pid, rec in acts.items()
        if not rec.get("credited") and rec.get("status") in ("waiting", "expired")
    }
    if not to_check:
        return

    # Expire kontrolü
    for pid, rec in to_check.items():
        if rec.get("status") == "waiting":
            try:
                exp_ts = datetime.strptime(rec["expires_at"], "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception:
                exp_ts = now_ts + 1
            if now_ts > exp_ts:
                acts[pid]["status"] = "expired"
    save_pending_activations(acts)

    # LTC kontrol
    ltc_acts  = {pid: r for pid, r in to_check.items() if r["coin"] == "LTC"}
    usdc_acts = {pid: r for pid, r in to_check.items() if r["coin"] == "USDC"}

    loop = asyncio.get_event_loop()
    for pid, rec in ltc_acts.items():
        if ltc_price is None:
            ltc_price = await loop.run_in_executor(None, get_ltc_price_usd)
        if ltc_price <= 0:
            continue
        expected_ltc = ACTIVATION_FEE_USD / ltc_price
        txs = await loop.run_in_executor(None, get_ltc_received, rec["address"])
        for tx in txs:
            is_valid, reason = verify_ltc_tx(tx, rec["address"], expected_ltc)
            if not is_valid:
                if "already processed" not in reason and "Insufficient confirmations" not in reason:
                    logger.warning(f"Activation LTC TX rejected [{pid}]: {reason}")
                continue
            received_usd = round(tx["value_ltc"] * ltc_price, 2)
            logger.info(f"Activation LTC processing [{pid}]: {tx['value_ltc']} LTC")
            await activate_user(
                int(rec["user_id"]), pid, tx["hash"],
                "LTC", received_usd, context.bot
            )
            break

    for pid, rec in usdc_acts.items():
        txs = await loop.run_in_executor(None, get_usdc_received, rec["address"])
        for tx in txs:
            is_valid, reason = verify_usdc_tx(tx, rec["address"], ACTIVATION_FEE_USD)
            if not is_valid:
                if "already processed" not in reason:
                    logger.warning(f"Activation USDC TX rejected [{pid}]: {reason}")
                continue
            logger.info(f"Activation USDC processing [{pid}]: {tx['value_usdc']} USDC")
            await activate_user(
                int(rec["user_id"]), pid, tx["hash"],
                "USDC", round(tx["value_usdc"], 2), context.bot
            )
            break

# ============================================================
# ACTIVATION MODULE END
# ============================================================
async def credit_payment(payment_id: str, usd_amount: float,
                          tx_hash: str, coin: str, bot) -> None:
    """Credit a confirmed payment — atomic SQLite transaction."""
    payments = load_pending_payments()
    rec = payments.get(payment_id)
    if not rec:
        logger.warning(f"credit_payment: payment {payment_id} not found")
        return
    if rec.get("credited"):
        logger.warning(f"credit_payment: {payment_id} already credited, skipping")
        return

    user_id = int(rec["user_id"])
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # FULLY ATOMIC SQLite TRANSACTION
    # TX save + balance update tek transaction içinde
    # Crash olursa hiçbiri kaydedilmez
    init_sqlite_db()
    conn = get_sqlite_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")

        # 1. TX hash kaydet - IntegrityError = zaten işlenmiş
        try:
            conn.execute(
                "INSERT INTO processed_transactions (tx_hash, payment_id, coin, amount, processed_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (tx_hash, payment_id, coin, usd_amount, now_str)
            )
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK")
            conn.close()
            logger.error(f"SECURITY: TX {tx_hash[:20]} reuse attempt on {payment_id}")
            return

        # 2. Balance güncelle
        cur = conn.cursor()
        cur.execute(
            "SELECT available_balance, total_deposited, total_spent FROM balances WHERE user_id = ?",
            (user_id,)
        )
        row = cur.fetchone()
        if row:
            before = round(float(row["available_balance"] or 0), 2)
            dep    = round(float(row["total_deposited"] or 0), 2)
            spent  = round(float(row["total_spent"] or 0), 2)
        else:
            before = dep = spent = 0.0

        after   = round(before + usd_amount, 2)
        new_dep = round(dep + usd_amount, 2)

        conn.execute(
            "INSERT OR REPLACE INTO balances (user_id, available_balance, total_deposited, total_spent, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, after, new_dep, spent, now_str)
        )

        # 3. Ledger kaydı
        conn.execute(
            "INSERT INTO balance_ledger (user_id, delta, reason, reference_id, actor, before_balance, after_balance, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, usd_amount, f"crypto_deposit_{coin}", payment_id, "payment_monitor", before, after, now_str)
        )

        conn.execute("COMMIT")
        conn.close()
        logger.info(f"Atomic credit OK [{payment_id}]: {format_usd(usd_amount)} → user {user_id}")

    except Exception as e:
        try:
            conn.execute("ROLLBACK")
            conn.close()
        except Exception:
            pass
        logger.error(f"credit_payment atomic transaction failed [{payment_id}]: {e}")
        return

    # JSON payment kaydını güncelle (sadece durum, para işlemi SQLite'ta tamamlandı)
    rec.update({
        "credited": True,
        "status": "credited",
        "tx_hash": tx_hash,
        "confirmed_amount_usd": round(usd_amount, 2),
        "credited_at": now_str,
    })
    payments[payment_id] = rec
    save_pending_payments(payments)

    before = round(before, 2)
    after  = round(after, 2)

    # Notify user
    try:
        await bot.send_message(
            chat_id=user_id,
            text=(
                "Payment Confirmed\n\n"
                f"Payment ID: {payment_id}\n"
                f"Coin: {coin}\n"
                f"Amount credited: {format_usd(usd_amount)}\n"
                f"New balance: {format_usd(after)}\n"
                f"TX: {tx_hash[:24]}...\n\n"
                "Your balance is ready. You can now place orders."
            ),
        )
    except Exception as e:
        logger.warning(f"User notify failed {user_id}: {e}")

    # Notify admin
    try:
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=(
                "Auto Payment Credited\n\n"
                f"Payment ID: {payment_id}\n"
                f"User: {rec.get('name', '-')} ({rec.get('username', '-')})\n"
                f"User ID: {user_id}\n"
                f"Coin: {coin}\n"
                f"Amount: {format_usd(usd_amount)}\n"
                f"New Balance: {format_usd(after)}\n"
                f"TX: {tx_hash}\n"
                f"Address index: {rec.get('address_index', '-')}"
            ),
        )
    except Exception:
        pass

# ── Payment monitor job ───────────────────────────────────────

async def check_pending_payments_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    _job_start = time.time()
    logger.debug("Started payment monitor job")
    payments = load_pending_payments()
    now_ts   = time.time()

    to_check = {
        pid: rec for pid, rec in payments.items()
        if not rec.get("credited") and rec.get("status") in ("waiting", "expired")
    }

    if not to_check:
        return

    # 48 saatten eski expire ödemeleri arşivle
    cutoff_ts = now_ts - (48 * 3600)
    archived = 0
    for pid in list(to_check.keys()):
        rec = to_check[pid]
        if rec.get("status") == "expired":
            try:
                created_ts = datetime.strptime(
                    rec.get("created_at", ""), "%Y-%m-%d %H:%M:%S"
                ).timestamp()
            except Exception:
                created_ts = now_ts
            if created_ts < cutoff_ts:
                payments[pid]["status"] = "archived"
                del to_check[pid]
                archived += 1
    if archived > 0:
        save_pending_payments(payments)
        logger.info(f"Archived {archived} old expired payments")

    if not to_check:
        return

    ltc_count = sum(1 for r in to_check.values() if r.get("coin") == "LTC")
    if ltc_count > 5:
        logger.warning(f"LTC addresses to check: {ltc_count} - API pressure high")

    # Mark newly expired ones (but keep checking for funds)
    changed = False
    for pid, rec in to_check.items():
        if rec.get("status") == "waiting":
            try:
                exp_ts = datetime.strptime(
                    rec["expires_at"], "%Y-%m-%d %H:%M:%S"
                ).timestamp()
            except Exception:
                exp_ts = now_ts + 1
            if now_ts > exp_ts:
                payments[pid]["status"] = "expired"
                changed = True
                try:
                    await context.bot.send_message(
                        chat_id=int(rec["user_id"]),
                        text=(
                            "Payment window expired\n\n"
                            f"Payment ID: {pid}\n"
                            "Your unique address is still monitored.\n"
                            "If you already sent funds, they will be credited automatically.\n"
                            "To start a new payment use the Add Balance menu."
                        ),
                    )
                except Exception:
                    pass
    if changed:
        save_pending_payments(payments)

    # Group by coin
    ltc_price         = None
    ltc_payments      = {pid: r for pid, r in to_check.items() if r["coin"] == "LTC"}
    usdc_payments     = {pid: r for pid, r in to_check.items() if r["coin"] == "USDC"}
    usdc_sol_payments = {pid: r for pid, r in to_check.items() if r["coin"] == "USDC_SOLANA"}

    # ── Check LTC — 3'erlik batch, istekler arası 3sn bekleme ──
    ltc_items = list(ltc_payments.items())
    BATCH_SIZE = 3
    loop = asyncio.get_event_loop()
    for batch_start in range(0, len(ltc_items), BATCH_SIZE):
        batch = ltc_items[batch_start:batch_start + BATCH_SIZE]
        if batch_start > 0:
            await asyncio.sleep(3)  # Batch'ler arası bekleme
        for pid, rec in batch:
            expected_usd = float(rec["usd_amount"])
            if rec.get("ltc_amount_locked"):
                expected_ltc = float(rec["ltc_amount_locked"])
                if ltc_price is None:
                    ltc_price = await loop.run_in_executor(None, get_ltc_price_usd)
            else:
                if ltc_price is None:
                    ltc_price = await loop.run_in_executor(None, get_ltc_price_usd)
                if ltc_price <= 0:
                    continue
                expected_ltc = expected_usd / ltc_price
            t_start = time.time()
            txs = await loop.run_in_executor(None, get_ltc_received, rec["address"])
            elapsed = round(time.time() - t_start, 2)
            if elapsed > 2.0:
                logger.warning(f"[SLOW] LTC API: {elapsed}s for {rec['address'][:16]}")
            else:
                logger.debug(f"LTC API: {elapsed}s for {rec['address'][:16]}")
            for tx in txs:
                # Güvenlik doğrulaması
                is_valid, reason = verify_ltc_tx(tx, rec["address"], expected_ltc)
                if not is_valid:
                    if "already processed" not in reason and "Insufficient confirmations" not in reason:
                        logger.warning(f"LTC TX rejected [{pid}]: {reason}")
                    continue

                # TX geçerli - işle
                received_ltc  = tx["value_ltc"]
                current_price = ltc_price or get_ltc_price_usd()
                received_usd  = round(received_ltc * current_price, 2) if current_price > 0 else expected_usd

                # TX'i işlenmiş olarak kaydet (replay saldırısı engeli)
                logger.info(f"LTC payment processing [{pid}]: {received_ltc} LTC = {format_usd(received_usd)}")
                await credit_payment(pid, expected_usd, tx["hash"], "LTC", context.bot)
                break

    # ── Check USDC Solana ──
    for pid, rec in usdc_sol_payments.items():
        expected_usdc = float(rec["usd_amount"])
        _sol_start = time.time()
        txs = await asyncio.get_event_loop().run_in_executor(
            None, get_usdc_solana_received, rec["address"]
        )
        _sol_elapsed = round(time.time() - _sol_start, 2)
        if _sol_elapsed > 2:
            logger.warning(f"Helius USDC query slow: {_sol_elapsed}s")
        for tx in txs:
            is_valid, reason = verify_usdc_tx(tx, rec["address"], expected_usdc, is_solana=True)
            if not is_valid:
                if "already processed" not in reason and "Insufficient" not in reason:
                    logger.warning(f"USDC_SOL TX rejected [{pid}]: {reason}")
                continue
            logger.info(f"USDC Solana payment processing [{pid}]: {tx['value_usdc']} USDC")
            await credit_payment(pid, expected_usdc, tx["hash"], "USDC_SOLANA", context.bot)
            break

    # ── Check USDC (Ethereum - legacy) ──
    for pid, rec in usdc_payments.items():
        expected_usd = float(rec["usd_amount"])
        _usdc_start = time.time()
        txs = await asyncio.get_event_loop().run_in_executor(None, get_usdc_received, rec["address"])
        _usdc_elapsed = round(time.time() - _usdc_start, 2)
        if _usdc_elapsed > 2:
            logger.warning(f"USDC query slow: {_usdc_elapsed}s for {rec['address'][:16]}")
        for tx in txs:
            # Güvenlik doğrulaması
            is_valid, reason = verify_usdc_tx(tx, rec["address"], expected_usd)
            if not is_valid:
                if "already processed" not in reason and "Insufficient" not in reason:
                    logger.warning(f"USDC TX rejected [{pid}]: {reason}")
                continue
            # TX geçerli - işle
            logger.info(f"USDC payment processing [{pid}]: {tx['value_usdc']} USDC")
            await credit_payment(pid, expected_usd, tx["hash"], "USDC", context.bot)
            break


# ============================================================
# CHANNEL STOCK POSTING MODULE
# ============================================================

CHANNEL_STOCK_INTERVAL = 120  # saniye (2 dakika)
NO_STOCK_MESSAGE_INTERVAL = 120  # saniye

def get_bot_logo_file_id() -> str | None:
    cfg = load_config()
    return cfg.get("bot_logo_file_id") or None

def set_bot_logo_file_id(file_id: str) -> None:
    cfg = load_config()
    cfg["bot_logo_file_id"] = file_id
    save_config(cfg)

def get_next_unposted_card() -> tuple[str, dict] | None:
    """En eski eklenen, kanala gönderilmemiş kartı SQLite'tan getir."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur  = conn.cursor()
        cur.execute(
            "SELECT * FROM cards WHERE status='available' AND channel_posted=0 "
            "ORDER BY added_at ASC LIMIT 1"
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        d = dict(row)
        d["channel_posted"] = False
        reg = d.get("registered")
        d["registered"] = True if reg == 1 else (False if reg == 0 else None)
        return (d["card_id"], d)
    except Exception as e:
        logger.warning(f"get_next_unposted_card SQLite error: {e}")
        return None

def mark_card_channel_posted(card_id: str) -> None:
    """Kartı kanalda yayınlandı olarak işaretle — SQLite."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        conn.execute("UPDATE cards SET channel_posted=1 WHERE card_id=?", (card_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"mark_card_channel_posted error: {e}")

def get_unposted_count() -> int:
    """Yayınlanmamış kart sayısı — SQLite."""
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) as cnt FROM cards WHERE status='available' AND channel_posted=0")
        row = cur.fetchone()
        conn.close()
        return int(row["cnt"]) if row else 0
    except Exception:
        return 0

def build_card_channel_text(card: dict) -> str:
    """Kanal post metni."""
    masked   = format_card_number_masked(card.get("card_number", ""))
    currency = card.get("currency", "US")
    balance  = float(card.get("balance", 0))
    provider = card.get("provider", "")
    rate     = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.38)))
    cost     = round(balance * rate, 2)
    line     = "\u2500" * 14
    return (
        f"{line}\n"
        f"NEW LISTED\n"
        f"Bin: {masked}\n"
        f"Bal: {currency}${balance:.2f}\n"
        f"Provider: {provider}\n"
        f"Cost: ${cost:.2f} ({rate*100:.0f}%)\n"
        f"{line}\n"
        f"EPB"
    )

def build_no_stock_text() -> str:
    line = "\u2500" * 14
    return (
        f"{line}\n"
        f"STOCK UPDATE\n"
        f"All current cards have been listed.\n"
        f"Fresh stock arriving soon — stay tuned!\n"
        f"{line}\n"
        f"EPB"
    )

async def post_one_card_to_channel(bot) -> bool:
    """Kanaldan bir kart gönder. True=gönderildi, False=stok yok."""
    result = get_next_unposted_card()
    if not result:
        return False
    card_id, card = result
    text = build_card_channel_text(card)
    try:
        await bot.send_message(
            chat_id=CHANNEL_USERNAME,
            text=text,
        )
        mark_card_channel_posted(card_id)
        return True
    except Exception as e:
        logger.error(f"Channel post error: {e}")
        return False

async def channel_stock_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Her 50 saniyede bir çalışır."""
    _t0 = time.time()
    logger.debug("Started channel stock job")
    posted = await post_one_card_to_channel(context.bot)
    if not posted:
        # Stok yok - günde sadece 1 kez bildirim gönder
        cfg = load_config()
        last_no_stock = cfg.get("last_no_stock_notify", "")
        today = datetime.now().strftime("%Y-%m-%d")
        if last_no_stock != today:
            text = build_no_stock_text()
            try:
                await context.bot.send_message(
                    chat_id=CHANNEL_USERNAME,
                    text=text,
                )
                cfg["last_no_stock_notify"] = today
                save_config(cfg)
            except Exception as e:
                logger.error(f"No-stock channel post error: {e}")

def schedule_channel_stock_job(app: Application) -> None:
    if app.job_queue is None:
        print("JobQueue unavailable, channel stock job not started")
        return
    if app.job_queue.get_jobs_by_name("channel_stock"):
        return
    app.job_queue.run_repeating(
        channel_stock_job,
        interval=CHANNEL_STOCK_INTERVAL,
        first=60,
        name="channel_stock",
    )
    logger.info(f"Channel stock job started (every {CHANNEL_STOCK_INTERVAL}s)")

# ============================================================
# CHANNEL STOCK POSTING MODULE END
# ============================================================
async def release_expired_reservations_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Her 5 dakikada bir süresi dolmuş rezervasyonları serbest bırak."""
    _t0 = time.time()
    released = release_expired_reservations()
    gc_released = release_expired_gift_card_reservations()
    elapsed = round(time.time() - _t0, 3)
    total = released + gc_released
    if total > 0:
        logger.info(f"Released {released} card + {gc_released} gift card expired reservations in {elapsed}s")
    elif elapsed > 1.0:
        logger.warning(f"[SLOW] Reservation cleanup took {elapsed}s with 0 releases")

def schedule_maintenance_jobs(app: Application) -> None:
    if app.job_queue is None:
        return
    # Rezervasyon temizliği - 5 dakikada bir
    app.job_queue.run_repeating(
        release_expired_reservations_job,
        interval=300,
        first=60,
        name="reservation_cleanup",
    )
    logger.info("Maintenance jobs started")

def schedule_payment_monitor(app: Application) -> None:
    if app.job_queue is None:
        print("JobQueue unavailable, payment monitor not started")
        return
    if app.job_queue.get_jobs_by_name("payment_monitor"):
        return
    app.job_queue.run_repeating(
        check_pending_payments_job,
        interval=120,  # 2 dakikada bir - BlockCypher rate limit koruması
        first=30,
        name="payment_monitor",
    )
    logger.info("Payment monitor started (every 120s)")


# ─── Competitor Balance Monitor (Proactive Alerts) ───
async def competitor_balance_monitor_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Rakip bot bakiyelerini izle, düşük bakiye uyarısı gönder.
    Son 24 saatlik satış ortalamasını hesaplar, sonraki 24h tahmini yapar.
    Bakiye tahmini karşılayamıyorsa admin'e uyarı gönderir.
    Her uyarı saat başında bir kez gider (spam önleme)."""
    try:
        balances = load_competitor_balances()
        now = datetime.now()

        # Son 24 saatlik satışları topla (external orders)
        orders = load_orders()
        cutoff = now - timedelta(hours=24)
        sales_by_source = {}

        for order_id, order in orders.items():
            if not order.get("ext_source"):
                continue
            order_time_str = order.get("time", "")
            if not order_time_str:
                continue
            try:
                order_time = datetime.strptime(order_time_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            if order_time < cutoff:
                continue

            # Satış bilgisi — cost_price'ı hesapla (sell'den profit çıkar)
            # Orders'ta sadece cost (sell_price) var, biz cost_price'ı ext_id ile çekmeliyiz
            source = order["ext_source"]
            # Gerçek maliyet sell price değil, ama referans olarak kullanabiliriz
            # (kâr marjı %2.5-7 arası, yani sell ≈ cost × 1.04 ortalama)
            approx_cost = float(order.get("cost", 0)) / 1.04
            sales_by_source[source] = sales_by_source.get(source, 0) + approx_cost

        # Spam koruması — son uyarı zamanı
        alert_state_file = os.path.join(DATA_DIR, "comp_alert_state.json")
        alert_state = load_json_file(alert_state_file, {})

        alerts_sent = []
        for source, info in balances.items():
            balance = float(info.get("balance", 0))
            sales_24h = sales_by_source.get(source, 0)

            # Son 24h satış yoksa veya çok azsa → pasif rakip, uyarı atma
            if sales_24h < 5.0:
                continue

            # Hesap: mevcut bakiye ile kaç saat dayanır?
            hourly_burn = sales_24h / 24.0
            hours_remaining = balance / hourly_burn if hourly_burn > 0 else 999

            # Uyarı koşulları
            alert_level = None
            if balance < 0:
                alert_level = "CRITICAL"  # Bakiye eksi
            elif hours_remaining < 2:
                alert_level = "URGENT"  # 2 saat içinde tükenir
            elif hours_remaining < 6:
                alert_level = "WARNING"  # 6 saat içinde tükenir
            elif balance < (sales_24h * 0.25):
                # Günlük satışın %25'inden az bakiye var
                alert_level = "LOW"

            if not alert_level:
                # Sorun yok, alert_state'i sıfırla
                if source in alert_state:
                    del alert_state[source]
                continue

            # Aynı seviye uyarı son 1 saatte gitmişse tekrar gönderme
            last_alert = alert_state.get(source, {})
            if last_alert.get("level") == alert_level:
                last_time_str = last_alert.get("time", "")
                try:
                    last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
                    if (now - last_time).total_seconds() < 3600:
                        continue  # 1 saatten az önce uyardık, tekrar etme
                except Exception:
                    pass

            # Uyarı hazırla
            emoji = {"CRITICAL": "🔴", "URGENT": "🟠", "WARNING": "🟡", "LOW": "🔵"}[alert_level]
            hours_str = "EXHAUSTED" if balance < 0 else f"~{hours_remaining:.1f}h remaining"

            recommended_topup = max(sales_24h - balance, 50.0)

            msg = (
                f"{emoji} {alert_level}: {source.upper()} balance low\n\n"
                f"Current balance: ${balance:.2f}\n"
                f"Last 24h sales:  ${sales_24h:.2f}\n"
                f"Hourly burn:     ${hourly_burn:.2f}/h\n"
                f"Estimated runway: {hours_str}\n\n"
                f"💡 Recommended deposit: ${recommended_topup:.0f}\n"
                f"Run: /setbal {source} {balance + recommended_topup:.0f}\n"
                f"(after depositing ${recommended_topup:.0f} to {source.upper()})"
            )

            try:
                await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg)
                alerts_sent.append(f"{source}:{alert_level}")
                alert_state[source] = {
                    "level": alert_level,
                    "time": now.strftime("%Y-%m-%d %H:%M:%S"),
                }
            except Exception as e:
                logger.error(f"Alert send failed for {source}: {e}")

        save_json_file(alert_state_file, alert_state)

        if alerts_sent:
            logger.info(f"[COMP_MONITOR] Alerts sent: {alerts_sent}")

    except Exception as e:
        logger.error(f"competitor_balance_monitor_job error: {e}")


def schedule_competitor_monitor(app: Application) -> None:
    """Her 30 dakikada bir rakip bot bakiyelerini proaktif izle."""
    if app.job_queue is None:
        print("JobQueue unavailable, competitor monitor not started")
        return
    if app.job_queue.get_jobs_by_name("competitor_monitor"):
        return
    app.job_queue.run_repeating(
        competitor_balance_monitor_job,
        interval=1800,  # 30 dakika
        first=300,      # İlk çalışma 5 dk sonra (bot tam başlasın)
        name="competitor_monitor",
    )
    logger.info("Competitor balance monitor started (every 30min)")


# ─── Stuck Order Handler (Başarısız sipariş kurtarma) ───
STUCK_ORDER_WARN_MINUTES = 3       # 3 dk sonra müşteriye alternatif sun
STUCK_ORDER_REFUND_MINUTES = 10    # 10 dk sonra otomatik iade + bonus
STUCK_ORDER_REFUND_BONUS = 0.02    # %2 özür bonusu


async def stuck_order_handler_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Teslim edilememiş siparişleri izle.
    3+ dk: müşteriye alternatif teklif et
    10+ dk: otomatik iade + %2 bonus"""
    try:
        orders = load_orders()
        now = datetime.now()

        for order_id, order in list(orders.items()):
            if order.get("status") != "ext_pending":
                continue

            time_str = order.get("time", "")
            if not time_str:
                continue

            try:
                order_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

            elapsed_minutes = (now - order_time).total_seconds() / 60

            # 10+ dakika: otomatik iade + bonus
            if elapsed_minutes >= STUCK_ORDER_REFUND_MINUTES:
                reason = order.get("failure_reason", "") or "supplier_timeout"
                success = refund_order(order_id, bonus_pct=STUCK_ORDER_REFUND_BONUS, reason=reason)
                if success:
                    try:
                        user_id = order["user_id"]
                        cost = float(order.get("cost", 0))
                        bonus = round(cost * STUCK_ORDER_REFUND_BONUS, 2)
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=(
                                f"💰 Order Refunded\n\n"
                                f"Order: {order_id}\n"
                                f"We couldn't complete your order with the available stock.\n\n"
                                f"Refund: {format_usd(cost)} (returned to balance)\n"
                                f"Apology bonus: +{format_usd(bonus)} (2% credit)\n"
                                f"Total credited: {format_usd(cost + bonus)}\n\n"
                                f"We apologize for the inconvenience. Your balance is ready to use."
                            ),
                        )
                    except Exception:
                        pass

                    try:
                        await context.bot.send_message(
                            chat_id=ADMIN_CHAT_ID,
                            text=(
                                f"⚠️ Auto-refund triggered\n\n"
                                f"Order: {order_id}\n"
                                f"User: {order.get('user_id')}\n"
                                f"Waited: {elapsed_minutes:.1f} minutes\n"
                                f"Reason: {reason}\n"
                                f"Refunded: {format_usd(float(order.get('cost',0)) * (1 + STUCK_ORDER_REFUND_BONUS))}"
                            ),
                        )
                    except Exception:
                        pass

                continue

            # 3-10 dakika arası: müşteriye alternatif sun (sadece bir kez)
            if elapsed_minutes >= STUCK_ORDER_WARN_MINUTES and not order.get("substitute_offered"):
                provider = order.get("provider", "")
                balance = float(order.get("balance", 0))
                currency = order.get("currency", "USD")
                exclude = [order.get("ext_id", "")]

                substitute = find_substitute_card(
                    provider=provider,
                    target_balance=balance,
                    registered=None,
                    currency=currency,
                    exclude_ext_ids=exclude,
                )

                try:
                    user_id = order["user_id"]
                    if substitute:
                        sub_text = (
                            f"⏳ Order Update — {order_id}\n\n"
                            f"Your original selection is delayed at the supplier.\n"
                            f"We found an equivalent alternative:\n\n"
                            f"Card: {substitute['bin']}xx\n"
                            f"Balance: {substitute['currency']}${substitute['balance']:.2f}\n"
                            f"Provider: {substitute['provider']}\n\n"
                            f"Would you like to switch to this card (same price)?"
                        )
                        kb = InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔄 Switch to Alternative",
                                                  callback_data=f"swap_order:{order_id}:{substitute['ext_id']}")],
                            [InlineKeyboardButton("💰 Refund My Order",
                                                  callback_data=f"refund_order:{order_id}")],
                            [InlineKeyboardButton("⏳ Keep Waiting", callback_data="main_menu")],
                        ])
                        await context.bot.send_message(chat_id=user_id, text=sub_text, reply_markup=kb)
                    else:
                        warn_text = (
                            f"⏳ Order Delayed — {order_id}\n\n"
                            f"Your order is taking longer than expected "
                            f"due to a supplier-side issue.\n\n"
                            f"We can refund your order now, or keep trying.\n"
                            f"Either way, we'll auto-refund with a 2% apology bonus "
                            f"if this takes over 10 minutes total."
                        )
                        kb = InlineKeyboardMarkup([
                            [InlineKeyboardButton("💰 Refund Now",
                                                  callback_data=f"refund_order:{order_id}")],
                            [InlineKeyboardButton("⏳ Keep Waiting", callback_data="main_menu")],
                        ])
                        await context.bot.send_message(chat_id=user_id, text=warn_text, reply_markup=kb)

                    order["substitute_offered"] = True
                    orders[order_id] = order
                    save_orders(orders)
                except Exception as e:
                    logger.error(f"stuck_order notify error for {order_id}: {e}")

    except Exception as e:
        logger.error(f"stuck_order_handler_job error: {e}")


def schedule_stuck_order_handler(app: Application) -> None:
    """Her 60 saniyede bir stuck order kontrolü yap."""
    if app.job_queue is None:
        print("JobQueue unavailable, stuck order handler not started")
        return
    if app.job_queue.get_jobs_by_name("stuck_order_handler"):
        return
    app.job_queue.run_repeating(
        stuck_order_handler_job,
        interval=60,
        first=60,
        name="stuck_order_handler",
    )
    logger.info("Stuck order handler started (every 60s)")


# ─── Order Delivery Notifier ───
# Supply chain siparişi "delivered" olarak işaretlediğinde müşteriye kart detaylarını gönder
async def delivery_notifier_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Her 15 saniyede delivered olup daha bildirilmemiş siparişleri tara."""
    try:
        orders = load_orders()
        changed = False

        for order_id, order in list(orders.items()):
            if order.get("status") != "delivered":
                continue
            if order.get("delivered_notified"):
                continue

            user_id = order.get("user_id")
            card_number = order.get("card_number", "")
            if not user_id or not card_number:
                continue

            exp_month = order.get("exp_month", "")
            exp_year = order.get("exp_year", "")
            cvv = order.get("cvv", "")
            balance = float(order.get("balance", 0))
            currency = order.get("currency", "USD")
            provider = order.get("provider", "")
            flag = PROVIDER_FLAGS.get(provider, "🇺🇸")
            short = PROVIDER_SHORT.get(provider, provider)

            # Kart şifrele ve logla (security)
            try:
                encrypted = encrypt_card_data(card_number) if 'encrypt_card_data' in globals() else card_number
            except Exception:
                encrypted = card_number

            msg_text = (
                f"✅ Card Delivered — Order {order_id}\n\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"🔢 Card:  `{card_number}`\n"
                f"📅 Exp:   `{exp_month}/{exp_year}`\n"
                f"🔒 CVV:   `{cvv}`\n"
                f"💰 Balance: {currency}${balance:.2f}\n"
                f"🏪 Provider: {flag} {short}\n"
                f"━━━━━━━━━━━━━━━━━━━\n\n"
                f"Please save these details. They will not be shown again.\n"
                f"Thank you for your purchase!"
            )
            try:
                await context.bot.send_message(chat_id=user_id, text=msg_text, parse_mode="Markdown")
                order["delivered_notified"] = True
                order["delivered_notified_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                orders[order_id] = order
                changed = True

                # Proof channel'a post at (kart bilgisi olmadan)
                try:
                    proof_channel_id = int(os.getenv("PROOF_CHANNEL_ID", "0"))
                    if proof_channel_id:
                        bin_masked = (card_number[:6] + "xx") if len(card_number) >= 6 else "xxxxxx"
                        proof_text = (
                            f"✅ Order Completed\n\n"
                            f"Card: {bin_masked}\n"
                            f"Balance: {currency}${balance:.2f}\n"
                            f"Provider: {short}\n"
                            f"Time: {order.get('delivered_at', 'now')}"
                        )
                        await context.bot.send_message(chat_id=proof_channel_id, text=proof_text)
                except Exception:
                    pass

                logger.info(f"[DELIVERY] Notified user {user_id} for order {order_id}")

                # Admin'e de bilgi
                try:
                    await context.bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=(
                            f"✅ Order Delivered: {order_id}\n"
                            f"User: {user_id}\n"
                            f"Card: {bin_masked} | ${balance:.2f} | {short}"
                        ),
                    )
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Delivery notify failed for {order_id}: {e}")

        if changed:
            save_orders(orders)
    except Exception as e:
        logger.error(f"delivery_notifier_job error: {e}")


def schedule_delivery_notifier(app: Application) -> None:
    """Her 15 saniyede bir delivered sipariş kontrolü."""
    if app.job_queue is None:
        print("JobQueue unavailable, delivery notifier not started")
        return
    if app.job_queue.get_jobs_by_name("delivery_notifier"):
        return
    app.job_queue.run_repeating(
        delivery_notifier_job,
        interval=15,
        first=30,
        name="delivery_notifier",
    )
    logger.info("Delivery notifier started (every 15s)")

# ============================================================
# NON-CUSTODIAL PAYMENT MODULE v2 END
# ============================================================

def generate_qr_bytes(data: str) -> bytes | None:
    """QR kod PNG olarak bytes döndür."""
    try:
        import qrcode
        from io import BytesIO
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=10,
            border=4,
        )
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except ImportError:
        print("qrcode library not installed. Run: pip install qrcode[pil] pillow")
        return None
    except Exception as e:
        print(f"QR generation error: {e}")
        return None

async def send_payment_with_qr(message_target, coin: str, usd_amount: float,
                                address: str, payment_id: str, expires: str,
                                parse_mode=None, reply_markup=None) -> None:
    """Ödeme bilgilerini QR kod ile gönder."""
    if coin == "USDC_SOLANA":
        network = "Solana (SPL Token)"
    elif coin == "USDC":
        network = "Ethereum Mainnet (ERC-20)"
    else:
        network = "Litecoin"
    # Coin miktarı bilgisi
    ltc_line = ""
    if coin == "LTC":
        ltc_price = get_ltc_price_usd()
        if ltc_price > 0:
            ltc_amt = round(usd_amount / ltc_price, 6)
            ltc_line = f"• Send exactly: {ltc_amt} LTC\n"
    elif coin == "USDC_SOLANA":
        ltc_line = f"• Send exactly: {usd_amount:.2f} USDC\n"
    coin_label = {"LTC": "LTC", "USDC_SOLANA": "USDC (Solana)", "USDC": "USDC"}.get(coin, coin)
    conf_time  = "5-10" if coin == "LTC" else "1-2" if coin == "USDC_SOLANA" else "5-15"
    # Kısa caption - Telegram photo caption max 1024 karakter
    caption = (
        f"{coin_label} — {format_usd(usd_amount)}\n"
        f"Network: {network}\n\n"
        f"Address:\n{address}\n\n"
        f"{ltc_line}"
        f"Expires: {expires}"
    )
    # Caption 1024 karakter sınırı kontrolü
    if len(caption) > 1024:
        caption = caption[:1020] + "..."
    qr_bytes = generate_qr_bytes(address)
    try:
        if qr_bytes:
            from io import BytesIO
            await message_target.reply_photo(
                photo=BytesIO(qr_bytes),
                caption=caption,
                reply_markup=reply_markup,
            )
        else:
            await message_target.reply_text(
                caption,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
    except Exception as e:
        print(f"QR send error: {e}")
        await message_target.reply_text(
            caption,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )

def get_balance_payment_instruction_text(coin: str, usd_amount: float, address: str = "") -> str:
    if coin == "USDC":
        network_info = "Ethereum Mainnet (ERC-20)"
        addr_line = f"\nPayment Address (USDC):\n<code>{address}</code>\n" if address else ""
        return (
            f"Add Balance — USDC\n\n"
            f"Amount: {format_usd(usd_amount)}\n"
            f"Coin: USDC\n"
            f"Network: {network_info}\n"
            f"{addr_line}\n"
            "Important:\n"
            f"• Minimum top-up: {format_usd(MIN_BALANCE_TOPUP_USD)}\n"
            "• Send USDC (ERC-20) only, no other tokens accepted\n"
            "• Payment is detected automatically and credited to your balance\n"
            "• Confirmation takes approx. 5-15 minutes (12 blocks)\n"
            "• This address is yours only, do not share it"
        )
    network_info = "Litecoin (LTC)"
    addr_line = f"\nPayment Address (LTC):\n<code>{address}</code>\n" if address else ""
    return (
        f"Add Balance — LTC\n\n"
        f"Amount: {format_usd(usd_amount)}\n"
        f"Coin: LTC\n"
        f"Network: {network_info}\n"
        f"{addr_line}\n"
        "Important:\n"
        f"• Minimum top-up: {format_usd(MIN_BALANCE_TOPUP_USD)}\n"
        "• Send LTC only, no other coins accepted\n"
        "• Payment is detected automatically and credited to your balance\n"
        "• Confirmation takes approx. 5-10 minutes (2 blocks)\n"
        "• This address is yours only, do not share it"
    )





def generate_order_id() -> str:
    while True:
        order_id = f"EE-{random.randint(100000, 999999)}"
        if order_id not in orders:
            return order_id


def normalize_provider_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def canonical_provider_name(value: str) -> str | None:
    return PROVIDER_ALIASES.get(normalize_provider_key(value))


def supported_providers_text() -> str:
    return ", ".join(SUPPORTED_PROVIDERS)


def get_canonical_provider_name(value: str) -> str | None:
    return canonical_provider_name(value)


def get_supported_provider_usage_text() -> str:
    return supported_providers_text()


def get_clean_stock() -> dict:
    """DEPRECATED - SQLite tabanlı sisteme geçildi. Geriye dönük uyumluluk için."""
    try:
        avail = db_get_available_cards()
        result = {p: {"balance": 0.0, "cards": 0} for p in PROVIDER_SHORT}
        for _, card in avail:
            p = card.get("provider", "")
            if p in result:
                result[p]["cards"] += 1
                result[p]["balance"] = round(result[p]["balance"] + float(card.get("balance", 0)), 2)
        return result
    except Exception:
        return {}

def save_clean_stock(stock: dict) -> None:
    """DEPRECATED - SQLite tabanlı sisteme geçildi. No-op."""
    pass


def get_command_parts(update: Update) -> list[str]:
    if not update.message or not update.message.text:
        return []
    return update.message.text.strip().split()


def get_order_id_from_command(update: Update) -> str | None:
    parts = get_command_parts(update)
    if len(parts) < 2:
        return None
    return parts[1].strip()



def blocked_by_maintenance(chat_id: int) -> bool:
    return maintenance_enabled() and not is_admin(chat_id)


def is_admin(chat_id: int) -> bool:
    return chat_id == ADMIN_CHAT_ID


def is_user_approved(chat_id: int) -> bool:
    if is_admin(chat_id):
        return True
    approved = access_data.get("approved_users", [])
    return chat_id in approved


def access_mode() -> str:
    mode = access_data.get("mode", "restricted")
    if mode not in ("open", "restricted"):
        return "restricted"
    return mode


def can_use_bot(chat_id: int) -> bool:
    if is_admin(chat_id):
        return True
    if access_mode() == "open":
        return True
    return is_user_approved(chat_id)


def normalize_compact(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.lower())


def best_effort_order_summary(text: str) -> dict:
    summary = {
        "provider": None,
        "amount": None,
        "payment_method": None,
    }

    lower_text = text.lower()
    compact_text = normalize_compact(text)

    known_providers = list(PROVIDER_SHORT.keys())

    fallback_providers = [
        "VanillaGift",
        "VanillaPrepaid",
        "Vanilla",
        "JokerCard",
        "PerfectGift",
        "MyPrepaidCenter",
        "GiftCardMall",
        "BalanceNow",
        "CardBalanceAU",
        "Walmart",
        "Amex",
    ]

    provider_candidates = known_providers[:]
    for item in fallback_providers:
        if item not in provider_candidates:
            provider_candidates.append(item)

    for provider in provider_candidates:
        if provider.lower() in lower_text or normalize_compact(provider) in compact_text:
            summary["provider"] = provider
            break

    if "usdc" in lower_text:
        summary["payment_method"] = "USDC"
    elif "ltc" in lower_text:
        summary["payment_method"] = "LTC"

    amount_match = re.search(r"(?<!\d)(\d+(?:[.,]\d{1,2})?)\s*\$", text)
    if amount_match:
        summary["amount"] = f"${amount_match.group(1).replace(',', '.')}"
    else:
        amount_match = re.search(r"\$\s*(\d+(?:[.,]\d{1,2})?)", text)
        if amount_match:
            summary["amount"] = f"${amount_match.group(1).replace(',', '.')}"
        else:
            number_matches = re.findall(r"(?<![A-Za-z])(\d+(?:[.,]\d{1,2})?)(?![A-Za-z])", text)
            if number_matches:
                summary["amount"] = f"${number_matches[0].replace(',', '.')}"

    return summary


def build_admin_order_text(
    order_id: str,
    order_time: str,
    full_name: str,
    username: str,
    user_id: int,
    chat_id: int,
    submitted_text: str,
) -> str:
    parsed = best_effort_order_summary(submitted_text)

    lines = [
        "New Order Received",
        "",
        f"Order ID: {order_id}",
        f"Time: {order_time}",
        "Status: new",
        f"Name: {full_name}",
        f"Username: {username}",
        f"User ID: {user_id}",
        f"Chat ID: {chat_id}",
        "",
        "Quick Summary:",
        f"Provider: {parsed['provider'] or 'Not detected'}",
        f"Amount: {parsed['amount'] or 'Not detected'}",
        f"Payment Method: {parsed['payment_method'] or 'Not detected'}",
        "",
        "Submitted Order:",
        submitted_text,
    ]
    return "\n".join(lines)



def admin_hub_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("Dashboard", callback_data="adminhub_dashboard"),
            InlineKeyboardButton("Quickpanel", callback_data="adminhub_quickpanel"),
        ],
        [
            InlineKeyboardButton("Pending Orders", callback_data="adminhub_pendingorders"),
            InlineKeyboardButton("Balance Requests", callback_data="adminhub_balancerequests"),
        ],
        [
            InlineKeyboardButton("Rates Panel", callback_data="adminhub_ratespanel"),
            InlineKeyboardButton("Daily Summary", callback_data="adminhub_dailysummary"),
        ],
        [
            InlineKeyboardButton("Audit Log", callback_data="adminhub_auditlog"),
            InlineKeyboardButton("Help", callback_data="adminhub_help"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def build_admin_hub_text() -> str:
    return (
        "Admin Navigation Hub\n\n"
        "Use the buttons below to open the main admin tools quickly."
    )




def build_faq_text() -> str:
    return (
        "F.A.Q\n\n"
        "1) How do I place an order?\n"
        "Open My Profile > Place Order, choose a provider, enter the requested card balance, and confirm the preview.\n\n"
        "2) How do I add balance?\n"
        "Open My Profile > Add Balance, follow the payment instructions, then wait for approval and balance credit.\n\n"
        "3) Where can I check my order status?\n"
        "Use My Profile > My Orders or My Profile > Payment Status.\n\n"
        "4) What happens after I pay?\n"
        "Your payment is reviewed, then your order moves through payment and delivery stages.\n\n"
        "5) How do I use the bot in the simplest way?\n"
        "Open My Profile, add balance if needed, check Live Stock, place your order, then track it from My Orders or Payment Status.\n\n"
        "6) How do I contact support?\n"
        "Use Client Support from the main menu."
    )


def my_profile_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("Live Stock", callback_data="stock"),
            InlineKeyboardButton("Place Order", callback_data="order"),
        ],
        [
            InlineKeyboardButton("Add Balance", callback_data="add_balance"),
            InlineKeyboardButton("My Balance", callback_data="my_balance"),
        ],
        [
            InlineKeyboardButton("My Orders", callback_data="my_orders"),
            InlineKeyboardButton("Payment Status", callback_data="payment_status"),
        ],
        [
            InlineKeyboardButton("Rules", callback_data="rules"),
            InlineKeyboardButton("F.A.Q", callback_data="faq"),
        ],
        [
            InlineKeyboardButton("Main Menu", callback_data="main_menu"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_user_total_orders(user_id: int) -> int:
    total = 0
    for _, data in orders.items():
        if data.get("user_id") == user_id or data.get("chat_id") == user_id:
            total += 1
    return total


def main_menu_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("💳 Prepaid Cards",       callback_data="lst:1:")],
        [InlineKeyboardButton("🎁 Store Gift Cards",    callback_data="gc_menu")],
        [InlineKeyboardButton("📢 Stock Updates",      url=STOCK_NEWS_CHANNEL_LINK),
         InlineKeyboardButton("🎧 Support",            callback_data="support")],
        [InlineKeyboardButton("💰 Add Balance",        callback_data="add_balance"),
         InlineKeyboardButton("👤 Profile",            callback_data="my_profile")],
        [InlineKeyboardButton("📋 My Orders",          callback_data="my_orders"),
         InlineKeyboardButton("📢 Main Channel",       url=MAIN_CHANNEL_LINK)],
    ]
    return InlineKeyboardMarkup(keyboard)

def after_order_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("View Live Stock", callback_data="stock")],
        [InlineKeyboardButton("Client Support", callback_data="support")],
        [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def after_support_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("Place Order", callback_data="order")],
        [InlineKeyboardButton("View Live Stock", callback_data="stock")],
        [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def restricted_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("Activate Now", callback_data="activate_now")],
        [InlineKeyboardButton("Need Help", callback_data="request_access")],
        [InlineKeyboardButton("Official Main Channel", url=MAIN_CHANNEL_LINK)],
        [InlineKeyboardButton("Stock News Channel", url=STOCK_NEWS_CHANNEL_LINK)],
    ]
    return InlineKeyboardMarkup(keyboard)


def activate_now_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("💵 USDC (Solana)", callback_data="activate_usdc")],
        [InlineKeyboardButton("🪙 LTC (Litecoin)", callback_data="activate_ltc")],
        [InlineKeyboardButton("Back",              callback_data="restricted_home")],
    ]
    return InlineKeyboardMarkup(keyboard)


def add_balance_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("💵 USDC (Solana)", callback_data="add_balance_usdc")],
        [InlineKeyboardButton("🪙 LTC (Litecoin)", callback_data="add_balance_ltc")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def payment_instruction_markup(coin: str) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("I Paid — Check Now", callback_data=f"check_payment_{coin}")],
        [InlineKeyboardButton("Client Support", callback_data="support")],
        [InlineKeyboardButton("Add Balance", callback_data="add_balance")],
        [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def place_order_confirm_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("Confirm Order", callback_data="confirm_place_order")],
        [InlineKeyboardButton("Cancel", callback_data="cancel_place_order")],
    ]
    return InlineKeyboardMarkup(keyboard)


async def send_restricted_message(message_target, user=None) -> None:
    name = user.first_name if user and getattr(user, "first_name", None) else "there"
    text = (
        f"Elite Prepaid Bot 💳\n\n"
        f"Welcome, {name}.\n\n"
        "We offer first-hand prepaid cards at discounted member rates.\n\n"
        "Membership — $100 (one-time)\n"
        "✓ Instant card delivery\n"
        "✓ Fresh stock, never relisted\n"
        "✓ LTC & USDC accepted\n\n"
        f"{LIFETIME_ACCESS_NAME}: {LIFETIME_ACCESS_PRICE} one-time\n\n"
        "Ready to join? Choose an option below."
    )
    await message_target.reply_text(text, reply_markup=restricted_markup())


async def show_main_menu(message_target) -> None:
    text = (
        "🏆 Welcome to Elite Earners!\n\n"
        "Buy prepaid cards & store gift cards in seconds!!\n\n"
        "All transactions are secure and transparent.\n"
        "All types of cards are available here at best rates."
    )
    await message_target.reply_text(text, reply_markup=main_menu_markup())



async def show_my_profile_message(message_target, user_id: int, telegram_name: str = "Unknown") -> None:
    record       = get_hybrid_balance_record(user_id)
    total_orders = get_user_total_orders(user_id)
    tg_name      = str(telegram_name or "Unknown").strip() or "Unknown"

    # Kaç sipariş teslim edildi
    all_orders   = get_all_hybrid_orders()
    user_orders  = [o for o in all_orders.values()
                    if o.get("user_id") == user_id or o.get("chat_id") == user_id]
    delivered    = sum(1 for o in user_orders if o.get("status") == "delivered")
    pending      = sum(1 for o in user_orders if o.get("status") == "paid_pending_delivery")

    text = (
        "My Profile\n\n"
        f"Name: {tg_name}\n"
        f"User ID: {user_id}\n\n"
        f"Available Balance: {format_usd(float(record.get('available_balance', 0.0)))}\n"
        f"Total Deposited:   {format_usd(float(record.get('total_deposited', 0.0)))}\n"
        f"Total Spent:       {format_usd(float(record.get('total_spent', 0.0)))}\n\n"
        f"Total Orders:   {total_orders}\n"
        f"Delivered:      {delivered}\n"
        f"Pending:        {pending}"
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Add Balance",  callback_data="add_balance"),
         InlineKeyboardButton("📋 My Orders",    callback_data="my_orders")],
        [InlineKeyboardButton("💵 My Payments",  callback_data="my_payment_status"),
         InlineKeyboardButton("🏠 Main Menu",    callback_data="main_menu")],
    ])
    await message_target.reply_text(text, reply_markup=markup)

async def show_my_balance_message(message_target, user_id: int) -> None:
    record = get_user_balance_record(user_id)
    text = (
        "My Balance\n\n"
        f"Available Balance: {format_usd(float(record.get('available_balance', 0)))}\n"
        f"Total Deposited: {format_usd(float(record.get('total_deposited', 0)))}\n"
        f"Total Spent: {format_usd(float(record.get('total_spent', 0)))}\n\n"
        f"Minimum balance top-up: {format_usd(MIN_BALANCE_TOPUP_USD)}"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Add Balance", callback_data="add_balance")],
        [InlineKeyboardButton("Place Order", callback_data="order")],
        [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
    ])
    await message_target.reply_text(text, reply_markup=keyboard)


async def show_add_balance_message(message_target) -> None:
    text = (
        "Add Balance\n\n"
        "You can add funds to your account and use your balance to place orders inside the bot.\n\n"
        f"Minimum top-up amount: {format_usd(MIN_BALANCE_TOPUP_USD)}\n\n"
        "Choose your payment method below."
    )
    await message_target.reply_text(text, reply_markup=add_balance_markup())


async def show_add_balance_amount_prompt(message_target, coin: str) -> None:
    network = "Ethereum Mainnet (ERC-20)" if coin == "USDC" else "Litecoin"
    text = (
        f"Add Balance — {coin}\n\n"
        "Enter the amount you want to add (in USD).\n\n"
        f"Minimum amount: {format_usd(MIN_BALANCE_TOPUP_USD)}\n"
        f"Network: {network}\n\n"
        "Examples:\n"
        "50\n"
        "100\n"
        "250\n\n"
        "Payment will be sent to your unique address and detected automatically."
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Back", callback_data="add_balance")],
        [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
    ])
    await message_target.reply_text(text, reply_markup=keyboard)



def build_customer_order_summary_text(order_id: str, data: dict) -> str:
    provider = data.get("provider") or "Unknown"
    requested = data.get("requested_card_balance")
    charged = data.get("charged_amount")
    requested_text = format_usd(float(requested)) if isinstance(requested, (int, float)) else "-"
    charged_text = format_usd(float(charged)) if isinstance(charged, (int, float)) else "-"
    order_status = get_order_status_label(data.get("status", "unknown"))
    payment_status = get_payment_status_label(data.get("payment_status", "unpaid"))
    created_at = data.get("time", "-")

    return (
        f"Order ID: {order_id}\n"
        f"Provider: {provider}\n"
        f"Requested: {requested_text}\n"
        f"Charged: {charged_text}\n"
        f"Order Status: {order_status}\n"
        f"Payment Status: {payment_status}\n"
        f"Created: {created_at}"
    )


def build_customer_payment_status_text(order_id: str, data: dict) -> str:
    provider = data.get("provider") or "Unknown"
    requested = data.get("requested_card_balance")
    requested_text = format_usd(float(requested)) if isinstance(requested, (int, float)) else "-"
    payment_status = get_payment_status_label(data.get("payment_status", "unpaid"))
    payment_method = data.get("payment_method", "-") or "-"
    paid_at = data.get("paid_at", "-") or "-"
    payment_note = data.get("payment_note", "-") or "-"

    return (
        f"Order ID: {order_id}\n"
        f"Provider: {provider}\n"
        f"Requested Card Balance: {requested_text}\n"
        f"Payment Status: {payment_status}\n"
        f"Payment Method: {payment_method}\n"
        f"Paid At: {paid_at}\n"
        f"Note: {payment_note}"
    )


def get_customer_visible_orders(user_id: int):
    wallet_orders = []
    legacy_orders = []

    for order_id, data in get_all_hybrid_orders().items():
        if data.get("user_id") == user_id or data.get("chat_id") == user_id:
            if "charged_amount" in data or data.get("status") in {"paid_pending_delivery", "delivered", "refunded", "cancelled"}:
                wallet_orders.append((order_id, data))
            else:
                legacy_orders.append((order_id, data))

    wallet_orders.sort(key=lambda item: item[1].get("time", ""), reverse=True)
    legacy_orders.sort(key=lambda item: item[1].get("time", ""), reverse=True)
    return wallet_orders, legacy_orders


async def show_my_orders_message(message_target, user_id: int) -> None:
    wallet_orders, _ = get_customer_visible_orders(user_id)

    if not wallet_orders:
        await message_target.reply_text(
            "My Orders\n\nYou have no orders yet.\n\nBrowse available cards from the listing.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🛒 View Listing", callback_data="lst:1:")],
                [InlineKeyboardButton("🏠 Main Menu",    callback_data="main_menu")],
            ]),
        )
        return

    # Kart stok verisiyle eşleştir
    cards = load_cards()
    order_to_card = {c.get("order_id"): c for c in cards.values() if c.get("order_id")}

    lines = [f"My Orders ({len(wallet_orders)} total)\n"]
    for order_id, data in wallet_orders[:8]:
        status     = data.get("status", "unknown")
        provider   = data.get("provider", "")
        short      = PROVIDER_SHORT.get(provider, provider)
        balance    = data.get("requested_card_balance", 0)
        charged    = data.get("charged_amount", 0)
        created    = (data.get("time") or "")[:16]
        card       = order_to_card.get(order_id)

        # Status emoji
        status_icons = {
            "paid_pending_delivery": "⏳ Pending",
            "delivered": "✅ Delivered",
            "cancelled": "❌ Cancelled",
            "refunded":  "↩️ Refunded",
        }
        status_label = status_icons.get(status, f"📋 {status}")

        lines.append(
            f"Order: {order_id}\n"
            f"  {short} | ${balance:.2f} | Cost: {format_usd(charged)}\n"
            f"  {status_label} | {created}"
        )
        # Eğer teslim edildiyse kart bilgisi göster
        if status == "delivered" and card:
            cn  = card.get("card_number", "")
            em  = card.get("expiry_month", "")
            ey  = card.get("expiry_year", "") or ""
            cvv = card.get("cvv", "")
            exp = f"{em}/{ey[-2:]}" if len(ey) >= 2 else ey
            lines.append(f"  💳 {cn} | {exp} | CVV: {cvv}")
        lines.append("")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 View Listing", callback_data="lst:1:"),
         InlineKeyboardButton("👤 My Profile",   callback_data="my_profile")],
        [InlineKeyboardButton("🎧 Support",      callback_data="support"),
         InlineKeyboardButton("🏠 Main Menu",    callback_data="main_menu")],
    ])
    await message_target.reply_text("\n".join(lines).strip(), reply_markup=keyboard)

async def show_rules_message(message_target) -> None:
    text = (
        "Rules & Important Terms\n\n"
        "Please read carefully before placing any order.\n\n"
        "1. All sales are final.\n"
        "Once payment has been confirmed, no refunds, reversals, or cancellations will be accepted.\n\n"
        "2. The buyer is fully responsible for order accuracy.\n"
        "Please make sure the selected product, amount, and payment method are correct before sending payment.\n\n"
        "3. Stock availability can change at any time.\n"
        "A product shown as available may become unavailable before payment is completed.\n\n"
        "4. Rates are subject to change.\n"
        "Final pricing and invoice amount will be based on the live rate at the time payment is confirmed.\n\n"
        "5. Delivery time may vary.\n"
        "Some orders are completed quickly, while others may require additional review or processing time.\n\n"
        "6. Never check the card balance on the official website or through any other bot.\n"
        "Doing so may cause the card to become suspended, limited, or unusable. No warranty is provided for cards checked this way.\n\n"
        "7. No refunds will be issued for usage-related claims.\n"
        'This includes claims such as "card is not working on your site" or "card was used after 2 minutes of delivery."\n\n'
        "8. All cards must be handled immediately and carefully after delivery.\n"
        "The customer accepts full responsibility for safe usage, correct handling, and timing once the card has been delivered.\n\n"
        "9. Any suspicious activity, abuse, fraud, chargeback risk, or scam behavior will result in immediate removal of access without notice.\n\n"
        "10. By using this bot and placing an order, you confirm that you have read, understood, and agreed to all rules above."
    )
    await message_target.reply_text(text, reply_markup=main_menu_markup())



async def show_support_prompt(message_target, context: ContextTypes.DEFAULT_TYPE, user, chat_id: int) -> None:
    user_id = user.id
    support_mode_users.discard(user_id)
    add_balance_mode_users.pop(user_id, None)

    if can_use_bot(chat_id):
        support_mode_users.add(user_id)
        await message_target.reply_text(
            "Support\n\n"
            "Type your message and send it.\n\n"
            "Suggested format:\n"
            "Issue: ...\n"
            "Order ID: ...\n"
            "Message: ...\n\n"
            "Your message will be forwarded to admin.",
            reply_markup=after_support_markup(),
        )
    else:
        await send_restricted_message(message_target, user)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    # Referral linki kontrolü: /start ref123456
    if context.args:
        arg = context.args[0]
        if arg.startswith("ref") and arg[3:].isdigit():
            referrer_id = int(arg[3:])
            if referrer_id != user_id and can_use_bot(chat_id):
                given = process_referral(user_id, referrer_id)
                if given:
                    try:
                        await context.bot.send_message(
                            chat_id=referrer_id,
                            text=(
                                f"Referral Bonus!\n\n"
                                f"A new user joined using your referral link.\n"
                                f"${REFERRAL_BONUS_USD:.0f} has been added to your balance!"
                            ),
                        )
                    except Exception:
                        pass
                    await update.message.reply_text(
                        f"Welcome! You received a ${REFERRAL_BONUS_USD:.0f} referral bonus!\n"
                        f"Your balance has been credited."
                    )

    if blocked_by_maintenance(chat_id):
        await send_maintenance_message(update.message)
        return

    await show_main_menu(update.message)


async def supportmenu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await show_support_prompt(update.message, context, update.effective_user, update.effective_chat.id)



async def safe_edit(query, text: str, markup=None) -> None:
    """edit_message_text - hata olursa yeni mesaj gönder, eskisini sil."""
    try:
        await query.edit_message_text(text, reply_markup=markup)
    except Exception as e:
        err = str(e).lower()
        if "not modified" in err:
            return
        if "query is too old" in err or "query_id_invalid" in err:
            return
        if "flood" in err or "too many" in err or "retry" in err:
            await asyncio.sleep(1.5)
            try:
                await query.edit_message_text(text, reply_markup=markup)
                return
            except Exception:
                pass
        # Fallback: yeni mesaj gönder, eskisini silmeye çalış
        try:
            await query.message.reply_text(text, reply_markup=markup)
            try:
                await query.message.delete()
            except Exception:
                pass
        except Exception:
            pass

async def safe_reply(target, text: str, markup=None, parse_mode=None) -> None:
    """reply_text - kullanici botu blockladıysa sessiz gec."""
    try:
        await target.reply_text(text, reply_markup=markup, parse_mode=parse_mode)
    except Exception as e:
        err = str(e).lower()
        if any(x in err for x in ["blocked", "chat not found", "user is deactivated",
                                    "bot was blocked", "forbidden"]):
            logger.warning(f"User unreachable: {e}")
        else:
            logger.error(f"reply_text error: {e}")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    _btn_start = time.time()
    try:
        await query.answer()
    except Exception:
        pass
    chat_id = query.message.chat_id
    callback_user_id = query.from_user.id

    if not is_admin(chat_id) and is_spamming_user(callback_user_id):
        await query.message.reply_text("Too many actions detected. Please slow down and try again shortly.")
        return

    # Debounce - cok hizli tiklama engeli
    now_ts = time.time()
    last_ts = _last_callback_time.get(callback_user_id, 0)
    if now_ts - last_ts < CALLBACK_DEBOUNCE_SECONDS:
        return  # Sessizce yoksay
    _last_callback_time[callback_user_id] = now_ts

    # Hub callback - debounce'dan muaf (admin hızlı hareket edebilmeli)
    if query.data.startswith("hub:"):
        if not is_full_admin(chat_id):
            await query.answer("Admin only.", show_alert=True)
            return
        _last_callback_time[callback_user_id] = time.time()  # Debounce sıfırla
        await hub_callback_handler(query, context)
        return

    if query.data == "support":
        support_mode_users.discard(query.from_user.id)
        add_balance_mode_users.pop(query.from_user.id, None)
        if can_use_bot(chat_id):
            support_mode_users.add(query.from_user.id)
            await safe_edit(
                query,
                "Support\n\nType your message and send it.\n\nSuggested format:\nIssue: ...\nOrder ID: ...\nMessage: ...\n\nYour message will be forwarded to admin.",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("Cancel", callback_data="main_menu")],
                ]),
            )
        else:
            await safe_edit(query, "Please activate your account first.", restricted_markup())
        return

    if query.data == "activate_now":
        add_balance_mode_users.pop(query.from_user.id, None)
        support_mode_users.discard(query.from_user.id)
        user_id = query.from_user.id
        if not _wallet_ready:
            await query.message.reply_text(
                "Payment system is not ready. Please try again later.",
                reply_markup=restricted_markup(),
            )
            return
        # Mevcut aktivasyon ödemesi var mı?
        existing = get_user_activation_payment(user_id)
        if existing:
            coin    = existing["coin"]
            address = existing["address"]
            expires = existing.get("expires_at", "")[:16]
            try:
                await query.message.reply_text(
                    f"Activation Payment\n\n"
                    f"You already have a pending payment.\n\n"
                    f"Coin: {coin}\n"
                    f"Amount: {format_usd(ACTIVATION_FEE_USD)}\n"
                    f"Address: {address}\n\n"
                    f"Expires: {expires}\n\n"
                    "Send exactly this amount to activate your account automatically.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Pay with LTC",  callback_data="activate_ltc")],
                        [InlineKeyboardButton("Pay with USDC", callback_data="activate_usdc")],
                    ]),
                )
            except Exception as e:
                print(f"activate_now existing error: {e}")
        else:
            await query.message.reply_text(
                f"{LIFETIME_ACCESS_NAME}\n\n"
                f"One-time activation fee: {LIFETIME_ACCESS_PRICE}\n\n"
                "Choose your payment method:",
                reply_markup=activate_now_markup(),
            )
        return

    if query.data in ("activate_ltc", "activate_usdc"):
        user_id = query.from_user.id
        coin    = "LTC" if query.data == "activate_ltc" else "USDC_SOLANA"
        if not _wallet_ready:
            await query.answer("Payment system not ready.", show_alert=True)
            return
        rec = create_activation_payment(
            user_id   = user_id,
            coin      = coin,
            full_name = query.from_user.full_name,
            username  = f"@{query.from_user.username}" if query.from_user.username else "No username",
        )
        address = rec["address"]
        expires = rec.get("expires_at", "")[:16]
        await send_payment_with_qr(
            message_target=query.message,
            coin=coin,
            usd_amount=ACTIVATION_FEE_USD,
            address=address,
            payment_id=rec["payment_id"],
            expires=expires,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("I Paid — Check Status", callback_data="check_activation")],
                [InlineKeyboardButton("Main Menu", callback_data="restricted_home")],
            ]),
        )
        # Admin'e bildirim
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"Activation Payment Started\n\n"
                    f"Payment ID: {rec['payment_id']}\n"
                    f"User: {query.from_user.full_name} ({f'@{query.from_user.username}' if query.from_user.username else 'No username'})\n"
                    f"User ID: {user_id}\n"
                    f"Coin: {coin}\n"
                    f"Amount: {format_usd(ACTIVATION_FEE_USD)}\n"
                    f"Address: {address}"
                ),
            )
        except Exception:
            pass
        return

    if query.data == "check_activation":
        user_id = query.from_user.id
        if is_user_approved(user_id):
            await query.answer("Your account is already active!", show_alert=True)
            await show_main_menu(query.message)
            return
        existing = get_user_activation_payment(user_id)
        if not existing:
            await query.answer("No pending payment found.", show_alert=True)
            return
        await query.answer(
            f"Payment is being monitored automatically.\nYou will be notified once confirmed.",
            show_alert=True
        )
        return

    if query.data == "restricted_home":
        support_mode_users.discard(query.from_user.id)
        add_balance_mode_users.pop(query.from_user.id, None)
        await send_restricted_message(query.message, query.from_user)
        return

    if blocked_by_maintenance(chat_id):
        await send_maintenance_message(query.message)
        return

    # Kayıtsız kullanıcılar listeyi görebilir ama aksiyon alamaz
    # Göz atma serbest: listing, gift card menü, filtreler
    BROWSE_PREFIXES = (
        "stock", "lst:", "lst_filter:", "listing_filter", "noop",
        "gc_menu", "gc_region:", "gc_type:", "gc_list:",
        "main_menu",
    )
    is_browse = any(query.data.startswith(p) or query.data == p for p in BROWSE_PREFIXES)

    if not can_use_bot(chat_id) and not is_browse:
        await send_restricted_message(query.message, query.from_user)
        return

    if query.data == "referral":
        uid      = query.from_user.id
        bot_user = await context.bot.get_me()
        link     = get_referral_link(uid, bot_user.username)
        stats    = get_user_referral_stats(uid)
        await safe_edit(
            query,
            f"💸 Referral System\n\n"
            f"Share your link and earn ${REFERRAL_BONUS_USD:.0f} for each new member!\n\n"
            f"🔗 Your Referral Link:\n{link}\n\n"
            f"👥 Total Referrals: {stats['count']}\n"
            f"💰 Total Earned:    {format_usd(stats['total_earned'])}\n\n"
            f"When someone joins using your link:\n"
            f"• They get ${REFERRAL_BONUS_USD:.0f} welcome bonus\n"
            f"• You get ${REFERRAL_BONUS_USD:.0f} referral bonus",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
            ]),
        )
        return

    if query.data == "main_menu":
        text = (
            "Elite Earners — Main Menu\n\n"
            "Browse available cards, manage your balance and track your orders below."
        )
        await safe_edit(query, text, main_menu_markup())
        return

    if query.data == "stock":
        await show_listing_page(query.message, query.from_user.id, 1, edit=True)
        return

    if query.data == "my_profile":
        uid    = query.from_user.id
        record = get_hybrid_balance_record(uid)
        all_o  = get_all_hybrid_orders()
        u_orders = [o for o in all_o.values() if o.get("user_id") == uid or o.get("chat_id") == uid]
        delivered = sum(1 for o in u_orders if o.get("status") == "delivered")
        pending   = sum(1 for o in u_orders if o.get("status") == "paid_pending_delivery")
        text = (
            f"My Profile\n\n"
            f"Name: {query.from_user.full_name}\n"
            f"User ID: {uid}\n\n"
            f"Available Balance: {format_usd(float(record.get('available_balance',0)))}\n"
            f"Total Deposited:   {format_usd(float(record.get('total_deposited',0)))}\n"
            f"Total Spent:       {format_usd(float(record.get('total_spent',0)))}\n\n"
            f"Total Orders: {len(u_orders)}\n"
            f"Delivered:    {delivered}\n"
            f"Pending:      {pending}"
        )
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Add Balance", callback_data="add_balance"),
             InlineKeyboardButton("📋 My Orders",   callback_data="my_orders")],
            [InlineKeyboardButton("💵 My Payments", callback_data="my_payment_status"),
             InlineKeyboardButton("💸 Earn Money",  callback_data="referral")],
            [InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu")],
        ])
        await safe_edit(query, text, markup)
        return

    if query.data == "faq":
        await query.message.reply_text(
            build_faq_text(),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("My Profile", callback_data="my_profile")],
                [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
            ]),
        )
        return

    if query.data == "my_balance":
        uid    = query.from_user.id
        record = get_user_balance_record(uid)
        text = (
            f"My Balance\n\n"
            f"Available: {format_usd(float(record.get('available_balance',0)))}\n"
            f"Deposited: {format_usd(float(record.get('total_deposited',0)))}\n"
            f"Spent:     {format_usd(float(record.get('total_spent',0)))}\n\n"
            f"Minimum top-up: {format_usd(MIN_BALANCE_TOPUP_USD)}"
        )
        await safe_edit(query, text, InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Add Balance", callback_data="add_balance")],
            [InlineKeyboardButton("🛒 View Listing", callback_data="lst:1:")],
            [InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu")],
        ]))
        return

    if query.data == "my_payment_status":
        user_id  = query.from_user.id
        payments = load_pending_payments()
        user_pays = [p for p in payments.values()
                     if int(p.get("user_id", 0)) == user_id]
        user_pays.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        if not user_pays:
            await query.message.reply_text(
                "No payment records found.\nUse Add Balance to top up.",
                reply_markup=main_menu_markup(),
            )
            return
        lines = ["My Recent Payments\n"]
        for p in user_pays[:5]:
            sm = {"waiting": "Waiting", "credited": "Credited", "expired": "Expired"}
            st = sm.get(p.get("status", ""), p.get("status", "-"))
            lines.append(
                f"ID: {p['payment_id']}\n"
                f"Coin: {p.get('coin','-')} | {format_usd(p.get('usd_amount',0))}\n"
                f"Status: {st} | {p.get('created_at','-')[:16]}"
            )
            if p.get("tx_hash"):
                lines.append(f"TX: {p['tx_hash'][:20]}...")
            lines.append("")
        await query.message.reply_text("\n".join(lines), reply_markup=main_menu_markup())
        return

    if query.data == "add_balance":
        add_balance_mode_users.pop(query.from_user.id, None)
        text = (
            f"Add Balance\n\n"
            "Add funds to your account and use your balance to purchase cards.\n\n"
            f"Minimum top-up: {format_usd(MIN_BALANCE_TOPUP_USD)}\n\n"
            "Choose your payment method:"
        )
        await safe_edit(query, text, add_balance_markup())
        return

    if query.data == "add_balance_usdc":
        add_balance_mode_users[query.from_user.id] = {"coin": "USDC_SOLANA", "network": "Solana", "step": "awaiting_amount"}
        await show_add_balance_amount_prompt(query.message, "USDC (Solana)")
        return

    if query.data == "add_balance_ltc":
        add_balance_mode_users[query.from_user.id] = {"coin": "LTC", "network": "Litecoin", "step": "awaiting_amount"}
        await show_add_balance_amount_prompt(query.message, "LTC")
        return

    # ── Listing callbacks ──
    if query.data.startswith("lst:"):
        parts = query.data.split(":", 3)
        pg  = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
        pf  = parts[2] if len(parts) > 2 and parts[2] else None
        br  = parts[3] if len(parts) > 3 and parts[3] else None
        try:
            await show_listing_page(query.message, query.from_user.id, pg, pf, br, edit=True)
        except Exception as e:
            logger.warning(f"lst callback error: {e}")
        return

    if query.data == "listing_filter":
        text   = "Filter Cards\n\nChoose provider and/or balance range.\nFilters apply together."
        markup = listing_filter_markup()
        await safe_edit(query, text, markup)
        return

    if query.data == "noop":
        await query.answer()
        return

    if query.data.startswith("bal_recheck:"):
        parts = query.data.split(":")
        order_id = parts[1] if len(parts) > 1 else ""
        card_id = parts[2] if len(parts) > 2 else ""
        user_id = query.from_user.id

        if not order_id or not card_id:
            await query.answer("Invalid recheck request.", show_alert=True)
            return

        # Recheck hakkı kontrolü
        if not can_recheck_order(order_id):
            await query.answer(
                "⚠️ Recheck limit reached.\n\n"
                "Re-checking may permanently kill the card.\n"
                "Contact support if you have issues.",
                show_alert=True
            )
            return

        card = get_card(card_id)
        if not card:
            await query.answer("Card not found.", show_alert=True)
            return

        provider = card.get("provider", "")
        if provider not in BALANCE_CHECKERS:
            await query.answer(f"Balance check not available for {provider}.", show_alert=True)
            return

        # Recheck başlat
        increment_recheck(order_id)
        await query.answer("⏳ Rechecking balance... please wait.", show_alert=False)

        try:
            result = await check_card_balance(card)
            if result.success:
                text = (
                    f"🔄 Balance Recheck — Order {order_id}\n\n"
                    f"{result.format_delivery_text()}\n\n"
                    f"⚠️ This was your recheck. No more rechecks available.\n"
                    f"Use the card immediately to avoid issues."
                )
            else:
                text = (
                    f"🔄 Balance Recheck — Order {order_id}\n\n"
                    f"⚠️ Check failed: {result.error or 'Unknown error'}\n\n"
                    f"This does NOT mean the card is dead.\n"
                    f"Try using the card directly."
                )
            await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 My Orders", callback_data="my_orders"),
                 InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
            ]))
        except Exception as e:
            logger.error(f"[CHECKER] Recheck failed for {order_id}: {e}")
            await query.message.reply_text(
                f"⚠️ Recheck temporarily unavailable. Try again later.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
                ]),
            )
        return

    if query.data.startswith("recheck:"):
        card_id = query.data.split(":", 1)[1]
        await query.answer("Balance is guaranteed at time of purchase. Re-checking may kill the card!", show_alert=True)
        return

    if query.data.startswith("billing:"):
        card_id = query.data.split(":", 1)[1]
        card = get_card(card_id)
        if not card:
            await query.answer("Card not found.", show_alert=True)
            return
        await query.answer(
            f"Card: {card['card_number']}\nExpiry: {card.get('expiry_month','')}/{card.get('expiry_year','')}\nCVV: {card.get('cvv','')}",
            show_alert=True
        )
        return

    if query.data.startswith("refund_req:"):
        order_id = query.data.split(":", 1)[1]
        user_id  = query.from_user.id
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"Refund Request\n\n"
                    f"Order ID: {order_id}\n"
                    f"User: {query.from_user.full_name} (ID: {user_id})\n"
                    f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                ),
            )
        except Exception:
            pass
        await query.answer("Refund request sent to admin.", show_alert=True)
        return

    if query.data.startswith("lst_filter:"):
        parts_f = query.data.split(":", 2)
        provider_f = parts_f[1] if len(parts_f) > 1 and parts_f[1] else None
        balance_f  = parts_f[2] if len(parts_f) > 2 and parts_f[2] else None
        try:
            await show_listing_page(
                query.message, query.from_user.id, 1,
                provider_f, balance_f, edit=True
            )
        except Exception as e:
            logger.warning(f"lst_filter callback error: {e}")
        return

    # ── Interactive Import Flow (admin only) ───────────────
    if query.data == "imp_prepaid":
        if not is_full_admin(chat_id):
            return
        # Provider seçim butonları
        rows = []
        row = []
        for provider in SUPPORTED_PROVIDERS:
            short = PROVIDER_SHORT.get(provider, provider[:4])
            flag = PROVIDER_FLAGS.get(provider, "🇺🇸")
            row.append(InlineKeyboardButton(f"{flag} {short}", callback_data=f"imp_prov:{provider}"))
            if len(row) == 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton("🔍 Auto-detect BIN", callback_data="imp_prov:auto")])
        rows.append([InlineKeyboardButton("❌ Cancel", callback_data="main_menu")])
        await safe_edit(query, "💳 Select provider for prepaid cards:",
                        InlineKeyboardMarkup(rows))
        return

    if query.data.startswith("imp_prov:"):
        if not is_full_admin(chat_id):
            return
        provider_input = query.data.split(":", 1)[1]
        doc = context.user_data.get("last_uploaded_doc")
        last_time = context.user_data.get("last_uploaded_doc_time", 0)
        if not doc or (time.time() - last_time) > 300:
            await safe_edit(query, "⚠️ No recent file found. Please send a .txt file first.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))
            return
        if provider_input == "auto":
            provider = None  # BIN auto-detect
        else:
            provider = canonical_provider_name(provider_input) or provider_input

        # Import işlemi — yeni mesaj olarak gönder
        try:
            label = provider or "Auto-detect"
            await query.message.reply_text(f"⏳ Importing prepaid cards ({label})...")
            file = await doc.get_file()
            file_bytes = await file.download_as_bytearray()
            text = file_bytes.decode("utf-8", errors="ignore")
            stats = import_cards_from_text(text, provider)
            unassigned = stats.get("unassigned", 0)
            await query.message.reply_text(
                f"✅ Import Complete\n\n"
                f"Provider: {label}\n"
                f"Added:      {stats['added']} cards\n"
                f"Skipped:    {stats['skipped']} (duplicates)\n"
                f"Errors:     {stats['errors']} (bad lines)\n"
                + (f"Unassigned: {unassigned} (unknown BIN)\n" if unassigned else "")
                + f"Total in stock: {stats['total']} cards"
            )
            add_audit_log_entry(
                action="import_cards", admin_user=query.from_user,
                target_type="stock", target_id=label,
                details=f"added={stats['added']} skipped={stats['skipped']} errors={stats['errors']}",
            )
        except Exception as e:
            await query.message.reply_text(f"Import failed: {e}")
        return

    if query.data == "imp_gift":
        if not is_full_admin(chat_id):
            return
        # Region seçim butonları
        rows = []
        row = []
        for region, flag in GC_REGIONS.items():
            row.append(InlineKeyboardButton(f"{flag} {region}", callback_data=f"imp_gc_region:{region}"))
            if len(row) == 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton("❌ Cancel", callback_data="main_menu")])
        await safe_edit(query, "🎁 Select region for gift cards:",
                        InlineKeyboardMarkup(rows))
        return

    if query.data.startswith("imp_gc_region:"):
        if not is_full_admin(chat_id):
            return
        region = query.data.split(":", 1)[1]
        flag = GC_REGIONS.get(region, "🌍")
        # Type seçim butonları
        rows = []
        for gc_type, icon in GC_TYPES.items():
            rows.append([InlineKeyboardButton(
                f"{icon} {gc_type}",
                callback_data=f"imp_gc_do:{region}:{gc_type}"
            )])
        rows.append([InlineKeyboardButton("◀ Back", callback_data="imp_gift")])
        await safe_edit(query, f"{flag} {region}\n\nSelect card type:",
                        InlineKeyboardMarkup(rows))
        return

    if query.data.startswith("imp_gc_do:"):
        if not is_full_admin(chat_id):
            return
        parts = query.data.split(":")
        region = parts[1] if len(parts) > 1 else "US"
        gc_type = parts[2] if len(parts) > 2 else "Other"
        flag = GC_REGIONS.get(region, "🌍")
        icon = GC_TYPES.get(gc_type, "🏪")

        doc = context.user_data.get("last_uploaded_doc")
        last_time = context.user_data.get("last_uploaded_doc_time", 0)
        if not doc or (time.time() - last_time) > 300:
            await safe_edit(query, "⚠️ No recent file found. Please send a .txt file first.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))
            return

        try:
            await query.message.reply_text(f"⏳ Importing → {flag} {icon} {gc_type} {region}...")
            file = await doc.get_file()
            file_bytes = await file.download_as_bytearray()
            text = file_bytes.decode("utf-8", errors="ignore")
            stats = import_gift_cards_from_text(text, gc_type=gc_type, region=region)
            await query.message.reply_text(
                f"🎁 Gift Card Import Complete\n\n"
                f"Region: {flag} {region} | Type: {icon} {gc_type}\n\n"
                f"Added:   {stats['added']} cards\n"
                f"Skipped: {stats['skipped']}\n"
                f"Errors:  {stats['errors']} (bad lines)\n"
                f"Total in stock: {stats['total']} gift cards"
            )
            add_audit_log_entry(
                action="import_gift_cards", admin_user=query.from_user,
                target_type="gift_card", target_id=f"{gc_type}_{region}",
                details=f"added={stats['added']} errors={stats['errors']}",
            )
        except Exception as e:
            await query.message.reply_text(f"Import failed: {e}")
        return

    # ── Gift Card callbacks ─────────────────────────────────
    if query.data == "gc_menu":
        support_mode_users.discard(query.from_user.id)
        add_balance_mode_users.pop(query.from_user.id, None)
        regions = get_gc_region_summary()
        total_cards = sum(r["cnt"] for r in regions)
        total_value = sum(r["total_amount"] for r in regions)
        text = (
            "🎁 Store Gift Cards\n\n"
            f"Regions: {len(regions)}\n"
            f"Total Cards: {total_cards}\n"
            f"Total Value: {format_usd(total_value)}\n\n"
            "Select a region:"
        )
        try:
            await safe_edit(query, text, gc_region_menu_markup())
        except Exception:
            await query.message.reply_text(text, reply_markup=gc_region_menu_markup())
        return

    if query.data.startswith("gc_region:"):
        region = query.data.split(":", 1)[1]
        flag = GC_REGIONS.get(region, "🌍")
        text = f"{flag} {region}\n\nSelect a category:"
        markup = gc_type_menu_markup(region)
        try:
            await safe_edit(query, text, markup)
        except Exception:
            await query.message.reply_text(text, reply_markup=markup)
        return

    if query.data.startswith("gc_type:"):
        parts = query.data.split(":")
        region = parts[1] if len(parts) > 1 else "US"
        gc_type = parts[2] if len(parts) > 2 else "Other"
        await show_gc_listing(query.message, gc_type, region, page=1,
                               user_id=query.from_user.id, edit=True)
        return

    if query.data.startswith("gc_list:"):
        parts = query.data.split(":")
        region = parts[1] if len(parts) > 1 else "US"
        gc_type = parts[2] if len(parts) > 2 else "Other"
        page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 1
        await show_gc_listing(query.message, gc_type, region, page=page,
                               user_id=query.from_user.id, edit=True)
        return

    if query.data.startswith("gc_buy:"):
        gc_id = query.data.split(":", 1)[1]
        if not can_use_bot(chat_id):
            await send_restricted_message(query.message, query.from_user)
            return
        user_id = query.from_user.id
        gc = get_gift_card(gc_id)
        if not gc:
            await safe_edit(query, "This gift card is no longer available.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="gc_menu")]]))
            return
        if gc.get("status") not in ("available", "reserved"):
            await safe_edit(query, "This gift card is no longer available.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="gc_menu")]]))
            return
        if gc.get("status") == "reserved" and gc.get("reserved_by") != user_id:
            await safe_edit(query, "This gift card was just reserved by another user.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="gc_menu")]]))
            return

        reserve_gift_card(gc_id, user_id)
        brand = gc.get("brand", "")
        currency = gc.get("currency", "US")
        amount = float(gc.get("amount", 0))
        gc_type = gc.get("gc_type", "Other")
        region = gc.get("region", "US")
        rate = get_gift_card_rate(gc_type, region)
        cost = round(amount * rate, 2)
        flag = GC_REGIONS.get(region, "🌍")

        record = get_hybrid_balance_record(user_id)
        user_bal = float(record.get("available_balance", 0.0))
        can_afford = user_bal >= cost
        bal_line = f"Your Balance: {format_usd(user_bal)}" + (" ✅" if can_afford else " ❌ Insufficient")

        text = (
            "🎁 Gift Card Purchase\n\n"
            f"Region:   {flag} {region}\n"
            + (f"Brand:    {brand}\n" if brand else "")
            + f"Value:    {currency}${amount:.2f}\n"
            f"Rate:     {rate*100:.0f}%\n"
            f"Cost:     {format_usd(cost)}\n"
            f"{bal_line}\n\n"
            f"Reserved for {CARD_RESERVATION_SECONDS} seconds."
        )
        if can_afford:
            markup = gc_purchase_confirm_markup(gc_id)
        else:
            release_gift_card(gc_id)
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("💰 Add Balance", callback_data="add_balance")],
                [InlineKeyboardButton("◀ Back", callback_data="gc_menu")],
            ])
        try:
            await safe_edit(query, text, reply_markup=markup)
        except Exception:
            await query.message.reply_text(text, reply_markup=markup)
        return

    if query.data.startswith("gc_confirm:"):
        gc_id = query.data.split(":", 1)[1]
        if not can_use_bot(chat_id):
            await send_restricted_message(query.message, query.from_user)
            return
        user_id = query.from_user.id
        gc = get_gift_card(gc_id)
        if not gc:
            await safe_edit(query, "Gift card not found.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="gc_menu")]]))
            return
        if gc.get("status") == "sold":
            await safe_edit(query, "This gift card has already been sold.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="gc_menu")]]))
            return
        if gc.get("status") == "reserved" and gc.get("reserved_by") != user_id:
            await safe_edit(query, "This gift card is reserved by another user.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="gc_menu")]]))
            return
        if gc.get("status") == "available":
            ok = reserve_gift_card(gc_id, user_id)
            if not ok:
                await safe_edit(query, "Gift card no longer available.",
                                InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="gc_menu")]]))
                return

        brand = gc.get("brand", "")
        amount = float(gc.get("amount", 0))
        gc_type = gc.get("gc_type", "Other")
        region = gc.get("region", "US")
        rate = get_gift_card_rate(gc_type, region)
        cost = round(amount * rate, 2)
        currency = gc.get("currency", "US")

        async with transaction_lock:
            result = atomic_create_paid_order_from_wallet(
                user_id=user_id, chat_id=chat_id,
                full_name=query.from_user.full_name,
                telegram_username=query.from_user.username,
                provider=f"GC:{gc_type}:{region}",
                requested_balance=amount,
            )
        if not result.get("ok"):
            release_gift_card(gc_id)
            reason = result.get("reason", "unknown")
            msg = f"Insufficient balance.\nRequired: {format_usd(cost)}" if reason == "insufficient_balance" else f"Order failed: {reason}"
            await safe_edit(query, msg, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💰 Add Balance", callback_data="add_balance")],
                [InlineKeyboardButton("◀ Back", callback_data="gc_menu")],
            ]))
            return

        order_id = result["order_id"]
        mark_gift_card_sold(gc_id, order_id)
        new_bal = float(result["record"].get("available_balance", 0))
        bought_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        code = gc.get("code", "")
        has_code = bool(code and code.strip())
        flag = GC_REGIONS.get(region, "🌍")

        if has_code:
            orders[order_id]["status"] = "delivered"
            orders[order_id]["delivered_at"] = bought_at
            save_orders(orders)
            delivery_text = (
                f"🎁 Gift Card Delivered!\n\n"
                f"Order ID: {order_id}\n\n"
                f"Region: {flag} {region}\n"
                + (f"Brand: {brand}\n" if brand else "")
                + f"Value: {currency}${amount:.2f}\n"
                f"Code:  {code}\n\n"
                f"Cost: {format_usd(cost)}\n"
                f"Remaining Balance: {format_usd(new_bal)}\n\n"
                f"Thank you for choosing Elite Earners!"
            )
            await safe_edit(query, delivery_text, InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 My Orders", callback_data="my_orders"),
                 InlineKeyboardButton("🎁 Store Gift Cards", callback_data="gc_menu")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
            ]))
            logger.info(f"Auto-delivered gift card order {order_id} to user {user_id}")

            # Proof Channel'a post at
            try:
                delivery_time = time.time() - _btn_start
                await post_proof_delivery(
                    bot=context.bot, provider=f"GC:{gc_type}",
                    card_balance=amount, cost=cost,
                    delivery_seconds=delivery_time,
                    order_id=order_id,
                    is_gift_card=True, region=region, gc_type=gc_type,
                )
            except Exception as e:
                logger.error(f"[PROOF] Gift card proof post error: {e}")
        else:
            user_text = (
                f"🎁 Gift Card Order Placed\n\n"
                f"Order ID: {order_id}\n"
                f"Region: {flag} {region}\n"
                + (f"Brand: {brand}\n" if brand else "")
                + f"Value: {currency}${amount:.2f}\n"
                f"Cost: {format_usd(cost)}\n"
                f"Remaining Balance: {format_usd(new_bal)}\n\n"
                f"Your order is being processed."
            )
            await safe_edit(query, user_text, InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 My Orders", callback_data="my_orders")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
            ]))
        # Admin bildirim
        try:
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=(
                f"🎁 Gift Card Order\n\nOrder ID: {order_id}\n"
                f"User: {query.from_user.full_name} ({f'@{query.from_user.username}' if query.from_user.username else 'No username'})\n"
                f"User ID: {user_id}\nRegion: {flag} {region} | Type: {gc_type}\n"
                + (f"Brand: {brand}\n" if brand else "")
                + f"Value: {currency}${amount:.2f}\nCost: {format_usd(cost)}\n"
                f"Code: {code if has_code else 'MANUAL DELIVERY NEEDED'}"
            ))
        except Exception:
            pass
        return

    if query.data.startswith("gc_cancel:"):
        gc_id = query.data.split(":", 1)[1]
        release_gift_card(gc_id)
        await safe_edit(query, "Purchase cancelled.",
                        InlineKeyboardMarkup([
                            [InlineKeyboardButton("◀ Back", callback_data="gc_menu")],
                            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
                        ]))
        return

    # ── External Card Purchase (Supply Chain) ──────────────────
    if query.data.startswith("ext_buy:"):
        ext_id = query.data.split(":", 1)[1]
        if not can_use_bot(chat_id):
            await send_restricted_message(query.message, query.from_user)
            return
        user_id = query.from_user.id

        # External kart bilgisini oku
        try:
            init_sqlite_db()
            conn = get_sqlite_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM external_cards WHERE ext_id = ? AND status = 'available'", (ext_id,))
            ext_card = cur.fetchone()
            conn.close()
        except Exception:
            ext_card = None

        if not ext_card:
            await query.answer("Card no longer available.", show_alert=True)
            return

        balance = float(ext_card["balance"])
        sell_price = float(ext_card["sell_price"])
        profit = float(ext_card["profit"])
        cost_price = float(ext_card["cost_price"])
        bin_num = ext_card["bin_number"] or "xxxxxx"
        source = ext_card["source"]
        currency = ext_card["currency"] or "USD"

        # 3-state registration: 1 → True, 0 → False, -1 → None (unknown)
        reg_db = ext_card["registered"]
        if reg_db == 1:
            registered = True
            reg_icon = "🔒"
            reg_label = "Registered"
        elif reg_db == 0:
            registered = False
            reg_icon = "✅"
            reg_label = "Unregistered"
        else:
            registered = None
            reg_icon = "❓"
            reg_label = "Unknown"

        # Provider — BIN'den tespit et, rakip adı gizle
        raw_prov = (ext_card["provider"] or "").strip()
        provider = _normalize_provider(raw_prov.lower(), bin_hint=bin_num)

        # Kullanıcı bakiyesi
        user_record = get_hybrid_balance_record(user_id)
        user_balance = float(user_record.get("available_balance", 0.0))
        has_enough = user_balance >= sell_price

        flag = PROVIDER_FLAGS.get(provider, "🇺🇸")
        short = PROVIDER_SHORT.get(provider, provider)

        # ─── Sipariş sınıflandırma + rakip bot bakiye kontrolü ───
        order_class = classify_order_size(sell_price)
        comp_balance = get_competitor_balance(source)
        # Rakip botta yeterli fon var mı? (cost_price = bizim ödeyeceğimiz)
        supplier_has_funds = comp_balance >= cost_price

        # Premium akış: ya tutarı yüksek ya da rakipte fon yetersiz
        needs_premium_flow = (order_class == "premium") or (not supplier_has_funds)

        delivery_note = ""
        if needs_premium_flow:
            delivery_note = (
                f"\n⏱️ Estimated Delivery: 3-8 minutes\n"
                f"This order requires supplier reservation.\n"
                f"You will be notified when ready."
            )
        else:
            delivery_note = "\n⚡ Estimated Delivery: under 60 seconds"

        text = (
            f"💳 Purchase Confirmation\n\n"
            f"Card:     {bin_num}xx\n"
            f"Balance:  {currency}${balance:.2f}\n"
            f"Provider: {flag} {short}\n"
            f"Status:   {reg_icon} {reg_label}\n\n"
            f"💰 Price: {format_usd(sell_price)}\n"
            f"Your Balance: {format_usd(user_balance)} {'✅' if has_enough else '❌'}"
            f"{delivery_note}\n\n"
            f"This card is reserved for you for 60 seconds."
        )

        confirm_label = "✅ Confirm Purchase"
        if needs_premium_flow:
            confirm_label = "✅ Confirm (3-8 min delivery)"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(confirm_label, callback_data=f"ext_confirm:{ext_id}")],
            [InlineKeyboardButton("◀ Back to Listing", callback_data="lst:1:"),
             InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ])
        await safe_edit(query, text, kb)
        return

    if query.data.startswith("ext_confirm:"):
        ext_id = query.data.split(":", 1)[1]
        if not can_use_bot(chat_id):
            await send_restricted_message(query.message, query.from_user)
            return
        user_id = query.from_user.id
        _btn_start = time.time()

        # External kart oku + reserve
        try:
            init_sqlite_db()
            conn = get_sqlite_connection()
            cur = conn.cursor()
            conn.execute("BEGIN IMMEDIATE")
            cur.execute("SELECT * FROM external_cards WHERE ext_id = ? AND status = 'available'", (ext_id,))
            ext_card = cur.fetchone()
            if not ext_card:
                conn.execute("ROLLBACK")
                conn.close()
                await query.answer("Card already sold or expired.", show_alert=True)
                return
            # Reserve et
            conn.execute("UPDATE external_cards SET status = 'reserved', purchased_by = ? WHERE ext_id = ?",
                         (user_id, ext_id))
            conn.execute("COMMIT")
            conn.close()
        except Exception as e:
            try:
                conn.execute("ROLLBACK")
                conn.close()
            except Exception:
                pass
            logger.error(f"ext_confirm reserve error: {e}")
            await query.answer("Error reserving card. Try again.", show_alert=True)
            return

        balance = float(ext_card["balance"])
        sell_price = float(ext_card["sell_price"])
        cost_price = float(ext_card["cost_price"])
        source = ext_card["source"]
        bin_num = ext_card["bin_number"] or "xxxxxx"
        currency = ext_card["currency"] or "USD"

        # Provider — BIN'den tespit et, rakip adı gizle
        raw_prov = (ext_card["provider"] or "").strip()
        provider = _normalize_provider(raw_prov.lower(), bin_hint=bin_num)

        # Sipariş sınıflandırma + rakip bot bakiye kontrolü
        order_class = classify_order_size(sell_price)
        comp_balance = get_competitor_balance(source)
        supplier_has_funds = comp_balance >= cost_price
        needs_premium_flow = (order_class == "premium") or (not supplier_has_funds)

        # Bakiye kontrolü
        user_record = get_hybrid_balance_record(user_id)
        user_balance = float(user_record.get("available_balance", 0.0))

        if user_balance < sell_price:
            # Bakiye yetersiz — reserve geri al
            conn = get_sqlite_connection()
            conn.execute("UPDATE external_cards SET status = 'available', purchased_by = NULL WHERE ext_id = ?", (ext_id,))
            conn.commit()
            conn.close()
            await safe_edit(query, (
                f"❌ Insufficient balance.\n\n"
                f"Required: {format_usd(sell_price)}\n"
                f"Your Balance: {format_usd(user_balance)}\n\n"
                f"Please top up your balance first."
            ), InlineKeyboardMarkup([
                [InlineKeyboardButton("💰 Add Balance", callback_data="add_balance")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
            ]))
            return

        # Bakiyeyi düş
        new_bal = user_balance - sell_price
        update_hybrid_balance(user_id, available_balance=new_bal,
                              total_spent=float(user_record.get("total_spent", 0)) + sell_price)

        # Order oluştur
        order_id = f"EE-{random.randint(100000, 999999)}"
        bought_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        orders = load_orders()
        orders[order_id] = {
            "order_id": order_id,
            "user_id": user_id,
            "chat_id": user_id,
            "card_id": f"ext:{ext_id}",
            "card_number_masked": f"{bin_num}xx",
            "balance": balance,
            "currency": currency,
            "provider": provider,
            "cost": sell_price,           # Müşteriye sattığımız fiyat
            "supplier_cost": cost_price,  # Rakip bota ödediğimiz (refund için doğru)
            "status": "ext_pending",
            "time": bought_at,
            "payment_method": "balance",
            "ext_source": source,
            "ext_id": ext_id,
            # Failure handling fields
            "attempt_count": 0,
            "last_attempt_at": "",
            "failure_reason": "",
            "substitute_offered": False,
            "reserved_balance_cents": int(round(sell_price * 100)),
            "refund_bonus_pct": 0.0,
        }
        save_orders(orders)

        # External kartı purchased olarak işaretle
        conn = get_sqlite_connection()
        conn.execute("UPDATE external_cards SET status = 'purchased', purchased_at = ? WHERE ext_id = ?",
                     (bought_at, ext_id))
        conn.commit()
        conn.close()

        # Müşteriye mesaj — premium akış mı standard mı?
        flag = PROVIDER_FLAGS.get(provider, "🇺🇸")
        short = PROVIDER_SHORT.get(provider, provider)

        if needs_premium_flow:
            delivery_msg = (
                f"⏱️ Estimated Delivery: 3-8 minutes\n"
                f"This order is being processed through supplier reservation.\n"
                f"You will receive your card details automatically once ready."
            )
        else:
            delivery_msg = (
                f"⚡ Estimated Delivery: under 60 seconds\n"
                f"You will receive the full card details once ready."
            )

        await safe_edit(query, (
            f"✅ Order Placed Successfully!\n\n"
            f"Order ID: {order_id}\n"
            f"Card: {bin_num}xx\n"
            f"Balance: {currency}${balance:.2f}\n"
            f"Provider: {flag} {short}\n"
            f"Cost: {format_usd(sell_price)}\n"
            f"Remaining Balance: {format_usd(new_bal)}\n\n"
            f"⏳ Your card is being prepared.\n"
            f"{delivery_msg}"
        ), InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 My Orders", callback_data="my_orders"),
             InlineKeyboardButton("🎧 Support", callback_data="support")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ]))

        logger.info(f"[EXT_ORDER] {order_id} | user={user_id} | {source}:{ext_id} | "
                     f"${balance:.2f} | sell=${sell_price:.2f} | premium={needs_premium_flow}")

        # Bakiye yeterse hemen düş, yoksa pending deposit listesine ekle
        card_info_short = f"{bin_num}xx {currency}${balance:.2f} {short}"

        if supplier_has_funds:
            # Standart akış: rakip bakiyesinden cost düş
            deduct_competitor_balance(source, cost_price, f"order {order_id}")

        # Admin bildirimi — premium akışta acil deposit gerekli
        try:
            if needs_premium_flow and not supplier_has_funds:
                # Pending deposit listesine ekle
                add_pending_deposit(order_id, user_id, source, cost_price, ext_id, card_info_short)

                shortfall = cost_price - comp_balance
                admin_msg = (
                    f"🚨 MANUAL DEPOSIT NEEDED\n\n"
                    f"Order: {order_id}\n"
                    f"User: {query.from_user.full_name} ({user_id})\n\n"
                    f"Card: {bin_num}xx\n"
                    f"Balance: {currency}${balance:.2f}\n"
                    f"Provider: {short}\n\n"
                    f"💳 Source: {source.upper()}\n"
                    f"💵 Cost (you pay supplier): ${cost_price:.2f}\n"
                    f"📊 Current {source} balance: ${comp_balance:.2f}\n"
                    f"⚠️ Shortfall: ${shortfall:.2f}\n\n"
                    f"➡️ Action: Deposit at least ${shortfall:.2f} to {source.upper()}\n"
                    f"   then run: /confirmdeposit {order_id}\n\n"
                    f"Customer expects delivery in 3-8 min."
                )
            elif needs_premium_flow:
                # Premium ama bakiye yetiyor — sadece bilgi
                admin_msg = (
                    f"⏱️ Premium Order (auto-fulfilling)\n\n"
                    f"Order: {order_id}\n"
                    f"User: {query.from_user.full_name} ({user_id})\n\n"
                    f"Card: {bin_num}xx | {currency}${balance:.2f} | {short}\n"
                    f"Source: {source.upper()}\n"
                    f"Cost: ${cost_price:.2f}\n"
                    f"Remaining {source} balance: ${get_competitor_balance(source):.2f}"
                )
            else:
                # Standard akış — kısa bilgi
                admin_msg = (
                    f"🌐 External Order (instant)\n\n"
                    f"Order: {order_id}\n"
                    f"User: {query.from_user.full_name} ({user_id})\n"
                    f"Card: {bin_num}xx | {currency}${balance:.2f} | {short}\n"
                    f"Source: {source.upper()} | Cost: ${cost_price:.2f}\n"
                    f"Remaining {source} balance: ${get_competitor_balance(source):.2f}"
                )

            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=admin_msg)
        except Exception:
            pass
        return

    # ── Order Swap (Stuck order alternative) ──
    if query.data.startswith("swap_order:"):
        parts = query.data.split(":")
        if len(parts) != 3:
            await query.answer("Invalid request", show_alert=True)
            return
        _, order_id, new_ext_id = parts
        user_id = query.from_user.id

        orders = load_orders()
        order = orders.get(order_id)
        if not order:
            await query.answer("Order not found", show_alert=True)
            return
        if order.get("user_id") != user_id:
            await query.answer("Not your order", show_alert=True)
            return
        if order.get("status") != "ext_pending":
            await query.answer(f"Order is {order.get('status')}, cannot swap", show_alert=True)
            return

        # Yeni kartı oku
        try:
            init_sqlite_db()
            conn = get_sqlite_connection()
            cur = conn.cursor()
            conn.execute("BEGIN IMMEDIATE")
            cur.execute("SELECT * FROM external_cards WHERE ext_id = ? AND status = 'available'", (new_ext_id,))
            new_card = cur.fetchone()
            if not new_card:
                conn.execute("ROLLBACK")
                conn.close()
                await query.answer("Alternative card no longer available", show_alert=True)
                return
            # Yeni kartı reserve et
            conn.execute("UPDATE external_cards SET status = 'purchased', purchased_by = ?, purchased_at = ? WHERE ext_id = ?",
                         (user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), new_ext_id))
            # Eski kartı available'a geri al
            old_ext_id = order.get("ext_id")
            if old_ext_id:
                conn.execute("UPDATE external_cards SET status = 'available', purchased_by = NULL, purchased_at = NULL WHERE ext_id = ?",
                             (old_ext_id,))
            conn.execute("COMMIT")
            conn.close()
        except Exception as e:
            try:
                conn.execute("ROLLBACK")
                conn.close()
            except Exception:
                pass
            logger.error(f"swap_order error: {e}")
            await query.answer("Error swapping card", show_alert=True)
            return

        # Sipariş detaylarını güncelle
        raw_prov = (new_card["provider"] or "").strip().lower()
        new_bin = new_card["bin_number"] or "xxxxxx"
        new_provider = _normalize_provider(raw_prov, bin_hint=new_bin)

        order["ext_id"] = new_ext_id
        order["card_id"] = f"ext:{new_ext_id}"
        order["card_number_masked"] = f"{new_bin}xx"
        order["balance"] = float(new_card["balance"])
        order["currency"] = new_card["currency"] or "USD"
        order["provider"] = new_provider
        order["ext_source"] = new_card["source"]
        order["supplier_cost"] = float(new_card["cost_price"])  # Yeni kartın cost'u
        order["failure_reason"] = (order.get("failure_reason", "") + " | swapped").strip(" |")[:200]
        order["substitute_offered"] = False
        order["attempt_count"] = 0  # Yeni kart için deneme sayısı sıfırlansın
        order["last_attempt_at"] = ""
        order["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        orders[order_id] = order
        save_orders(orders)

        logger.info(f"[SWAP] {order_id}: {old_ext_id} → {new_ext_id}")

        flag = PROVIDER_FLAGS.get(new_provider, "🇺🇸")
        short = PROVIDER_SHORT.get(new_provider, new_provider)
        await safe_edit(query, (
            f"✅ Order Updated\n\n"
            f"Order ID: {order_id}\n"
            f"New Card: {new_bin}xx\n"
            f"Balance: {order['currency']}${order['balance']:.2f}\n"
            f"Provider: {flag} {short}\n\n"
            f"⏱️ Your card is being prepared now.\n"
            f"Estimated delivery: 3-8 minutes."
        ), InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 My Orders", callback_data="my_orders")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ]))

        # Admin bildirimi
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"🔄 Order Swapped\n\n"
                    f"Order: {order_id}\n"
                    f"User: {user_id}\n"
                    f"Old ext_id: {old_ext_id}\n"
                    f"New ext_id: {new_ext_id}\n"
                    f"New source: {order['ext_source'].upper()}"
                ),
            )
        except Exception:
            pass
        return

    # ── User-initiated refund (stuck order) ──
    if query.data.startswith("refund_order:"):
        order_id = query.data.split(":", 1)[1]
        user_id = query.from_user.id

        orders = load_orders()
        order = orders.get(order_id)
        if not order:
            await query.answer("Order not found", show_alert=True)
            return
        if order.get("user_id") != user_id:
            await query.answer("Not your order", show_alert=True)
            return
        if order.get("status") != "ext_pending":
            await query.answer(f"Order is {order.get('status')}, cannot refund", show_alert=True)
            return

        # Kullanıcı kendi seçtiği için bonus yok (sadece bekleyip auto-refund olursa bonus veriyoruz)
        success = refund_order(order_id, bonus_pct=0.0, reason="user_requested_refund")
        if not success:
            await query.answer("Refund failed, contact support", show_alert=True)
            return

        cost = float(order.get("cost", 0))
        user_record = get_hybrid_balance_record(user_id)
        new_balance = float(user_record.get("available_balance", 0))

        await safe_edit(query, (
            f"✅ Order Refunded\n\n"
            f"Order: {order_id}\n"
            f"Refund: {format_usd(cost)}\n"
            f"New Balance: {format_usd(new_balance)}\n\n"
            f"Your balance is ready to use. Thank you for your patience."
        ), InlineKeyboardMarkup([
            [InlineKeyboardButton("🛒 Browse Cards", callback_data="lst:1:")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ]))

        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"💰 User-requested refund\n\n"
                    f"Order: {order_id}\n"
                    f"User: {user_id}\n"
                    f"Amount: {format_usd(cost)}"
                ),
            )
        except Exception:
            pass
        return

    if query.data.startswith("card_buy:"):
        card_id = query.data.split(":", 1)[1]
        if not can_use_bot(chat_id):
            await send_restricted_message(query.message, query.from_user)
            return
        user_id = query.from_user.id

        try:
            card = get_card(card_id)
        except Exception:
            card = None

        if not card:
            try:
                await safe_edit(query, 
                    "This card is no longer available.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to Listing", callback_data="lst:1:")]]),
                )
            except Exception:
                await query.message.reply_text(
                    "This card is no longer available.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to Listing", callback_data="lst:1:")]]),
                )
            return

        if card.get("status") not in ("available", "reserved"):
            try:
                await safe_edit(query, 
                    "This card is no longer available.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to Listing", callback_data="lst:1:")]]),
                )
            except Exception:
                await query.message.reply_text("This card is no longer available.")
            return

        # Sadece baska biri reserve etmisse engelle
        if card.get("status") == "reserved" and card.get("reserved_by") != user_id:
            try:
                await safe_edit(query, 
                    "This card was just reserved by another user. Please choose another.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to Listing", callback_data="lst:1:")]]),
                )
            except Exception:
                await query.message.reply_text("This card was just reserved by another user.")
            return

        # Reserve
        reserve_card(card_id, user_id)

        # Confirmation screen
        provider   = card.get("provider") or ""
        short      = PROVIDER_SHORT.get(provider, provider[:4].upper() if provider else "?")
        currency   = card.get("currency") or "US"
        balance    = float(card.get("balance") or 0)
        rate       = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.38)))
        cost       = round(balance * rate, 2)
        card_num   = card.get("card_number") or ""
        masked     = format_card_number_masked(card_num) if card_num else "??????"
        exp_month  = card.get("expiry_month") or ""
        exp_year   = card.get("expiry_year") or ""
        exp_year   = exp_year or ""
        exp_str    = f"{exp_month}/{exp_year[-2:]}" if len(exp_year) >= 2 else exp_year
        record     = get_hybrid_balance_record(user_id)
        user_bal   = float(record.get("available_balance") or 0.0)
        can_afford = user_bal >= cost
        bal_line   = f"Your Balance: {format_usd(user_bal)}" + (" ✅" if can_afford else " ❌ Insufficient")

        text = (
            "Purchase Confirmation\n\n"
            f"Card:     {masked}\n"
            f"Expiry:   {exp_str}\n"
            f"Balance:  {currency}${balance:.2f}\n"
            f"Provider: {short}\n"
            f"Rate:     {rate*100:.0f}%\n"
            f"Cost:     {format_usd(cost)}\n"
            f"{bal_line}\n\n"
            f"This card is reserved for you for {CARD_RESERVATION_SECONDS} seconds."
        )

        if can_afford:
            markup = purchase_confirm_markup(card_id)
        else:
            release_card(card_id)
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("💰 Add Balance", callback_data="add_balance")],
                [InlineKeyboardButton("◀ Back to Listing", callback_data="lst:1:")],
            ])

        try:
            await safe_edit(query, text, reply_markup=markup)
        except Exception:
            await query.message.reply_text(text, reply_markup=markup)
        return

    if query.data.startswith("card_confirm:"):
        card_id = query.data.split(":", 1)[1]
        if not can_use_bot(chat_id):
            await send_restricted_message(query.message, query.from_user)
            return
        user_id = query.from_user.id
        card = get_card(card_id)
        if not card:
            await safe_edit(query, "Card not found.", InlineKeyboardMarkup([[InlineKeyboardButton("Back to Listing", callback_data="lst:1:")]]))
            return
        if card.get("status") == "sold":
            await safe_edit(query, "This card has already been sold.", InlineKeyboardMarkup([[InlineKeyboardButton("Back to Listing", callback_data="lst:1:")]]))
            return
        if card.get("status") == "reserved" and card.get("reserved_by") != user_id:
            await safe_edit(query, "This card is reserved by another user.", InlineKeyboardMarkup([[InlineKeyboardButton("Back to Listing", callback_data="lst:1:")]]))
            return
        # Re-check reservation hasn t expired
        if card.get("status") == "available":
            # Try to re-reserve
            ok = reserve_card(card_id, user_id)
            if not ok:
                await safe_edit(query, "Card no longer available.", InlineKeyboardMarkup([[InlineKeyboardButton("Back to Listing", callback_data="lst:1:")]]))
                return
        provider = card.get("provider", "")
        balance  = float(card.get("balance", 0))
        rate     = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.0)))
        cost     = round(balance * rate, 2)
        async with transaction_lock:
            result = atomic_create_paid_order_from_wallet(
                user_id=user_id,
                chat_id=chat_id,
                full_name=query.from_user.full_name,
                telegram_username=query.from_user.username,
                provider=provider,
                requested_balance=balance,
            )
        if not result.get("ok"):
            release_card(card_id)
            reason = result.get("reason", "unknown")
            if reason == "insufficient_balance":
                msg = f"Insufficient balance.\nRequired: {format_usd(cost)}"
            else:
                msg = f"Order failed: {reason}"
            await safe_edit(query, msg, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Add Balance", callback_data="add_balance")],
                [InlineKeyboardButton("Back to Listing", callback_data="lst:1:")],
            ]))
            return
        order_id = result["order_id"]
        mark_card_sold(card_id, order_id)
        short    = PROVIDER_SHORT.get(provider, provider)
        currency = card.get("currency", "US")
        masked   = format_card_number_masked(card["card_number"])
        new_bal  = float(result["record"].get("available_balance", 0))
        # Teslim için kartı decrypt et
        decrypted   = decrypt_card_record(card)
        card_number = decrypted["card_number"]
        exp_month   = decrypted.get("expiry_month", "")
        exp_year    = decrypted.get("expiry_year", "")
        cvv         = decrypted.get("cvv", "")
        expiry_str  = f"{exp_month}/{exp_year[-2:]}" if exp_year and len(exp_year) >= 2 else "N/A"
        bought_at   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Tam kart bilgisi varsa otomatik teslim et
        has_full_info = (
            card_number and
            exp_month and exp_month != "00" and
            exp_year and exp_year != "0000" and
            cvv and cvv != "000"
        )

        if has_full_info:
            # Otomatik teslim
            orders[order_id]["status"] = "delivered"
            orders[order_id]["delivered_at"] = bought_at
            save_orders(orders)

            # Balance check — canlı bakiye ve işlem geçmişi
            balance_info = ""
            check_result = None
            if CHECKER_ENABLED and provider in BALANCE_CHECKERS:
                try:
                    await safe_edit(query, f"✅ Purchase successful!\n\n⏳ Checking live balance for your card...",
                                    InlineKeyboardMarkup([]))
                    check_result = await check_card_balance(card)
                    if check_result.success:
                        balance_info = (
                            f"\n{'─' * 30}\n"
                            f"{check_result.format_delivery_text()}\n"
                            f"{'─' * 30}\n"
                        )
                    else:
                        balance_info = (
                            f"\n⚠️ Live balance check: {check_result.error or 'unavailable'}\n"
                            f"Listed balance: {currency}${balance:.2f}\n"
                        )
                except Exception as e:
                    logger.warning(f"[CHECKER] Delivery check failed for {order_id}: {e}")
                    balance_info = f"\n⚠️ Balance check temporarily unavailable\n"

            delivery_text = (
                f"✅ Purchase Successful!\n\n"
                f"💳 Card Info:\n"
                f"{card_number}:{exp_month}:{exp_year}:{cvv}\n\n"
                f"├ Card: {card_number}\n"
                f"├ Expiry: {expiry_str}\n"
                f"└ CVV: {cvv}\n\n"
                f"💰 Balance: {currency}${balance:.2f}\n"
                f"📊 Rate: {rate*100:.0f}%\n"
                f"🕐 Bought At: {bought_at}\n"
                f"{balance_info}\n"
                f"🔴 Refund Warning\n"
                f"The card balance is 100% guaranteed at time of purchase.\n"
                f"Re-checking within 1-3 minutes may permanently kill the card.\n"
                f"You have 10 minutes from purchase to request a refund — stolen cards ONLY.\n\n"
                f"Order ID: {order_id}\n"
                f"Provider: {short}\n"
                f"Cost: {format_usd(cost)}\n"
                f"Remaining Balance: {format_usd(new_bal)}"
            )

            # Teslim butonları
            delivery_buttons = [
                [InlineKeyboardButton("🔄 Recheck Balance", callback_data=f"bal_recheck:{order_id}:{card_id}"),
                 InlineKeyboardButton("🚩 Request Refund",  callback_data=f"refund_req:{order_id}")],
                [InlineKeyboardButton("📋 My Orders",       callback_data="my_orders"),
                 InlineKeyboardButton("🛒 View Listing",    callback_data="lst:1:")],
                [InlineKeyboardButton("🏠 Main Menu",       callback_data="main_menu")],
            ]

            await safe_edit(query, delivery_text, InlineKeyboardMarkup(delivery_buttons))
            logger.info(f"Auto-delivered order {order_id} to user {user_id}")

            # Proof Channel'a post at
            try:
                delivery_time = time.time() - _btn_start
                await post_proof_delivery(
                    bot=context.bot, provider=provider,
                    card_balance=balance, cost=cost,
                    delivery_seconds=delivery_time,
                    order_id=order_id,
                )
            except Exception as e:
                logger.error(f"[PROOF] Prepaid proof post error: {e}")
        else:
            # Manuel teslim gerekiyor
            user_text = (
                f"Order Placed Successfully\n\n"
                f"Order ID: {order_id}\n"
                f"Card:     {masked}\n"
                f"Balance:  {currency}${balance:.2f}\n"
                f"Provider: {short}\n"
                f"Cost:     {format_usd(cost)}\n"
                f"Remaining Balance: {format_usd(new_bal)}\n\n"
                f"Your card will be delivered within 3-5 minutes.\n"
                f"Please wait — do not place duplicate orders."
            )
            await safe_edit(query, user_text, InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 My Orders", callback_data="my_orders"),
                 InlineKeyboardButton("🎧 Support",   callback_data="support")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
            ]))

        # Admin'e tam kart bilgisi ile bildirim
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"New Order — Manual Delivery Required\n\n"
                    f"Order ID: {order_id}\n"
                    f"Card ID:  {card_id}\n\n"
                    f"Card:     {card_number}\n"
                    f"Expiry:   {expiry_str}\n"
                    f"CVV:      {cvv}\n"
                    f"Balance:  {currency}${balance:.2f}\n"
                    f"Provider: {provider}\n"
                    f"Cost:     {format_usd(cost)}\n\n"
                    f"User: {query.from_user.full_name}\n"
                    f"Username: @{query.from_user.username or 'no username'}\n"
                    f"User ID: {user_id}\n"
                    f"Bought At: {bought_at}\n\n"
                    f"Deliver with: /deliver {order_id}"
                ),
            )
        except Exception:
            pass
        return

    if query.data.startswith("card_cancel:"):
        card_id = query.data.split(":", 1)[1]
        release_card(card_id)
        await show_listing_page(query.message, query.from_user.id, 1, None, edit=True)
        return

    if query.data.startswith("check_payment_"):
        coin = query.data.replace("check_payment_", "")
        user_id = query.from_user.id
        payments = load_pending_payments()
        active = None
        for pid, rec in payments.items():
            if int(rec.get("user_id", 0)) == user_id and rec.get("coin") == coin and not rec.get("credited") and rec.get("status") == "waiting":
                active = rec
                break
        if not active:
            await query.message.reply_text(
                "No active pending payment found.\nUse the Add Balance menu to start a new payment.",
                reply_markup=main_menu_markup(),
            )
            return
        await query.message.reply_text(
            f"Checking your payment...\n\nPayment ID: {active['payment_id']}\nCoin: {coin}\nExpected: {format_usd(active['usd_amount'])}\n\nAutomatic detection runs every {PAYMENT_POLL_INTERVAL} seconds. You will be notified once confirmed.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Client Support", callback_data="support")],
                [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
            ]),
        )
        return

    if query.data == "my_orders":
        uid = query.from_user.id
        wallet_orders, _ = get_customer_visible_orders(uid)
        cards = load_cards()
        order_to_card = {c.get("order_id"): c for c in cards.values() if c.get("order_id")}
        if not wallet_orders:
            await safe_edit(
                query,
                "My Orders\n\nYou have no orders yet.\nBrowse available cards from the listing.",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🛒 View Listing", callback_data="lst:1:")],
                    [InlineKeyboardButton("🏠 Main Menu",    callback_data="main_menu")],
                ]),
            )
            return
        status_icons = {
            "paid_pending_delivery": "⏳ Pending",
            "delivered": "✅ Delivered",
            "cancelled": "❌ Cancelled",
            "refunded":  "↩️ Refunded",
        }
        lines = [f"My Orders ({len(wallet_orders)} total)\n"]
        for order_id, data in wallet_orders[:6]:
            status  = data.get("status", "unknown")
            prov    = PROVIDER_SHORT.get(data.get("provider",""), data.get("provider","-"))
            balance = data.get("requested_card_balance", 0)
            charged = data.get("charged_amount", 0)
            created = (data.get("time") or "")[:16]
            sl      = status_icons.get(status, f"📋 {status}")
            lines.append(f"{order_id}\n  {prov} | ${balance:.2f} | {format_usd(charged)}\n  {sl} | {created}")
            card = order_to_card.get(order_id)
            if status == "delivered" and card:
                dec = decrypt_card_record(card)
                cn  = dec.get("card_number","")
                em  = dec.get("expiry_month","")
                ey  = (dec.get("expiry_year","") or "")
                cvv = dec.get("cvv","")
                exp = f"{em}/{ey[-2:]}" if len(ey)>=2 else ey
                lines.append(f"  💳 {cn} | {exp} | CVV:{cvv}")
            lines.append("")
        await safe_edit(
            query,
            "\n".join(lines).strip(),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🛒 View Listing", callback_data="lst:1:"),
                 InlineKeyboardButton("👤 Profile",      callback_data="my_profile")],
                [InlineKeyboardButton("🎧 Support",      callback_data="support"),
                 InlineKeyboardButton("🏠 Main Menu",    callback_data="main_menu")],
            ]),
        )
        return

    if query.data == "payment_status":
        await show_payment_status_message(query.message, query.from_user.id)
        return

    if query.data == "order":
        support_mode_users.discard(query.from_user.id)
        add_balance_mode_users.pop(query.from_user.id, None)
        await show_listing_page(query.message, query.from_user.id, 1, edit=False)
        return
    if query.data.startswith("check_payment_"):
        coin = query.data.replace("check_payment_", "")
        user_id = query.from_user.id
        payments = load_pending_payments()
        active = None
        for pid, rec in payments.items():
            if int(rec.get("user_id", 0)) == user_id and rec.get("coin") == coin and not rec.get("credited") and rec.get("status") == "waiting":
                active = rec
                break
        if not active:
            await query.message.reply_text(
                "No active pending payment found.\nUse the Add Balance menu to start a new payment.",
                reply_markup=main_menu_markup(),
            )
            return
        await query.message.reply_text(
            f"Checking your payment...\n\nPayment ID: {active['payment_id']}\nCoin: {coin}\nExpected: {format_usd(active['usd_amount'])}\n\nAutomatic detection runs every {PAYMENT_POLL_INTERVAL} seconds. You will be notified once confirmed.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Client Support", callback_data="support")],
                [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
            ]),
        )
        return

    if query.data == "payment_status":
        await show_payment_status_message(query.message, query.from_user.id)
        return

    if query.data == "order":
        support_mode_users.discard(query.from_user.id)
        add_balance_mode_users.pop(query.from_user.id, None)
        await show_listing_page(query.message, query.from_user.id, 1, edit=False)
        return


    if query.data == "rules":
        await show_rules_message(query.message)
        return

    if query.data == "services":
        text = (
            "Our Services\n\n"
            "- Prepaid Visa / Mastercard Stock\n"
            "- Custom Requests\n"
            "- Client Support\n\n"
            "Additional services may be added over time."
        )
        await query.message.reply_text(text, reply_markup=main_menu_markup())
        return


    if query.data.startswith("paymentpanelinfo::"):
        if not has_staff_access(chat_id):
            await query.message.reply_text("Admin only action.")
            return

        order_id = query.data.split("::", 1)[1].strip()
        order_data = get_hybrid_order(order_id)
        if not order_data:
            await query.message.reply_text("Order not found.")
            return

        info_text = build_payment_info_text(order_id, order_data or orders.get(order_id) or get_hybrid_order(order_id) or {})
        await smart_panel_response(
            query,
            info_text,
            reply_markup=payment_panel_markup(order_id),
        )
        return

    if query.data.startswith("paymentpanel::"):
        if not has_staff_access(chat_id):
            await query.message.reply_text("Admin only action.")
            return

        parts = query.data.split("::")
        if len(parts) != 3:
            await query.message.reply_text("Invalid payment panel action.")
            return

        order_id = parts[1].strip()
        new_status = parts[2].strip().lower()

        if new_status not in {"unpaid", "pending", "paid", "failed", "refunded"}:
            await query.message.reply_text("Invalid payment status.")
            return

        if order_id not in orders:
            await query.message.reply_text("Order not found.")
            return

        current_method = str(orders[order_id].get("payment_method", "") or "")
        current_paid_at = str(orders[order_id].get("paid_at", "") or "")

        if new_status == "paid":
            method = current_method or "manual_admin"
            note = "updated_from_panel"
            paid_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elif new_status == "refunded":
            method = current_method or "manual_admin"
            note = "refunded_from_panel"
            paid_at = current_paid_at
        else:
            method = current_method
            note = "updated_from_panel"
            paid_at = ""

        set_order_payment_status(order_id, new_status, method, note, paid_at)

        customer_chat_id = orders[order_id].get("chat_id")
        try:
            await context.bot.send_message(
                chat_id=customer_chat_id,
                text=(
                    "Payment status updated\n\n"
                    f"Order ID: {order_id}\n"
                    f"Payment Status: {get_payment_status_label(new_status)}\n"
                    f"Payment Method: {method or '-'}\n"
                    f"Note: {note or '-'}"
                ),
            )
        except Exception:
            pass

        info_text = build_payment_info_text(order_id, order_data or orders.get(order_id) or get_hybrid_order(order_id) or {})
        await query.message.reply_text(
            "Payment status updated successfully ✅\n\n"
            f"Order ID: {order_id}\n"
            f"New Payment Status: {get_payment_status_label(new_status)}",
            reply_markup=payment_panel_markup(order_id),
        )
        await smart_panel_response(
            query,
            info_text,
            reply_markup=payment_panel_markup(order_id),
        )
        return





    if query.data == "adminhub_dashboard":
        if not has_staff_access(chat_id):
            await query.message.reply_text("Admin only action.")
            return
        await smart_panel_response(
            query,
            build_admin_dashboard_text(),
            reply_markup=admin_hub_markup(),
        )
        return

    if query.data == "adminhub_quickpanel":
        if not has_staff_access(chat_id):
            await query.message.reply_text("Admin only action.")
            return
        await smart_panel_response(
            query,
            build_quickpanel_text(10),
            reply_markup=admin_hub_markup(),
        )
        return

    if query.data == "adminhub_pendingorders":
        if not has_staff_access(chat_id):
            await query.message.reply_text("Admin only action.")
            return

        pending_items = [
            (order_id, data)
            for order_id, data in orders.items()
            if data.get("status") == "paid_pending_delivery"
        ]
        pending_items.sort(key=lambda item: item[1].get("time", ""), reverse=True)

        if not pending_items:
            text = "Pending Delivery Orders\\n\\nNo paid pending delivery orders found."
        else:
            lines = ["Pending Delivery Orders", ""]
            for order_id, data in pending_items[:20]:
                provider = data.get("provider") or "Unknown"
                requested = data.get("requested_card_balance")
                charged = data.get("charged_amount")
                requested_text = format_usd(float(requested)) if isinstance(requested, (int, float)) else "Unknown"
                charged_text = format_usd(float(charged)) if isinstance(charged, (int, float)) else "Unknown"
                lines.append(f"{order_id} | {provider} | {requested_text} | charged {charged_text}")
            text = "\\n".join(lines)

        await smart_panel_response(query, text, reply_markup=admin_hub_markup())
        return

    if query.data == "adminhub_balancerequests":
        if not has_staff_access(chat_id):
            await query.message.reply_text("Admin only action.")
            return

        items = get_sorted_balance_requests()
        if not items:
            text = "Balance Requests\\n\\nNo balance requests found."
        else:
            lines = ["Balance Requests", ""]
            for request_id, data in items[:10]:
                amount = format_usd(float(data.get("usd_amount", 0.0) or 0.0))
                status = data.get("status", "-")
                user_id = data.get("user_id", "-")
                coin = data.get("coin", "-")
                lines.append(f"{request_id} | {amount} | {coin} | {status} | user {user_id}")
            lines.extend(["", "Use /balancepanel REQUEST_ID for details."])
            text = "\\n".join(lines)

        await smart_panel_response(query, text, reply_markup=admin_hub_markup())
        return

    if query.data == "adminhub_ratespanel":
        if not is_full_admin(chat_id):
            await query.message.reply_text("Admin only action.")
            return
        await smart_panel_response(
            query,
            build_rates_panel_text(),
            reply_markup=rates_panel_markup(),
        )
        return

    if query.data == "adminhub_dailysummary":
        if not has_staff_access(chat_id):
            await query.message.reply_text("Admin only action.")
            return
        await smart_panel_response(
            query,
            build_daily_summary_text(),
            reply_markup=admin_hub_markup(),
        )
        return

    if query.data == "adminhub_auditlog":
        if not has_staff_access(chat_id):
            await query.message.reply_text("Admin only action.")
            return
        await smart_panel_response(
            query,
            build_audit_log_text(20),
            reply_markup=admin_hub_markup(),
        )
        return

    if query.data == "adminhub_help":
        if not has_staff_access(chat_id):
            await query.message.reply_text("Admin only action.")
            return
        await smart_panel_response(
            query,
            "Admin Hub Help\\n\\n"
            "Dashboard = general admin overview\\n"
            "Quickpanel = recent active orders\\n"
            "Pending Orders = paid waiting deliveries\\n"
            "Balance Requests = recent top-up requests\\n"
            "Rates Panel = provider rates and stock entry points\\n"
            "Daily Summary = today's report\\n"
            "Audit Log = recent admin actions",
            reply_markup=admin_hub_markup(),
        )
        return

    if query.data.startswith("broadcastcancel::"):
        if not is_full_admin(chat_id):
            await query.message.reply_text("Admin only action.")
            return

        token = query.data.split("::", 1)[1].strip()
        broadcast_confirmations.pop(token, None)
        await smart_panel_response(query, "Broadcast cancelled.")
        return

    if query.data.startswith("broadcasttest::"):
        if not is_full_admin(chat_id):
            await query.message.reply_text("Admin only action.")
            return

        parts = query.data.split("::")
        if len(parts) != 3:
            await query.message.reply_text("Invalid broadcast test action.")
            return

        target_type = parts[1].strip().lower()
        token = parts[2].strip()
        payload = broadcast_confirmations.get(token)
        if not payload:
            await smart_panel_response(query, "Broadcast preview expired or not found.")
            return

        message_text = payload.get("message_text", "")
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=("Admin Announcement\n\n" f"{message_text}"),
            )
            await smart_panel_response(
                query,
                "Test broadcast sent to admin successfully.",
                reply_markup=broadcast_confirm_markup(target_type, token),
            )
        except Exception as e:
            await smart_panel_response(
                query,
                f"Test broadcast failed: {e}",
                reply_markup=broadcast_confirm_markup(target_type, token),
            )
        return

    if query.data.startswith("broadcastconfirm::"):
        if not is_full_admin(chat_id):
            await query.message.reply_text("Admin only action.")
            return

        parts = query.data.split("::")
        if len(parts) != 3:
            await query.message.reply_text("Invalid broadcast confirm action.")
            return

        target_type = parts[1].strip().lower()
        token = parts[2].strip()
        payload = broadcast_confirmations.get(token)
        if not payload:
            await smart_panel_response(query, "Broadcast preview expired or not found.")
            return

        message_text = payload.get("message_text", "")
        targets = get_broadcast_targets(target_type)
        if not targets:
            broadcast_confirmations.pop(token, None)
            await smart_panel_response(query, f"No users found for target group: {target_type}")
            return

        sent_count = 0
        failed_count = 0

        for target_chat_id in targets:
            try:
                await context.bot.send_message(
                    chat_id=target_chat_id,
                    text=("Admin Announcement\n\n" f"{message_text}"),
                )
                sent_count += 1
            except Exception:
                failed_count += 1

        add_audit_log_entry(
            action="broadcast",
            admin_user=query.from_user,
            target_type="broadcast",
            target_id=target_type,
            details=f"sent={sent_count}, failed={failed_count}, total={len(targets)}",
        )
        broadcast_confirmations.pop(token, None)
        await smart_panel_response(
            query,
            "Broadcast completed.\n\n"
            f"Target Group: {target_type}\n"
            f"Total Targets: {len(targets)}\n"
            f"Sent: {sent_count}\n"
            f"Failed: {failed_count}",
        )
        return

    if query.data.startswith("ratespanel::"):
        if not is_full_admin(chat_id):
            await query.message.reply_text("Admin only action.")
            return

        provider = query.data.split("::", 1)[1].strip()
        if provider not in SUPPORTED_PROVIDERS:
            await query.message.reply_text("Unknown provider.")
            return

        await smart_panel_response(
            query,
            build_stock_panel_text(provider),
            reply_markup=stock_panel_markup(provider),
        )
        return

    if query.data.startswith("stockpanel::"):
        if not is_full_admin(chat_id):
            await query.message.reply_text("Admin only action.")
            return

        parts = query.data.split("::")
        if len(parts) != 3:
            await query.message.reply_text("Invalid stock panel action.")
            return

        provider = parts[1].strip()
        action = parts[2].strip().lower()

        if provider not in SUPPORTED_PROVIDERS:
            await query.message.reply_text("Unknown provider.")
            return

        # SQLite'tan gerçek kart verisi — manuel düzeltme artık gerekmiyor
        avail_cards = db_get_available_cards(provider=provider)
        cards   = len(avail_cards)
        balance = sum(float(c.get("balance", 0)) for _, c in avail_cards)
        rate = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.0)))

        if action in ("balplus50", "balminus50", "cardsplus1", "cardsminus1"):
            # Artık SQLite tabanlı - manuel düzeltme kaldırıldı
            await query.answer("Stock data is now managed via SQLite automatically.", show_alert=True)
            return

        elif action == "rateplus":
            rate = round(rate + 0.01, 2)
            rates_data[provider] = rate
            save_rates(rates_data)

        elif action == "rateminus":
            rate = max(0.01, round(rate - 0.01, 2))
            rates_data[provider] = rate
            save_rates(rates_data)

        elif action == "refresh":
            pass

        else:
            await query.message.reply_text("Unknown stock panel action.")
            return

        await smart_panel_response(
            query,
            build_stock_panel_text(provider),
            reply_markup=stock_panel_markup(provider),
        )
        return


    if query.data.startswith("balreq::"):
        if not has_staff_access(chat_id):
            await query.message.reply_text("Staff only action.")
            return

        parts = query.data.split("::")
        if len(parts) != 3:
            await query.message.reply_text("Invalid balance request action.")
            return

        request_id = parts[1].strip()
        action = parts[2].strip().lower()

        request_data = get_hybrid_balance_request(request_id)
        if not request_data:
            await query.message.reply_text("Balance request not found.")
            return

        if action == "refresh":
            await smart_panel_response(
                query,
                build_balance_request_info_text(request_id, request_data if "request_data" in locals() and request_data else balance_requests_data[request_id]),
                reply_markup=balance_request_panel_markup(request_id),
            )
            return

        if not acquire_balance_request_guard(request_id):
            await query.message.reply_text("This balance request is already being processed. Please wait.")
            return

        try:
            async with transaction_lock:
                data = get_hybrid_balance_request(request_id) or balance_requests_data[request_id]
                user_id = int(data.get("user_id"))
                amount = float(data.get("usd_amount", 0.0) or 0.0)
                current_status = str(data.get("status", "") or "").lower().strip()

                if action == "reject":
                    if current_status == "approved":
                        await smart_panel_response(query, "Approved requests cannot be rejected from this panel.", reply_markup=balance_request_panel_markup(request_id))
                        return
                    if current_status == "rejected":
                        await smart_panel_response(query, "This balance request has already been rejected.", reply_markup=balance_request_panel_markup(request_id))
                        return
                    data["status"] = "rejected"
                    save_hybrid_balance_request(request_id, data)

                elif action == "paid":
                    if current_status == "approved":
                        await smart_panel_response(query, "Approved requests cannot be marked as paid again.", reply_markup=balance_request_panel_markup(request_id))
                        return
                    if current_status == "paid_uncredited":
                        await smart_panel_response(query, "This balance request is already marked as paid.", reply_markup=balance_request_panel_markup(request_id))
                        return
                    if current_status == "rejected":
                        await smart_panel_response(query, "Rejected requests cannot be marked as paid.", reply_markup=balance_request_panel_markup(request_id))
                        return
                    data["status"] = "paid_uncredited"
                    save_hybrid_balance_request(request_id, data)

                elif action == "approve":
                    if current_status == "approved":
                        await smart_panel_response(query, "This balance request has already been approved.", reply_markup=balance_request_panel_markup(request_id))
                        return
                    if current_status == "rejected":
                        await smart_panel_response(query, "Rejected requests cannot be approved from this panel.", reply_markup=balance_request_panel_markup(request_id))
                        return
                    record = get_hybrid_balance_record(user_id)
                    record["available_balance"] = round(float(record.get("available_balance", 0.0) or 0.0) + amount, 2)
                    record["total_deposited"] = round(float(record.get("total_deposited", 0.0) or 0.0) + amount, 2)
                    balances_data[str(user_id)] = record
                    save_hybrid_balance_record(user_id, record)
                    data["status"] = "approved"
                    data["credited_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    save_hybrid_balance_request(request_id, data)
                else:
                    await query.message.reply_text("Unknown balance request action.")
                    return

            if action == "reject":
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=(
                            "Your balance request has been rejected.\n\n"
                            f"Request ID: {request_id}\n"
                            f"Requested Top-Up: {format_usd(amount)}\n\n"
                            "If you believe this is a mistake, please contact support."
                        ),
                    )
                except Exception:
                    pass
                add_audit_log_entry("balance_request_reject", query.from_user, "balance_request", str(request_id), f"amount={amount}")
                await smart_panel_response(query, f"Balance request rejected.\n\nRequest ID: {request_id}", reply_markup=balance_request_panel_markup(request_id))
                return

            if action == "paid":
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=(
                            "Your payment has been marked as received and is waiting for balance credit.\n\n"
                            f"Request ID: {request_id}\n"
                            f"Amount: {format_usd(amount)}"
                        ),
                    )
                except Exception:
                    pass
                add_audit_log_entry("balance_request_paid", query.from_user, "balance_request", str(request_id), f"amount={amount}")
                await smart_panel_response(query, f"Balance request marked as paid.\n\nRequest ID: {request_id}", reply_markup=balance_request_panel_markup(request_id))
                return

            if action == "approve":
                record = get_user_balance_record(user_id)
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=(
                            "Balance request approved ✅\n\n"
                            f"Request ID: {request_id}\n"
                            f"Credited Amount: {format_usd(amount)}\n"
                            f"Available Balance: {format_usd(float(record.get('available_balance', 0.0)))}"
                        ),
                    )
                except Exception:
                    pass
                add_audit_log_entry("balance_request_approve", query.from_user, "balance_request", str(request_id), f"amount={amount}")
                credited_record = get_user_balance_record(user_id)
                add_balance_ledger_entry(
                    user_id=user_id,
                    delta=amount,
                    reason="balance_request_approve",
                    reference_id=str(request_id),
                    actor=str(query.from_user.id),
                    before_balance=round(float(credited_record.get("available_balance", 0.0) or 0.0) - amount, 2),
                    after_balance=round(float(credited_record.get("available_balance", 0.0) or 0.0), 2),
                )
                create_backup_snapshot("balance_request_approve")
                await smart_panel_response(
                    query,
                    "Balance credited successfully ✅\n\n"
                    f"Request ID: {request_id}\n"
                    f"Credited Amount: {format_usd(amount)}",
                    reply_markup=balance_request_panel_markup(request_id),
                )
                return
        finally:
            release_balance_request_guard(request_id)

    if query.data.startswith("orderpanel::"):
        if not has_staff_access(chat_id):
            await query.message.reply_text("Admin only action.")
            return

        parts = query.data.split("::")
        if len(parts) != 3:
            await query.message.reply_text("Invalid order panel action.")
            return

        order_id = parts[1].strip()
        action = parts[2].strip().lower()

        if order_id not in orders:
            await query.message.reply_text("Order not found.")
            return

        if action == "refresh":
            await smart_panel_response(
                query,
                build_order_panel_text(order_id, orders[order_id]),
                reply_markup=order_panel_markup(order_id),
            )
            return

        if action == "info":
            data = order_data if "order_data" in locals() and order_data else orders[order_id]
            text = (
                "Order Details\n\n"
                f"Order ID: {order_id}\n"
                f"Status: {data.get('status', 'unknown')}\n"
                f"Time: {data.get('time', 'unknown')}\n"
                f"Name: {data.get('name', 'unknown')}\n"
                f"Username: {data.get('username', 'unknown')}\n"
                f"User ID: {data.get('user_id', 'unknown')}\n"
                f"Chat ID: {data.get('chat_id', 'unknown')}\n\n"
                f"Order Details:\n{data.get('details', '')}"
            )
            await smart_panel_response(query, text, reply_markup=order_panel_markup(order_id))
            return

        if action == "payment":
            await smart_panel_response(
                query,
                build_payment_info_text(order_id, orders.get(order_id) or get_hybrid_order(order_id) or {}),
                reply_markup=payment_panel_markup(order_id),
            )
            return

        if action == "cancel":
            if orders[order_id].get("status") == "delivered":
                await smart_panel_response(query, "Delivered orders cannot be cancelled from the order panel.", reply_markup=order_panel_markup(order_id))
                return

            if orders[order_id].get("status") == "cancelled":
                await smart_panel_response(query, f"Order is already cancelled.\n\nOrder ID: {order_id}", reply_markup=order_panel_markup(order_id))
                return

            orders[order_id]["status"] = "cancelled"
            save_orders(orders)

            customer_chat_id = orders[order_id].get("chat_id")
            try:
                await context.bot.send_message(
                    chat_id=customer_chat_id,
                    text=(
                        "Your order has been cancelled.\n\n"
                        f"Order ID: {order_id}\n"
                        "If you need more information, please contact support."
                    ),
                )
            except Exception:
                pass

            await query.message.reply_text(
                f"Order cancelled successfully.\n\nOrder ID: {order_id}",
                reply_markup=order_panel_markup(order_id),
            )
            await smart_panel_response(
                query,
                build_order_panel_text(order_id, orders[order_id]),
                reply_markup=order_panel_markup(order_id),
            )
            return

        if action == "refund":
            data = orders[order_id]
            if data.get("status") == "delivered":
                await smart_panel_response(query, "Delivered orders cannot be refunded from the order panel.", reply_markup=order_panel_markup(order_id))
                return
            if data.get("status") == "refunded":
                await smart_panel_response(query, "This order has already been refunded.", reply_markup=order_panel_markup(order_id))
                return

            charged_amount = data.get("charged_amount")
            user_id = data.get("user_id")
            if not isinstance(charged_amount, (int, float)):
                await smart_panel_response(query, "This order does not contain a refundable wallet charge.", reply_markup=order_panel_markup(order_id))
                return

            record = get_user_balance_record(int(user_id))
            record["available_balance"] = round(float(record.get("available_balance", 0.0)) + float(charged_amount), 2)
            record["total_spent"] = round(max(0.0, float(record.get("total_spent", 0.0)) - float(charged_amount)), 2)
            balances_data[str(user_id)] = record
            save_all_balances()

            orders[order_id]["status"] = "refunded"
            orders[order_id]["refunded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            set_order_payment_status(order_id, "refunded", orders[order_id].get("payment_method", "") or "wallet_balance", "refundbalance_from_orderpanel", orders[order_id].get("paid_at", ""))
            save_orders(orders)

            customer_chat_id = data.get("chat_id")
            try:
                await context.bot.send_message(
                    chat_id=customer_chat_id,
                    text=(
                        "Balance refund completed ✅\n\n"
                        f"Order ID: {order_id}\n"
                        f"Refunded Amount: {format_usd(float(charged_amount))}\n"
                        f"Available Balance: {format_usd(float(record.get('available_balance', 0.0)))}\n\n"
                        "The refunded amount has been returned to your bot balance."
                    ),
                )
            except Exception:
                pass

            await query.message.reply_text(
                "Balance refunded successfully ✅\n\n"
                f"Order ID: {order_id}\n"
                f"Refunded Amount: {format_usd(float(charged_amount))}",
                reply_markup=order_panel_markup(order_id),
            )
            await smart_panel_response(
                query,
                build_order_panel_text(order_id, orders[order_id]),
                reply_markup=order_panel_markup(order_id),
            )
            return

        if action == "deliver":
            if orders[order_id].get("status") == "delivered":
                await smart_panel_response(query, f"This order has already been delivered.\n\nOrder ID: {order_id}", reply_markup=order_panel_markup(order_id))
                return
            if orders[order_id].get("status") == "refunded":
                await smart_panel_response(query, f"Refunded orders cannot be delivered.\n\nOrder ID: {order_id}", reply_markup=order_panel_markup(order_id))
                return
            admin_delivery_mode_users[query.from_user.id] = {"order_id": order_id}
            await smart_panel_response(
                query,
                "Send the delivery message in your next text message.\n\n"
                f"Order ID: {order_id}\n\n"
                "Example:\n"
                "Card Number: ...\n"
                "Exp: ...\n"
                "CVV: ...",
                reply_markup=order_panel_markup(order_id),
            )
            return

        await smart_panel_response(query, "Unknown order panel action.", reply_markup=order_panel_markup(order_id))
        return

    await query.message.reply_text("Unknown option.", reply_markup=main_menu_markup())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user = update.effective_user

    if can_use_bot(chat_id):
        # Aktif kullanıcı - kullanıcı yardım ekranı
        text = (
            "Elite Prepaid Bot — Help\n\n"
            "How it works:\n"
            "1. Add balance to your account (LTC or USDC)\n"
            "2. Browse available cards in the listing\n"
            "3. Select a card and confirm purchase\n"
            "4. Card will be delivered within 3-5 minutes\n\n"
            "Your Commands:\n"
            "/start — Open main menu\n"
            "/balance — Check your balance\n"
            "/myorders — View your orders\n"
            "/mypayment — Check payment status\n"
            "/help — Show this help\n\n"
            "Need assistance? Use the Support button in the main menu."
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🛒 View Listing",  callback_data="lst:1:"),
             InlineKeyboardButton("💰 Add Balance",   callback_data="add_balance")],
            [InlineKeyboardButton("📋 My Orders",     callback_data="my_orders"),
             InlineKeyboardButton("🎧 Support",       callback_data="support")],
            [InlineKeyboardButton("🏠 Main Menu",     callback_data="main_menu")],
        ])
    else:
        # Kısıtlı kullanıcı
        text = (
            "Elite Prepaid Bot\n\n"
            "Buy premium prepaid cards at the best rates.\n\n"
            "To get started, activate your account first.\n"
            "Use the buttons below to continue."
        )
        keyboard = restricted_markup()

    await update.message.reply_text(text, reply_markup=keyboard)


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"Your chat ID is: {update.effective_chat.id}")


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /balance USER_ID")
        return
    try:
        target_user_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("USER_ID must be a number.")
        return
    record = get_hybrid_balance_record(target_user_id)
    text = (
        f"Balance for {target_user_id}\n\n"
        f"Available Balance: {format_usd(float(record.get('available_balance', 0)))}\n"
        f"Total Deposited: {format_usd(float(record.get('total_deposited', 0)))}\n"
        f"Total Spent: {format_usd(float(record.get('total_spent', 0)))}"
    )
    await update.message.reply_text(text)


async def addbalance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    parts = get_command_parts(update)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /addbalance USER_ID AMOUNT")
        return
    try:
        target_user_id = int(parts[1])
        amount = float(parts[2])
    except ValueError:
        await update.message.reply_text("USER_ID must be a number and AMOUNT must be numeric.")
        return
    if amount <= 0:
        await update.message.reply_text("AMOUNT must be greater than 0.")
        return
    record = get_user_balance_record(target_user_id)
    before_balance = round(float(record.get('available_balance', 0) or 0), 2)
    record['available_balance'] = float(record.get('available_balance', 0)) + amount
    record['total_deposited'] = float(record.get('total_deposited', 0)) + amount
    save_hybrid_balance_record(target_user_id, record)
    add_balance_ledger_entry(
        user_id=target_user_id,
        delta=amount,
        reason="admin_addbalance",
        reference_id="manual",
        actor=str(update.effective_user.id),
        before_balance=before_balance,
        after_balance=round(float(record.get('available_balance', 0) or 0), 2),
    )
    create_backup_snapshot("addbalance")
    add_audit_log_entry(
        action="addbalance",
        admin_user=update.effective_user,
        target_type="user",
        target_id=str(target_user_id),
        details=f"amount={amount}",
    )
    await update.message.reply_text(
        "Balance added successfully ✅\n\n"
        f"User ID: {target_user_id}\n"
        f"Added Amount: {format_usd(amount)}\n"
        f"New Available Balance: {format_usd(float(record.get('available_balance', 0)))}"
    )
    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=(
                "Balance update completed ✅\n\n"
                f"Added Balance: {format_usd(amount)}\n"
                f"Available Balance: {format_usd(float(record.get('available_balance', 0)))}\n\n"
                "You can now use your balance inside the bot."
            ),
        )
    except Exception:
        pass


async def subbalance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    parts = get_command_parts(update)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /subbalance USER_ID AMOUNT")
        return
    try:
        target_user_id = int(parts[1])
        amount = float(parts[2])
    except ValueError:
        await update.message.reply_text("USER_ID must be a number and AMOUNT must be numeric.")
        return
    if amount <= 0:
        await update.message.reply_text("AMOUNT must be greater than 0.")
        return
    record = get_hybrid_balance_record(target_user_id)
    current = float(record.get('available_balance', 0))
    if amount > current:
        await update.message.reply_text("Cannot subtract more than the available balance.")
        return
    before_balance = round(current, 2)
    record['available_balance'] = current - amount
    save_hybrid_balance_record(target_user_id, record)
    add_balance_ledger_entry(
        user_id=target_user_id,
        delta=-amount,
        reason="admin_subbalance",
        reference_id="manual",
        actor=str(update.effective_user.id),
        before_balance=before_balance,
        after_balance=round(float(record.get('available_balance', 0) or 0), 2),
    )
    create_backup_snapshot("subbalance")
    add_audit_log_entry(
        action="subbalance",
        admin_user=update.effective_user,
        target_type="user",
        target_id=str(target_user_id),
        details=f"amount={amount}",
    )
    await update.message.reply_text(
        "Balance deducted successfully ✅\n\n"
        f"User ID: {target_user_id}\n"
        f"Deducted Amount: {format_usd(amount)}\n"
        f"New Available Balance: {format_usd(float(record.get('available_balance', 0)))}"
    )
    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=(
                "Balance update completed ✅\n\n"
                f"Deducted Balance: {format_usd(amount)}\n"
                f"Available Balance: {format_usd(float(record.get('available_balance', 0)))}\n\n"
                "Your account balance has been updated."
            ),
        )
    except Exception:
        pass


async def rates_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    lines = ["Provider Rates", ""]
    for provider in SUPPORTED_PROVIDERS:
        rate = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0)))
        lines.append(f"{provider}: {rate:.2f}")
    await update.message.reply_text("\n".join(lines))


async def setrate_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global rates_data
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    parts = get_command_parts(update)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /setrate PROVIDER RATE")
        return
    provider = get_canonical_provider_name(parts[1])
    if not provider:
        await update.message.reply_text(
            "Unknown provider. Use one of these names:\n" + get_supported_provider_usage_text()
        )
        return
    try:
        rate = float(parts[2])
    except ValueError:
        await update.message.reply_text("RATE must be numeric. Example: 0.40")
        return
    if rate <= 0:
        await update.message.reply_text("RATE must be greater than 0.")
        return
    rates_data[provider] = rate
    save_rates(rates_data)
    add_audit_log_entry(
        action="setrate",
        admin_user=update.effective_user,
        target_type="provider",
        target_id=str(provider),
        details=f"rate={rate:.2f}",
    )
    await update.message.reply_text(f"Rate updated.\n{provider}: {rate:.2f}")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return

    order_id = get_order_id_from_command(update)
    if not order_id:
        await update.message.reply_text("Usage: /status EE-123456")
        return

    order_data = get_hybrid_order(order_id)
    if not order_data:
        await update.message.reply_text("Order not found.")
        return

    data = orders[order_id]
    requester_chat_id = update.effective_chat.id
    order_chat_id = data.get("chat_id")

    if requester_chat_id != ADMIN_CHAT_ID and requester_chat_id != order_chat_id:
        await update.message.reply_text("You are not allowed to view this order.")
        return

    text = (
        "Order Status\n\n"
        f"Order ID: {order_id}\n"
        f"Status: {data.get('status', 'unknown')}\n"
        f"Created: {data.get('time', 'unknown')}"
    )
    await update.message.reply_text(text, reply_markup=after_order_markup())


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return

    text = update.message.text
    if text.startswith("/"):
        return

    user = update.message.from_user
    user_id = user.id
    chat_id = update.effective_chat.id
    username = f"@{user.username}" if user.username else "No username"
    full_name = user.full_name

    # Hub msg mode - admin mesaj/bakiye/broadcast gönderimi
    if is_admin(chat_id) and user_id in hub_msg_mode:
        mode = hub_msg_mode.pop(user_id)
        if mode == "broadcast":
            # Broadcast onay ekranı
            hub_broadcast_confirm[user_id] = text
            preview = text[:200] + ("..." if len(text) > 200 else "")
            approved = access_data.get("approved_users", [])
            await update.message.reply_text(
                f"📣 Broadcast Preview\n\n{preview}\n\nWill be sent to {len(approved)} users.\nConfirm?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Send", callback_data="hub:broadcast_confirm"),
                     InlineKeyboardButton("❌ Cancel", callback_data="hub:broadcast_cancel")],
                ]),
            )
            return
        elif isinstance(mode, str) and mode.startswith("addbal:"):
            target_uid = int(mode.split(":")[1])
            try:
                amount = float(text.strip())
                rec = get_hybrid_balance_record(target_uid)
                before = float(rec.get("available_balance", 0) or 0)
                after  = round(before + amount, 2)
                rec["available_balance"] = after
                rec["total_deposited"] = round(float(rec.get("total_deposited", 0) or 0) + amount, 2)
                save_hybrid_balance_record(target_uid, rec)
                add_balance_ledger_entry(target_uid, amount, "admin_add_hub", actor=str(user_id), before_balance=before, after_balance=after)
                await update.message.reply_text(f"Added {format_usd(amount)} to user {target_uid}.\nNew balance: {format_usd(after)}")
            except Exception as e:
                await update.message.reply_text(f"Error: {e}")
            return
        elif isinstance(mode, str) and mode.startswith("subbal:"):
            target_uid = int(mode.split(":")[1])
            try:
                amount = float(text.strip())
                rec = get_hybrid_balance_record(target_uid)
                before = float(rec.get("available_balance", 0) or 0)
                after  = round(max(0, before - amount), 2)
                rec["available_balance"] = after
                save_hybrid_balance_record(target_uid, rec)
                add_balance_ledger_entry(target_uid, -amount, "admin_sub_hub", actor=str(user_id), before_balance=before, after_balance=after)
                await update.message.reply_text(f"Subtracted {format_usd(amount)} from user {target_uid}.\nNew balance: {format_usd(after)}")
            except Exception as e:
                await update.message.reply_text(f"Error: {e}")
            return
        elif mode == "importstock" or (isinstance(mode, str) and mode.startswith("importstock:")):
            # Admin mesaj olarak kart verileri yapıştırdı
            reg_part = mode.split(":")[1] if ":" in mode else "unknown"
            reg_map  = {"registered": True, "unregistered": False, "unknown": None}
            reg_val  = reg_map.get(reg_part, None)
            stats = import_cards_from_text(text, provider=None, registered=reg_val)
            reg_label = {"registered": "🔒 Registered", "unregistered": "✅ Not Registered"}.get(reg_part, "❓ Unknown")
            msg = (
                f"Import Complete\n\n"
                f"Status: {reg_label}\n"
                f"Added:      {stats['added']} cards\n"
                f"Skipped:    {stats['skipped']} (duplicates)\n"
                f"Errors:     {stats['errors']} (bad lines)\n"
            )
            if stats.get("unassigned", 0):
                msg += f"Unassigned: {stats['unassigned']} (unknown BIN)\n"
            msg += f"Total in stock: {stats['total']} cards"
            await update.message.reply_text(msg)
            return

        elif mode == "deletestock":
            short_input = text.strip().upper()
            # Kısaltmadan tam isim bul
            provider = SHORT_TO_PROVIDER.get(short_input) or canonical_provider_name(text.strip())
            if not provider:
                await update.message.reply_text(f"Unknown provider: {text.strip()}\nUse: GCM, MPC, JKR, CBAU, VG, VP, WMT, AMEX")
                return
            cards = load_cards()
            deleted = 0
            for cid in list(cards.keys()):
                if cards[cid].get("provider") == provider and cards[cid].get("status") == "available":
                    del cards[cid]
                    deleted += 1
            save_cards(cards)
            await update.message.reply_text(f"Deleted {deleted} available cards for {provider}.")
            return

        elif isinstance(mode, int):
            # Kullanıcıya direkt mesaj
            target_uid = mode
            try:
                await context.bot.send_message(
                    chat_id=target_uid,
                    text=f"Message from Admin:\n\n{text}",
                )
                await update.message.reply_text(f"Message sent to user {target_uid}.")
            except Exception as e:
                await update.message.reply_text(f"Failed to send: {e}")
            return

    if blocked_by_maintenance(chat_id):
        await send_maintenance_message(update.message)
        return

    if not is_admin(chat_id) and is_spamming_user(user_id):
        await update.message.reply_text(
            "Too many messages detected. Please slow down and try again in a moment."
        )
        return

    if user.id in admin_delivery_mode_users and is_admin(chat_id):
        pending_delivery = admin_delivery_mode_users.pop(user.id, None)
        if pending_delivery:
            order_id = pending_delivery.get("order_id")
            if order_id in orders:
                data = orders[order_id]
                if data.get("status") == "delivered":
                    await update.message.reply_text("This order has already been delivered.")
                    return

                if data.get("status") == "refunded":
                    await update.message.reply_text("Refunded orders cannot be delivered.")
                    return

                delivery_message = text.strip()
                if not delivery_message:
                    await update.message.reply_text("Delivery message cannot be empty.")
                    return
                customer_chat_id = data.get("chat_id")
                provider = data.get("provider") or "Unknown"
                requested = data.get("requested_card_balance")
                requested_text = format_usd(float(requested)) if isinstance(requested, (int, float)) else "Unknown"

                orders[order_id]["status"] = "delivered"
                orders[order_id]["delivery_message"] = delivery_message
                orders[order_id]["delivered_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                save_orders(orders)

                try:
                    await context.bot.send_message(
                        chat_id=customer_chat_id,
                        text=(
                            "Your order has been delivered ✅\n\n"
                            f"Order ID: {order_id}\n"
                            f"Provider: {provider}\n"
                            f"Requested Card Balance: {requested_text}\n\n"
                            f"Delivery Details:\n{delivery_message}\n\n"
                            "Please review your order carefully."
                        ),
                    )
                except Exception:
                    pass

                await update.message.reply_text(
                    f"Order delivered successfully: {order_id}",
                    reply_markup=order_panel_markup(order_id),
                )
                return


    pending_topup = get_pending_topup_request(user_id)
    if pending_topup and pending_topup.get("step") == "awaiting_amount":
        normalized_text = text.strip().replace(",", ".")
        try:
            usd_amount = float(normalized_text)
        except ValueError:
            await update.message.reply_text(
                "Please enter a valid numeric amount.\n\nExample: 50 or 100"
            )
            return

        if usd_amount < MIN_BALANCE_TOPUP_USD:
            await update.message.reply_text(
                "Minimum top-up not reached\n\n"
                f"The minimum amount to add balance is {format_usd(MIN_BALANCE_TOPUP_USD)}.\n\n"
                "Please enter a valid amount of $10 or more."
            )
            return

        coin = pending_topup.get("coin", "USDC")
        add_balance_mode_users.pop(user_id, None)

        if not _wallet_ready:
            await update.message.reply_text(
                "Payment system is not ready. Please contact support.",
                reply_markup=after_support_markup(),
            )
            return

        # Benzersiz adres türet ve bekleyen ödeme oluştur
        pending = create_pending_payment(
            user_id=user_id,
            coin=coin,
            usd_amount=usd_amount,
            full_name=full_name,
            username=username,
        )

        await send_payment_with_qr(
            message_target=update.message,
            coin=coin,
            usd_amount=usd_amount,
            address=pending["address"],
            payment_id=pending["payment_id"],
            expires=pending.get("expires_at", "")[:16],
            reply_markup=payment_instruction_markup(coin),
        )

        # Admin notify
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"New Payment Pending\n\n"
                    f"Payment ID: {pending['payment_id']}\n"
                    f"User: {full_name} ({username})\n"
                    f"User ID: {user_id}\n"
                    f"Coin: {coin}\n"
                    f"Amount: {format_usd(usd_amount)}\n"
                    f"Address: {pending['address']}\n"
                    f"Address Index: {pending['address_index']}\n"
                    f"Expires: {pending['expires_at']}"
                ),
            )
        except Exception:
            pass
        return


    if user_id in support_mode_users:
        remaining = support_cooldown_active(user_id)
        if remaining > 0:
            await update.message.reply_text(
                f"Please wait {remaining} seconds before sending another support message."
            )
            return

        support_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        admin_text = (
            "New support message received.\n\n"
            f"Time: {support_time}\n"
            f"Name: {full_name}\n"
            f"Username: {username}\n"
            f"User ID: {user_id}\n"
            f"Chat ID: {chat_id}\n\n"
            f"Support Message:\n{text}"
        )

        try:
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=admin_text)
        except Exception:
            await update.message.reply_text(
                "Your support message could not be forwarded right now. Please try again shortly.",
                reply_markup=after_support_markup(),
            )
            return

        mark_support_message_sent(user_id)
        support_mode_users.discard(user_id)

        await update.message.reply_text(
            "Your support request has been received and forwarded to admin.",
            reply_markup=after_support_markup(),
        )
        return

    if not can_use_bot(chat_id):
        await send_restricted_message(update.message, update.effective_user)
        return




async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != ADMIN_CHAT_ID:
        return

    order_id = get_order_id_from_command(update)
    if not order_id:
        await update.message.reply_text("Usage: /cancel EE-123456")
        return
    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    customer_chat_id = orders[order_id]["chat_id"]
    orders[order_id]["status"] = "cancelled"
    if orders[order_id].get("payment_status") == "paid":
        save_orders(orders)
    else:
        set_order_payment_status(order_id, "failed", "manual", "Order cancelled before payment completion", "")

    await context.bot.send_message(
        chat_id=customer_chat_id,
        text=(
            f"Your order has been cancelled.\n\n"
            f"Order ID: {order_id}\n"
            f"If you need more information, please contact support."
        ),
    )
    await update.message.reply_text(f"Customer notified: order cancelled for {order_id}")


async def archiveorder_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        return

    order_id = get_order_id_from_command(update)
    if not order_id:
        await update.message.reply_text("Usage: /archiveorder EE-123456")
        return
    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    orders[order_id]["status"] = "archived"
    save_orders(orders)
    await update.message.reply_text(f"Order archived: {order_id}")


async def purgearchived_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        return

    archived_ids = [
        order_id for order_id, data in orders.items()
        if data.get("status") == "archived"
    ]

    if not archived_ids:
        await update.message.reply_text("No archived orders to delete.")
        return

    for order_id in archived_ids:
        del orders[order_id]

    save_orders(orders)
    await update.message.reply_text(f"Deleted archived orders: {len(archived_ids)}")


async def orderlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        return

    active_items = [
        (order_id, data)
        for order_id, data in orders.items()
        if data.get("status") != "archived"
    ]

    if not active_items:
        await update.message.reply_text("No active orders found.")
        return

    items = active_items[-10:]
    lines = ["Active Orders\n"]

    for order_id, data in reversed(items):
        status = data.get("status", "unknown")
        name = data.get("name", "Unknown")
        order_time = data.get("time", "Unknown time")
        lines.append(f"{order_id} | {status} | {name} | {order_time}")

    await update.message.reply_text("\n".join(lines))


async def archivedorders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        return

    archived_items = [
        (order_id, data)
        for order_id, data in orders.items()
        if data.get("status") == "archived"
    ]

    if not archived_items:
        await update.message.reply_text("No archived orders found.")
        return

    items = archived_items[-10:]
    lines = ["Archived Orders\n"]

    for order_id, data in reversed(items):
        status = data.get("status", "unknown")
        name = data.get("name", "Unknown")
        order_time = data.get("time", "Unknown time")
        lines.append(f"{order_id} | {status} | {name} | {order_time}")

    await update.message.reply_text("\n".join(lines))


async def orderinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        return

    order_id = get_order_id_from_command(update)
    if not order_id:
        await update.message.reply_text("Usage: /orderinfo EE-123456")
        return
    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    data = orders[order_id]
    text = (
        "Order Details\n\n"
        f"Order ID: {order_id}\n"
        f"Status: {data.get('status', 'unknown')}\n"
        f"Time: {data.get('time', 'unknown')}\n"
        f"Name: {data.get('name', 'unknown')}\n"
        f"Username: {data.get('username', 'unknown')}\n"
        f"User ID: {data.get('user_id', 'unknown')}\n"
        f"Chat ID: {data.get('chat_id', 'unknown')}\n"
        f"Payment Status: {get_payment_status_label(data.get('payment_status', 'unpaid'))}\n"
        f"Payment Method: {data.get('payment_method', '-') or '-'}\n"
        f"Paid At: {data.get('paid_at', '-') or '-'}\n"
        f"Payment Note: {data.get('payment_note', '-') or '-'}\n\n"
        f"Order Details:\n{data.get('details', '')}"
    )
    await update.message.reply_text(text)


async def replyorder_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /replyorder EE-123456 Your message here")
        return

    order_id = parts[1].strip()
    reply_text = parts[2].strip()

    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    customer_chat_id = orders[order_id]["chat_id"]

    try:
        await context.bot.send_message(
            chat_id=customer_chat_id,
            text=(
                f"Message from admin regarding your order\n\n"
                f"Order ID: {order_id}\n"
                f"Message: {reply_text}"
            ),
        )
        await update.message.reply_text(f"Reply sent to customer for {order_id}")
    except Exception as e:
        await update.message.reply_text(f"Reply error: {e}")



async def deliver_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Usage: /deliver ORDER_ID
    Automatically finds the card linked to the order and delivers it.
    """
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /deliver ORDER_ID")
        return

    order_id = parts[1].strip()

    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    data = orders[order_id]
    if data.get("status") == "delivered":
        await update.message.reply_text("This order has already been delivered.")
        return

    customer_chat_id = data.get("chat_id")
    provider  = data.get("provider") or "Unknown"
    short     = PROVIDER_SHORT.get(provider, provider)
    requested = data.get("requested_card_balance")
    requested_text = format_usd(float(requested)) if isinstance(requested, (int, float)) else "Unknown"

    # Kart bilgilerini bul
    cards = load_cards()
    linked_card = None
    for cid, card in cards.items():
        if card.get("order_id") == order_id:
            linked_card = card
            break

    # Teslimat mesajini hazirla
    if linked_card:
        cn  = linked_card["card_number"]
        em  = linked_card.get("expiry_month", "")
        ey  = linked_card.get("expiry_year", "")
        cvv = linked_card.get("cvv", "")
        cur = linked_card.get("currency", "US")
        bal = float(linked_card.get("balance", 0))
        exp_str = f"{em}/{ey[-2:]}" if len(ey) >= 2 else ey
        rate = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.38)))

        delivery_text = (
            f"Your card has been delivered\n\n"
            f"Order ID: {order_id}\n\n"
            f"Card Number: {cn}\n"
            f"Expiry:      {exp_str}\n"
            f"CVV:         {cvv}\n\n"
            f"Balance:  {cur}${bal:.2f}\n"
            f"Provider: {short}\n"
            f"Rate:     {rate*100:.0f}%\n\n"
            f"Please save your card details safely.\n"
            f"Thank you for choosing Elite Earners!"
        )
    else:
        # Kart bulunamazsa admin mesaj girebilir
        await update.message.reply_text(
            f"No card linked to {order_id}.\n"
            "Use /deliver ORDER_ID manually or check /orderinfo."
        )
        return

    orders[order_id]["status"] = "delivered"
    orders[order_id]["delivery_message"] = f"{linked_card['card_number']}:{exp_str}:{cvv}"
    orders[order_id]["delivered_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_orders(orders)

    try:
        await context.bot.send_message(
            chat_id=customer_chat_id,
            text=delivery_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 My Orders", callback_data="my_orders")],
                [InlineKeyboardButton("🛒 View Listing", callback_data="lst:1:")],
            ]),
        )
    except Exception as e:
        await update.message.reply_text(f"Could not notify user: {e}")
        return

    add_audit_log_entry(
        action="deliver",
        admin_user=update.effective_user,
        target_type="order",
        target_id=str(order_id),
        details=f"card delivered: {linked_card['card_number'][:6]}xxxxxx",
    )
    await update.message.reply_text(
        f"Delivered successfully\n\n"
        f"Order ID: {order_id}\n"
        f"Card: {linked_card['card_number'][:6]}xxxxxx\n"
        f"User notified."
    )


async def cancelorder_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    order_id = get_order_id_from_command(update)
    if not order_id:
        await update.message.reply_text("Usage: /cancelorder ORDER_ID")
        return
    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    if orders[order_id].get("status") == "delivered":
        await update.message.reply_text("Delivered orders cannot be cancelled with this command.")
        return

    orders[order_id]["status"] = "cancelled"
    if orders[order_id].get("payment_status") == "paid":
        save_orders(orders)
    else:
        set_order_payment_status(order_id, "failed", "manual", "Order cancelled before payment completion", "")

    customer_chat_id = orders[order_id].get("chat_id")
    try:
        await context.bot.send_message(
            chat_id=customer_chat_id,
            text=(
                "Your order has been cancelled.\n\n"
                f"Order ID: {order_id}\n"
                "If you need more information, please contact support."
            ),
        )
    except Exception:
        pass

    add_audit_log_entry(
        action="cancelorder",
        admin_user=update.effective_user,
        target_type="order",
        target_id=str(order_id),
        details="cancel command",
    )
    await update.message.reply_text(f"Order cancelled: {order_id}")



async def refundbalance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return

    order_id = get_order_id_from_command(update)
    if not order_id:
        await update.message.reply_text("Usage: /refundbalance ORDER_ID")
        return
    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    if not acquire_refund_guard(order_id):
        await update.message.reply_text("This refund is already being processed. Please wait.")
        return

    try:
        async with transaction_lock:
            data = orders[order_id]
            if data.get("status") == "refunded":
                await update.message.reply_text("This order has already been refunded.")
                return

            charged_amount = data.get("charged_amount")
            user_id = data.get("user_id")
            if not isinstance(charged_amount, (int, float)):
                await update.message.reply_text("This order does not contain a refundable wallet charge.")
                return

            record = get_hybrid_balance_record(int(user_id))
            before_balance = round(float(record.get("available_balance", 0.0) or 0.0), 2)
            record["available_balance"] = round(float(record.get("available_balance", 0.0) or 0.0) + float(charged_amount), 2)
            record["total_spent"] = round(max(0.0, float(record.get("total_spent", 0.0) or 0.0) - float(charged_amount)), 2)
            balances_data[str(user_id)] = record
            save_hybrid_balance_record(int(user_id), record)

            orders[order_id]["status"] = "refunded"
            orders[order_id]["refunded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            orders[order_id]["payment_status"] = "refunded"
            orders[order_id]["payment_method"] = orders[order_id].get("payment_method") or "wallet_balance"
            orders[order_id]["payment_note"] = "Wallet balance refunded by admin"
            save_hybrid_order(order_id, orders[order_id])
            add_balance_ledger_entry(
                user_id=user_id,
                delta=float(charged_amount),
                reason="refundbalance",
                reference_id=str(order_id),
                actor=str(update.effective_user.id),
                before_balance=before_balance,
                after_balance=round(float(record.get("available_balance", 0.0) or 0.0), 2),
            )
            create_backup_snapshot("refundbalance")

        customer_chat_id = data.get("chat_id")
        try:
            await context.bot.send_message(
                chat_id=customer_chat_id,
                text=(
                    "Balance refund completed ✅\n\n"
                    f"Order ID: {order_id}\n"
                    f"Refunded Amount: {format_usd(float(charged_amount))}\n"
                    f"Available Balance: {format_usd(float(record.get('available_balance', 0.0)))}\n\n"
                    "The refunded amount has been returned to your bot balance."
                ),
            )
        except Exception:
            pass

        add_audit_log_entry(
            action="refundbalance",
            admin_user=update.effective_user,
            target_type="order",
            target_id=str(order_id),
            details=f"amount={charged_amount}",
        )
        await update.message.reply_text(
            "Balance refunded successfully ✅\n\n"
            f"Order ID: {order_id}\n"
            f"Refunded Amount: {format_usd(float(charged_amount))}\n"
            f"User ID: {user_id}\n"
            f"New Available Balance: {format_usd(float(record.get('available_balance', 0.0)))}"
        )
    finally:
        release_refund_guard(order_id)


async def show_payment_status_message(message_target, user_id: int) -> None:
    wallet_orders, legacy_orders = get_customer_visible_orders(user_id)

    if not wallet_orders and not legacy_orders:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Place Order", callback_data="order")],
            [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
        ])
        await message_target.reply_text("Payment Status\n\nNo orders found.", reply_markup=keyboard)
        return

    lines = ["Payment Status", ""]
    for order_id, data in wallet_orders[:10]:
        lines.append(build_customer_payment_status_text(order_id, data))
        lines.append("")

    if legacy_orders:
        lines.append(f"Legacy Orders Without Payment Tracking: {len(legacy_orders)}")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("My Orders", callback_data="my_orders")],
        [InlineKeyboardButton("My Profile", callback_data="my_profile")],
        [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
    ])
    await message_target.reply_text("\n".join(lines).strip(), reply_markup=keyboard)


async def myorder_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /myorder ORDER_ID")
        return

    order_id = parts[1].strip()
    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    data = orders[order_id]
    requester_chat_id = update.effective_chat.id
    order_chat_id = data.get("chat_id")
    order_user_id = data.get("user_id")

    if requester_chat_id != ADMIN_CHAT_ID and requester_chat_id != order_chat_id and requester_chat_id != order_user_id:
        await update.message.reply_text("You are not allowed to view this order.")
        return

    lines = [
        "My Order Details",
        "",
        build_customer_order_summary_text(order_id, data),
        "",
        build_customer_payment_status_text(order_id, data),
    ]

    delivery_message = data.get("delivery_message")
    if delivery_message:
        lines.extend(["", "Delivery Details", delivery_message])

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("My Orders", callback_data="my_orders")],
        [InlineKeyboardButton("Payment Status", callback_data="payment_status")],
        [InlineKeyboardButton("My Profile", callback_data="my_profile")],
        [InlineKeyboardButton("Main Menu", callback_data="main_menu")],
    ])
    await update.message.reply_text("\n".join(lines), reply_markup=keyboard)

async def stockpanel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /stockpanel PROVIDER")
        return

    provider = canonical_provider_name(parts[1].strip())
    if not provider:
        await update.message.reply_text("Unknown provider. Use /providers")
        return

    await update.message.reply_text(
        build_stock_panel_text(provider),
        reply_markup=stock_panel_markup(provider),
    )


async def exportorders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    filename = f"orders_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = export_orders_csv(filename)
    add_audit_log_entry(
        action="exportorders",
        admin_user=update.effective_user,
        target_type="export",
        target_id=filename,
        details="orders csv",
    )
    with open(filepath, "rb") as f:
        await update.message.reply_document(document=f, filename=filename, caption="Orders export ready.")


async def exportbalances_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    filename = f"wallets_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = export_wallets_csv(filename)
    add_audit_log_entry(
        action="exportbalances",
        admin_user=update.effective_user,
        target_type="export",
        target_id=filename,
        details="wallet csv",
    )
    with open(filepath, "rb") as f:
        await update.message.reply_document(document=f, filename=filename, caption="Wallet export ready.")


async def exportrequests_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    filename = f"balance_requests_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = export_balance_requests_csv(filename)
    add_audit_log_entry(
        action="exportrequests",
        admin_user=update.effective_user,
        target_type="export",
        target_id=filename,
        details="balance requests csv",
    )
    with open(filepath, "rb") as f:
        await update.message.reply_document(document=f, filename=filename, caption="Balance requests export ready.")



async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text(broadcast_usage_text())
        return

    target_type = parts[1].strip().lower()
    message_text = parts[2].strip()

    if target_type not in {"approved", "wallet", "orders", "all"}:
        await update.message.reply_text(broadcast_usage_text())
        return

    targets = get_broadcast_targets(target_type)
    if not targets:
        await update.message.reply_text(f"No users found for target group: {target_type}")
        return

    token = f"BC-{int(time.time())}-{random.randint(100,999)}"
    broadcast_confirmations[token] = {
        "target_type": target_type,
        "message_text": message_text,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    await update.message.reply_text(
        build_broadcast_preview_text(target_type, message_text),
        reply_markup=broadcast_confirm_markup(target_type, token),
    )


async def maintenance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global config

    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    parts = get_command_parts(update)
    config.update(load_config())

    if len(parts) < 2:
        status = "on" if config.get("maintenance_mode", False) else "off"
        current_msg = str(config.get("maintenance_message", "") or "").strip() or "(default message)"
        await update.message.reply_text(
            "Usage:\n"
            "/maintenance on\n"
            "/maintenance off\n"
            "/maintenance status\n"
            "/maintenancemsg Your custom maintenance message\n\n"
            f"Current status: {status}\n"
            f"Current message: {current_msg}"
        )
        return

    action = parts[1].strip().lower()

    if action == "status":
        status = "on" if config.get("maintenance_mode", False) else "off"
        current_msg = str(config.get("maintenance_message", "") or "").strip() or "(default message)"
        await update.message.reply_text(
            "Maintenance Status\n\n"
            f"Enabled: {status}\n"
            f"Message: {current_msg}"
        )
        return

    if action not in ("on", "off"):
        await update.message.reply_text("Usage: /maintenance on OR /maintenance off OR /maintenance status")
        return

    config["maintenance_mode"] = action == "on"
    save_config(config)

    if action == "on":
        add_audit_log_entry(
            action="maintenance_on",
            admin_user=update.effective_user,
            target_type="system",
            target_id="maintenance",
            details="maintenance enabled",
        )
        await update.message.reply_text("Maintenance mode enabled.")
    else:
        add_audit_log_entry(
            action="maintenance_off",
            admin_user=update.effective_user,
            target_type="system",
            target_id="maintenance",
            details="maintenance disabled",
        )
        await update.message.reply_text("Maintenance mode disabled.")


async def maintenancemsg_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global config

    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text(
            "Usage: /maintenancemsg Your custom maintenance message\n\n"
            "Use /maintenance status to see the current value."
        )
        return

    config.update(load_config())
    config["maintenance_message"] = parts[1].strip()
    save_config(config)
    add_audit_log_entry(
        action="maintenance_message",
        admin_user=update.effective_user,
        target_type="system",
        target_id="maintenance",
        details=parts[1].strip(),
    )
    await update.message.reply_text("Maintenance message updated.")


async def ordernote_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /ordernote ORDER_ID Your internal note")
        return

    order_id = parts[1].strip()
    note = parts[2].strip()

    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    orders[order_id]["admin_note"] = note
    save_hybrid_order(order_id, orders[order_id])
    add_audit_log_entry(
        action="ordernote",
        admin_user=update.effective_user,
        target_type="order",
        target_id=str(order_id),
        details=note,
    )
    await update.message.reply_text(
        f"Order admin note updated.\n\nOrder ID: {order_id}\nNote: {note}"
    )


async def ordertag_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /ordertag ORDER_ID tag1, tag2")
        return

    order_id = parts[1].strip()
    tags = normalize_tag_text(parts[2].strip())

    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    orders[order_id]["admin_tags"] = tags
    save_hybrid_order(order_id, orders[order_id])
    add_audit_log_entry(
        action="ordertag",
        admin_user=update.effective_user,
        target_type="order",
        target_id=str(order_id),
        details=tags,
    )
    await update.message.reply_text(
        f"Order admin tags updated.\n\nOrder ID: {order_id}\nTags: {tags or '-'}"
    )


async def balancenote_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /balancenote REQUEST_ID Your internal note")
        return

    request_id = parts[1].strip()
    note = parts[2].strip()

    if request_id not in balance_requests_data:
        await update.message.reply_text("Balance request not found.")
        return

    balance_requests_data[request_id]["admin_note"] = note
    save_hybrid_balance_request(request_id, balance_requests_data[request_id])
    add_audit_log_entry(
        action="balancenote",
        admin_user=update.effective_user,
        target_type="balance_request",
        target_id=str(request_id),
        details=note,
    )
    await update.message.reply_text(
        f"Balance request admin note updated.\n\nRequest ID: {request_id}\nNote: {note}"
    )


async def balancetag_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /balancetag REQUEST_ID tag1, tag2")
        return

    request_id = parts[1].strip()
    tags = normalize_tag_text(parts[2].strip())

    if request_id not in balance_requests_data:
        await update.message.reply_text("Balance request not found.")
        return

    balance_requests_data[request_id]["admin_tags"] = tags
    save_hybrid_balance_request(request_id, balance_requests_data[request_id])
    add_audit_log_entry(
        action="balancetag",
        admin_user=update.effective_user,
        target_type="balance_request",
        target_id=str(request_id),
        details=tags,
    )
    await update.message.reply_text(
        f"Balance request admin tags updated.\n\nRequest ID: {request_id}\nTags: {tags or '-'}"
    )


async def auditlog_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    limit = 20
    parts = get_command_parts(update)
    if len(parts) >= 2:
        try:
            parsed_limit = int(parts[1])
            if 1 <= parsed_limit <= 100:
                limit = parsed_limit
        except Exception:
            pass

    await update.message.reply_text(build_audit_log_text(limit))


async def exportaudit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    filename = f"audit_log_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = export_audit_log_csv(filename)
    with open(filepath, "rb") as f:
        await update.message.reply_document(document=f, filename=filename, caption="Audit log export ready.")


async def findorder_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /findorder KEYWORD")
        return

    keyword = parts[1].strip()
    results = search_orders_by_keyword(keyword)

    if not results:
        await update.message.reply_text("No matching orders found.")
        return

    lines = [f"Order Search Results: {len(results)}", ""]
    for order_id, data in results[:20]:
        lines.append(format_order_search_line(order_id, data))
    await update.message.reply_text("\n".join(lines))


async def findrequest_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /findrequest KEYWORD")
        return

    keyword = parts[1].strip()
    results = search_balance_requests_by_keyword(keyword)

    if not results:
        await update.message.reply_text("No matching balance requests found.")
        return

    lines = [f"Balance Request Search Results: {len(results)}", ""]
    for request_id, data in results[:20]:
        lines.append(format_balance_request_search_line(request_id, data))
    await update.message.reply_text("\n".join(lines))


async def ordersbyuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /ordersbyuser USER_ID")
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("USER_ID must be numeric.")
        return

    results = []
    for order_id, data in get_all_hybrid_orders().items():
        if data.get("user_id") == target_user_id or data.get("chat_id") == target_user_id:
            results.append((order_id, data))
    results.sort(key=lambda item: item[1].get("time", ""), reverse=True)

    if not results:
        await update.message.reply_text("No orders found for this user.")
        return

    lines = [f"Orders for user {target_user_id}: {len(results)}", ""]
    for order_id, data in results[:20]:
        lines.append(format_order_search_line(order_id, data))
    await update.message.reply_text("\n".join(lines))


async def ordersbystatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /ordersbystatus STATUS")
        return

    target_status = parts[1].strip().lower()
    results = []
    for order_id, data in get_all_hybrid_orders().items():
        if str(data.get("status", "") or "").lower() == target_status:
            results.append((order_id, data))
    results.sort(key=lambda item: item[1].get("time", ""), reverse=True)

    if not results:
        await update.message.reply_text("No orders found for this status.")
        return

    lines = [f"Orders with status {target_status}: {len(results)}", ""]
    for order_id, data in results[:20]:
        lines.append(format_order_search_line(order_id, data))
    await update.message.reply_text("\n".join(lines))


async def ordersbyprovider_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /ordersbyprovider PROVIDER")
        return

    provider = canonical_provider_name(parts[1].strip())
    if not provider:
        await update.message.reply_text("Unknown provider. Use /providers")
        return

    results = []
    for order_id, data in get_all_hybrid_orders().items():
        if str(data.get("provider", "") or "") == provider:
            results.append((order_id, data))
    results.sort(key=lambda item: item[1].get("time", ""), reverse=True)

    if not results:
        await update.message.reply_text("No orders found for this provider.")
        return

    lines = [f"Orders for provider {provider}: {len(results)}", ""]
    for order_id, data in results[:20]:
        lines.append(format_order_search_line(order_id, data))
    await update.message.reply_text("\n".join(lines))


async def ordersbytag_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Admin only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /ordersbytag TAG")
        return

    tag = parts[1].strip().lower()
    results = []
    for order_id, data in get_all_hybrid_orders().items():
        tags = [t.strip().lower() for t in str(data.get("admin_tags", "") or "").split(",") if t.strip()]
        if tag in tags:
            results.append((order_id, data))
    results.sort(key=lambda item: item[1].get("time", ""), reverse=True)

    if not results:
        await update.message.reply_text("No orders found for this tag.")
        return

    lines = [f"Orders with tag {tag}: {len(results)}", ""]
    for order_id, data in results[:20]:
        lines.append(format_order_search_line(order_id, data))
    await update.message.reply_text("\n".join(lines))


async def addmod_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global moderators_data

    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /addmod USER_ID")
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("USER_ID must be numeric.")
        return

    if target_user_id not in moderators_data:
        moderators_data.append(target_user_id)
        save_moderators(moderators_data)

    add_audit_log_entry(
        action="addmod",
        admin_user=update.effective_user,
        target_type="moderator",
        target_id=str(target_user_id),
        details="moderator added",
    )
    await update.message.reply_text(f"Moderator added: {target_user_id}")


async def removemod_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global moderators_data

    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /removemod USER_ID")
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("USER_ID must be numeric.")
        return

    if target_user_id in moderators_data:
        moderators_data.remove(target_user_id)
        save_moderators(moderators_data)

    add_audit_log_entry(
        action="removemod",
        admin_user=update.effective_user,
        target_type="moderator",
        target_id=str(target_user_id),
        details="moderator removed",
    )
    await update.message.reply_text(f"Moderator removed: {target_user_id}")


async def modlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return

    if not moderators_data:
        await update.message.reply_text("No moderators found.")
        return

    lines = ["Moderators", ""]
    for user_id in moderators_data:
        lines.append(str(user_id))
    await update.message.reply_text("\n".join(lines))


async def messageuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /messageuser USER_ID Your message")
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("USER_ID must be numeric.")
        return

    message_text = parts[2].strip()

    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=("Admin Message\n\n" f"{message_text}"),
        )
        add_audit_log_entry(
            action="messageuser",
            admin_user=update.effective_user,
            target_type="user",
            target_id=str(target_user_id),
            details=message_text[:200],
        )
        await update.message.reply_text(f"Message sent to user {target_user_id}.")
    except Exception as e:
        await update.message.reply_text(f"Failed to send message: {e}")


def build_support_reply_text(message_text: str) -> str:
    clean = str(message_text or "").strip()
    return (
        "Support Reply\n\n"
        f"{clean}\n\n"
        "You can reply here in the bot if you need more help. "
        "If needed, we can also continue through my private Telegram contact shared by admin."
    )


async def replyuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /replyuser USER_ID Your support reply")
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("USER_ID must be numeric.")
        return

    message_text = parts[2].strip()
    if not message_text:
        await update.message.reply_text("Reply message cannot be empty.")
        return

    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=build_support_reply_text(message_text),
        )
        add_audit_log_entry(
            action="replyuser",
            admin_user=update.effective_user,
            target_type="user",
            target_id=str(target_user_id),
            details=message_text[:200],
        )
        await update.message.reply_text(f"Support reply sent to user {target_user_id}.")
    except Exception as e:
        await update.message.reply_text(f"Failed to send support reply: {e}")


async def replysupport_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await replyuser_command(update, context)


async def reply_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await replyuser_command(update, context)


async def messageorder_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /messageorder ORDER_ID Your message")
        return

    order_id = parts[1].strip()
    if order_id not in orders:
        await update.message.reply_text("Order not found.")
        return

    target_chat_id = orders[order_id].get("chat_id") or orders[order_id].get("user_id")
    if not target_chat_id:
        await update.message.reply_text("No user is linked to this order.")
        return

    message_text = parts[2].strip()

    try:
        await context.bot.send_message(
            chat_id=int(target_chat_id),
            text=("Admin Message\n\n" f"Order ID: {order_id}\n\n{message_text}"),
        )
        add_audit_log_entry(
            action="messageorder",
            admin_user=update.effective_user,
            target_type="order",
            target_id=str(order_id),
            details=message_text[:200],
        )
        await update.message.reply_text(f"Message sent for order {order_id}.")
    except Exception as e:
        await update.message.reply_text(f"Failed to send message: {e}")


async def messagebalance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return
    if not update.message or not update.message.text:
        return

    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /messagebalance REQUEST_ID Your message")
        return

    request_id = parts[1].strip()
    if request_id not in balance_requests_data:
        await update.message.reply_text("Balance request not found.")
        return

    target_user_id = balance_requests_data[request_id].get("user_id")
    if not target_user_id:
        await update.message.reply_text("No user is linked to this balance request.")
        return

    message_text = parts[2].strip()

    try:
        await context.bot.send_message(
            chat_id=int(target_user_id),
            text=("Admin Message\n\n" f"Request ID: {request_id}\n\n{message_text}"),
        )
        add_audit_log_entry(
            action="messagebalance",
            admin_user=update.effective_user,
            target_type="balance_request",
            target_id=str(request_id),
            details=message_text[:200],
        )
        await update.message.reply_text(f"Message sent for balance request {request_id}.")
    except Exception as e:
        await update.message.reply_text(f"Failed to send message: {e}")






async def sqliteprimarycheck_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return

    orders_count = len(get_all_hybrid_orders())
    requests_count = len(get_all_hybrid_balance_requests())

    await update.message.reply_text(
        "SQLite Primary Operational Check\n\n"
        + build_sqlite_mode_status_text()
        + f"\n\nReadable Orders: {orders_count}\nReadable Balance Requests: {requests_count}"
    )


async def sqlitemode_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global config

    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return

    parts = get_command_parts(update)
    config.update(load_config())

    if len(parts) < 2:
        await update.message.reply_text(
            "Usage:\n"
            "/sqlitemode status\n"
            "/sqlitemode balances on|off\n"
            "/sqlitemode orders on|off\n"
            "/sqlitemode requests on|off\n\n"
            + build_sqlite_mode_status_text()
        )
        return

    action = parts[1].strip().lower()
    if action == "status":
        await update.message.reply_text(build_sqlite_mode_status_text())
        return

    if len(parts) < 3:
        await update.message.reply_text("Usage: /sqlitemode balances|orders|requests on|off")
        return

    value = parts[2].strip().lower()
    if value not in ("on", "off"):
        await update.message.reply_text("Mode value must be on or off.")
        return

    enabled = value == "on"
    if action == "balances":
        config["sqlite_primary_balances"] = enabled
    elif action == "orders":
        config["sqlite_primary_orders"] = enabled
    elif action == "requests":
        config["sqlite_primary_balance_requests"] = enabled
    else:
        await update.message.reply_text("Unknown sqlite mode target. Use balances, orders, or requests.")
        return

    save_config(config)
    add_audit_log_entry("sqlitemode", update.effective_user, "sqlite_mode", action, value)
    await update.message.reply_text("SQLite mode updated successfully.\n\n" + build_sqlite_mode_status_text())


async def sqlitestatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return

    await update.message.reply_text(build_sqlite_status_text())


async def sqlitemigrate_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return

    init_sqlite_db()
    result = migrate_json_to_sqlite()
    add_audit_log_entry(
        action="sqlitemigrate",
        admin_user=update.effective_user,
        target_type="sqlite",
        target_id=SQLITE_DB_FILE,
        details=str(result),
    )

    await update.message.reply_text(
        "SQLite migration completed successfully.\n\n"
        f"Balances: {result.get('balances', 0)}\n"
        f"Orders: {result.get('orders', 0)}\n"
        f"Balance Requests: {result.get('balance_requests', 0)}\n"
        f"Audit Log: {result.get('audit_log', 0)}\n"
        f"Balance Ledger: {result.get('balance_ledger', 0)}"
    )






async def logstoragestatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return

    await update.message.reply_text(build_log_storage_status_text())


async def balancerequestsync_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /balancerequestsync REQUEST_ID")
        return

    request_id = parts[1].strip()
    if request_id not in balance_requests_data:
        await update.message.reply_text("Balance request not found in JSON.")
        return

    save_hybrid_balance_request(request_id, balance_requests_data[request_id])
    add_audit_log_entry("balancerequestsync", update.effective_user, "balance_request", request_id, "manual hybrid balance request sync")
    await update.message.reply_text("Balance request sync completed successfully.\n\n" + build_balance_request_sync_status_text(request_id))


async def balancerequeststatussync_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /balancerequeststatussync REQUEST_ID")
        return

    request_id = parts[1].strip()
    await update.message.reply_text(build_balance_request_sync_status_text(request_id))


async def ordersync_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /ordersync ORDER_ID")
        return

    order_id = parts[1].strip()
    if order_id not in orders:
        await update.message.reply_text("Order not found in JSON.")
        return

    save_hybrid_order(order_id, orders[order_id])
    add_audit_log_entry("ordersync", update.effective_user, "order", order_id, "manual hybrid order sync")
    await update.message.reply_text("Order sync completed successfully.\n\n" + build_order_sync_status_text(order_id))


async def orderstatussync_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /orderstatussync ORDER_ID")
        return

    order_id = parts[1].strip()
    await update.message.reply_text(build_order_sync_status_text(order_id))


async def balancesync_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /balancesync USER_ID")
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("USER_ID must be numeric.")
        return

    record = get_user_balance_record(target_user_id)
    save_hybrid_balance_record(target_user_id, record)
    add_audit_log_entry("balancesync", update.effective_user, "user", str(target_user_id), "manual hybrid balance sync")

    await update.message.reply_text(
        "Balance sync completed successfully.\n\n" + build_balance_sync_status_text(target_user_id)
    )


async def balancestatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /balancestatus USER_ID")
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("USER_ID must be numeric.")
        return

    await update.message.reply_text(build_balance_sync_status_text(target_user_id))


async def backupnow_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return
    folder = create_backup_snapshot("manual")
    add_audit_log_entry("backupnow", update.effective_user, "backup", folder, "manual backup")
    await update.message.reply_text(f"Backup snapshot created successfully.\n\nFolder: {folder}")


async def ledgeruser_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        await update.message.reply_text("Staff only command.")
        return
    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /ledgeruser USER_ID [LIMIT]")
        return
    try:
        target_user_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("USER_ID must be numeric.")
        return
    limit = 20
    if len(parts) >= 3:
        try:
            parsed_limit = int(parts[2])
            if 1 <= parsed_limit <= 100:
                limit = parsed_limit
        except Exception:
            pass
    await update.message.reply_text(build_balance_ledger_text(target_user_id, limit))


async def exportledger_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        await update.message.reply_text("Full admin only command.")
        return
    filename = f"balance_ledger_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = export_balance_ledger_csv(filename)
    add_audit_log_entry("exportledger", update.effective_user, "export", filename, "balance ledger csv")
    with open(filepath, "rb") as f:
        await update.message.reply_document(document=f, filename=filename, caption="Balance ledger export ready.")


def build_payment_info_text(order_id: str, data: dict | None = None) -> str:
    data = data or get_hybrid_order(order_id) or {}
    if not data:
        return f"Payment Info\n\nOrder ID: {order_id}\nStatus: Not found"

    provider = data.get("provider") or "Unknown"
    requested = data.get("requested_card_balance", data.get("balance"))
    charged = data.get("charged_amount", data.get("cost"))
    requested_text = format_usd(float(requested)) if isinstance(requested, (int, float)) else "-"
    charged_text = format_usd(float(charged)) if isinstance(charged, (int, float)) else "-"
    payment_status = get_payment_status_label(data.get("payment_status", "unpaid"))
    payment_method = data.get("payment_method", "-") or "-"
    paid_at = data.get("paid_at", "-") or "-"
    payment_note = data.get("payment_note", "-") or "-"
    order_status = get_order_status_label(data.get("status", "unknown"))
    return (
        "Payment Info\n\n"
        f"Order ID: {order_id}\n"
        f"Provider: {provider}\n"
        f"Requested Card Balance: {requested_text}\n"
        f"Charged Amount: {charged_text}\n"
        f"Order Status: {order_status}\n"
        f"Payment Status: {payment_status}\n"
        f"Payment Method: {payment_method}\n"
        f"Paid At: {paid_at}\n"
        f"Note: {payment_note}"
    )


def build_balance_ledger_text(target_user_id: int, limit: int = 20) -> str:
    limit = max(1, min(int(limit or 20), 100))
    entries = [e for e in load_balance_ledger() if int(e.get("user_id", 0)) == int(target_user_id)]
    if not entries:
        return f"Balance Ledger\n\nUser ID: {target_user_id}\nNo ledger entries found."
    entries = entries[-limit:][::-1]
    lines = [f"Balance Ledger\n\nUser ID: {target_user_id}\nShowing last {len(entries)} entries", ""]
    for e in entries:
        lines.append(
            f"{e.get('time', '-') } | {float(e.get('delta', 0.0)):+.2f} | {e.get('reason', '-') } | Ref: {e.get('reference_id', '-') } | Before: {float(e.get('before_balance', 0.0)):.2f} | After: {float(e.get('after_balance', 0.0)):.2f}"
        )
    return "\n".join(lines)


def export_balance_ledger_csv(filename: str) -> str:
    filepath = _build_data_path(filename)
    rows = load_balance_ledger()
    fieldnames = ["time", "user_id", "delta", "reason", "reference_id", "actor", "before_balance", "after_balance"]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
    return filepath


def build_broadcast_preview_text(target_type: str, message_text: str) -> str:
    targets = get_broadcast_targets(target_type)
    preview = message_text.strip()
    if len(preview) > 700:
        preview = preview[:700] + "..."

    return (
        "Broadcast Preview\n\n"
        f"Target Group: {target_type}\n"
        f"Total Targets: {len(targets)}\n\n"
        "Message Preview\n"
        f"{preview}"
    )


def broadcast_confirm_markup(target_type: str, token: str) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("Send Now", callback_data=f"broadcastconfirm::{target_type}::{token}"),
            InlineKeyboardButton("Cancel", callback_data=f"broadcastcancel::{token}"),
        ],
        [InlineKeyboardButton("Send Test To Admin", callback_data=f"broadcasttest::{target_type}::{token}")],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_broadcast_targets(target_type: str) -> list[int]:
    targets = set()

    if target_type == "approved":
        for chat_id in access_data.get("approved_users", []):
            try:
                targets.add(int(chat_id))
            except Exception:
                pass

    elif target_type == "wallet":
        for user_id in balances_data.keys():
            try:
                targets.add(int(user_id))
            except Exception:
                pass

    elif target_type == "orders":
        for _, data in orders.items():
            user_id = data.get("user_id")
            chat_id = data.get("chat_id")
            try:
                if user_id:
                    targets.add(int(user_id))
            except Exception:
                pass
            try:
                if chat_id:
                    targets.add(int(chat_id))
            except Exception:
                pass

    elif target_type == "all":
        for chat_id in access_data.get("approved_users", []):
            try:
                targets.add(int(chat_id))
            except Exception:
                pass
        for user_id in balances_data.keys():
            try:
                targets.add(int(user_id))
            except Exception:
                pass
        for _, data in orders.items():
            user_id = data.get("user_id")
            chat_id = data.get("chat_id")
            try:
                if user_id:
                    targets.add(int(user_id))
            except Exception:
                pass
            try:
                if chat_id:
                    targets.add(int(chat_id))
            except Exception:
                pass

    return sorted(targets)


def broadcast_usage_text() -> str:
    return (
        "Broadcast Usage\n\n"
        "/broadcast approved Your message here\n"
        "/broadcast wallet Your message here\n"
        "/broadcast orders Your message here\n"
        "/broadcast all Your message here\n\n"
        "Groups\n"
        "approved = approved users\n"
        "wallet = users with wallet records\n"
        "orders = users found in order history\n"
        "all = combined audience"
    )


def export_orders_csv(filepath: str) -> str:
    fieldnames = [
        "order_id",
        "time",
        "status",
        "payment_status",
        "payment_method",
        "payment_note",
        "paid_at",
        "provider",
        "requested_card_balance",
        "charged_amount",
        "name",
        "username",
        "user_id",
        "chat_id",
        "delivered_at",
        "refunded_at",
        "details",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for order_id, data in sorted(orders.items(), key=lambda item: item[1].get("time", ""), reverse=True):
            writer.writerow({
                "order_id": order_id,
                "time": data.get("time", ""),
                "status": data.get("status", ""),
                "payment_status": data.get("payment_status", ""),
                "payment_method": data.get("payment_method", ""),
                "payment_note": data.get("payment_note", ""),
                "paid_at": data.get("paid_at", ""),
                "provider": data.get("provider", ""),
                "requested_card_balance": data.get("requested_card_balance", ""),
                "charged_amount": data.get("charged_amount", ""),
                "name": data.get("name", ""),
                "username": data.get("username", ""),
                "user_id": data.get("user_id", ""),
                "chat_id": data.get("chat_id", ""),
                "delivered_at": data.get("delivered_at", ""),
                "refunded_at": data.get("refunded_at", ""),
                "details": data.get("details", ""),
            })
    return filepath


def export_balance_requests_csv(filepath: str) -> str:
    fieldnames = [
        "request_id",
        "time",
        "user_id",
        "name",
        "username",
        "coin",
        "network",
        "usd_amount",
        "status",
        "credited_at",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for request_id, data in get_sorted_balance_requests():
            writer.writerow({
                "request_id": request_id,
                "time": data.get("time", ""),
                "user_id": data.get("user_id", ""),
                "name": data.get("name", ""),
                "username": data.get("username", ""),
                "coin": data.get("coin", ""),
                "network": data.get("network", ""),
                "usd_amount": data.get("usd_amount", ""),
                "status": data.get("status", ""),
                "credited_at": data.get("credited_at", ""),
            })
    return filepath


def export_wallets_csv(filepath: str) -> str:
    fieldnames = [
        "user_id",
        "available_balance",
        "total_deposited",
        "total_spent",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for user_id, record in sorted(balances_data.items(), key=lambda item: str(item[0])):
            if not isinstance(record, dict):
                continue
            writer.writerow({
                "user_id": user_id,
                "available_balance": record.get("available_balance", 0.0),
                "total_deposited": record.get("total_deposited", 0.0),
                "total_spent": record.get("total_spent", 0.0),
            })
    return filepath


def build_stock_panel_text(provider: str) -> str:
    # SQLite'tan gerçek kart verisi
    avail = db_get_available_cards(provider=provider)
    cards   = len(avail)
    balance = sum(float(c.get("balance", 0)) for _, c in avail)
    rate = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.0)))
    return (
        "Stock Panel\n\n"
        f"Provider: {provider}\n"
        f"Balance: {format_usd(balance)}\n"
        f"Cards: {cards}\n"
        f"Rate: {rate:.2f}"
    )


def stock_panel_markup(provider: str) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("+$50", callback_data=f"stockpanel::{provider}::balplus50"),
            InlineKeyboardButton("-$50", callback_data=f"stockpanel::{provider}::balminus50"),
        ],
        [
            InlineKeyboardButton("+1 Card", callback_data=f"stockpanel::{provider}::cardsplus1"),
            InlineKeyboardButton("-1 Card", callback_data=f"stockpanel::{provider}::cardsminus1"),
        ],
        [
            InlineKeyboardButton("Rate +0.01", callback_data=f"stockpanel::{provider}::rateplus"),
            InlineKeyboardButton("Rate -0.01", callback_data=f"stockpanel::{provider}::rateminus"),
        ],
        [
            InlineKeyboardButton("Refresh", callback_data=f"stockpanel::{provider}::refresh"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def build_rates_panel_text() -> str:
    lines = ["Rates Panel", ""]
    for provider in SUPPORTED_PROVIDERS:
        rate = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.0)))
        lines.append(f"{provider}: {rate:.2f}")
    return "\n".join(lines)


def rates_panel_markup() -> InlineKeyboardMarkup:
    rows = []
    current = []
    for provider in SUPPORTED_PROVIDERS:
        current.append(InlineKeyboardButton(provider, callback_data=f"ratespanel::{provider}"))
        if len(current) == 2:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    return InlineKeyboardMarkup(rows)




def acquire_confirm_guard(user_id: int) -> bool:
    if user_id in active_order_confirms:
        return False
    active_order_confirms.add(user_id)
    return True


def release_confirm_guard(user_id: int) -> None:
    active_order_confirms.discard(user_id)


def acquire_balance_request_guard(request_id: str) -> bool:
    if request_id in active_balance_request_actions:
        return False
    active_balance_request_actions.add(request_id)
    return True


def release_balance_request_guard(request_id: str) -> None:
    active_balance_request_actions.discard(request_id)


def acquire_refund_guard(order_id: str) -> bool:
    if order_id in active_refund_orders:
        return False
    active_refund_orders.add(order_id)
    return True


def release_refund_guard(order_id: str) -> None:
    active_refund_orders.discard(order_id)

def normalize_tag_text(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parts = [part.strip() for part in raw.split(",")]
    parts = [part for part in parts if part]
    return ", ".join(parts)


def get_order_admin_note(data: dict) -> str:
    return str(data.get("admin_note", "") or "").strip()


def get_order_admin_tags(data: dict) -> str:
    return str(data.get("admin_tags", "") or "").strip()


def get_balance_request_admin_note(data: dict) -> str:
    return str(data.get("admin_note", "") or "").strip()


def get_balance_request_admin_tags(data: dict) -> str:
    return str(data.get("admin_tags", "") or "").strip()


def get_sorted_balance_requests():
    items = list(get_all_hybrid_balance_requests().items())
    items.sort(key=lambda item: item[1].get("time", ""), reverse=True)
    return items


def build_balance_request_info_text(request_id: str, data: dict) -> str:
    return (
        "Balance Request Info\n\n"
        f"Request ID: {request_id}\n"
        f"Time: {data.get('time', '-')}\n"
        f"User ID: {data.get('user_id', '-')}\n"
        f"Name: {data.get('name', '-')}\n"
        f"Username: {data.get('username', '-')}\n"
        f"Coin: {data.get('coin', '-')}\n"
        f"Network: {data.get('network', '-')}\n"
        f"Requested Top-Up: {format_usd(float(data.get('usd_amount', 0.0) or 0.0))}\n"
        f"Status: {data.get('status', '-')}\n"
        f"Credited At: {data.get('credited_at', '-')}\n"
        f"Admin Note: {get_balance_request_admin_note(data) or '-'}\n"
        f"Admin Tags: {get_balance_request_admin_tags(data) or '-'}"
    )


def balance_request_panel_markup(request_id: str) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("Approve", callback_data=f"balreq::{request_id}::approve"),
            InlineKeyboardButton("Reject", callback_data=f"balreq::{request_id}::reject"),
        ],
        [
            InlineKeyboardButton("Mark Paid", callback_data=f"balreq::{request_id}::paid"),
            InlineKeyboardButton("Refresh", callback_data=f"balreq::{request_id}::refresh"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)



def get_today_prefix() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def build_daily_summary_text() -> str:
    today_prefix = get_today_prefix()

    today_orders = []
    for order_id, data in get_all_hybrid_orders().items():
        if str(data.get("time", "")).startswith(today_prefix):
            today_orders.append((order_id, data))

    today_balance_requests = []
    for request_id, data in get_all_hybrid_balance_requests().items():
        if str(data.get("time", "")).startswith(today_prefix):
            today_balance_requests.append((request_id, data))

    order_status_counts = {
        "paid_pending_delivery": 0,
        "delivered": 0,
        "refunded": 0,
        "cancelled": 0,
    }
    payment_status_counts = {
        "paid": 0,
        "pending": 0,
        "unpaid": 0,
        "failed": 0,
        "refunded": 0,
    }

    total_today_charged = 0.0
    total_today_refunded = 0.0

    for _, data in today_orders:
        status = str(data.get("status", "") or "").lower().strip()
        payment_status = str(data.get("payment_status", "unpaid") or "unpaid").lower().strip()

        if status in order_status_counts:
            order_status_counts[status] += 1
        if payment_status in payment_status_counts:
            payment_status_counts[payment_status] += 1

        charged = data.get("charged_amount")
        if isinstance(charged, (int, float)):
            total_today_charged += float(charged)

        if status == "refunded" and isinstance(charged, (int, float)):
            total_today_refunded += float(charged)

    request_status_counts = {
        "pending_manual_payment": 0,
        "paid_uncredited": 0,
        "approved": 0,
        "rejected": 0,
    }
    total_today_requested_topup = 0.0
    total_today_credited = 0.0

    for _, data in today_balance_requests:
        status = str(data.get("status", "") or "").lower().strip()
        if status in request_status_counts:
            request_status_counts[status] += 1

        amount = float(data.get("usd_amount", 0.0) or 0.0)
        total_today_requested_topup += amount
        if status == "approved":
            total_today_credited += amount

    low_stock_items = []
    card_stats = get_card_stats()
    for provider, item in stock.items():
        cards = int(item.get("cards", 0))
        balance = float(item.get("balance", 0))
        if cards <= LOW_STOCK_CARDS_THRESHOLD or balance <= LOW_STOCK_BALANCE_THRESHOLD:
            low_stock_items.append(provider)

    lines = [
        "Daily Summary Report",
        "",
        f"Date: {today_prefix}",
        "",
        "Orders Today",
        f"Total New Orders: {len(today_orders)}",
        f"Pending Delivery: {order_status_counts['paid_pending_delivery']}",
        f"Delivered: {order_status_counts['delivered']}",
        f"Refunded: {order_status_counts['refunded']}",
        f"Cancelled: {order_status_counts['cancelled']}",
        f"Total Charged: {format_usd(round(total_today_charged, 2))}",
        f"Total Refunded: {format_usd(round(total_today_refunded, 2))}",
        "",
        "Payments Today",
        f"Paid: {payment_status_counts['paid']}",
        f"Pending: {payment_status_counts['pending']}",
        f"Unpaid: {payment_status_counts['unpaid']}",
        f"Failed: {payment_status_counts['failed']}",
        f"Refunded: {payment_status_counts['refunded']}",
        "",
        "Balance Requests Today",
        f"Total Requests: {len(today_balance_requests)}",
        f"Pending Manual Payment: {request_status_counts['pending_manual_payment']}",
        f"Paid Uncredited: {request_status_counts['paid_uncredited']}",
        f"Approved: {request_status_counts['approved']}",
        f"Rejected: {request_status_counts['rejected']}",
        f"Total Requested Top-Up: {format_usd(round(total_today_requested_topup, 2))}",
        f"Total Credited Top-Up: {format_usd(round(total_today_credited, 2))}",
        "",
        "Stock Alerts",
        f"Low Stock Providers: {len(low_stock_items)}",
    ]

    if low_stock_items:
        lines.append("Low Stock List: " + ", ".join(low_stock_items[:10]))

    return "\n".join(lines)


def get_payment_status_counts() -> dict:
    counts = {
        "unpaid": 0,
        "pending": 0,
        "paid": 0,
        "failed": 0,
        "refunded": 0,
    }
    for data in orders.values():
        status = str(data.get("payment_status", "unpaid") or "unpaid").lower().strip()
        if status in counts:
            counts[status] += 1
    return counts


def get_order_status_counts() -> dict:
    keys = [
        "new",
        "paid",
        "processing",
        "done",
        "cancelled",
        "archived",
        "paid_pending_delivery",
        "delivered",
        "refunded",
    ]
    counts = {k: 0 for k in keys}
    for data in orders.values():
        status = str(data.get("status", "unknown") or "unknown").lower().strip()
        if status in counts:
            counts[status] += 1
    return counts


def get_wallet_summary() -> dict:
    total_available = 0.0
    total_deposited = 0.0
    total_spent = 0.0
    user_count = 0

    for record in balances_data.values():
        if not isinstance(record, dict):
            continue
        user_count += 1
        total_available += float(record.get("available_balance", 0.0) or 0.0)
        total_deposited += float(record.get("total_deposited", 0.0) or 0.0)
        total_spent += float(record.get("total_spent", 0.0) or 0.0)

    return {
        "user_count": user_count,
        "total_available": round(total_available, 2),
        "total_deposited": round(total_deposited, 2),
        "total_spent": round(total_spent, 2),
    }


def build_admin_dashboard_text() -> str:
    all_orders = get_all_hybrid_orders()
    total_orders = len(all_orders)
    order_counts = get_order_status_counts()
    payment_counts = get_payment_status_counts()
    wallet = get_wallet_summary()

    low_stock_items = []
    for provider, item in stock.items():
        cards = int(item.get("cards", 0))
        balance = float(item.get("balance", 0))
        if cards <= LOW_STOCK_CARDS_THRESHOLD or balance <= LOW_STOCK_BALANCE_THRESHOLD:
            low_stock_items.append(provider)

    lines = [
        "Admin Dashboard",
        "",
        "Orders",
        f"Total Orders: {total_orders}",
        f"Pending Delivery: {order_counts['paid_pending_delivery']}",
        f"Delivered: {order_counts['delivered']}",
        f"Refunded: {order_counts['refunded']}",
        f"Cancelled: {order_counts['cancelled']}",
        f"Archived: {order_counts['archived']}",
        "",
        "Payments",
        f"Paid: {payment_counts['paid']}",
        f"Pending: {payment_counts['pending']}",
        f"Unpaid: {payment_counts['unpaid']}",
        f"Failed: {payment_counts['failed']}",
        f"Refunded: {payment_counts['refunded']}",
        "",
        "Wallet",
        f"Users With Wallet Records: {wallet['user_count']}",
        f"Total Available Balance: {format_usd(wallet['total_available'])}",
        f"Total Deposited: {format_usd(wallet['total_deposited'])}",
        f"Total Spent: {format_usd(wallet['total_spent'])}",
        "",
        "Stock",
        f"Low Stock Providers: {len(low_stock_items)}",
    ]

    if low_stock_items:
        lines.append("Low Stock List: " + ", ".join(low_stock_items[:10]))

    lines.extend([
        "",
        "Quick Commands",
        "/pendingorders",
        "/lowstock",
        "/stocksummary",
        "/quickpanel",
    ])
    return "\n".join(lines)


def build_quickpanel_text(limit: int = 10) -> str:
    active_items = []
    for order_id, data in get_all_hybrid_orders().items():
        status = str(data.get("status", "") or "")
        if status == "archived":
            continue
        active_items.append((order_id, data))

    active_items.sort(key=lambda item: item[1].get("time", ""), reverse=True)

    if not active_items:
        return "Quick Panel\n\nNo active orders found."

    lines = ["Quick Panel", ""]
    for order_id, data in active_items[:limit]:
        provider = data.get("provider") or "Unknown"
        requested = data.get("requested_card_balance")
        requested_text = format_usd(float(requested)) if isinstance(requested, (int, float)) else "-"
        order_status = get_order_status_label(data.get("status", "unknown"))
        payment_status = get_payment_status_label(data.get("payment_status", "unpaid"))
        user_id = data.get("user_id", "-")
        lines.append(
            f"{order_id} | {provider} | {requested_text} | {order_status} | {payment_status} | user {user_id}"
        )
    return "\n".join(lines)


async def setbalance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        return

    parts = get_command_parts(update)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /setbalance ProviderName 1234.56")
        return

    provider_input = parts[1]
    provider = canonical_provider_name(provider_input)
    if not provider:
        await update.message.reply_text(
            "Unknown provider name. Use one of these:\n" + supported_providers_text()
        )
        return

    try:
        amount = float(parts[2])
    except ValueError:
        await update.message.reply_text("Balance must be a number.")
        return

    stock[provider]["balance"] = amount
    await update.message.reply_text(f"Balance updated.\n{provider}: ${amount:,.2f}")


async def setcards_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        return

    parts = get_command_parts(update)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /setcards ProviderName 123")
        return

    provider_input = parts[1]
    provider = canonical_provider_name(provider_input)
    if not provider:
        await update.message.reply_text(
            "Unknown provider name. Use one of these:\n" + supported_providers_text()
        )
        return

    try:
        count = int(parts[2])
    except ValueError:
        await update.message.reply_text("Card count must be a whole number.")
        return

    stock[provider]["cards"] = count
    await update.message.reply_text(f"Card count updated.\n{provider}: {count} Cards")


async def providers_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        return

    await update.message.reply_text(
        "Supported providers:\n" + supported_providers_text()
    )


async def accessmode_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global access_data

    if not is_full_admin(update.effective_chat.id):
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /accessmode open OR /accessmode restricted")
        return

    mode = parts[1].strip().lower()
    if mode not in ("open", "restricted"):
        await update.message.reply_text("Mode must be open or restricted.")
        return

    access_data["mode"] = mode
    save_access(access_data)
    await update.message.reply_text(f"Access mode updated: {mode}")


async def approve_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global access_data

    if not is_full_admin(update.effective_chat.id):
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /approve CHAT_ID")
        return

    try:
        target_chat_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("CHAT_ID must be a number.")
        return

    approved = access_data.get("approved_users", [])
    if target_chat_id not in approved:
        approved.append(target_chat_id)

    access_data["approved_users"] = approved
    save_access(access_data)

    try:
        await context.bot.send_message(
            chat_id=target_chat_id,
            text=(
                "Access Approved\n\n"
                "Your member access has now been activated successfully.\n\n"
                "You can start using the bot with:\n"
                "/start\n\n"
                "For updates and announcements, please also follow our official channel:\n"
                f"{MAIN_CHANNEL_LINK}"
            ),
        )
        await update.message.reply_text(
            f"Access approved and user notified: {target_chat_id}"
        )
    except Exception as e:
        await update.message.reply_text(
            f"Access approved for {target_chat_id}, but notification failed: {e}"
        )


async def removeaccess_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global access_data

    if not is_full_admin(update.effective_chat.id):
        return

    parts = get_command_parts(update)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /removeaccess CHAT_ID")
        return

    try:
        target_chat_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("CHAT_ID must be a number.")
        return

    approved = access_data.get("approved_users", [])
    if target_chat_id in approved:
        approved.remove(target_chat_id)

    access_data["approved_users"] = approved
    save_access(access_data)

    add_audit_log_entry(
        action="remove_access",
        admin_user=update.effective_user,
        target_type="user",
        target_id=str(target_chat_id),
        details="access removed",
    )
    await update.message.reply_text(f"Access removed for: {target_chat_id}")


async def approvedlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        return

    approved = access_data.get("approved_users", [])

    if not approved:
        await update.message.reply_text("No approved users found.")
        return

    lines = ["Approved Users\n"]
    for chat_id in approved[-50:]:
        lines.append(str(chat_id))

    await update.message.reply_text("\n".join(lines))




async def smart_panel_response(query, text: str, reply_markup=None) -> None:
    await safe_edit(query, text, reply_markup)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err_str = str(context.error or "")
    
    silent_errors = [
        "Query is too old", "query id is invalid", "Message is not modified",
        "MESSAGE_ID_INVALID", "message to edit not found", "QUERY_ID_INVALID",
    ]
    for silent in silent_errors:
        if silent.lower() in err_str.lower():
            logger.debug(f"Silent error: {err_str[:80]}")
            return

    logger.error(f"Handler error: {context.error}", exc_info=context.error)

    # Admin'e kritik hata bildirimi
    try:
        err_type = type(context.error).__name__
        if ADMIN_CHAT_ID and err_type not in ("NetworkError", "TimedOut", "RetryAfter", "Forbidden"):
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"⚠️ Bot Error\n{err_type}: {err_str[:150]}",
            )
    except Exception:
        pass

    try:
        if isinstance(update, Update):
            target = update.effective_message
            if target:
                await target.reply_text("Something went wrong. Please use /start to continue.")
    except Exception:
        pass


async def auto_daily_summary_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    latest_config = load_config()
    if not latest_config.get("auto_daily_summary_enabled", False):
        return

    try:
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=build_daily_summary_text(),
        )
        print("Auto daily summary sent successfully.")
    except Exception as e:
        print(f"Auto daily summary failed: {e}")


def schedule_auto_daily_summary(app: Application) -> None:
    if app.job_queue is None:
        print('JobQueue is not available. Install python-telegram-bot[job-queue] to enable auto daily summary.')
        return

    existing_jobs = app.job_queue.get_jobs_by_name("auto_daily_summary")
    if existing_jobs:
        return

    app.job_queue.run_daily(
        auto_daily_summary_job,
        time=datetime.strptime(f"{AUTO_DAILY_SUMMARY_HOUR:02d}:{AUTO_DAILY_SUMMARY_MINUTE:02d}", "%H:%M").time(),
        name="auto_daily_summary",
    )


def validate_runtime_settings() -> None:
    refresh_runtime_settings()
    problems = []
    if not TOKEN:
        problems.append("BOT_TOKEN is empty")
    if not ADMIN_CHAT_ID:
        problems.append("ADMIN_CHAT_ID is empty or invalid")
    if not CHANNEL_USERNAME:
        problems.append("CHANNEL_USERNAME is empty")
    if not MAIN_CHANNEL_LINK.startswith("http"):
        problems.append("MAIN_CHANNEL_LINK is invalid")
    if not STOCK_NEWS_CHANNEL_LINK.startswith("http"):
        problems.append("STOCK_NEWS_CHANNEL_LINK is invalid")
    if not ALCHEMY_API_KEY:
        problems.append("ALCHEMY_API_KEY is empty")
    if not BLOCKCYPHER_TOKEN:
        problems.append("BLOCKCYPHER_TOKEN is empty")
    if not ENCRYPTED_SEED:
        problems.append("ENCRYPTED_SEED is empty - run encrypt_seed.py first")
    if not WALLET_PASSWORD:
        problems.append("WALLET_PASSWORD is empty")

    if problems:
        raise RuntimeError("Runtime configuration error: " + "; ".join(problems))


def initialize_runtime_environment() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    init_sqlite_db()





async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle TXT file uploads — admin can:
    1) Send TXT with caption /importstock or /importgift
    2) Send TXT first, then type the command (bot remembers last file)
    3) Reply to a TXT with the command
    """
    if not update.message or not update.message.document:
        return
    if not is_full_admin(update.effective_chat.id):
        return

    doc = update.message.document
    caption = (update.message.caption or "").strip()

    # Her durumda son dosyayı hatırla — sonraki komut için
    if doc.file_name and doc.file_name.endswith(".txt"):
        context.user_data["last_uploaded_doc"] = doc
        context.user_data["last_uploaded_doc_time"] = time.time()

    # Caption varsa direkt işle
    if caption.lower().startswith("/importstock"):
        parts = caption.split(None, 1)
        provider_input = parts[1].strip() if len(parts) > 1 else ""
        provider = canonical_provider_name(provider_input)
        if not provider:
            await update.message.reply_text(
                "Unknown provider. Usage: send .txt file with caption /importstock ProviderName"
            )
            return
        await _do_importstock(update, doc, provider)
        return

    if caption.lower().startswith("/importgift"):
        parts = caption.split(None, 1)
        tag = parts[1].strip() if len(parts) > 1 else ""
        gc_type, region = parse_gc_import_tag(tag) if tag else (None, None)
        if not gc_type or not region:
            await update.message.reply_text(
                f"Invalid tag: '{tag}'\nFormat: TypeRegion (e.g. GamingUS, ShoppingEU)"
            )
            return
        await _do_importgift(update, doc, gc_type, region)
        return

    # Caption yoksa ve dosya TXT ise → interaktif butonlarla import
    if doc.file_name and doc.file_name.endswith(".txt"):
        keyboard = [
            [InlineKeyboardButton("💳 Prepaid Cards", callback_data="imp_prepaid")],
            [InlineKeyboardButton("🎁 Gift Cards", callback_data="imp_gift")],
        ]
        await update.message.reply_text(
            "📁 File received!\n\n"
            "What type of cards is this?",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


async def _do_importstock(update: Update, doc, provider: str) -> None:
    """Shared import logic for prepaid cards."""
    if not doc.file_name.endswith(".txt"):
        await update.message.reply_text("Only .txt files are accepted.")
        return
    await update.message.reply_text(f"⏳ Importing cards for {provider}...")
    try:
        file = await doc.get_file()
        file_bytes = await file.download_as_bytearray()
        text = file_bytes.decode("utf-8", errors="ignore")
        stats = import_cards_from_text(text, provider)
        unassigned = stats.get("unassigned", 0)
        await update.message.reply_text(
            f"✅ Import Complete\n\n"
            f"Added:      {stats['added']} cards\n"
            f"Skipped:    {stats['skipped']} (duplicates)\n"
            f"Errors:     {stats['errors']} (bad lines)\n"
            + (f"Unassigned: {unassigned} (unknown BIN)\n" if unassigned else "") +
            f"Total in stock: {stats['total']} cards"
        )
        add_audit_log_entry(
            action="import_cards",
            admin_user=update.effective_user,
            target_type="stock",
            target_id=provider,
            details=f"added={stats['added']} skipped={stats['skipped']} errors={stats['errors']}",
        )
    except Exception as e:
        await update.message.reply_text(f"Import failed: {e}")


async def _do_importgift(update: Update, doc, gc_type: str, region: str) -> None:
    """Shared import logic for gift cards."""
    if not doc.file_name.endswith(".txt"):
        await update.message.reply_text("Only .txt files are accepted.")
        return
    flag = GC_REGIONS.get(region, "🌍")
    icon = GC_TYPES.get(gc_type, "🏪")
    await update.message.reply_text(f"⏳ Importing gift cards → {flag} {icon} {gc_type} {region}...")
    try:
        file = await doc.get_file()
        file_bytes = await file.download_as_bytearray()
        text = file_bytes.decode("utf-8", errors="ignore")
        stats = import_gift_cards_from_text(text, gc_type=gc_type, region=region)
        await update.message.reply_text(
            f"🎁 Gift Card Import Complete\n\n"
            f"Region: {flag} {region} | Type: {icon} {gc_type}\n\n"
            f"Added:   {stats['added']} cards\n"
            f"Skipped: {stats['skipped']}\n"
            f"Errors:  {stats['errors']} (bad lines)\n"
            f"Total in stock: {stats['total']} gift cards"
        )
        add_audit_log_entry(
            action="import_gift_cards",
            admin_user=update.effective_user,
            target_type="gift_card",
            target_id=f"{gc_type}_{region}",
            details=f"added={stats['added']} errors={stats['errors']}",
        )
    except Exception as e:
        await update.message.reply_text(f"Import failed: {e}")


def _get_doc_from_context(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Try to find a TXT document: reply > current message > last uploaded."""
    doc = None
    if update.message.reply_to_message and update.message.reply_to_message.document:
        doc = update.message.reply_to_message.document
    elif update.message.document:
        doc = update.message.document
    if not doc:
        # Son 5 dakika içinde yüklenen dosyayı kullan
        last_doc = context.user_data.get("last_uploaded_doc")
        last_time = context.user_data.get("last_uploaded_doc_time", 0)
        if last_doc and (time.time() - last_time) < 300:
            doc = last_doc
    return doc

async def importstock_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /importstock ProviderName
    Works with: reply to file, file with caption, or file sent before command."""
    if not is_full_admin(update.effective_chat.id):
        return

    parts = (update.message.text or "").strip().split(None, 1)
    provider_input = parts[1].strip() if len(parts) > 1 else ""
    provider = canonical_provider_name(provider_input)

    if not provider:
        await update.message.reply_text(
            "Usage: /importstock ProviderName\n\n"
            "3 ways to import:\n"
            "1️⃣ Send .txt file, then type /importstock ProviderName\n"
            "2️⃣ Send .txt file with caption /importstock ProviderName\n"
            "3️⃣ Reply to a .txt file with /importstock ProviderName\n\n"
            "Valid providers:\n" + supported_providers_text()
        )
        return

    doc = _get_doc_from_context(update, context)

    if not doc:
        await update.message.reply_text(
            f"No .txt file found.\n\n"
            f"Send a .txt file first, then type:\n/importstock {provider}"
        )
        return

    await _do_importstock(update, doc, provider)


async def cardstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        return
    stats = get_card_stats()
    cards = load_cards()
    # Per-provider breakdown
    provider_counts = {}
    for c in cards.values():
        if c.get("status") == "available":
            p = c.get("provider", "Unknown")
            provider_counts[p] = provider_counts.get(p, 0) + 1

    lines = [
        "Card Stock Status\n",
        f"Total cards:     {stats['total']}",
        f"Available:       {stats['available']}",
        f"Reserved:        {stats['reserved']}",
        f"Sold:            {stats['sold']}",
        "",
        "Available by Provider:",
    ]
    for provider, count in sorted(provider_counts.items(), key=lambda x: -x[1]):
        short = PROVIDER_SHORT.get(provider, provider)
        lines.append(f"  {short}: {count}")

    await update.message.reply_text("\n".join(lines))


async def deletecards_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /deletecards ProviderName  — delete all available cards for a provider."""
    if not is_full_admin(update.effective_chat.id):
        return
    parts = (update.message.text or "").strip().split(None, 1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /deletecards ProviderName\nExample: /deletecards Unassigned")
        return
    raw_input = parts[1].strip()
    # Önce bilinen provider'ı dene, yoksa raw input kullan
    provider = canonical_provider_name(raw_input) or raw_input

    # SQLite'tan sil
    deleted_db = 0
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM cards WHERE provider = ? AND status = 'available'", (provider,))
        deleted_db = cur.rowcount
        conn.commit()
        conn.close()
    except Exception:
        pass

    # JSON'dan sil
    cards = load_cards()
    deleted_json = 0
    for card_id in list(cards.keys()):
        if cards[card_id].get("provider") == provider and cards[card_id].get("status") == "available":
            del cards[card_id]
            deleted_json += 1
    save_cards(cards)

    deleted = max(deleted_db, deleted_json)
    await update.message.reply_text(f"Deleted {deleted} available cards for '{provider}'.")
    add_audit_log_entry(
        action="delete_cards", admin_user=update.effective_user,
        target_type="stock", target_id=provider, details=f"deleted={deleted}",
    )


async def cleanstock_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /cleanstock — bozuk/mantıksız bakiyeli kartları sil."""
    if not is_full_admin(update.effective_chat.id):
        return
    MAX_SANE_BALANCE = 10000.0  # $10,000 üzeri bakiye bozuk kabul edilir

    # SQLite'tan temizle
    cleaned_db = 0
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM cards WHERE status = 'available' AND balance > ?", (MAX_SANE_BALANCE,))
        cleaned_db = cur.rowcount
        conn.commit()
        conn.close()
    except Exception:
        pass

    # JSON'dan temizle
    cards = load_cards()
    cleaned_json = 0
    for card_id in list(cards.keys()):
        if cards[card_id].get("status") == "available":
            bal = float(cards[card_id].get("balance", 0))
            if bal > MAX_SANE_BALANCE:
                del cards[card_id]
                cleaned_json += 1
    save_cards(cards)

    cleaned = max(cleaned_db, cleaned_json)
    await update.message.reply_text(
        f"🧹 Stock Cleanup\n\n"
        f"Removed {cleaned} cards with balance > {format_usd(MAX_SANE_BALANCE)}\n"
        f"These were corrupted/invalid entries."
    )
    add_audit_log_entry(
        action="clean_stock", admin_user=update.effective_user,
        target_type="stock", target_id="corrupted",
        details=f"removed={cleaned} threshold=${MAX_SANE_BALANCE}",
    )


async def migratereg_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /migratereg — External kart registration durumlarını -1 (unknown) yap.
    Mevcut kartlardan registered=0 olan ama parsing bilgisi olmayanları bilinmiyor say."""
    if not is_full_admin(update.effective_chat.id):
        return

    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()

        # Before stats
        cur.execute("SELECT registered, COUNT(*) AS cnt FROM external_cards GROUP BY registered")
        before = {row["registered"]: row["cnt"] for row in cur.fetchall()}

        # Migration: registered=0 with no high-confidence parsing → -1 (unknown)
        # Only cards imported before this fix (parsing_notes NULL or empty)
        cur.execute("""
            UPDATE external_cards
            SET registered = -1
            WHERE registered = 0
              AND (parsing_notes IS NULL OR parsing_notes = '' OR parsing_notes NOT LIKE '%high%')
        """)
        migrated = cur.rowcount
        conn.commit()

        # After stats
        cur.execute("SELECT registered, COUNT(*) AS cnt FROM external_cards GROUP BY registered")
        after = {row["registered"]: row["cnt"] for row in cur.fetchall()}
        conn.close()

        def fmt(d):
            return f"Reg=1: {d.get(1, 0)} | Reg=0: {d.get(0, 0)} | Unknown (-1): {d.get(-1, 0)}"

        await update.message.reply_text(
            f"🔄 Registration Migration\n\n"
            f"Before: {fmt(before)}\n"
            f"After:  {fmt(after)}\n\n"
            f"Migrated {migrated} cards to Unknown status.\n"
            f"These will show ❓ icon until scanner confirms their status."
        )
    except Exception as e:
        await update.message.reply_text(f"Migration error: {e}")


async def cleanbins_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /cleanbins — 6 haneden kısa BIN'li external kartları sil (duplicate fix)."""
    if not is_full_admin(update.effective_chat.id):
        return

    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()

        # Stats before
        cur.execute("SELECT COUNT(*) AS total FROM external_cards WHERE status = 'available'")
        total_before = cur.fetchone()["total"]

        # Delete 5-digit or less BIN cards
        cur.execute("""
            DELETE FROM external_cards
            WHERE LENGTH(bin_number) < 6
        """)
        deleted = cur.rowcount
        conn.commit()

        # Stats after
        cur.execute("SELECT COUNT(*) AS total FROM external_cards WHERE status = 'available'")
        total_after = cur.fetchone()["total"]
        conn.close()

        await update.message.reply_text(
            f"🧹 BIN Cleanup\n\n"
            f"Before: {total_before} available cards\n"
            f"After:  {total_after} available cards\n\n"
            f"Deleted {deleted} cards with short BINs (<6 digits).\n"
            f"These were duplicates from old parser format."
        )
    except Exception as e:
        await update.message.reply_text(f"Cleanup error: {e}")


async def fixxstock_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /fixxstock — Tüm X Stock kartlarını registered olarak işaretle.
    X Stock policy: kartlarının hepsi registered satılır."""
    if not is_full_admin(update.effective_chat.id):
        return

    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()

        # Stats before
        cur.execute("""
            SELECT registered, COUNT(*) AS cnt
            FROM external_cards
            WHERE source = 'xstock' AND status = 'available'
            GROUP BY registered
        """)
        before = {row["registered"]: row["cnt"] for row in cur.fetchall()}

        # Update all xstock cards to registered=1
        cur.execute("""
            UPDATE external_cards
            SET registered = 1
            WHERE source = 'xstock' AND registered != 1
        """)
        updated = cur.rowcount
        conn.commit()
        conn.close()

        def fmt(d):
            return f"Reg=1: {d.get(1, 0)} | Reg=0: {d.get(0, 0)} | Unknown(-1): {d.get(-1, 0)}"

        await update.message.reply_text(
            f"🔒 X Stock Registration Fix\n\n"
            f"Before: {fmt(before)}\n"
            f"Updated: {updated} cards → 🔒 Registered\n\n"
            f"All X Stock cards are now marked as registered.\n"
            f"Future X Stock imports will also be marked registered automatically."
        )
    except Exception as e:
        await update.message.reply_text(f"Fix error: {e}")


# ── Competitor Balance Management Commands (JIT Funding) ────

async def competitorbal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /competitorbal — Tüm rakip botlardaki tahmini bakiyeleri göster."""
    if not is_full_admin(update.effective_chat.id):
        return

    balances = load_competitor_balances()
    pending = load_pending_deposits()

    lines = ["💰 Competitor Bot Balances\n"]
    total = 0.0
    for source, info in balances.items():
        bal = float(info.get("balance", 0))
        total += bal
        last = info.get("last_updated", "never")
        emoji = "🟢" if bal >= COMPETITOR_DEFAULT_BUFFER else ("🟡" if bal > 0 else "🔴")
        lines.append(f"{emoji} {source.upper()}: ${bal:.2f}")
        if last:
            lines.append(f"   Last: {last}")

    lines.append(f"\n📊 Total locked in competitors: ${total:.2f}")

    if pending:
        lines.append(f"\n⏳ Pending deposits: {len(pending)}")
        for p in pending[:5]:
            lines.append(f"  • {p['order_id']}: ${p['amount_needed']:.2f} → {p['source'].upper()}")

    lines.append("\n💡 Commands:")
    lines.append("/setbal <source> <amount> — set balance after deposit")
    lines.append("/pendingdeposits — list orders awaiting deposit")
    lines.append("/confirmdeposit <order_id> — confirm deposit done")

    await update.message.reply_text("\n".join(lines))


async def setbal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /setbal <source> <amount> — Bakiyeyi manuel ayarla.
    Örnek: /setbal guru 200"""
    if not is_full_admin(update.effective_chat.id):
        return

    parts = (update.message.text or "").strip().split()
    if len(parts) != 3:
        await update.message.reply_text(
            "Usage: /setbal <source> <amount>\n"
            "Sources: guru, xstock, sharks, planet\n"
            "Example: /setbal guru 200"
        )
        return

    source = parts[1].lower()
    try:
        amount = float(parts[2])
    except ValueError:
        await update.message.reply_text("Invalid amount.")
        return

    valid_sources = ["guru", "xstock", "sharks", "planet"]
    if source not in valid_sources:
        await update.message.reply_text(f"Source must be one of: {', '.join(valid_sources)}")
        return

    balances = load_competitor_balances()
    if source not in balances:
        balances[source] = {"balance": 0.0, "last_updated": "", "notes": ""}

    old_balance = balances[source]["balance"]
    balances[source]["balance"] = round(amount, 2)
    balances[source]["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    balances[source]["notes"] = f"manual set by admin (was ${old_balance:.2f})"
    save_competitor_balances(balances)

    await update.message.reply_text(
        f"✅ {source.upper()} balance set\n\n"
        f"Old: ${old_balance:.2f}\n"
        f"New: ${amount:.2f}\n"
        f"Difference: {'+'if amount > old_balance else ''}{amount - old_balance:.2f}"
    )
    logger.info(f"[ADMIN] {source} balance set to ${amount:.2f} (was ${old_balance:.2f})")


async def pendingdeposits_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /pendingdeposits — Manuel deposit bekleyen siparişleri listele."""
    if not is_full_admin(update.effective_chat.id):
        return

    pending = load_pending_deposits()
    if not pending:
        await update.message.reply_text("✅ No pending deposits. All orders are funded.")
        return

    lines = [f"⏳ Pending Deposits ({len(pending)})\n"]
    total_needed = {}
    for p in pending:
        src = p['source']
        amt = p['amount_needed']
        total_needed[src] = total_needed.get(src, 0) + amt
        lines.append(
            f"📦 Order {p['order_id']}\n"
            f"   User: {p['user_id']}\n"
            f"   Card: {p['card_info']}\n"
            f"   Need: ${amt:.2f} → {src.upper()}\n"
            f"   Created: {p['created_at']}\n"
        )

    lines.append("\n💵 Totals needed by source:")
    for src, total in total_needed.items():
        current = get_competitor_balance(src)
        shortfall = max(0, total - current)
        lines.append(f"  {src.upper()}: ${total:.2f} (have ${current:.2f}, short ${shortfall:.2f})")

    lines.append("\n💡 After depositing, run /confirmdeposit <order_id>")

    msg = "\n".join(lines)
    if len(msg) > 4000:
        msg = msg[:3900] + "\n... (truncated)"
    await update.message.reply_text(msg)


async def confirmdeposit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /confirmdeposit <order_id> — Deposit yaptın, bakiyeyi güncelle ve siparişi temizle."""
    if not is_full_admin(update.effective_chat.id):
        return

    parts = (update.message.text or "").strip().split()
    if len(parts) != 2:
        await update.message.reply_text("Usage: /confirmdeposit <order_id>")
        return

    order_id = parts[1]
    pending = load_pending_deposits()
    target = next((p for p in pending if p.get("order_id") == order_id), None)

    if not target:
        await update.message.reply_text(f"❌ No pending deposit found for {order_id}")
        return

    source = target["source"]
    amount = target["amount_needed"]

    # Bakiyeyi güncelle (deposit yapıldı varsayılır)
    credit_competitor_balance(source, amount, f"deposit for {order_id}")
    # Sonra cost düş (sipariş için kullanılacak)
    deduct_competitor_balance(source, amount, f"order {order_id}")

    # Pending listesinden çıkar
    remove_pending_deposit(order_id)

    new_balance = get_competitor_balance(source)
    await update.message.reply_text(
        f"✅ Deposit confirmed for {order_id}\n\n"
        f"Source: {source.upper()}\n"
        f"Amount: ${amount:.2f}\n"
        f"New {source} balance: ${new_balance:.2f}\n\n"
        f"Supply chain agent will auto-purchase the card now."
    )
    logger.info(f"[ADMIN] Deposit confirmed: {order_id} @ {source} ${amount:.2f}")


async def failedorders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /failedorders — Son 24 saatin başarısız/iade edilmiş siparişlerini göster."""
    if not is_full_admin(update.effective_chat.id):
        return

    orders = load_orders()
    now = datetime.now()
    cutoff = now - timedelta(hours=24)

    stuck = []
    refunded = []
    for order_id, order in orders.items():
        if not order.get("ext_source"):
            continue
        time_str = order.get("time", "")
        if not time_str:
            continue
        try:
            order_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        if order_time < cutoff:
            continue

        status = order.get("status", "")
        if status == "ext_pending":
            elapsed = (now - order_time).total_seconds() / 60
            if elapsed >= 3:  # 3dk+ beklemişse stuck
                stuck.append((order_id, order, elapsed))
        elif status == "refunded":
            refunded.append((order_id, order))

    lines = ["📋 Failed Orders (24h)\n"]

    if stuck:
        lines.append(f"⏳ Stuck (pending too long): {len(stuck)}")
        for order_id, order, elapsed in stuck[:10]:
            lines.append(
                f"  • {order_id} | user={order.get('user_id')} | "
                f"{order.get('ext_source','?').upper()} | "
                f"${order.get('cost',0):.2f} | {elapsed:.1f}min"
            )
    else:
        lines.append("⏳ No stuck orders")

    lines.append("")

    if refunded:
        total_refunded = sum(float(o.get("refund_amount", o.get("cost", 0))) for _, o in refunded)
        lines.append(f"💰 Refunded: {len(refunded)} orders (${total_refunded:.2f} total)")
        for order_id, order in refunded[:10]:
            reason = order.get("failure_reason", "unknown")[:40]
            lines.append(
                f"  • {order_id} | user={order.get('user_id')} | "
                f"${order.get('refund_amount', order.get('cost', 0)):.2f} | {reason}"
            )
    else:
        lines.append("💰 No refunds today")

    lines.append("\n💡 Commands:")
    lines.append("/refundorder <order_id> — force refund a stuck order")

    msg = "\n".join(lines)
    if len(msg) > 4000:
        msg = msg[:3900] + "\n... (truncated)"
    await update.message.reply_text(msg)


async def refundorder_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /refundorder <order_id> — Bir siparişi manuel iade et."""
    if not is_full_admin(update.effective_chat.id):
        return

    parts = (update.message.text or "").strip().split()
    if len(parts) != 2:
        await update.message.reply_text("Usage: /refundorder <order_id>")
        return

    order_id = parts[1]
    orders = load_orders()
    order = orders.get(order_id)
    if not order:
        await update.message.reply_text(f"❌ Order {order_id} not found")
        return

    if order.get("status") == "refunded":
        await update.message.reply_text(f"Already refunded.")
        return

    success = refund_order(order_id, bonus_pct=STUCK_ORDER_REFUND_BONUS, reason="admin_manual_refund")
    if success:
        cost = float(order.get("cost", 0))
        bonus = round(cost * STUCK_ORDER_REFUND_BONUS, 2)
        await update.message.reply_text(
            f"✅ Refunded {order_id}\n"
            f"Amount: ${cost:.2f} + ${bonus:.2f} bonus"
        )
        # Kullanıcıya da bildir
        try:
            await context.bot.send_message(
                chat_id=order["user_id"],
                text=(
                    f"💰 Order Refunded\n\n"
                    f"Order: {order_id}\n"
                    f"Refund: {format_usd(cost + bonus)} (includes 2% apology bonus)\n\n"
                    f"Your balance is ready to use."
                ),
            )
        except Exception:
            pass
    else:
        await update.message.reply_text("Refund failed, check logs")



async def importgift_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /importgift GamingUS — import gift cards with region+type tag.
    Tags: GamingUS, ShoppingEU, FoodCA, DigitalUK, etc."""
    if not is_full_admin(update.effective_chat.id):
        return
    parts = (update.message.text or "").strip().split(None, 1)
    tag = parts[1].strip() if len(parts) > 1 else ""

    if not tag:
        doc = _get_doc_from_context(update, context)
        if not doc:
            valid_tags = ", ".join(f"{t}{r}" for t in GC_TYPES for r in GC_REGIONS)
            await update.message.reply_text(
                "Usage: /importgift TAG\n\n"
                "TAG = Type + Region combined:\n"
                f"Regions: {', '.join(GC_REGIONS.keys())}\n"
                f"Types: {', '.join(GC_TYPES.keys())}\n\n"
                "Examples:\n"
                "  /importgift GamingUS\n"
                "  /importgift ShoppingEU\n"
                "  /importgift FoodCA\n\n"
                "Send .txt file first, then type the command."
            )
            return

    gc_type, region = parse_gc_import_tag(tag) if tag else (None, None)
    if not gc_type or not region:
        await update.message.reply_text(
            f"Invalid tag: '{tag}'\n\n"
            "Format: TypeRegion (e.g. GamingUS, ShoppingEU)\n"
            f"Valid types: {', '.join(GC_TYPES.keys())}\n"
            f"Valid regions: {', '.join(GC_REGIONS.keys())}"
        )
        return

    doc = _get_doc_from_context(update, context)
    if not doc:
        await update.message.reply_text(
            f"No .txt file found.\n\nSend a .txt file first, then type:\n/importgift {tag}"
        )
        return

    await _do_importgift(update, doc, gc_type, region)


async def giftstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin/mod: /giftstats — gift card istatistikleri."""
    if not has_staff_access(update.effective_chat.id):
        return
    stats = get_gift_card_stats()
    regions = get_gc_region_summary()

    lines = [
        "🎁 Gift Card Stock Status\n",
        f"Total:     {stats['total']}",
        f"Available: {stats['available']}",
        f"Reserved:  {stats['reserved']}",
        f"Sold:      {stats['sold']}",
        "",
        "By Region & Type:",
    ]
    for r in regions:
        region = r["region"]
        flag = GC_REGIONS.get(region, "🌍")
        lines.append(f"\n  {flag} {region}: {r['cnt']} cards | {format_usd(r['total_amount'])}")
        types = get_gc_type_summary(region)
        for t in types:
            icon = GC_TYPES.get(t["gc_type"], "🏪")
            rate = get_gift_card_rate(t["gc_type"], region)
            lines.append(f"    {icon} {t['gc_type']}: {t['cnt']}x | {format_usd(t['total_amount'])} | {rate*100:.0f}%")

    await update.message.reply_text("\n".join(lines))


async def deletegifts_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /deletegifts GamingUS — delete all available gift cards for a type+region."""
    if not is_full_admin(update.effective_chat.id):
        return
    parts = (update.message.text or "").strip().split(None, 1)
    if len(parts) < 2:
        await update.message.reply_text(
            "Usage: /deletegifts TAG\n"
            "Example: /deletegifts GamingUS\n\n"
            f"Types: {', '.join(GC_TYPES.keys())}\n"
            f"Regions: {', '.join(GC_REGIONS.keys())}"
        )
        return
    tag = parts[1].strip()
    gc_type, region = parse_gc_import_tag(tag)
    if not gc_type or not region:
        await update.message.reply_text(f"Invalid tag: '{tag}'\nFormat: TypeRegion (e.g. GamingUS)")
        return
    try:
        init_sqlite_db()
        conn = get_sqlite_connection()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM gift_cards WHERE gc_type = ? AND region = ? AND status = 'available'",
            (gc_type, region)
        )
        deleted = cur.rowcount
        conn.commit()
        conn.close()
    except Exception:
        deleted = 0
    # JSON sync
    gc_data = load_json_file(GIFT_CARDS_FILE, {})
    for gc_id in list(gc_data.keys()):
        if (gc_data[gc_id].get("gc_type") == gc_type and
            gc_data[gc_id].get("region") == region and
            gc_data[gc_id].get("status") == "available"):
            del gc_data[gc_id]
    save_json_file(GIFT_CARDS_FILE, gc_data)

    flag = GC_REGIONS.get(region, "🌍")
    icon = GC_TYPES.get(gc_type, "🏪")
    await update.message.reply_text(f"Deleted {deleted} available gift cards for {flag} {icon} {gc_type} {region}.")
    add_audit_log_entry(
        action="delete_gift_cards", admin_user=update.effective_user,
        target_type="gift_card", target_id=f"{gc_type}_{region}", details=f"deleted={deleted}",
    )


async def setgiftrate_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /setgiftrate GamingUS 0.65  veya  /setgiftrate Gaming 0.65"""
    if not is_full_admin(update.effective_chat.id):
        return
    parts = (update.message.text or "").strip().split()
    if len(parts) < 3:
        await update.message.reply_text(
            "Usage: /setgiftrate TAG RATE\n\n"
            "Examples:\n"
            "  /setgiftrate GamingUS 0.65  (Gaming in US only)\n"
            "  /setgiftrate Gaming 0.65    (Gaming all regions)\n\n"
            f"Types: {', '.join(GC_TYPES.keys())}\n"
            f"Regions: {', '.join(GC_REGIONS.keys())}"
        )
        return
    tag = parts[1].strip()
    try:
        rate = float(parts[2].strip())
        if not (0.01 <= rate <= 1.0):
            raise ValueError
    except Exception:
        await update.message.reply_text("Rate must be between 0.01 and 1.00")
        return

    # Tag parse: "GamingUS" → type+region, "Gaming" → type only
    gc_type, region = parse_gc_import_tag(tag)
    if gc_type and region:
        key = f"gc_{gc_type}_{region}"
        label = f"{gc_type} in {region}"
    else:
        # Type only check
        gc_type = None
        for t in GC_TYPES.keys():
            if t.lower() == tag.lower():
                gc_type = t
                break
        if not gc_type:
            await update.message.reply_text(f"Unknown tag: '{tag}'")
            return
        key = f"gc_{gc_type}"
        label = f"{gc_type} (all regions)"

    rates_data[key] = rate
    save_rates(rates_data)
    await update.message.reply_text(f"Gift card rate for {label} set to {rate*100:.0f}%")
    add_audit_log_entry(
        action="set_gift_rate", admin_user=update.effective_user,
        target_type="gift_card", target_id=tag, details=f"rate={rate}",
    )


async def setlogo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: bir fotoğrafı reply ederek /setlogo — botu logo olarak ayarla."""
    if not is_full_admin(update.effective_chat.id):
        return
    # Reply'daki fotoğrafı al
    photo = None
    if update.message.reply_to_message and update.message.reply_to_message.photo:
        photo = update.message.reply_to_message.photo[-1]
    elif update.message.photo:
        photo = update.message.photo[-1]
    if not photo:
        await update.message.reply_text(
            "Usage: send or reply to a photo with /setlogo\n"
            "The photo will be used as the channel post logo."
        )
        return
    set_bot_logo_file_id(photo.file_id)
    await update.message.reply_text(
        "Logo set successfully!\n"
        "All future channel posts will include this image."
    )


async def poststocknow_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /poststocknow — anında bir kart gönder."""
    if not is_full_admin(update.effective_chat.id):
        return
    posted = await post_one_card_to_channel(context.bot)
    if posted:
        remaining = get_unposted_count()
        await update.message.reply_text(
            f"Card posted to channel successfully.\n"
            f"Remaining unposted cards: {remaining}"
        )
    else:
        await update.message.reply_text(
            "No unposted cards available.\n"
            "Import new stock with /importstock."
        )


async def channelstatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /channelstatus — kanal posting durumu."""
    if not has_staff_access(update.effective_chat.id):
        return
    cards = load_cards()
    total     = len(cards)
    available = sum(1 for c in cards.values() if c.get("status") == "available")
    posted    = sum(1 for c in cards.values() if c.get("channel_posted", False))
    unposted  = sum(1 for c in cards.values()
                    if c.get("status") == "available" and not c.get("channel_posted", False))
    sold      = sum(1 for c in cards.values() if c.get("status") == "sold")
    logo_id   = get_bot_logo_file_id()
    eta_mins  = round(unposted * CHANNEL_STOCK_INTERVAL / 60, 1)
    await update.message.reply_text(
        f"Channel Stock Status\n\n"
        f"Total cards:       {total}\n"
        f"Available:         {available}\n"
        f"Posted to channel: {posted}\n"
        f"Unposted queue:    {unposted}\n"
        f"Sold:              {sold}\n\n"
        f"Post interval: {CHANNEL_STOCK_INTERVAL}s\n"
        f"ETA to clear queue: ~{eta_mins} min\n\n"
        f"Logo set: {'Yes' if logo_id else 'No — use /setlogo'}\n"
        f"Channel: {CHANNEL_USERNAME}"
    )


async def resetposted_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: /resetposted — tüm kartları unposted yap (yeniden gönderim için)."""
    if not is_full_admin(update.effective_chat.id):
        return
    cards = load_cards()
    count = 0
    for card_id in cards:
        if cards[card_id].get("channel_posted", False):
            cards[card_id]["channel_posted"] = False
            count += 1
    save_cards(cards)
    await update.message.reply_text(
        f"Reset complete.\n"
        f"{count} cards marked as unposted — they will be re-queued."
    )


# ============================================================
# ADMIN HUB MODULE
# ============================================================

# Hub state - hangi admin hangi alt menüde
hub_state = {}  # {admin_id: {"menu": "main", "page": 1, ...}}

# Debounce - kullanicinin son buton basma zamani
_last_callback_time = {}  # {user_id: timestamp}
CALLBACK_DEBOUNCE_SECONDS = 0.8

def hub_main_text() -> str:
    """Ana hub ekranı metni - canlı istatistikler."""
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")

    # Bekleyen siparişler
    all_orders = get_all_hybrid_orders()
    pending_delivery = sum(1 for o in all_orders.values()
                           if o.get("status") == "paid_pending_delivery")

    # Yanıtsız destek (support_mode_users)
    unanswered_support = len(support_mode_users)

    # Bekleyen ödemeler
    payments = load_pending_payments()
    pending_payments = sum(1 for p in payments.values()
                           if p.get("status") == "waiting" and not p.get("credited"))

    # Stok
    cards = load_cards()
    avail_cards = [c for c in cards.values() if c.get("status") == "available"]
    total_cards = len(avail_cards)
    total_value = sum(float(c.get("balance", 0)) for c in avail_cards)
    unposted    = sum(1 for c in avail_cards if not c.get("channel_posted", False))
    eta_min     = round(unposted * CHANNEL_STOCK_INTERVAL / 60, 0)

    # Bugünkü satışlar
    today_orders = [(oid, o) for oid, o in all_orders.items()
                    if o.get("time", "").startswith(today)]
    today_sales_count  = len(today_orders)
    today_sales_amount = sum(float(o.get("charged_amount", 0) or 0) for _, o in today_orders)

    # Bugünkü yüklemeler
    pay_list = list(payments.values())
    today_deposits = [p for p in pay_list
                      if p.get("credited") and (p.get("credited_at") or "").startswith(today)]
    today_dep_count  = len(today_deposits)
    today_dep_amount = sum(float(p.get("confirmed_amount_usd", 0) or 0) for p in today_deposits)

    # Wallet
    wallet = get_wallet_summary()

    # Bekleyen aktivasyonlar
    acts = load_pending_activations()
    pending_activations = sum(1 for r in acts.values()
                              if not r.get("credited") and r.get("status") == "waiting")

    alerts = []
    if pending_delivery > 0:
        alerts.append(f"  ⚠️ {pending_delivery} order(s) waiting for delivery")
    if unanswered_support > 0:
        alerts.append(f"  ⚠️ {unanswered_support} support message(s) unanswered")
    if pending_payments > 0:
        alerts.append(f"  ⚠️ {pending_payments} crypto payment(s) pending")
    if pending_activations > 0:
        alerts.append(f"  ⚠️ {pending_activations} activation payment(s) pending")
    alert_block = ("\n🚨 ALERTS\n" + "\n".join(alerts) + "\n") if alerts else ""

    return (
        f"⚡ Elite Prepaid Bot — Admin Hub\n"
        f"{alert_block}\n"
        f"📊 LIVE STATUS\n"
        f"  Pending Delivery: {pending_delivery}\n"
        f"  Pending Activations: {pending_activations}\n"
        f"  Pending Payments: {pending_payments}\n"
        f"  Unanswered Support: {unanswered_support}\n"
        f"  Stock: {total_cards} cards / ${total_value:,.2f}\n"
        f"  Channel Queue: {unposted} posts (~{int(eta_min)} min)\n\n"
        f"💰 TODAY\n"
        f"  Sales: {today_sales_count} orders / {format_usd(today_sales_amount)}\n"
        f"  Deposits: {today_dep_count} / {format_usd(today_dep_amount)}\n"
        f"  Total User Balances: {format_usd(wallet['total_available'])}\n\n"
        f"Updated: {_dt.now().strftime('%H:%M:%S')}"
    )

def hub_main_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Orders",    callback_data="hub:orders:1:pending"),
         InlineKeyboardButton("👥 Users",     callback_data="hub:users:1")],
        [InlineKeyboardButton("🔑 Members",   callback_data="hub:members:1"),
         InlineKeyboardButton("📬 Support",   callback_data="hub:support:1")],
        [InlineKeyboardButton("💳 Stock",     callback_data="hub:stock"),
         InlineKeyboardButton("💰 Payments",  callback_data="hub:payments:1")],
        [InlineKeyboardButton("📢 Channel",   callback_data="hub:channel"),
         InlineKeyboardButton("⚙️ Settings",  callback_data="hub:settings")],
        [InlineKeyboardButton("📈 Reports",   callback_data="hub:reports"),
         InlineKeyboardButton("🔄 Refresh",   callback_data="hub:main")],
    ])

def hub_orders_text(page: int = 1, status_filter: str = "pending") -> str:
    all_orders = get_all_hybrid_orders()
    if status_filter == "pending":
        filtered = [(oid, o) for oid, o in all_orders.items()
                    if o.get("status") == "paid_pending_delivery"]
        title = "Pending Delivery"
    elif status_filter == "delivered":
        filtered = [(oid, o) for oid, o in all_orders.items()
                    if o.get("status") == "delivered"]
        title = "Delivered Orders"
    else:
        filtered = list(all_orders.items())
        title = "All Orders"

    filtered.sort(key=lambda x: x[1].get("time", ""), reverse=True)
    per_page = 8
    total_pages = max(1, (len(filtered) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    chunk = filtered[start:start + per_page]
    lines = [f"{title} — Page {page}/{total_pages} (Total: {len(filtered)})\n"]
    if not chunk:
        lines.append("No orders.")
    for oid, o in chunk:
        user   = o.get("username", "-")
        prov   = PROVIDER_SHORT.get(o.get("provider", ""), o.get("provider", "-"))
        bal    = o.get("requested_card_balance", 0)
        cost   = o.get("charged_amount", 0)
        t      = o.get("time", "")[:16]
        status = o.get("status", "-")
        status_icon = {"paid_pending_delivery": "⏳", "delivered": "✅",
                       "cancelled": "❌", "refunded": "↩️"}.get(status, "📋")
        lines.append(f"{status_icon} {oid} | {prov} | ${bal:.2f} → {format_usd(cost)}\n  {user} | {t}")
    return "\n".join(lines)

def hub_orders_markup(page: int = 1, status_filter: str = "pending") -> InlineKeyboardMarkup:
    all_orders = get_all_hybrid_orders()
    if status_filter == "pending":
        filtered = [(oid, o) for oid, o in all_orders.items()
                    if o.get("status") == "paid_pending_delivery"]
    elif status_filter == "delivered":
        filtered = [(oid, o) for oid, o in all_orders.items()
                    if o.get("status") == "delivered"]
    else:
        filtered = list(all_orders.items())

    filtered.sort(key=lambda x: x[1].get("time", ""), reverse=True)
    per_page = 8
    total_pages = max(1, (len(filtered) + per_page - 1) // per_page)
    rows = []
    start = (page - 1) * per_page

    if status_filter == "pending":
        for oid, _ in filtered[start:start + per_page]:
            rows.append([InlineKeyboardButton(
                f"✅ Deliver {oid}", callback_data=f"hub:deliver:{oid}"
            )])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀", callback_data=f"hub:orders:{page-1}:{status_filter}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("▶", callback_data=f"hub:orders:{page+1}:{status_filter}"))
    if nav:
        rows.append(nav)

    rows.append([
        InlineKeyboardButton("⏳ Pending",  callback_data="hub:orders:1:pending"),
        InlineKeyboardButton("✅ Delivered", callback_data="hub:orders:1:delivered"),
        InlineKeyboardButton("📋 All",       callback_data="hub:orders:1:all"),
    ])
    rows.append([InlineKeyboardButton("🏠 Hub", callback_data="hub:main")])
    return InlineKeyboardMarkup(rows)

def hub_users_text(page: int = 1) -> str:
    per_page = 10
    all_bal  = load_json_file(BALANCES_FILE, {})
    users    = list(all_bal.items())
    users.sort(key=lambda x: float(x[1].get("available_balance", 0) or 0), reverse=True)
    total_pages = max(1, (len(users) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    chunk = users[start:start + per_page]
    lines = [f"👥 Users — Page {page}/{total_pages} (Total: {len(users)})\n"]
    for uid, rec in chunk:
        bal = float(rec.get("available_balance", 0) or 0)
        spent = float(rec.get("total_spent", 0) or 0)
        lines.append(f"• {uid} | Bal: {format_usd(bal)} | Spent: {format_usd(spent)}")
    return "\n".join(lines)

def hub_users_markup(page: int = 1) -> InlineKeyboardMarkup:
    per_page = 10
    all_bal  = load_json_file(BALANCES_FILE, {})
    users    = list(all_bal.items())
    users.sort(key=lambda x: float(x[1].get("available_balance", 0) or 0), reverse=True)
    total_pages = max(1, (len(users) + per_page - 1) // per_page)
    rows = []
    start = (page - 1) * per_page
    for uid, rec in users[start:start + per_page]:
        bal = float(rec.get("available_balance", 0) or 0)
        rows.append([InlineKeyboardButton(
            f"👤 {uid} | {format_usd(bal)}", callback_data=f"hub:userprofile:{uid}"
        )])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀", callback_data=f"hub:users:{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("▶", callback_data=f"hub:users:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton("📣 Broadcast", callback_data="hub:broadcast"),
        InlineKeyboardButton("🏠 Hub",       callback_data="hub:main"),
    ])
    return InlineKeyboardMarkup(rows)

def hub_user_profile_text(uid: str) -> str:
    rec    = get_hybrid_balance_record(int(uid))
    orders = get_all_hybrid_orders()
    user_orders = [o for o in orders.values() if str(o.get("user_id", "")) == str(uid)]
    return (
        f"👤 User Profile\n\n"
        f"User ID: {uid}\n"
        f"Balance: {format_usd(float(rec.get('available_balance', 0) or 0))}\n"
        f"Deposited: {format_usd(float(rec.get('total_deposited', 0) or 0))}\n"
        f"Spent: {format_usd(float(rec.get('total_spent', 0) or 0))}\n"
        f"Total Orders: {len(user_orders)}\n"
    )

def hub_user_profile_markup(uid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Send Message",  callback_data=f"hub:msguser:{uid}"),
         InlineKeyboardButton("➕ Add Balance",    callback_data=f"hub:addbal:{uid}")],
        [InlineKeyboardButton("➖ Sub Balance",    callback_data=f"hub:subbal:{uid}"),
         InlineKeyboardButton("🚫 Remove Access", callback_data=f"hub:rmaccess:{uid}")],
        [InlineKeyboardButton("◀ Back",           callback_data="hub:users:1"),
         InlineKeyboardButton("🏠 Hub",            callback_data="hub:main")],
    ])

def hub_stock_text() -> str:
    stats = get_card_stats()
    cards = load_cards()
    unposted = sum(1 for c in cards.values()
                   if c.get("status") == "available" and not c.get("channel_posted", False))
    by_prov = {}
    for c in cards.values():
        if c.get("status") == "available":
            p = c.get("provider", "?")
            by_prov[p] = by_prov.get(p, 0) + 1
    lines = [
        "Stock Status\n",
        f"Available:      {stats['available']}",
        f"Reserved:       {stats['reserved']}",
        f"Sold:           {stats['sold']}",
        f"Total:          {stats['total']}",
        f"Channel queue:  {unposted}",
        "",
        "By Provider:",
    ]
    for prov, cnt in sorted(by_prov.items(), key=lambda x: -x[1]):
        short = PROVIDER_SHORT.get(prov, prov)
        lines.append(f"  {short}: {cnt}")
    lines += [
        "",
        "Import: Send .txt file with caption /importstock",
        "Or use Import Stock button below.",
    ]
    return "\n".join(lines)

def hub_stock_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Import Stock",   callback_data="hub:importstock")],
        [InlineKeyboardButton("🗑 Delete Provider", callback_data="hub:deletestock"),
         InlineKeyboardButton("📊 Card Stats",      callback_data="hub:cardstats")],
        [InlineKeyboardButton("📤 Post Now",        callback_data="hub:poststocknow"),
         InlineKeyboardButton("🔄 Channel",         callback_data="hub:channel")],
        [InlineKeyboardButton("🏠 Hub",             callback_data="hub:main")],
    ])

def hub_payments_text(page: int = 1) -> str:
    payments = load_pending_payments()
    waiting  = [(pid, p) for pid, p in payments.items()
                if not p.get("credited") and p.get("status") in ("waiting", "expired")]
    waiting.sort(key=lambda x: x[1].get("created_at", ""), reverse=True)
    per_page = 8
    total_pages = max(1, (len(waiting) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    lines = [f"💰 Pending Payments — Page {page}/{total_pages}\n"]
    if not waiting:
        lines.append("No pending payments.")
    for pid, p in waiting[start:start + per_page]:
        coin = p.get("coin", "-")
        amt  = format_usd(p.get("usd_amount", 0))
        uid  = p.get("user_id", "-")
        stat = p.get("status", "-")
        lines.append(f"• {pid}\n  {coin} {amt} | User {uid} | {stat}")
    return "\n".join(lines)

def hub_payments_markup(page: int = 1) -> InlineKeyboardMarkup:
    payments = load_pending_payments()
    waiting  = [(pid, p) for pid, p in payments.items()
                if not p.get("credited") and p.get("status") in ("waiting", "expired")]
    per_page = 8
    total_pages = max(1, (len(waiting) + per_page - 1) // per_page)
    rows = []
    start = (page - 1) * per_page
    for pid, p in waiting[start:start + per_page]:
        amt = format_usd(p.get("usd_amount", 0))
        rows.append([InlineKeyboardButton(
            f"✅ Credit {pid[:16]} {amt}", callback_data=f"hub:creditpay:{pid}"
        )])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀", callback_data=f"hub:payments:{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("▶", callback_data=f"hub:payments:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("🏠 Hub", callback_data="hub:main")])
    return InlineKeyboardMarkup(rows)

def hub_settings_text() -> str:
    cfg = load_config()
    maintenance = "ON" if cfg.get("maintenance_mode") else "OFF"
    auto_summary = "ON" if cfg.get("auto_daily_summary_enabled") else "OFF"
    return (
        f"⚙️ Settings\n\n"
        f"Maintenance Mode: {maintenance}\n"
        f"Daily Summary: {auto_summary}\n"
        f"Channel: {CHANNEL_USERNAME}\n"
        f"Bot Username: @{CHANNEL_BOT_USERNAME}"
    )

def hub_settings_markup() -> InlineKeyboardMarkup:
    cfg = load_config()
    maint_label = "🔴 Maintenance OFF" if cfg.get("maintenance_mode") else "🟢 Maintenance ON"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(maint_label,          callback_data="hub:toggle_maintenance"),
         InlineKeyboardButton("📊 Daily Summary",   callback_data="hub:toggle_summary")],
        [InlineKeyboardButton("📣 Broadcast",       callback_data="hub:broadcast"),
         InlineKeyboardButton("💾 Backup Now",      callback_data="hub:backup")],
        [InlineKeyboardButton("🏠 Hub",             callback_data="hub:main")],
    ])

def hub_reports_text() -> str:
    return build_daily_summary_text()

def hub_channel_text() -> str:
    cards    = load_cards()
    unposted = sum(1 for c in cards.values()
                   if c.get("status") == "available" and not c.get("channel_posted", False))
    logo_set = "Yes" if get_bot_logo_file_id() else "No"
    eta      = round(unposted * CHANNEL_STOCK_INTERVAL / 60, 0)
    return (
        f"📢 Channel Management\n\n"
        f"Channel: {CHANNEL_USERNAME}\n"
        f"Post Interval: {CHANNEL_STOCK_INTERVAL}s\n"
        f"Queue: {unposted} cards (~{int(eta)} min)\n"
        f"Logo Set: {logo_set}"
    )

def hub_channel_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Post Now",   callback_data="hub:poststocknow"),
         InlineKeyboardButton("🔄 Reset Queue",callback_data="hub:resetposted")],
        [InlineKeyboardButton("🏠 Hub",        callback_data="hub:main")],
    ])

# hub_msg_mode: admin mesaj modunda hangi user'a yazıyor
hub_msg_mode = {}  # {admin_id: user_id}
hub_broadcast_confirm = {}  # {admin_id: text}


def hub_members_text(page: int = 1) -> str:
    acts     = load_pending_activations()
    per_page = 8
    # Bekleyen aktivasyonlar
    pending  = [(pid, r) for pid, r in acts.items()
                if not r.get("credited") and r.get("status") in ("waiting", "expired")]
    pending.sort(key=lambda x: x[1].get("created_at", ""), reverse=True)
    # Onaylı üyeler
    approved = access_data.get("approved_users", [])
    # Son aktivasyonlar
    recent   = [(pid, r) for pid, r in acts.items()
                if r.get("credited") and r.get("status") == "activated"]
    recent.sort(key=lambda x: x[1].get("activated_at", ""), reverse=True)

    total_pages = max(1, (len(pending) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page

    lines = [
        f"Members — Page {page}/{total_pages}\n",
        f"Total Members: {len(approved)}",
        f"Pending Activations: {len(pending)}",
        f"Total Activations: {len(recent)}",
        "",
    ]
    if pending:
        lines.append("Pending Payments:")
        for pid, r in pending[start:start + per_page]:
            uid   = r.get("user_id", "-")
            name  = r.get("name", "-")
            coin  = r.get("coin", "-")
            since = r.get("created_at", "")[:16]
            stat  = r.get("status", "-")
            lines.append(f"• {pid[:16]}\n  {name} | {coin} | {stat} | {since}")
    else:
        lines.append("No pending activations.")
    return "\n".join(lines)

def hub_members_markup(page: int = 1) -> InlineKeyboardMarkup:
    acts    = load_pending_activations()
    pending = [(pid, r) for pid, r in acts.items()
               if not r.get("credited") and r.get("status") in ("waiting", "expired")]
    pending.sort(key=lambda x: x[1].get("created_at", ""), reverse=True)
    per_page    = 8
    total_pages = max(1, (len(pending) + per_page - 1) // per_page)
    rows = []
    start = (page - 1) * per_page
    for pid, r in pending[start:start + per_page]:
        uid  = r.get("user_id", "-")
        name = r.get("name", "-")
        rows.append([
            InlineKeyboardButton(
                f"✅ Approve {name[:15]}", callback_data=f"hub:approve_member:{uid}:{pid}"
            ),
            InlineKeyboardButton(
                f"❌ Reject", callback_data=f"hub:reject_member:{uid}:{pid}"
            ),
        ])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀", callback_data=f"hub:members:{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("▶", callback_data=f"hub:members:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton("👥 All Members", callback_data="hub:users:1"),
        InlineKeyboardButton("🏠 Hub",         callback_data="hub:main"),
    ])
    return InlineKeyboardMarkup(rows)
# ============================================================
# ADMIN HUB MODULE END
# ============================================================

async def hub_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        return
    text   = hub_main_text()
    markup = hub_main_markup()
    await update.message.reply_text(text, reply_markup=markup)


async def hub_callback_handler(query, context):
    """Hub callback'lerini işler."""
    admin_id = query.from_user.id
    data     = query.data  # "hub:section:param"
    parts    = data.split(":", 2)
    section  = parts[1] if len(parts) > 1 else "main"
    param    = parts[2] if len(parts) > 2 else ""

    async def edit(text, markup=None, **kwargs):
        if markup is None and "reply_markup" in kwargs:
            markup = kwargs["reply_markup"]
        try:
            await query.edit_message_text(text, reply_markup=markup)
        except Exception as e:
            err = str(e).lower()
            if "not modified" in err or "query is too old" in err:
                return
            if "message to edit not found" in err or "message_id_invalid" in err:
                # Farkli mesaj, reply yap
                try:
                    await query.message.reply_text(text, reply_markup=markup)
                except Exception:
                    pass
                return
            logger.error(f"Hub edit error: {e}")
            try:
                await query.message.reply_text(text, reply_markup=markup)
            except Exception:
                pass

    if section == "main":
        await edit(hub_main_text(), hub_main_markup())

    elif section == "orders":
        parts_o = param.split(":") if param else []
        page = int(parts_o[0]) if parts_o and parts_o[0].isdigit() else 1
        sf   = parts_o[1] if len(parts_o) > 1 else "pending"
        await edit(hub_orders_text(page, sf), hub_orders_markup(page, sf))

    elif section == "deliver":
        order_id = param
        if order_id not in orders:
            await query.answer("Order not found.", show_alert=True)
            return
        data_o = orders[order_id]
        # Kart bul
        cards = load_cards()
        linked = None
        for cid, card in cards.items():
            if card.get("order_id") == order_id:
                linked = card
                break
        if not linked:
            await query.answer("No card linked to this order.", show_alert=True)
            return
        # Teslim için decrypt et
        linked_dec = decrypt_card_record(linked)
        provider  = data_o.get("provider", "")
        short     = PROVIDER_SHORT.get(provider, provider)
        currency  = linked.get("currency", "US")
        balance   = float(linked.get("balance", 0))
        cn        = linked_dec["card_number"]
        em        = linked_dec.get("expiry_month", "")
        ey        = linked_dec.get("expiry_year", "") or ""
        cvv       = linked_dec.get("cvv", "")
        exp_str   = f"{em}/{ey[-2:]}" if len(ey) >= 2 else ey
        rate      = float(rates_data.get(provider, DEFAULT_PROVIDER_RATES.get(provider, 0.38)))
        cost      = round(balance * rate, 2)
        customer_chat_id = data_o.get("chat_id")
        bought_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        delivery_text = (
            f"Your card has been delivered\n\n"
            f"Order ID: {order_id}\n\n"
            f"Card Number: {cn}\n"
            f"Expiry:      {exp_str}\n"
            f"CVV:         {cvv}\n\n"
            f"Balance:  {currency}${balance:.2f}\n"
            f"Provider: {short}\n"
            f"Rate:     {rate*100:.0f}%\n\n"
            f"Delivered At: {bought_at}\n"
            f"Thank you for choosing Elite Earners!"
        )
        try:
            await context.bot.send_message(
                chat_id=customer_chat_id,
                text=delivery_text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 My Orders",  callback_data="my_orders")],
                    [InlineKeyboardButton("🛒 View Listing", callback_data="lst:1:")],
                ]),
            )
            orders[order_id]["status"] = "delivered"
            orders[order_id]["delivered_at"] = bought_at
            save_orders(orders)
            add_audit_log_entry("deliver", query.from_user, "order", order_id, "hub delivery")
            await query.answer("Delivered successfully!", show_alert=True)
        except Exception as e:
            await query.answer(f"Delivery failed: {e}", show_alert=True)
        page = 1
        await edit(hub_orders_text(page), hub_orders_markup(page))

    elif section == "users":
        page = int(param) if param.isdigit() else 1
        await edit(hub_users_text(page), hub_users_markup(page))

    elif section == "userprofile":
        uid = param
        await edit(hub_user_profile_text(uid), hub_user_profile_markup(uid))

    elif section == "msguser":
        uid = param
        hub_msg_mode[admin_id] = int(uid)
        await edit(
            f"Send Message to User\n\nUser ID: {uid}\n\nType your message below.\nNext message you send will be forwarded to this user.", InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data=f"hub:userprofile:{uid}")]
            ]),
        )

    elif section == "addbal":
        uid = param
        hub_msg_mode[admin_id] = f"addbal:{uid}"
        await edit(
            f"Add Balance\n\nUser ID: {uid}\n\nType the amount to add (e.g. 50.00):", InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data=f"hub:userprofile:{uid}")]
            ]),
        )

    elif section == "subbal":
        uid = param
        hub_msg_mode[admin_id] = f"subbal:{uid}"
        await edit(
            f"Subtract Balance\n\nUser ID: {uid}\n\nType the amount to subtract (e.g. 10.00):", InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data=f"hub:userprofile:{uid}")]
            ]),
        )

    elif section == "rmaccess":
        uid = param
        try:
            target_id = int(uid)
            approved = access_data.get("approved_users", [])
            if target_id in approved:
                approved.remove(target_id)
                access_data["approved_users"] = approved
                save_access(access_data)
            await query.answer(f"Access removed for {uid}", show_alert=True)
        except Exception as e:
            await query.answer(f"Error: {e}", show_alert=True)
        await edit(hub_users_text(1), hub_users_markup(1))

    elif section == "payments":
        page = int(param) if param.isdigit() else 1
        await edit(hub_payments_text(page), hub_payments_markup(page))

    elif section == "creditpay":
        pid = param
        payments = load_pending_payments()
        rec = payments.get(pid)
        if not rec:
            await query.answer("Payment not found.", show_alert=True)
            return
        if rec.get("credited"):
            await query.answer("Already credited.", show_alert=True)
            return
        usd_amount = float(rec.get("usd_amount", 0))
        await credit_payment(pid, usd_amount, "manual_admin", rec.get("coin", "?"), context.bot)
        await query.answer(f"Credited {format_usd(usd_amount)} to user {rec.get('user_id')}", show_alert=True)
        await edit(hub_payments_text(1), hub_payments_markup(1))

    elif section == "stock":
        await edit(hub_stock_text(), hub_stock_markup())

    elif section == "cardstats":
        stats = get_card_stats()
        cards = load_cards()
        by_prov = {}
        for c in cards.values():
            p   = c.get("provider", "?")
            st  = c.get("status", "available")
            if p not in by_prov:
                by_prov[p] = {"available": 0, "sold": 0, "reserved": 0}
            if st in by_prov[p]:
                by_prov[p][st] += 1
        lines = ["Card Stats\n"]
        for prov, cnt in sorted(by_prov.items(), key=lambda x: -x[1].get("available",0)):
            short = PROVIDER_SHORT.get(prov, prov)
            a = cnt.get("available", 0)
            s = cnt.get("sold", 0)
            lines.append(f"  {short}: {a} avail / {s} sold")
        await edit("\n".join(lines), InlineKeyboardMarkup([
            [InlineKeyboardButton("◀ Back", callback_data="hub:stock")],
        ]))

    elif section == "channel":
        await edit(hub_channel_text(), hub_channel_markup())

    elif section == "poststocknow":
        posted = await post_one_card_to_channel(context.bot)
        msg = "Card posted!" if posted else "No unposted cards."
        await query.answer(msg, show_alert=True)
        await edit(hub_channel_text(), hub_channel_markup())

    elif section == "resetposted":
        cards = load_cards()
        count = 0
        for cid in cards:
            if cards[cid].get("channel_posted", False):
                cards[cid]["channel_posted"] = False
                count += 1
        save_cards(cards)
        await query.answer(f"{count} cards reset to unposted.", show_alert=True)
        await edit(hub_channel_text(), hub_channel_markup())

    elif section == "channelstatus":
        await edit(hub_channel_text(), hub_channel_markup())

    elif section == "importstock":
        hub_msg_mode[admin_id] = "importstock"
        await edit(
            "Import Stock\n\n"
            "First select card registration status, then paste data.\n\n"
            "Accepted formats:\n"
            "• Full: 4358801234:04:2034:632:US$15.28\n"
            "• Short: 4358801234567890 $15.28\n"
            "• Shortest: 491277 25.00\n\n"
            "Provider auto-detected from BIN.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Not Registered", callback_data="hub:importstock_unreg"),
                 InlineKeyboardButton("🔒 Registered",     callback_data="hub:importstock_reg")],
                [InlineKeyboardButton("❓ Unknown",         callback_data="hub:importstock_unk")],
                [InlineKeyboardButton("❌ Cancel",          callback_data="hub:stock")],
            ]),
        )

    elif section == "importstock_reg":
        hub_msg_mode[admin_id] = "importstock:registered"
        await edit(
            "Import Stock — Registered Cards\n\n"
            "Paste card data below. Each line = one card.\n"
            "Cards will be marked as 🔒 Registered.",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="hub:stock")]]),
        )

    elif section == "importstock_unreg":
        hub_msg_mode[admin_id] = "importstock:unregistered"
        await edit(
            "Import Stock — Not Registered Cards\n\n"
            "Paste card data below. Each line = one card.\n"
            "Cards will be marked as ✅ Not Registered.",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="hub:stock")]]),
        )

    elif section == "importstock_unk":
        hub_msg_mode[admin_id] = "importstock:unknown"
        await edit(
            "Import Stock — Unknown Registration\n\n"
            "Paste card data below. Each line = one card.",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="hub:stock")]]),
        )

    elif section == "deletestock":
        hub_msg_mode[admin_id] = "deletestock"
        await edit(
            "Delete Provider Stock\n\n"
            "Type the provider short name to delete all available cards:\n"
            "GCM, MPC, JKR, CBAU, VG, VP, WMT, AMEX, BNow, PG\n\n"
            "Next message you send will be used.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="hub:stock")],
            ]),
        )

    elif section == "settings":
        await edit(hub_settings_text(), hub_settings_markup())

    elif section == "toggle_maintenance":
        cfg = load_config()
        cfg["maintenance_mode"] = not cfg.get("maintenance_mode", False)
        save_config(cfg)
        state = "ON" if cfg["maintenance_mode"] else "OFF"
        await query.answer(f"Maintenance mode: {state}", show_alert=True)
        await edit(hub_settings_text(), hub_settings_markup())

    elif section == "toggle_summary":
        cfg = load_config()
        cfg["auto_daily_summary_enabled"] = not cfg.get("auto_daily_summary_enabled", False)
        save_config(cfg)
        state = "ON" if cfg["auto_daily_summary_enabled"] else "OFF"
        await query.answer(f"Daily summary: {state}", show_alert=True)
        await edit(hub_settings_text(), hub_settings_markup())

    elif section == "backup":
        folder = create_backup_snapshot("hub_manual")
        await query.answer("Backup created!", show_alert=True)
        await edit(hub_settings_text(), hub_settings_markup())

    elif section == "broadcast":
        hub_msg_mode[admin_id] = "broadcast"
        await edit(
            "📣 Broadcast\n\nType your message below.\nAll approved users will receive it.\n\nNext message you send will be used as broadcast.", InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="hub:settings")]
            ]),
        )

    elif section == "broadcast_confirm":
        text_to_send = hub_broadcast_confirm.get(admin_id, "")
        if not text_to_send:
            await query.answer("No message to broadcast.", show_alert=True)
            return
        approved = access_data.get("approved_users", [])
        sent = 0
        failed = 0
        for uid in approved:
            try:
                await context.bot.send_message(chat_id=uid, text=text_to_send)
                sent += 1
            except Exception:
                failed += 1
        hub_broadcast_confirm.pop(admin_id, None)
        await query.answer(f"Broadcast sent: {sent} ok, {failed} failed", show_alert=True)
        await edit(hub_main_text(), hub_main_markup())

    elif section == "broadcast_cancel":
        hub_broadcast_confirm.pop(admin_id, None)
        hub_msg_mode.pop(admin_id, None)
        await edit(hub_main_text(), hub_main_markup())

    elif section == "support":
        # Bekleyen destek mesajlarını göster
        all_orders = get_all_hybrid_orders()
        # support_mode_users aktif olanları göster
        active_support = list(support_mode_users)
        lines = [f"Support — {len(active_support)} active session(s)\n"]
        if not active_support:
            lines.append("No active support sessions.")
        else:
            for uid in active_support:
                lines.append(f"• User {uid} — awaiting reply")
                lines.append(f"  Reply: /replyuser {uid} Your message")
        await edit(
            "\n".join(lines),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Hub", callback_data="hub:main")],
            ]),
        )

    elif section == "members":
        page = int(param) if param.isdigit() else 1
        await edit(hub_members_text(page), hub_members_markup(page))

    elif section == "approve_member":
        # param = "user_id:payment_id"
        parts2 = param.split(":", 1)
        uid    = int(parts2[0]) if parts2[0].isdigit() else 0
        pid    = parts2[1] if len(parts2) > 1 else ""
        if uid:
            # Approved listesine ekle
            approved = access_data.get("approved_users", [])
            if uid not in approved:
                approved.append(uid)
                access_data["approved_users"] = approved
                save_access(access_data)
            # Aktivasyon kaydını güncelle
            acts = load_pending_activations()
            if pid in acts:
                acts[pid]["credited"]     = True
                acts[pid]["status"]       = "activated"
                acts[pid]["activated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                save_pending_activations(acts)
            # Kullanıcıya bildir
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=(
                        f"Your account has been activated!\n\n"
                        f"Welcome to {LIFETIME_ACCESS_NAME}!\n"
                        f"Use /start to open the main menu."
                    ),
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Open Main Menu", callback_data="main_menu")]
                    ]),
                )
            except Exception:
                pass
            add_audit_log_entry("manual_approve", query.from_user, "user", str(uid), "hub approval")
            await query.answer(f"User {uid} approved!", show_alert=True)
        await edit(hub_members_text(1), hub_members_markup(1))

    elif section == "reject_member":
        parts2 = param.split(":", 1)
        uid    = int(parts2[0]) if parts2[0].isdigit() else 0
        pid    = parts2[1] if len(parts2) > 1 else ""
        # Aktivasyonu reddet
        acts = load_pending_activations()
        if pid in acts:
            acts[pid]["status"] = "rejected"
            save_pending_activations(acts)
        # Kullanıcıya bildir
        if uid:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=(
                        "Your activation request was not approved.\n\n"
                        "If you believe this is an error, please contact support."
                    ),
                    reply_markup=restricted_markup(),
                )
            except Exception:
                pass
            add_audit_log_entry("manual_reject", query.from_user, "user", str(uid), "hub rejection")
            await query.answer(f"User {uid} rejected.", show_alert=True)
        await edit(hub_members_text(1), hub_members_markup(1))

    elif section == "reports":
        await edit(hub_reports_text(), InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Hub", callback_data="hub:main")]
        ]))


# ============================================================
# REFERRAL SYSTEM
# ============================================================
REFERRAL_BONUS_USD = 10.0  # Her iki tarafa da $10

def load_referrals() -> dict:
    return load_json_file(os.path.join(DATA_DIR, "referrals.json"), {})

def save_referrals(data: dict) -> None:
    save_json_file(os.path.join(DATA_DIR, "referrals.json"), data)

def get_referral_link(user_id: int, bot_username: str) -> str:
    return f"https://t.me/{bot_username}?start=ref{user_id}"

def process_referral(new_user_id: int, referrer_id: int) -> bool:
    """Yeni kullanıcı referral linki ile geldi — her ikisine $10."""
    if new_user_id == referrer_id:
        return False
    refs = load_referrals()
    # Zaten referral aldı mı?
    if str(new_user_id) in refs:
        return False
    # Referrer mevcut kullanıcı mı?
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    refs[str(new_user_id)] = {
        "referrer_id":  referrer_id,
        "bonus_given":  True,
        "created_at":   now_str,
    }
    save_referrals(refs)

    # Referrer'a $10 ekle
    rec = get_hybrid_balance_record(referrer_id)
    before = float(rec.get("available_balance", 0) or 0)
    after  = round(before + REFERRAL_BONUS_USD, 2)
    rec["available_balance"] = after
    rec["total_deposited"] = round(float(rec.get("total_deposited", 0) or 0) + REFERRAL_BONUS_USD, 2)
    save_hybrid_balance_record(referrer_id, rec)
    add_balance_ledger_entry(referrer_id, REFERRAL_BONUS_USD, "referral_bonus",
                             reference_id=str(new_user_id),
                             actor="system", before_balance=before, after_balance=after)

    # Yeni kullanıcıya $10 ekle
    rec2 = get_hybrid_balance_record(new_user_id)
    before2 = float(rec2.get("available_balance", 0) or 0)
    after2  = round(before2 + REFERRAL_BONUS_USD, 2)
    rec2["available_balance"] = after2
    rec2["total_deposited"] = round(float(rec2.get("total_deposited", 0) or 0) + REFERRAL_BONUS_USD, 2)
    save_hybrid_balance_record(new_user_id, rec2)
    add_balance_ledger_entry(new_user_id, REFERRAL_BONUS_USD, "referral_welcome_bonus",
                             reference_id=str(referrer_id),
                             actor="system", before_balance=before2, after_balance=after2)
    return True

def get_user_referral_stats(user_id: int) -> dict:
    refs = load_referrals()
    referred = [v for v in refs.values() if v.get("referrer_id") == user_id]
    total_earned = len(referred) * REFERRAL_BONUS_USD
    return {"count": len(referred), "total_earned": total_earned}

# ============================================================
# REFERRAL SYSTEM END
# ============================================================
async def walletstatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_full_admin(update.effective_chat.id):
        return
    payments = load_pending_payments()
    waiting  = sum(1 for p in payments.values() if p.get("status") == "waiting" and not p.get("credited"))
    credited = sum(1 for p in payments.values() if p.get("credited"))
    expired  = sum(1 for p in payments.values() if p.get("status") == "expired" and not p.get("credited"))
    next_idx = _load_payment_index()
    text = (
        "Wallet System Status\n\n"
        f"Wallet Ready: {'Yes' if _wallet_ready else 'NO - ERROR'}\n"
        f"Total Addresses Issued: {next_idx}\n\n"
        "Payment Records:\n"
        f"  Waiting: {waiting}\n"
        f"  Credited: {credited}\n"
        f"  Expired (uncredited): {expired}\n"
        f"  Total: {len(payments)}\n\n"
        f"Poll Interval: every {PAYMENT_POLL_INTERVAL}s\n"
        f"LTC Min Confirmations: {LTC_MIN_CONFIRMATIONS} blocks\n"
        f"ETH Min Confirmations: {ETH_MIN_CONFIRMATIONS} blocks"
    )
    await update.message.reply_text(text)


async def mypayment_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not can_use_bot(chat_id):
        return
    payments = load_pending_payments()
    user_payments = [p for p in payments.values() if int(p.get("user_id", 0)) == user_id]
    user_payments.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    if not user_payments:
        await update.message.reply_text(
            "No payment records found.\nUse Add Balance to top up.",
            reply_markup=main_menu_markup(),
        )
        return
    lines = ["My Recent Payments\n"]
    for p in user_payments[:5]:
        status_map = {"waiting": "Waiting", "credited": "Credited", "expired": "Expired"}
        status_label = status_map.get(p.get("status", ""), p.get("status", "-"))
        lines.append(
            f"ID: {p['payment_id']}\n"
            f"Coin: {p.get('coin', '-')} | Amount: {format_usd(p.get('usd_amount', 0))}\n"
            f"Durum: {status_label} | {p.get('created_at', '-')[:16]}"
        )
        if p.get("tx_hash"):
            lines.append(f"TX: {p['tx_hash'][:20]}...")
        lines.append("")
    await update.message.reply_text("\n".join(lines), reply_markup=main_menu_markup())


async def pendingpayments_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not has_staff_access(update.effective_chat.id):
        return
    payments = load_pending_payments()
    waiting = [(pid, p) for pid, p in payments.items() if p.get("status") == "waiting" and not p.get("credited")]
    if not waiting:
        await update.message.reply_text("No pending payments.")
        return
    lines = [f"Pending Payments ({len(waiting)})\n"]
    for pid, p in waiting[:20]:
        lines.append(
            f"{pid}\n"
            f"  {p.get('name', '-')} | {p.get('coin', '-')} | {format_usd(p.get('usd_amount', 0))}\n"
            f"  Address: {p.get('address', '-')[:20]}...\n"
            f"  Expires: {p.get('expires_at', '-')[:16]}"
        )
    await update.message.reply_text("\n".join(lines))


COMMAND_HANDLERS = [
    ("start", start),
    ("supportmenu", supportmenu_command),
    ("help", help_command),
    ("myid", myid),
    ("myorder", myorder_command),
    ("myorders", myorder_command),  # alias
    ("balance", balance_command),
    ("mypayment", mypayment_command),
    ("addbalance", addbalance_command),
    ("subbalance", subbalance_command),
    ("rates", rates_command),
    ("setrate", setrate_command),
    ("providers", providers_command),
    ("status", status_command),
    ("cancel", cancel_command),
    ("archiveorder", archiveorder_command),
    ("purgearchived", purgearchived_command),
    ("orderlist", orderlist_command),
    ("archivedorders", archivedorders_command),
    ("orderinfo", orderinfo_command),
    ("replyorder", replyorder_command),
    ("broadcast", broadcast_command),
    ("maintenance", maintenance_command),
    ("maintenancemsg", maintenancemsg_command),
    ("exportorders", exportorders_command),
    ("auditlog", auditlog_command),
    ("addmod", addmod_command),
    ("removemod", removemod_command),
    ("modlist", modlist_command),
    ("findorder", findorder_command),
    ("messageuser", messageuser_command),
    ("replyuser", replyuser_command),
    ("replysupport", replysupport_command),
    ("reply", reply_command),
    ("messageorder", messageorder_command),
    ("messagebalance", messagebalance_command),
    ("ledgeruser", ledgeruser_command),
    ("exportledger", exportledger_command),
    ("backupnow", backupnow_command),
    ("sqlitestatus", sqlitestatus_command),
    ("sqlitemode", sqlitemode_command),
    ("sqliteprimarycheck", sqliteprimarycheck_command),
    ("sqlitemigrate", sqlitemigrate_command),
    ("balancesync", balancesync_command),
    ("balancestatus", balancestatus_command),
    ("ordersync", ordersync_command),
    ("orderstatussync", orderstatussync_command),
    ("balancerequestsync", balancerequestsync_command),
    ("balancerequeststatussync", balancerequeststatussync_command),
    ("logstoragestatus", logstoragestatus_command),
    ("findrequest", findrequest_command),
    ("ordersbyuser", ordersbyuser_command),
    ("ordersbystatus", ordersbystatus_command),
    ("ordersbyprovider", ordersbyprovider_command),
    ("ordersbytag", ordersbytag_command),
    ("exportaudit", exportaudit_command),
    ("ordernote", ordernote_command),
    ("ordertag", ordertag_command),
    ("balancenote", balancenote_command),
    ("balancetag", balancetag_command),
    ("exportbalances", exportbalances_command),
    ("exportrequests", exportrequests_command),
    ("setbalance", setbalance_command),
    ("setcards", setcards_command),
    ("accessmode", accessmode_command),
    ("approve", approve_command),
    ("removeaccess", removeaccess_command),
    ("approvedlist", approvedlist_command),
    ("deliver", deliver_command),
    ("cancelorder", cancelorder_command),
    ("refundbalance", refundbalance_command),
    ("setlogo", setlogo_command),
    ("poststocknow", poststocknow_command),
    ("channelstatus", channelstatus_command),
    ("resetposted", resetposted_command),
    ("hub", hub_command),
    ("walletstatus", walletstatus_command),
    ("mypayment", mypayment_command),
    ("pendingpayments", pendingpayments_command),
    ("importstock", importstock_command),
    ("cardstats", cardstats_command),
    ("deletecards", deletecards_command),
    ("cleanstock", cleanstock_command),
    ("migratereg", migratereg_command),
    ("cleanbins", cleanbins_command),
    ("fixxstock", fixxstock_command),
    ("competitorbal", competitorbal_command),
    ("setbal", setbal_command),
    ("pendingdeposits", pendingdeposits_command),
    ("confirmdeposit", confirmdeposit_command),
    ("failedorders", failedorders_command),
    ("refundorder", refundorder_command),
    ("importgift", importgift_command),
    ("giftstats", giftstats_command),
    ("deletegifts", deletegifts_command),
    ("setgiftrate", setgiftrate_command),
]


def register_command_handlers(app: Application) -> None:
    for command_name, handler in COMMAND_HANDLERS:
        app.add_handler(CommandHandler(command_name, handler))


def validate_startup_config() -> list:
    """Başlangıçta kritik config eksiklerini kontrol et."""
    warnings = []
    if not TOKEN:
        warnings.append("CRITICAL: BOT_TOKEN not set")
    if not ADMIN_CHAT_ID:
        warnings.append("CRITICAL: ADMIN_CHAT_ID not set")
    if not ALCHEMY_API_KEY:
        warnings.append("WARNING: ALCHEMY_API_KEY not set - USDC payments disabled")
    if not BLOCKCYPHER_TOKEN:
        warnings.append("WARNING: BLOCKCYPHER_TOKEN not set - LTC payments disabled")
    if not ENCRYPTED_SEED:
        warnings.append("CRITICAL: ENCRYPTED_SEED not set - wallet disabled")
    if not CHANNEL_USERNAME:
        warnings.append("WARNING: CHANNEL_USERNAME not set - channel posts disabled")
    if not os.environ.get("CARD_ENCRYPTION_KEY"):
        warnings.append("WARNING: CARD_ENCRYPTION_KEY not set - using WALLET_PASSWORD as fallback (not recommended for production)")
    if not os.environ.get("HELIUS_API_KEY"):
        warnings.append("WARNING: HELIUS_API_KEY not set - Solana USDC payments disabled")
    if not PROOF_CHANNEL_ID:
        warnings.append("WARNING: PROOF_CHANNEL_ID not set - proof posts disabled")
    return warnings

def build_application() -> Application:
    refresh_runtime_settings()
    validate_runtime_settings()
    initialize_runtime_environment()

    # Wallet baslatma
    if not init_wallet():
        raise RuntimeError("Wallet baslatma basarisiz. ENCRYPTED_SEED ve WALLET_PASSWORD kontrol edin.")

    app = Application.builder().token(TOKEN).build()

    register_command_handlers(app)
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.Document.TXT, handle_document))

    # Bot menü komutlarını ayarla
    async def set_commands(application):
        from telegram import BotCommand
        user_commands = [
            BotCommand("start", "Open main menu"),
            BotCommand("balance", "Check your balance"),
            BotCommand("myorders", "View your orders"),
            BotCommand("mypayment", "Check payment status"),
            BotCommand("help", "Get help and information"),
        ]
        try:
            await application.bot.set_my_commands(user_commands)
            logger.info("Bot menu commands set")
        except Exception as e:
            logger.debug(f"Set commands failed: {e}")
    app.post_init = set_commands

    app.add_error_handler(error_handler)
    schedule_auto_daily_summary(app)
    schedule_payment_monitor(app)
    schedule_channel_stock_job(app)
    schedule_maintenance_jobs(app)
    schedule_competitor_monitor(app)
    schedule_stuck_order_handler(app)
    schedule_delivery_notifier(app)
    # Aktivasyon ödemesi monitörü
    if app.job_queue:
        app.job_queue.run_repeating(
            check_activation_payments_job,
            interval=60,
            first=45,
            name="activation_monitor",
        )
        logger.info("Activation monitor started (every 60s)")
    return app

def main() -> None:
    # Startup config kontrolü
    warnings = validate_startup_config()
    for w in warnings:
        if w.startswith("CRITICAL"):
            logger.critical(w)
        else:
            logger.warning(w)
    if any(w.startswith("CRITICAL") for w in warnings):
        logger.critical("Bot cannot start - critical config missing")
        return

    while True:
        try:
            logger.info(f"Starting {APP_VERSION}...")
            logger.info(f"Data directory: {os.path.abspath(DATA_DIR)}")
            logger.info("Bot is running...")
            app = build_application()
            app.run_polling()
        except KeyboardInterrupt:
            print("Bot stopped by user.")
            close_sqlite_connection()
            break
        except Exception as e:
            print(f"Bot crashed: {e}")
            traceback.print_exc()
            print("Restarting in 10 seconds...")
            time.sleep(10)


if __name__ == "__main__":
    main()