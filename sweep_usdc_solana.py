"""
Elite Prepaid Bot - USDC Solana Sweep Script
Tüm bot adreslerindeki USDC'yi tek bir hedef adrese toplar.
"""

import os
import json
import hmac
import hashlib
import base64
import struct
import sqlite3
import requests
from dotenv import load_dotenv
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes

load_dotenv()

TARGET_ADDRESS   = "2hSmdbbxh8zTNep7qW8LzLhRrBtMQJrWhUpRbnkQqhqg"
USDC_MINT        = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
HELIUS_API_KEY   = os.environ.get("HELIUS_API_KEY", "")
HELIUS_URL       = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
WALLET_PASSWORD  = os.environ.get("WALLET_PASSWORD", "")
ENCRYPTED_SEED   = os.environ.get("ENCRYPTED_SEED", "")
DATA_DIR         = os.environ.get("DATA_DIR", ".")

# ── Seed decrypt ──────────────────────────────────────────────
def _decrypt_seed() -> bytes:
    raw = base64.b64decode(ENCRYPTED_SEED)
    salt, nonce, ct = raw[:16], raw[16:28], raw[28:]
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100_000)
    from cryptography.hazmat.backends import default_backend
    key = kdf.derive(WALLET_PASSWORD.encode(), )
    return AESGCM(key).decrypt(nonce, ct, None)

# ── Solana adres türetme ──────────────────────────────────────
def _hmac_sha512(key, data):
    return hmac.new(key, data, hashlib.sha512).digest()

def derive_solana_keypair(index: int) -> tuple:
    seed = _decrypt_seed()
    def child(k, c, idx):
        h = idx + 0x80000000
        d = b"\x00" + k + h.to_bytes(4, "big")
        I = _hmac_sha512(c, d)
        return I[:32], I[32:]
    I = _hmac_sha512(b"ed25519 seed", seed)
    k, c = I[:32], I[32:]
    for lvl in [44, 501, 0, 0, index]:
        k, c = child(k, c, lvl)
    # Public key
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        priv_obj = Ed25519PrivateKey.from_private_bytes(k)
        pub = priv_obj.public_key().public_bytes_raw()
    except Exception as e:
        print(f"  Ed25519 error: {e}")
        pub = hashlib.sha256(k).digest()
    return k, pub

def b58encode(data: bytes) -> str:
    ALPHA = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    count = sum(1 for b in data if b == 0)
    num = int.from_bytes(data, "big")
    res = []
    while num > 0:
        num, r = divmod(num, 58)
        res.append(ALPHA[r])
    return "1" * count + "".join(reversed(res))

def derive_solana_address(index: int) -> tuple:
    priv, pub = derive_solana_keypair(index)
    return priv, b58encode(pub)

# ── Helius API ────────────────────────────────────────────────
def get_usdc_balance(address: str) -> float:
    """Adresin USDC bakiyesini getir."""
    try:
        r = requests.post(HELIUS_URL, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                address,
                {"mint": USDC_MINT},
                {"encoding": "jsonParsed"}
            ]
        }, timeout=10)
        accounts = r.json().get("result", {}).get("value", [])
        total = 0.0
        for acc in accounts:
            amt = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {}).get("tokenAmount", {})
            total += float(amt.get("uiAmount") or 0)
        return total
    except Exception as e:
        print(f"  Balance check error {address[:16]}: {e}")
        return 0.0

def get_payment_indexes() -> list:
    """Bot DB'sinden kullanılmış payment indexleri getir."""
    indexes = set()
    
    # SQLite'tan
    db_path = os.path.join(DATA_DIR, "elite_bot.db")
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            try:
                cur.execute("SELECT address_index FROM payments WHERE coin='USDC_SOLANA'")
                for row in cur.fetchall():
                    if row["address_index"] is not None:
                        indexes.add(int(row["address_index"]))
            except Exception:
                pass
            # Metadata'dan max index
            try:
                cur.execute("SELECT value FROM metadata WHERE key='payment_index'")
                row = cur.fetchone()
                if row:
                    for i in range(int(row["value"]) + 1):
                        indexes.add(i)
            except Exception:
                pass
            conn.close()
        except Exception as e:
            print(f"  SQLite error: {e}")
    
    # JSON'dan fallback
    json_path = os.path.join(DATA_DIR, "pending_payments.json")
    if os.path.exists(json_path):
        try:
            with open(json_path) as f:
                payments = json.load(f)
            for p in payments.values():
                if p.get("coin") == "USDC_SOLANA" and p.get("address_index") is not None:
                    indexes.add(int(p["address_index"]))
        except Exception:
            pass

    # En az 0-20 arası tara (güvenlik için)
    for i in range(21):
        indexes.add(i)
    
    return sorted(indexes)

# ── Ana sweep fonksiyonu ──────────────────────────────────────
def sweep():
    print("=" * 55)
    print("Elite Prepaid Bot — USDC Solana Sweep")
    print(f"Hedef adres: {TARGET_ADDRESS}")
    print("=" * 55)

    if not HELIUS_API_KEY:
        print("HATA: HELIUS_API_KEY bulunamadı!")
        return
    if not ENCRYPTED_SEED or not WALLET_PASSWORD:
        print("HATA: ENCRYPTED_SEED veya WALLET_PASSWORD bulunamadı!")
        return

    indexes = get_payment_indexes()
    print(f"\nKontrol edilecek adres sayısı: {len(indexes)}")
    print("-" * 55)

    found = []
    for idx in indexes:
        try:
            priv, address = derive_solana_address(idx)
            balance = get_usdc_balance(address)
            if balance > 0.001:
                print(f"✓ Index {idx:4d}: {address[:20]}... | {balance:.6f} USDC")
                found.append({"index": idx, "address": address, "balance": balance, "priv": priv})
            else:
                print(f"  Index {idx:4d}: {address[:20]}... | boş")
        except Exception as e:
            print(f"  Index {idx:4d}: HATA - {e}")

    print("-" * 55)
    total = sum(f["balance"] for f in found)
    print(f"\nBulundu: {len(found)} adres, toplam {total:.6f} USDC")

    if not found:
        print("\nSweep edilecek bakiye yok.")
        return

    print(f"\nNOT: Sweep işlemi için 'solana-py' kütüphanesi gerekiyor.")
    print(f"Şu an bu script bakiye tespiti yapıyor.")
    print(f"\nKurulum için:")
    print(f"  pip install solana solders")
    print(f"\nKurulumdan sonra bu scripti tekrar çalıştır,")
    print(f"otomatik transfer yapacak.")

    # solana-py kuruluysa sweep yap
    try:
        from solders.keypair import Keypair
        from solders.pubkey import Pubkey
        from solana.rpc.api import Client
        from spl.token.client import Token
        from spl.token.constants import TOKEN_PROGRAM_ID
        import spl

        print("\n✓ solana-py kurulu, sweep başlıyor...\n")
        client = Client(HELIUS_URL)
        target_pubkey = Pubkey.from_string(TARGET_ADDRESS)

        for item in found:
            print(f"Sweep: {item['address'][:20]}... → {TARGET_ADDRESS[:20]}...")
            try:
                kp = Keypair.from_seed(item["priv"])
                # Token hesabını bul ve transfer et
                token = Token(client, Pubkey.from_string(USDC_MINT), TOKEN_PROGRAM_ID, kp)
                source_accounts = token.get_accounts(kp.pubkey())
                if not source_accounts.value:
                    print(f"  Token account bulunamadı")
                    continue
                source_acc = source_accounts.value[0].pubkey
                dest_acc   = token.get_accounts(target_pubkey)
                if not dest_acc.value:
                    print(f"  Hedef token account oluşturuluyor...")
                    token.create_associated_token_account(target_pubkey)
                    dest_acc = token.get_accounts(target_pubkey)
                dest_pubkey = dest_acc.value[0].pubkey
                amount_raw  = int(item["balance"] * 1_000_000)  # USDC 6 decimal
                sig = token.transfer(source_acc, dest_pubkey, kp, amount_raw)
                print(f"  ✓ Transfer edildi: {sig}")
            except Exception as e:
                print(f"  HATA: {e}")

    except ImportError:
        print("\nsolana-py henüz kurulu değil, sadece bakiye taraması yapıldı.")

if __name__ == "__main__":
    sweep()
