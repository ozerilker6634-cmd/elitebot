import os
import base64
import getpass
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes

def derive_key(password: str, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=390000,
    )
    return kdf.derive(password.encode())

def encrypt_seed(seed: str, password: str) -> str:
    salt = os.urandom(16)
    key = derive_key(password, salt)
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ciphertext = aesgcm.encrypt(nonce, seed.encode(), None)
    combined = salt + nonce + ciphertext
    return base64.b64encode(combined).decode()

def main():
    print("=" * 50)
    print("Elite Bot V2 - Seed Şifreleme")
    print("=" * 50)
    print()
    print("UYARI: Seed phrase'inizi kimseyle paylaşmayın!")
    print("Bu script seed'inizi şifreleyip .env dosyasına ekler.")
    print()

    seed = getpass.getpass("Electrum seed phrase (12 kelime): ").strip()

    if len(seed.split()) not in (12, 24):
        print("HATA: Seed 12 veya 24 kelime olmalı!")
        return

    password = getpass.getpass("WALLET_PASSWORD (.env'deki şifreniz): ").strip()
    password_confirm = getpass.getpass("Şifreyi tekrar girin: ").strip()

    if password != password_confirm:
        print("HATA: Şifreler eşleşmiyor!")
        return

    encrypted = encrypt_seed(seed, password)

    env_path = os.path.join(os.path.dirname(__file__), ".env")

    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            content = f.read()

        if "ENCRYPTED_SEED=" in content:
            lines = content.splitlines()
            lines = [l for l in lines if not l.startswith("ENCRYPTED_SEED=")]
            content = "\n".join(lines) + "\n"

        with open(env_path, "a") as f:
            f.write(f"\nENCRYPTED_SEED={encrypted}\n")
    else:
        with open(env_path, "w") as f:
            f.write(f"ENCRYPTED_SEED={encrypted}\n")

    print()
    print("Seed başarıyla şifrelendi ve .env dosyasına eklendi.")
    print("Orijinal seed hiçbir yerde saklanmadı.")
    print()
    print("Şimdi bot.py'yi çalıştırabilirsiniz.")

if __name__ == "__main__":
    main()
