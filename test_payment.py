import asyncio
import random
import time
import sqlite3

# Önce WAL mode'u aktif et (bir kerelik)
try:
    _conn = sqlite3.connect("elite_bot.db", timeout=10)
    mode = _conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    _conn.execute("PRAGMA busy_timeout=30000")
    _conn.close()
    print(f"SQLite journal_mode: {mode}")
except Exception as e:
    print(f"WAL setup error: {e}")

from bot_v2 import save_processed_tx, is_tx_processed, verify_ltc_tx


def random_tx():
    return ''.join(random.choices('abcdef0123456789', k=64))


def create_valid_tx():
    return {
        "hash": random_tx(),
        "confirmations": 5,
        "address": "TEST_ADDRESS",
        "value_ltc": 0.01
    }


def test_replay_attack():
    print("\n=== REPLAY TEST ===")
    tx = create_valid_tx()
    first = save_processed_tx(tx["hash"], "TEST1", "LTC", 0.01)
    second = save_processed_tx(tx["hash"], "TEST1", "LTC", 0.01)
    print("First insert:", first)
    print("Second insert (should be False):", second)


def test_verification():
    print("\n=== VERIFICATION TEST ===")
    tx = create_valid_tx()

    valid, reason = verify_ltc_tx(tx, "TEST_ADDRESS", 0.01)
    print("Valid TX:", valid, reason)

    tx_bad = tx.copy()
    tx_bad["address"] = "WRONG"
    valid, reason = verify_ltc_tx(tx_bad, "TEST_ADDRESS", 0.01)
    print("Wrong address:", valid, reason)

    tx_low = tx.copy()
    tx_low["value_ltc"] = 0.002
    valid, reason = verify_ltc_tx(tx_low, "TEST_ADDRESS", 0.01)
    print("Low amount:", valid, reason)

    tx_conf = tx.copy()
    tx_conf["confirmations"] = 1
    valid, reason = verify_ltc_tx(tx_conf, "TEST_ADDRESS", 0.01)
    print("Low confirmation:", valid, reason)


async def stress_test():
    print("\n=== STRESS TEST (100 concurrent inserts) ===")
    t0 = time.time()
    tasks = []
    for i in range(100):
        tx = random_tx()
        tasks.append(asyncio.to_thread(save_processed_tx, tx, f"T{i}", "LTC", 0.01))
    results = await asyncio.gather(*tasks)
    elapsed = round(time.time() - t0, 2)
    success = sum(1 for r in results if r)
    print(f"Successful inserts: {success}/100 in {elapsed}s")
    if success < 100:
        print(f"FAILED: {100 - success} inserts lost!")
    else:
        print("ALL PASSED!")


async def stress_test_sequential_batch():
    """10 batch x 10 concurrent — daha gerçekçi senaryo."""
    print("\n=== STRESS TEST (10 batches x 10 concurrent) ===")
    t0 = time.time()
    total_success = 0
    for batch in range(10):
        tasks = []
        for i in range(10):
            tx = random_tx()
            tasks.append(asyncio.to_thread(save_processed_tx, tx, f"B{batch}-T{i}", "LTC", 0.01))
        results = await asyncio.gather(*tasks)
        total_success += sum(1 for r in results if r)
    elapsed = round(time.time() - t0, 2)
    print(f"Successful inserts: {total_success}/100 in {elapsed}s")
    if total_success < 100:
        print(f"FAILED: {100 - total_success} inserts lost!")
    else:
        print("ALL PASSED!")


if __name__ == "__main__":
    test_replay_attack()
    test_verification()
    asyncio.run(stress_test())
    asyncio.run(stress_test_sequential_batch())
