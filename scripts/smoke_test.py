from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.clients import CoinGeckoClient  # noqa: E402
from src.db import mysql_connection  # noqa: E402


def ensure_env_file() -> None:
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        raise FileNotFoundError(
            f"File .env tidak ditemukan di {env_path}. Buat dulu dari .env.example."
        )
    load_dotenv(env_path, override=False)


def check_mysql() -> None:
    print("[INFO] Mengecek koneksi MySQL...")
    with mysql_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 AS ok")
            row = cursor.fetchone()

    if not row or row.get("ok") != 1:
        raise RuntimeError("MySQL smoke test gagal: hasil SELECT 1 tidak valid.")

    print("[OK] Koneksi MySQL berhasil.")


def check_coingecko() -> None:
    print("[INFO] Mengecek koneksi CoinGecko...")
    with CoinGeckoClient() as client:
        payload = client.get_global()

    global_data = payload.get("data")
    if not isinstance(global_data, dict):
        raise RuntimeError("CoinGecko smoke test gagal: field 'data' tidak ditemukan.")

    if "active_cryptocurrencies" not in global_data:
        raise RuntimeError(
            "CoinGecko smoke test gagal: field 'active_cryptocurrencies' tidak ditemukan."
        )

    print("[OK] Koneksi CoinGecko berhasil.")


def run() -> None:
    ensure_env_file()
    check_mysql()
    check_coingecko()
    print("[DONE] Smoke test berhasil.")


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc
