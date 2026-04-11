from __future__ import annotations

import os
import sys
from pathlib import Path

import pymysql
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
DDL_DIR = ROOT_DIR / "sql" / "ddl"


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable wajib belum diisi: {name}")
    return value


def load_environment() -> None:
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        raise FileNotFoundError(
            f"File .env tidak ditemukan di {env_path}. "
            "Buat dulu dari .env.example."
        )
    load_dotenv(env_path)


def get_connection(database: str | None = None):
    return pymysql.connect(
        host=required_env("MYSQL_HOST"),
        port=int(required_env("MYSQL_PORT")),
        user=required_env("MYSQL_USER"),
        password=required_env("MYSQL_PASSWORD"),
        database=database,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
    )


def execute_sql_file(connection, sql_file: Path) -> None:
    sql_text = sql_file.read_text(encoding="utf-8").strip()
    if not sql_text:
        print(f"[SKIP] File kosong: {sql_file.name}")
        return

    print(f"[RUN ] Menjalankan: {sql_file.name}")
    with connection.cursor() as cursor:
        for statement in split_sql_statements(sql_text):
            cursor.execute(statement)
    print(f"[ OK ] Selesai: {sql_file.name}")


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    quote_char = ""

    for char in sql_text:
        if char in ("'", '"'):
            if not in_string:
                in_string = True
                quote_char = char
            elif quote_char == char:
                in_string = False
                quote_char = ""

        if char == ";" and not in_string:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


def ensure_ddl_files_exist() -> list[Path]:
    expected_files = [
        DDL_DIR / "create_crypto_raw.sql",
        DDL_DIR / "create_crypto_core.sql",
        DDL_DIR / "create_crypto_mart.sql",
        DDL_DIR / "create_crypto_ops.sql",
    ]

    missing = [file for file in expected_files if not file.exists()]
    if missing:
        missing_text = "\n".join(f"- {file}" for file in missing)
        raise FileNotFoundError(f"File DDL berikut belum ada:\n{missing_text}")

    return expected_files


def test_mysql_connection() -> None:
    print("[INFO] Mengecek koneksi MySQL...")
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            print(f"[INFO] Koneksi berhasil. MySQL version: {version[0]}")
    finally:
        connection.close()


def run() -> None:
    load_environment()
    ddl_files = ensure_ddl_files_exist()
    test_mysql_connection()

    connection = get_connection()
    try:
        for ddl_file in ddl_files:
            execute_sql_file(connection, ddl_file)
    finally:
        connection.close()

    print("[DONE] Semua database dan tabel berhasil diinisialisasi.")


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)