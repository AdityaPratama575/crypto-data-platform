from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pymysql
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
_ENV_LOADED = False


def load_environment() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    env_path = ROOT_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)

    _ENV_LOADED = True


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable wajib belum diisi: {name}")
    return value


def get_database_name(layer: str) -> str:
    load_environment()
    mapping = {
        "raw": "MYSQL_DATABASE_RAW",
        "core": "MYSQL_DATABASE_CORE",
        "mart": "MYSQL_DATABASE_MART",
        "ops": "MYSQL_DATABASE_OPS",
    }
    env_name = mapping.get(layer.lower())
    if not env_name:
        allowed_layers = ", ".join(sorted(mapping.keys()))
        raise ValueError(f"Layer database tidak valid: {layer}. Gunakan salah satu: {allowed_layers}")
    return required_env(env_name)


def get_mysql_connection(*, database: str | None = None, autocommit: bool = True):
    load_environment()
    return pymysql.connect(
        host=required_env("MYSQL_HOST"),
        port=int(required_env("MYSQL_PORT")),
        user=required_env("MYSQL_USER"),
        password=required_env("MYSQL_PASSWORD"),
        database=database,
        autocommit=autocommit,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


@contextmanager
def mysql_connection(
    *,
    database: str | None = None,
    layer: str | None = None,
    autocommit: bool = True,
) -> Iterator[pymysql.connections.Connection]:
    if database and layer:
        raise ValueError("Gunakan salah satu: database atau layer, bukan keduanya.")

    selected_database = database
    if layer:
        selected_database = get_database_name(layer)

    connection = get_mysql_connection(database=selected_database, autocommit=autocommit)
    try:
        yield connection
    finally:
        connection.close()
