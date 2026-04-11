from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_BASE_URL = "https://api.coingecko.com/api/v3"


class CoinGeckoClientError(RuntimeError):
    """Base exception for CoinGecko client errors."""


class RetryableCoinGeckoError(CoinGeckoClientError):
    """Retryable failures such as network issues, 429, or 5xx."""


def load_environment() -> None:
    env_path = ROOT_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise CoinGeckoClientError(f"Environment variable wajib belum diisi: {name}")
    return value


def env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError as exc:
        raise CoinGeckoClientError(
            f"Environment variable {name} harus berupa integer."
        ) from exc


class CoinGeckoClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout_seconds: int | None = None,
        max_retries: int | None = None,
        backoff_seconds: int | None = None,
        session: requests.Session | None = None,
    ) -> None:
        load_environment()

        self.base_url = (base_url or os.getenv("COINGECKO_BASE_URL") or DEFAULT_BASE_URL).rstrip(
            "/"
        )
        self.api_key = api_key or required_env("COINGECKO_API_KEY")
        self.timeout_seconds = timeout_seconds or env_int("REQUEST_TIMEOUT_SECONDS", 30)
        self.max_retries = max(1, max_retries or env_int("MAX_RETRIES", 3))
        self.backoff_seconds = max(1, backoff_seconds or env_int("BACKOFF_SECONDS", 2))
        self.vs_currency = os.getenv("COINGECKO_VS_CURRENCY", "usd")

        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "x-cg-demo-api-key": self.api_key,
            }
        )

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "CoinGeckoClient":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # noqa: ANN001
        self.close()

    def _request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        retrying = Retrying(
            reraise=True,
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=self.backoff_seconds, min=1, max=30),
            retry=retry_if_exception_type(RetryableCoinGeckoError),
        )

        for attempt in retrying:
            with attempt:
                return self._request_once(path=path, params=params)

        raise CoinGeckoClientError("Retry logic gagal mengeksekusi request.")

    def _request_once(self, *, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"

        try:
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            raise RetryableCoinGeckoError(f"Koneksi ke CoinGecko gagal untuk path: {path}") from exc

        if response.status_code == 429 or 500 <= response.status_code <= 599:
            raise RetryableCoinGeckoError(
                f"CoinGecko mengembalikan status retryable {response.status_code} untuk path: {path}"
            )

        if response.status_code >= 400:
            raise CoinGeckoClientError(
                f"CoinGecko request gagal: status={response.status_code}, path={path}"
            )

        try:
            return response.json()
        except ValueError as exc:
            raise CoinGeckoClientError(
                f"Response CoinGecko bukan JSON valid untuk path: {path}"
            ) from exc

    def get_coins_list(self, include_platform: bool = False) -> list[dict[str, Any]]:
        payload = self._request(
            path="/coins/list",
            params={"include_platform": str(include_platform).lower()},
        )
        if not isinstance(payload, list):
            raise CoinGeckoClientError("Response /coins/list bukan list.")
        return payload

    def get_coin_markets(
        self,
        *,
        ids: list[str] | None = None,
        vs_currency: str | None = None,
        order: str = "market_cap_desc",
        per_page: int = 100,
        page: int = 1,
        sparkline: bool = False,
        price_change_percentage: str = "24h",
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "vs_currency": vs_currency or self.vs_currency,
            "order": order,
            "per_page": per_page,
            "page": page,
            "sparkline": str(sparkline).lower(),
            "price_change_percentage": price_change_percentage,
        }
        if ids:
            params["ids"] = ",".join(ids)

        payload = self._request(path="/coins/markets", params=params)
        if not isinstance(payload, list):
            raise CoinGeckoClientError("Response /coins/markets bukan list.")
        return payload

    def get_coin_market_chart(
        self,
        coin_id: str,
        *,
        vs_currency: str | None = None,
        days: int | str = 1,
        interval: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "vs_currency": vs_currency or self.vs_currency,
            "days": str(days),
        }
        if interval:
            params["interval"] = interval

        payload = self._request(path=f"/coins/{coin_id}/market_chart", params=params)
        if not isinstance(payload, dict):
            raise CoinGeckoClientError(
                f"Response /coins/{coin_id}/market_chart bukan object."
            )
        return payload

    def get_global(self) -> dict[str, Any]:
        payload = self._request(path="/global")
        if not isinstance(payload, dict):
            raise CoinGeckoClientError("Response /global bukan object.")
        return payload

