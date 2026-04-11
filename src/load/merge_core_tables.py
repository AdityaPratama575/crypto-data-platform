from __future__ import annotations

from datetime import datetime, timezone

from src.db import get_database_name, mysql_connection


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _qualified_raw_table(table_name: str) -> str:
    raw_schema = get_database_name("raw")
    return f"`{raw_schema}`.`{table_name}`"


def merge_dim_coin_from_raw_coin_list_batch(batch_id: str) -> int:
    raw_table = _qualified_raw_table("raw_coin_list_snapshot")
    sql = """
    INSERT INTO dim_coin (
        coin_id,
        symbol,
        coin_name,
        is_active,
        first_seen_at_utc,
        last_seen_at_utc
    )
    SELECT
        r.coin_id,
        MIN(r.symbol) AS symbol,
        COALESCE(MIN(r.coin_name), r.coin_id) AS coin_name,
        1 AS is_active,
        MIN(r.extracted_at_utc) AS first_seen_at_utc,
        MAX(r.extracted_at_utc) AS last_seen_at_utc
    FROM {raw_table} r
    WHERE r.batch_id = %s
    GROUP BY r.coin_id
    ON DUPLICATE KEY UPDATE
        symbol = COALESCE(VALUES(symbol), dim_coin.symbol),
        coin_name = COALESCE(VALUES(coin_name), dim_coin.coin_name),
        is_active = 1,
        first_seen_at_utc = LEAST(dim_coin.first_seen_at_utc, VALUES(first_seen_at_utc)),
        last_seen_at_utc = GREATEST(dim_coin.last_seen_at_utc, VALUES(last_seen_at_utc))
    """.format(raw_table=raw_table)
    with mysql_connection(layer="core") as core_connection:
        with core_connection.cursor() as core_cursor:
            core_cursor.execute(sql, (batch_id,))
            return core_cursor.rowcount


def merge_dim_coin_from_raw_coin_markets_batch(batch_id: str) -> int:
    raw_table = _qualified_raw_table("raw_coin_markets_snapshot")
    sql = """
    INSERT INTO dim_coin (
        coin_id,
        symbol,
        coin_name,
        is_active,
        first_seen_at_utc,
        last_seen_at_utc
    )
    SELECT
        r.coin_id,
        MIN(r.symbol) AS symbol,
        COALESCE(MIN(r.coin_name), r.coin_id) AS coin_name,
        1 AS is_active,
        MIN(r.snapshot_ts_utc) AS first_seen_at_utc,
        MAX(r.snapshot_ts_utc) AS last_seen_at_utc
    FROM {raw_table} r
    WHERE r.batch_id = %s
    GROUP BY r.coin_id
    ON DUPLICATE KEY UPDATE
        symbol = COALESCE(VALUES(symbol), dim_coin.symbol),
        coin_name = COALESCE(VALUES(coin_name), dim_coin.coin_name),
        is_active = 1,
        first_seen_at_utc = LEAST(dim_coin.first_seen_at_utc, VALUES(first_seen_at_utc)),
        last_seen_at_utc = GREATEST(dim_coin.last_seen_at_utc, VALUES(last_seen_at_utc))
    """.format(raw_table=raw_table)
    with mysql_connection(layer="core") as core_connection:
        with core_connection.cursor() as core_cursor:
            core_cursor.execute(sql, (batch_id,))
            return core_cursor.rowcount


def merge_fact_coin_market_snapshot_from_raw_batch(batch_id: str) -> int:
    now_utc = _utc_now_naive()
    raw_table = _qualified_raw_table("raw_coin_markets_snapshot")
    sql = """
    INSERT INTO fact_coin_market_snapshot (
        snapshot_ts_utc,
        coin_id,
        vs_currency,
        current_price,
        market_cap,
        market_cap_rank,
        total_volume,
        high_24h,
        low_24h,
        price_change_24h,
        price_change_percentage_24h,
        circulating_supply,
        total_supply,
        max_supply,
        ath,
        atl,
        source_batch_id,
        inserted_at_utc,
        updated_at_utc
    )
    SELECT
        r.snapshot_ts_utc,
        r.coin_id,
        r.vs_currency,
        r.current_price,
        r.market_cap,
        r.market_cap_rank,
        r.total_volume,
        r.high_24h,
        r.low_24h,
        r.price_change_24h,
        r.price_change_percentage_24h,
        r.circulating_supply,
        r.total_supply,
        r.max_supply,
        r.ath,
        r.atl,
        r.batch_id AS source_batch_id,
        %s AS inserted_at_utc,
        %s AS updated_at_utc
    FROM {raw_table} r
    INNER JOIN (
        SELECT
            snapshot_ts_utc,
            coin_id,
            vs_currency,
            MAX(raw_id) AS max_raw_id
        FROM {raw_table}
        WHERE batch_id = %s
        GROUP BY snapshot_ts_utc, coin_id, vs_currency
    ) latest
        ON latest.max_raw_id = r.raw_id
    INNER JOIN dim_coin d
        ON d.coin_id = r.coin_id
    ON DUPLICATE KEY UPDATE
        current_price = VALUES(current_price),
        market_cap = VALUES(market_cap),
        market_cap_rank = VALUES(market_cap_rank),
        total_volume = VALUES(total_volume),
        high_24h = VALUES(high_24h),
        low_24h = VALUES(low_24h),
        price_change_24h = VALUES(price_change_24h),
        price_change_percentage_24h = VALUES(price_change_percentage_24h),
        circulating_supply = VALUES(circulating_supply),
        total_supply = VALUES(total_supply),
        max_supply = VALUES(max_supply),
        ath = VALUES(ath),
        atl = VALUES(atl),
        source_batch_id = VALUES(source_batch_id),
        updated_at_utc = VALUES(updated_at_utc)
    """.format(raw_table=raw_table)
    params = (now_utc, now_utc, batch_id)
    with mysql_connection(layer="core") as core_connection:
        with core_connection.cursor() as core_cursor:
            core_cursor.execute(sql, params)
            return core_cursor.rowcount


def merge_fact_global_market_from_raw_batch(batch_id: str) -> int:
    now_utc = _utc_now_naive()
    raw_table = _qualified_raw_table("raw_global_snapshot")
    sql = """
    INSERT INTO fact_global_market (
        snapshot_ts_utc,
        active_cryptocurrencies,
        upcoming_icos,
        ongoing_icos,
        ended_icos,
        markets,
        total_market_cap_usd,
        total_volume_usd,
        market_cap_percentage_btc,
        market_cap_percentage_eth,
        market_cap_change_percentage_24h_usd,
        source_batch_id,
        inserted_at_utc,
        updated_at_utc
    )
    SELECT
        r.snapshot_ts_utc,
        r.active_cryptocurrencies,
        r.upcoming_icos,
        r.ongoing_icos,
        r.ended_icos,
        r.markets,
        r.total_market_cap_usd,
        r.total_volume_usd,
        r.market_cap_percentage_btc,
        r.market_cap_percentage_eth,
        r.market_cap_change_percentage_24h_usd,
        r.batch_id AS source_batch_id,
        %s AS inserted_at_utc,
        %s AS updated_at_utc
    FROM {raw_table} r
    INNER JOIN (
        SELECT snapshot_ts_utc, MAX(raw_id) AS max_raw_id
        FROM {raw_table}
        WHERE batch_id = %s
        GROUP BY snapshot_ts_utc
    ) latest
        ON latest.max_raw_id = r.raw_id
    ON DUPLICATE KEY UPDATE
        active_cryptocurrencies = VALUES(active_cryptocurrencies),
        upcoming_icos = VALUES(upcoming_icos),
        ongoing_icos = VALUES(ongoing_icos),
        ended_icos = VALUES(ended_icos),
        markets = VALUES(markets),
        total_market_cap_usd = VALUES(total_market_cap_usd),
        total_volume_usd = VALUES(total_volume_usd),
        market_cap_percentage_btc = VALUES(market_cap_percentage_btc),
        market_cap_percentage_eth = VALUES(market_cap_percentage_eth),
        market_cap_change_percentage_24h_usd = VALUES(market_cap_change_percentage_24h_usd),
        source_batch_id = VALUES(source_batch_id),
        updated_at_utc = VALUES(updated_at_utc)
    """.format(raw_table=raw_table)
    params = (now_utc, now_utc, batch_id)
    with mysql_connection(layer="core") as core_connection:
        with core_connection.cursor() as core_cursor:
            core_cursor.execute(sql, params)
            return core_cursor.rowcount


def merge_fact_coin_price_history_from_raw_batch(batch_id: str) -> int:
    now_utc = _utc_now_naive()
    raw_table = _qualified_raw_table("raw_coin_market_chart_point")
    sql = """
    INSERT INTO fact_coin_price_history (
        price_ts_utc,
        coin_id,
        vs_currency,
        price_value,
        market_cap_value,
        total_volume_value,
        source_batch_id,
        inserted_at_utc,
        updated_at_utc
    )
    SELECT
        r.price_ts_utc,
        r.coin_id,
        r.vs_currency,
        r.price_value,
        r.market_cap_value,
        r.total_volume_value,
        r.batch_id AS source_batch_id,
        %s AS inserted_at_utc,
        %s AS updated_at_utc
    FROM {raw_table} r
    INNER JOIN (
        SELECT
            price_ts_utc,
            coin_id,
            vs_currency,
            MAX(raw_id) AS max_raw_id
        FROM {raw_table}
        WHERE batch_id = %s
        GROUP BY price_ts_utc, coin_id, vs_currency
    ) latest
        ON latest.max_raw_id = r.raw_id
    INNER JOIN dim_coin d
        ON d.coin_id = r.coin_id
    ON DUPLICATE KEY UPDATE
        price_value = VALUES(price_value),
        market_cap_value = VALUES(market_cap_value),
        total_volume_value = VALUES(total_volume_value),
        source_batch_id = VALUES(source_batch_id),
        updated_at_utc = VALUES(updated_at_utc)
    """.format(raw_table=raw_table)
    params = (now_utc, now_utc, batch_id)
    with mysql_connection(layer="core") as core_connection:
        with core_connection.cursor() as core_cursor:
            core_cursor.execute(sql, params)
            return core_cursor.rowcount
