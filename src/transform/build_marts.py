from __future__ import annotations

from datetime import datetime, timezone

from src.db import get_database_name, mysql_connection


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _qualified_core_table(table_name: str) -> str:
    core_schema = get_database_name("core")
    return f"`{core_schema}`.`{table_name}`"


def refresh_mart_top_coins_latest() -> int:
    now_utc = _utc_now_naive()
    fact_table = _qualified_core_table("fact_coin_market_snapshot")
    dim_table = _qualified_core_table("dim_coin")
    sql = f"""
    INSERT INTO mart_top_coins_latest (
        snapshot_ts_utc,
        coin_id,
        coin_name,
        symbol,
        vs_currency,
        market_cap_rank,
        current_price,
        market_cap,
        total_volume,
        price_change_percentage_24h,
        inserted_at_utc,
        updated_at_utc
    )
    SELECT
        f.snapshot_ts_utc,
        f.coin_id,
        d.coin_name,
        d.symbol,
        f.vs_currency,
        f.market_cap_rank,
        f.current_price,
        f.market_cap,
        f.total_volume,
        f.price_change_percentage_24h,
        %s AS inserted_at_utc,
        %s AS updated_at_utc
    FROM {fact_table} f
    INNER JOIN (
        SELECT MAX(snapshot_ts_utc) AS latest_snapshot_ts_utc
        FROM {fact_table}
    ) latest
        ON latest.latest_snapshot_ts_utc = f.snapshot_ts_utc
    INNER JOIN {dim_table} d
        ON d.coin_id = f.coin_id
    ON DUPLICATE KEY UPDATE
        coin_name = VALUES(coin_name),
        symbol = VALUES(symbol),
        market_cap_rank = VALUES(market_cap_rank),
        current_price = VALUES(current_price),
        market_cap = VALUES(market_cap),
        total_volume = VALUES(total_volume),
        price_change_percentage_24h = VALUES(price_change_percentage_24h),
        updated_at_utc = VALUES(updated_at_utc)
    """

    with mysql_connection(layer="mart") as mart_connection:
        with mart_connection.cursor() as mart_cursor:
            mart_cursor.execute(sql, (now_utc, now_utc))
            return mart_cursor.rowcount


def refresh_mart_market_summary_daily() -> int:
    now_utc = _utc_now_naive()
    fact_table = _qualified_core_table("fact_global_market")
    sql = f"""
    INSERT INTO mart_market_summary_daily (
        metric_date,
        total_market_cap_usd,
        total_volume_usd,
        market_cap_percentage_btc,
        market_cap_percentage_eth,
        market_cap_change_percentage_24h_usd,
        inserted_at_utc,
        updated_at_utc
    )
    SELECT
        DATE(f.snapshot_ts_utc) AS metric_date,
        f.total_market_cap_usd,
        f.total_volume_usd,
        f.market_cap_percentage_btc,
        f.market_cap_percentage_eth,
        f.market_cap_change_percentage_24h_usd,
        %s AS inserted_at_utc,
        %s AS updated_at_utc
    FROM {fact_table} f
    INNER JOIN (
        SELECT DATE(snapshot_ts_utc) AS metric_date, MAX(snapshot_ts_utc) AS latest_snapshot_ts_utc
        FROM {fact_table}
        GROUP BY DATE(snapshot_ts_utc)
    ) latest
        ON latest.latest_snapshot_ts_utc = f.snapshot_ts_utc
    ON DUPLICATE KEY UPDATE
        total_market_cap_usd = VALUES(total_market_cap_usd),
        total_volume_usd = VALUES(total_volume_usd),
        market_cap_percentage_btc = VALUES(market_cap_percentage_btc),
        market_cap_percentage_eth = VALUES(market_cap_percentage_eth),
        market_cap_change_percentage_24h_usd = VALUES(market_cap_change_percentage_24h_usd),
        updated_at_utc = VALUES(updated_at_utc)
    """

    with mysql_connection(layer="mart") as mart_connection:
        with mart_connection.cursor() as mart_cursor:
            mart_cursor.execute(sql, (now_utc, now_utc))
            return mart_cursor.rowcount


def refresh_mart_coin_daily_metrics() -> int:
    now_utc = _utc_now_naive()
    fact_history = _qualified_core_table("fact_coin_price_history")
    dim_coin = _qualified_core_table("dim_coin")
    sql = f"""
    INSERT INTO mart_coin_daily_metrics (
        metric_date,
        coin_id,
        coin_name,
        symbol,
        vs_currency,
        close_price,
        avg_price,
        max_price,
        min_price,
        total_volume,
        avg_market_cap,
        inserted_at_utc,
        updated_at_utc
    )
    SELECT
        agg.metric_date,
        agg.coin_id,
        d.coin_name,
        d.symbol,
        agg.vs_currency,
        close_row.price_value AS close_price,
        agg.avg_price,
        agg.max_price,
        agg.min_price,
        agg.total_volume,
        agg.avg_market_cap,
        %s AS inserted_at_utc,
        %s AS updated_at_utc
    FROM (
        SELECT
            DATE(f.price_ts_utc) AS metric_date,
            f.coin_id,
            f.vs_currency,
            AVG(f.price_value) AS avg_price,
            MAX(f.price_value) AS max_price,
            MIN(f.price_value) AS min_price,
            SUM(f.total_volume_value) AS total_volume,
            AVG(f.market_cap_value) AS avg_market_cap
        FROM {fact_history} f
        GROUP BY DATE(f.price_ts_utc), f.coin_id, f.vs_currency
    ) agg
    INNER JOIN (
        SELECT
            DATE(f.price_ts_utc) AS metric_date,
            f.coin_id,
            f.vs_currency,
            MAX(f.price_ts_utc) AS latest_price_ts_utc
        FROM {fact_history} f
        GROUP BY DATE(f.price_ts_utc), f.coin_id, f.vs_currency
    ) latest
        ON latest.metric_date = agg.metric_date
       AND latest.coin_id = agg.coin_id
       AND latest.vs_currency = agg.vs_currency
    INNER JOIN {fact_history} close_row
        ON close_row.price_ts_utc = latest.latest_price_ts_utc
       AND close_row.coin_id = latest.coin_id
       AND close_row.vs_currency = latest.vs_currency
    INNER JOIN {dim_coin} d
        ON d.coin_id = agg.coin_id
    ON DUPLICATE KEY UPDATE
        coin_name = VALUES(coin_name),
        symbol = VALUES(symbol),
        close_price = VALUES(close_price),
        avg_price = VALUES(avg_price),
        max_price = VALUES(max_price),
        min_price = VALUES(min_price),
        total_volume = VALUES(total_volume),
        avg_market_cap = VALUES(avg_market_cap),
        updated_at_utc = VALUES(updated_at_utc)
    """

    with mysql_connection(layer="mart") as mart_connection:
        with mart_connection.cursor() as mart_cursor:
            mart_cursor.execute(sql, (now_utc, now_utc))
            return mart_cursor.rowcount


def refresh_mart_coin_rolling_metrics() -> int:
    now_utc = _utc_now_naive()
    sql = """
    INSERT INTO mart_coin_rolling_metrics (
        metric_date,
        coin_id,
        coin_name,
        symbol,
        vs_currency,
        close_price,
        sma_7d,
        sma_30d,
        avg_volume_7d,
        avg_volume_30d,
        inserted_at_utc,
        updated_at_utc
    )
    SELECT
        base.metric_date,
        base.coin_id,
        base.coin_name,
        base.symbol,
        base.vs_currency,
        base.close_price,
        base.sma_7d,
        base.sma_30d,
        base.avg_volume_7d,
        base.avg_volume_30d,
        %s AS inserted_at_utc,
        %s AS updated_at_utc
    FROM (
        SELECT
            d.metric_date,
            d.coin_id,
            d.coin_name,
            d.symbol,
            d.vs_currency,
            d.close_price,
            AVG(d.close_price) OVER (
                PARTITION BY d.coin_id, d.vs_currency
                ORDER BY d.metric_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS sma_7d,
            AVG(d.close_price) OVER (
                PARTITION BY d.coin_id, d.vs_currency
                ORDER BY d.metric_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS sma_30d,
            AVG(d.total_volume) OVER (
                PARTITION BY d.coin_id, d.vs_currency
                ORDER BY d.metric_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS avg_volume_7d,
            AVG(d.total_volume) OVER (
                PARTITION BY d.coin_id, d.vs_currency
                ORDER BY d.metric_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS avg_volume_30d
        FROM mart_coin_daily_metrics d
    ) base
    ON DUPLICATE KEY UPDATE
        coin_name = VALUES(coin_name),
        symbol = VALUES(symbol),
        close_price = VALUES(close_price),
        sma_7d = VALUES(sma_7d),
        sma_30d = VALUES(sma_30d),
        avg_volume_7d = VALUES(avg_volume_7d),
        avg_volume_30d = VALUES(avg_volume_30d),
        updated_at_utc = VALUES(updated_at_utc)
    """

    with mysql_connection(layer="mart") as mart_connection:
        with mart_connection.cursor() as mart_cursor:
            mart_cursor.execute(sql, (now_utc, now_utc))
            return mart_cursor.rowcount
