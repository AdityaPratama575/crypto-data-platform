CREATE DATABASE IF NOT EXISTS crypto_mart;
USE crypto_mart;

CREATE TABLE IF NOT EXISTS mart_top_coins_latest (
    snapshot_ts_utc DATETIME(6) NOT NULL,
    coin_id VARCHAR(128) NOT NULL,
    coin_name VARCHAR(255) NOT NULL,
    symbol VARCHAR(64) NULL,
    vs_currency VARCHAR(16) NOT NULL,
    market_cap_rank INT NULL,
    current_price DECIMAL(38,10) NULL,
    market_cap DECIMAL(38,10) NULL,
    total_volume DECIMAL(38,10) NULL,
    price_change_percentage_24h DECIMAL(20,10) NULL,
    inserted_at_utc DATETIME(6) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (snapshot_ts_utc, coin_id, vs_currency),
    KEY idx_mtcl_rank (market_cap_rank)
);

CREATE TABLE IF NOT EXISTS mart_coin_daily_metrics (
    metric_date DATE NOT NULL,
    coin_id VARCHAR(128) NOT NULL,
    coin_name VARCHAR(255) NOT NULL,
    symbol VARCHAR(64) NULL,
    vs_currency VARCHAR(16) NOT NULL,
    close_price DECIMAL(38,10) NULL,
    avg_price DECIMAL(38,10) NULL,
    max_price DECIMAL(38,10) NULL,
    min_price DECIMAL(38,10) NULL,
    total_volume DECIMAL(38,10) NULL,
    avg_market_cap DECIMAL(38,10) NULL,
    inserted_at_utc DATETIME(6) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (metric_date, coin_id, vs_currency)
);

CREATE TABLE IF NOT EXISTS mart_coin_rolling_metrics (
    metric_date DATE NOT NULL,
    coin_id VARCHAR(128) NOT NULL,
    coin_name VARCHAR(255) NOT NULL,
    symbol VARCHAR(64) NULL,
    vs_currency VARCHAR(16) NOT NULL,
    close_price DECIMAL(38,10) NULL,
    sma_7d DECIMAL(38,10) NULL,
    sma_30d DECIMAL(38,10) NULL,
    avg_volume_7d DECIMAL(38,10) NULL,
    avg_volume_30d DECIMAL(38,10) NULL,
    inserted_at_utc DATETIME(6) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (metric_date, coin_id, vs_currency)
);

CREATE TABLE IF NOT EXISTS mart_market_summary_daily (
    metric_date DATE PRIMARY KEY,
    total_market_cap_usd DECIMAL(38,10) NULL,
    total_volume_usd DECIMAL(38,10) NULL,
    market_cap_percentage_btc DECIMAL(20,10) NULL,
    market_cap_percentage_eth DECIMAL(20,10) NULL,
    market_cap_change_percentage_24h_usd DECIMAL(20,10) NULL,
    inserted_at_utc DATETIME(6) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL
);