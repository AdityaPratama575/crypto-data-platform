CREATE DATABASE IF NOT EXISTS crypto_core;
USE crypto_core;

CREATE TABLE IF NOT EXISTS dim_coin (
    coin_id VARCHAR(128) PRIMARY KEY,
    symbol VARCHAR(64) NULL,
    coin_name VARCHAR(255) NOT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    first_seen_at_utc DATETIME(6) NOT NULL,
    last_seen_at_utc DATETIME(6) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_dim_coin_symbol (symbol),
    KEY idx_dim_coin_name (coin_name)
);

CREATE TABLE IF NOT EXISTS fact_coin_market_snapshot (
    snapshot_ts_utc DATETIME(6) NOT NULL,
    coin_id VARCHAR(128) NOT NULL,
    vs_currency VARCHAR(16) NOT NULL,
    current_price DECIMAL(38,10) NULL,
    market_cap DECIMAL(38,10) NULL,
    market_cap_rank INT NULL,
    total_volume DECIMAL(38,10) NULL,
    high_24h DECIMAL(38,10) NULL,
    low_24h DECIMAL(38,10) NULL,
    price_change_24h DECIMAL(38,10) NULL,
    price_change_percentage_24h DECIMAL(20,10) NULL,
    circulating_supply DECIMAL(38,10) NULL,
    total_supply DECIMAL(38,10) NULL,
    max_supply DECIMAL(38,10) NULL,
    ath DECIMAL(38,10) NULL,
    atl DECIMAL(38,10) NULL,
    source_batch_id VARCHAR(64) NOT NULL,
    inserted_at_utc DATETIME(6) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (snapshot_ts_utc, coin_id, vs_currency),
    CONSTRAINT fk_fcms_dim_coin FOREIGN KEY (coin_id) REFERENCES dim_coin (coin_id),
    KEY idx_fcms_rank (market_cap_rank),
    KEY idx_fcms_coin_id (coin_id)
);

CREATE TABLE IF NOT EXISTS fact_coin_price_history (
    price_ts_utc DATETIME(6) NOT NULL,
    coin_id VARCHAR(128) NOT NULL,
    vs_currency VARCHAR(16) NOT NULL,
    price_value DECIMAL(38,10) NULL,
    market_cap_value DECIMAL(38,10) NULL,
    total_volume_value DECIMAL(38,10) NULL,
    source_batch_id VARCHAR(64) NOT NULL,
    inserted_at_utc DATETIME(6) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (price_ts_utc, coin_id, vs_currency),
    CONSTRAINT fk_fcph_dim_coin FOREIGN KEY (coin_id) REFERENCES dim_coin (coin_id),
    KEY idx_fcph_coin_id (coin_id)
);

CREATE TABLE IF NOT EXISTS fact_global_market (
    snapshot_ts_utc DATETIME(6) PRIMARY KEY,
    active_cryptocurrencies INT NULL,
    upcoming_icos INT NULL,
    ongoing_icos INT NULL,
    ended_icos INT NULL,
    markets INT NULL,
    total_market_cap_usd DECIMAL(38,10) NULL,
    total_volume_usd DECIMAL(38,10) NULL,
    market_cap_percentage_btc DECIMAL(20,10) NULL,
    market_cap_percentage_eth DECIMAL(20,10) NULL,
    market_cap_change_percentage_24h_usd DECIMAL(20,10) NULL,
    source_batch_id VARCHAR(64) NOT NULL,
    inserted_at_utc DATETIME(6) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL
);