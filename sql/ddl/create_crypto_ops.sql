CREATE DATABASE IF NOT EXISTS crypto_ops;
USE crypto_ops;

CREATE TABLE IF NOT EXISTS pipeline_run (
    pipeline_run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dag_id VARCHAR(128) NOT NULL,
    dag_run_id VARCHAR(128) NOT NULL,
    logical_run_ts_utc DATETIME(6) NOT NULL,
    started_at_utc DATETIME(6) NOT NULL,
    ended_at_utc DATETIME(6) NULL,
    run_status VARCHAR(32) NOT NULL,
    message TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_pipeline_run (dag_id, dag_run_id)
);

CREATE TABLE IF NOT EXISTS task_run (
    task_run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dag_id VARCHAR(128) NOT NULL,
    dag_run_id VARCHAR(128) NOT NULL,
    task_id VARCHAR(128) NOT NULL,
    started_at_utc DATETIME(6) NOT NULL,
    ended_at_utc DATETIME(6) NULL,
    task_status VARCHAR(32) NOT NULL,
    rows_extracted BIGINT NULL,
    rows_loaded BIGINT NULL,
    rows_merged BIGINT NULL,
    error_message TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_task_run_lookup (dag_id, dag_run_id, task_id)
);

CREATE TABLE IF NOT EXISTS api_request_log (
    api_request_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dag_id VARCHAR(128) NULL,
    dag_run_id VARCHAR(128) NULL,
    task_id VARCHAR(128) NULL,
    endpoint_name VARCHAR(255) NOT NULL,
    request_url TEXT NOT NULL,
    request_params_json JSON NULL,
    http_status_code INT NULL,
    attempt_no INT NOT NULL DEFAULT 1,
    requested_at_utc DATETIME(6) NOT NULL,
    responded_at_utc DATETIME(6) NULL,
    is_success TINYINT(1) NOT NULL DEFAULT 0,
    error_message TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_api_request_endpoint (endpoint_name),
    KEY idx_api_request_time (requested_at_utc)
);

CREATE TABLE IF NOT EXISTS dq_check_result (
    dq_result_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dag_id VARCHAR(128) NULL,
    dag_run_id VARCHAR(128) NULL,
    task_id VARCHAR(128) NULL,
    check_name VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    check_status VARCHAR(32) NOT NULL,
    failed_row_count BIGINT NULL,
    checked_at_utc DATETIME(6) NOT NULL,
    detail_json JSON NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_dq_target_table (target_table),
    KEY idx_dq_checked_at (checked_at_utc)
);

CREATE TABLE IF NOT EXISTS ingestion_watermark (
    watermark_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    entity_name VARCHAR(255) NOT NULL,
    watermark_key VARCHAR(255) NOT NULL,
    watermark_value VARCHAR(255) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ingestion_watermark (entity_name, watermark_key)
);

CREATE TABLE IF NOT EXISTS quarantine_bad_record (
    quarantine_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_table VARCHAR(255) NOT NULL,
    batch_id VARCHAR(64) NULL,
    business_key VARCHAR(255) NULL,
    error_type VARCHAR(255) NOT NULL,
    error_detail TEXT NULL,
    raw_payload_json JSON NULL,
    quarantined_at_utc DATETIME(6) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_quarantine_source (source_table),
    KEY idx_quarantine_batch (batch_id)
);