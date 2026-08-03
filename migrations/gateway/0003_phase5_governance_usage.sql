CREATE TABLE quota_policies (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    slug VARCHAR(64) NOT NULL,
    name VARCHAR(128) NOT NULL,
    status ENUM('active', 'disabled') NOT NULL DEFAULT 'active',
    rpm_limit BIGINT UNSIGNED NULL,
    concurrency_limit INT UNSIGNED NULL,
    daily_budget_microusd BIGINT UNSIGNED NULL,
    monthly_budget_microusd BIGINT UNSIGNED NULL,
    reservation_per_attempt_microusd BIGINT UNSIGNED NOT NULL DEFAULT 0,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_quota_policies_tenant_slug (tenant_id, slug),
    KEY ix_quota_policies_id_tenant (id, tenant_id),
    CONSTRAINT fk_quota_policies_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE tenants
    ADD COLUMN quota_policy_id BIGINT UNSIGNED NULL AFTER status,
    ADD CONSTRAINT fk_tenants_quota_policy FOREIGN KEY (quota_policy_id)
        REFERENCES quota_policies(id);

ALTER TABLE api_keys
    ADD COLUMN quota_policy_id BIGINT UNSIGNED NULL AFTER policy_id,
    ADD CONSTRAINT fk_api_keys_quota_policy FOREIGN KEY (quota_policy_id)
        REFERENCES quota_policies(id);

ALTER TABLE provider_credentials
    ADD COLUMN quota_policy_id BIGINT UNSIGNED NULL AFTER provider_id,
    ADD CONSTRAINT fk_provider_credentials_quota_policy FOREIGN KEY (quota_policy_id)
        REFERENCES quota_policies(id);

CREATE TABLE model_prices (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    provider_id BIGINT UNSIGNED NOT NULL,
    upstream_model VARCHAR(128) NOT NULL,
    version VARCHAR(64) NOT NULL,
    effective_at TIMESTAMP(6) NOT NULL,
    input_per_million_microusd BIGINT UNSIGNED NOT NULL,
    cached_input_per_million_microusd BIGINT UNSIGNED NOT NULL,
    output_per_million_microusd BIGINT UNSIGNED NOT NULL,
    status ENUM('active', 'disabled') NOT NULL DEFAULT 'active',
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_model_prices_provider_model_version (provider_id, upstream_model, version),
    KEY ix_model_prices_effective (provider_id, upstream_model, effective_at),
    CONSTRAINT fk_model_prices_provider FOREIGN KEY (provider_id) REFERENCES providers(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_health_checks (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    provider_id BIGINT UNSIGNED NOT NULL,
    endpoint_id BIGINT UNSIGNED NOT NULL,
    credential_id BIGINT UNSIGNED NOT NULL,
    name VARCHAR(64) NOT NULL,
    method ENUM('GET', 'HEAD') NOT NULL DEFAULT 'GET',
    url VARCHAR(2048) NOT NULL,
    interval_ms INT UNSIGNED NOT NULL DEFAULT 30000,
    timeout_ms INT UNSIGNED NOT NULL DEFAULT 2000,
    status ENUM('active', 'disabled') NOT NULL DEFAULT 'active',
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_provider_health_checks_provider_name (provider_id, name),
    CONSTRAINT fk_provider_health_checks_provider FOREIGN KEY (provider_id)
        REFERENCES providers(id),
    CONSTRAINT fk_provider_health_checks_endpoint FOREIGN KEY (endpoint_id)
        REFERENCES provider_endpoints(id),
    CONSTRAINT fk_provider_health_checks_credential FOREIGN KEY (credential_id)
        REFERENCES provider_credentials(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE usage_records (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    request_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    tenant_id BIGINT UNSIGNED NOT NULL,
    api_key_id BIGINT UNSIGNED NOT NULL,
    logical_model_id BIGINT UNSIGNED NOT NULL,
    protocol VARCHAR(32) NOT NULL,
    stream BOOLEAN NOT NULL,
    state ENUM('started', 'succeeded', 'failed', 'cancelled', 'abandoned')
        NOT NULL DEFAULT 'started',
    final_mapping_id BIGINT UNSIGNED NULL,
    final_provider_id BIGINT UNSIGNED NULL,
    final_endpoint_id BIGINT UNSIGNED NULL,
    final_credential_id BIGINT UNSIGNED NULL,
    attempt_count TINYINT UNSIGNED NOT NULL DEFAULT 0,
    failover_count TINYINT UNSIGNED NOT NULL DEFAULT 0,
    http_status SMALLINT UNSIGNED NULL,
    error_class VARCHAR(64) NULL,
    request_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
    response_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
    first_token_ms BIGINT UNSIGNED NULL,
    duration_ms BIGINT UNSIGNED NULL,
    input_tokens BIGINT UNSIGNED NULL,
    cached_input_tokens BIGINT UNSIGNED NULL,
    output_tokens BIGINT UNSIGNED NULL,
    usage_quality ENUM('exact', 'estimated', 'partial', 'unknown') NOT NULL DEFAULT 'unknown',
    cost_microusd BIGINT UNSIGNED NULL,
    cost_quality ENUM('exact', 'estimated', 'partial', 'unknown', 'not_billable')
        NOT NULL DEFAULT 'unknown',
    started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at TIMESTAMP(6) NULL,
    UNIQUE KEY uq_usage_records_request_id (request_id),
    KEY ix_usage_records_tenant_started (tenant_id, started_at),
    CONSTRAINT fk_usage_records_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_usage_records_api_key FOREIGN KEY (api_key_id) REFERENCES api_keys(id),
    CONSTRAINT fk_usage_records_model FOREIGN KEY (logical_model_id) REFERENCES logical_models(id),
    CONSTRAINT fk_usage_records_mapping FOREIGN KEY (final_mapping_id) REFERENCES model_mappings(id),
    CONSTRAINT fk_usage_records_provider FOREIGN KEY (final_provider_id) REFERENCES providers(id),
    CONSTRAINT fk_usage_records_endpoint FOREIGN KEY (final_endpoint_id)
        REFERENCES provider_endpoints(id),
    CONSTRAINT fk_usage_records_credential FOREIGN KEY (final_credential_id)
        REFERENCES provider_credentials(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE budget_periods (
    scope_type ENUM('tenant', 'api_key') NOT NULL,
    scope_id BIGINT UNSIGNED NOT NULL,
    period_kind ENUM('day', 'month') NOT NULL,
    period_start DATE NOT NULL,
    limit_microusd BIGINT UNSIGNED NOT NULL,
    reserved_microusd BIGINT UNSIGNED NOT NULL DEFAULT 0,
    settled_microusd BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (scope_type, scope_id, period_kind, period_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE budget_reservations (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    usage_record_id BIGINT UNSIGNED NOT NULL,
    scope_type ENUM('tenant', 'api_key') NOT NULL,
    scope_id BIGINT UNSIGNED NOT NULL,
    period_kind ENUM('day', 'month') NOT NULL,
    period_start DATE NOT NULL,
    reserved_microusd BIGINT UNSIGNED NOT NULL,
    settled_microusd BIGINT UNSIGNED NOT NULL DEFAULT 0,
    state ENUM('reserved', 'settled', 'released') NOT NULL DEFAULT 'reserved',
    lease_expires_at TIMESTAMP(6) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_budget_reservation_usage_scope_period
        (usage_record_id, scope_type, scope_id, period_kind),
    KEY ix_budget_reservation_expiry (state, lease_expires_at),
    CONSTRAINT fk_budget_reservations_usage FOREIGN KEY (usage_record_id)
        REFERENCES usage_records(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE request_attempts
    ADD COLUMN provider_request_id VARCHAR(255) NULL AFTER provider_status,
    ADD COLUMN first_byte_ms BIGINT UNSIGNED NULL AFTER duration_ms,
    ADD COLUMN input_tokens BIGINT UNSIGNED NULL AFTER first_byte_ms,
    ADD COLUMN cached_input_tokens BIGINT UNSIGNED NULL AFTER input_tokens,
    ADD COLUMN output_tokens BIGINT UNSIGNED NULL AFTER cached_input_tokens,
    ADD COLUMN usage_quality ENUM('exact', 'estimated', 'unknown') NOT NULL DEFAULT 'unknown'
        AFTER output_tokens,
    ADD COLUMN model_price_id BIGINT UNSIGNED NULL AFTER usage_quality,
    ADD COLUMN cost_microusd BIGINT UNSIGNED NULL AFTER model_price_id,
    ADD COLUMN cost_quality ENUM('exact', 'estimated', 'unknown', 'not_billable')
        NOT NULL DEFAULT 'unknown' AFTER cost_microusd,
    ADD CONSTRAINT fk_request_attempts_price FOREIGN KEY (model_price_id)
        REFERENCES model_prices(id);
