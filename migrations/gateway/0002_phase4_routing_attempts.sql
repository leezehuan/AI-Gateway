CREATE TABLE route_policies (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    logical_model_id BIGINT UNSIGNED NOT NULL,
    scheduling_mode ENUM('fixed_order', 'load_balance', 'cache_affinity')
        NOT NULL DEFAULT 'fixed_order',
    max_attempts TINYINT UNSIGNED NOT NULL DEFAULT 3,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_route_policies_model (logical_model_id),
    CONSTRAINT chk_route_policies_max_attempts CHECK (max_attempts BETWEEN 1 AND 10),
    CONSTRAINT fk_route_policies_model FOREIGN KEY (logical_model_id)
        REFERENCES logical_models(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE model_mappings
    ADD COLUMN priority SMALLINT UNSIGNED NOT NULL DEFAULT 100 AFTER upstream_model;

CREATE TABLE request_attempts (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    attempt_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    attempt_number TINYINT UNSIGNED NOT NULL,
    tenant_id BIGINT UNSIGNED NOT NULL,
    api_key_id BIGINT UNSIGNED NOT NULL,
    logical_model_id BIGINT UNSIGNED NOT NULL,
    mapping_id BIGINT UNSIGNED NOT NULL,
    provider_id BIGINT UNSIGNED NOT NULL,
    endpoint_id BIGINT UNSIGNED NOT NULL,
    credential_id BIGINT UNSIGNED NOT NULL,
    stream BOOLEAN NOT NULL,
    state ENUM('started', 'succeeded', 'failed', 'cancelled') NOT NULL DEFAULT 'started',
    provider_status SMALLINT UNSIGNED NULL,
    error_class VARCHAR(64) NULL,
    retryable BOOLEAN NOT NULL DEFAULT FALSE,
    possible_duplicate_cost BOOLEAN NOT NULL DEFAULT FALSE,
    response_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
    started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at TIMESTAMP(6) NULL,
    duration_ms BIGINT UNSIGNED NULL,
    UNIQUE KEY uq_request_attempts_attempt_id (attempt_id),
    UNIQUE KEY uq_request_attempts_request_number (request_id, attempt_number),
    KEY ix_request_attempts_tenant_started (tenant_id, started_at),
    KEY ix_request_attempts_candidate_started (provider_id, endpoint_id, credential_id, started_at),
    CONSTRAINT fk_request_attempts_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_request_attempts_api_key FOREIGN KEY (api_key_id) REFERENCES api_keys(id),
    CONSTRAINT fk_request_attempts_model FOREIGN KEY (logical_model_id) REFERENCES logical_models(id),
    CONSTRAINT fk_request_attempts_mapping FOREIGN KEY (mapping_id) REFERENCES model_mappings(id),
    CONSTRAINT fk_request_attempts_provider FOREIGN KEY (provider_id) REFERENCES providers(id),
    CONSTRAINT fk_request_attempts_endpoint FOREIGN KEY (endpoint_id) REFERENCES provider_endpoints(id),
    CONSTRAINT fk_request_attempts_credential FOREIGN KEY (credential_id)
        REFERENCES provider_credentials(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
