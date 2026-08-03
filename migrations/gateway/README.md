# Gateway Schema Migrations

Gateway migrations use monotonically increasing filenames in the form
`NNNN_description.sql`. Apply them offline with:

```shell
build/gateway/bin/AiGatewayAdmin migrate --dir migrations/gateway
```

Applied versions and SHA-256 checksums are recorded in `schema_migrations`. Reapplying an unchanged
directory is idempotent; changing an applied file is a checksum error. The Gateway accepts exactly
the latest Phase 4 schema version and reports not-ready for an older or newer schema.

`0001_phase3_identity_policy.sql` owns Tenant, API Key, Access Policy, grant, Provider, Endpoint,
Credential, Logical Model, and Model Mapping data.

`0002_phase4_routing_attempts.sql` adds one Route Policy per Logical Model, Mapping priority, and an
immutable identity plus terminal status for each Provider Attempt. Attempt rows deliberately omit
Provider URLs, credential material, affinity/session values, Prompts, and response bodies.

Chat, conversation, Usage, quota, pricing, and budget tables do not belong in this migration
context. Migrations contain static DDL; runtime and administrative DML use prepared statements.
