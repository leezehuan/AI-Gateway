# Gateway Schema Migrations

Gateway migrations use monotonically increasing filenames in the form
`NNNN_description.sql`. Apply them offline with:

```shell
build/gateway/bin/AiGatewayAdmin migrate --dir migrations/gateway
```

Applied versions and SHA-256 checksums are recorded in `schema_migrations`. Reapplying an unchanged
directory is idempotent; changing an applied file is a checksum error. The Gateway accepts exactly
the Phase 3 schema version and reports not-ready for an older or newer schema.

`0001_phase3_identity_policy.sql` owns Tenant, API Key, Access Policy, grant, Provider, Endpoint,
Credential, Logical Model, and Model Mapping data. Chat, conversation, Usage, quota, pricing, and
request-attempt tables do not belong in this migration context.
