# Gateway Schema Migrations

Gateway migrations use monotonically increasing filenames in the form
`NNNN_description.sql`. They are applied offline and recorded in a future `schema_migrations` table.

Phase 0/1 deliberately contains no domain migration because the gateway uses static environment
configuration and does not access MySQL. Tenant, API key, Provider, route, and audit tables start in
Phase 3; chat tables are never copied into this directory.
