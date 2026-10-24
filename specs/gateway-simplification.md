# AI Gateway Simplification Before Coroutine Refactoring

## Problem Statement

Gateway request handling currently combines Redis session bindings, health-score routing,
periodic Provider health checks, duplicate cost calculations, callback-driven request orchestration,
and three native request protocols. This creates state and control flow that operators and
maintainers must understand even when the desired behavior is simply to select an eligible
Candidate, account for its Usage, and complete the request reliably.

The change should reduce that complexity without weakening authorization, quotas, billing,
Attempt and Usage persistence, cancellation, streaming backpressure, failover, or graceful drain.
Native Anthropic Messages support is no longer required. Existing persisted Anthropic configuration
must not be silently deleted or rewritten.

## Solution

Simplify routing to deterministic, stateless Candidate selection and circuit breaking. Remove
scheduled health probes, Redis affinity bindings, health-score state, and half-open probe leases.
Keep protocol-specific Usage parsing but calculate price in one shared place. Use coroutines to
orchestrate request stages while preserving the existing streaming chunk and backpressure contract
and the bounded workers used for blocking database operations. Remove native Anthropic Messages
support while retaining Responses and Chat Completions.

## User Stories

1. As a Gateway operator, I want related requests to select a Candidate through stable hashing,
   so that requests can reach the same Provider without a Redis session-binding record.
2. As a Gateway operator, I want stable Candidate identities to drive hashing,
   so that unrelated configuration-version changes do not reshuffle callers across Providers.
3. As a caller using cache-aware upstream fields, I want the Gateway to preserve those fields,
   so that the Provider can apply its own prompt-cache behavior.
4. As a Gateway operator, I want cache affinity to remain a routing preference rather than a
   cache-hit guarantee, so that Provider cache behavior is not misrepresented.
5. As a caller whose preferred Candidate fails, I want the current request to fail over only when
   the response has not been committed, so that the Gateway does not emit multiple responses.
6. As a Gateway operator, I want a successful fallback not to create a persistent session binding,
   so that future routing remains deterministic and stateless.
7. As an operator using primary/backup routing, I want fixed-order routing to remain available,
   so that configured priority behavior is preserved.
8. As a Gateway operator, I want scheduled Provider health checks and their configuration path
   removed, so that the Gateway does not generate separate synthetic upstream traffic.
9. As a Gateway operator, I want a Candidate circuit to reopen after repeated retryable request
   failures and cooldown, so that real request outcomes continue to protect routing.
10. As a Gateway operator, I want Candidates to become directly eligible after circuit cooldown,
    without a half-open probe lease, so that routing has no separate trial-request state.
11. As a Gateway operator, I want the health-score state and ranking removed, so that Candidate
    ordering depends on priority and the selected routing policy rather than a second health scale.
12. As a Tenant, I want both Responses and Chat Completions requests to continue working,
    so that removing one protocol does not disrupt the supported OpenAI-compatible APIs.
13. As a caller, I want `/v1/messages` to stop being recognized as a native Anthropic protocol,
    so that the Gateway exposes only the protocols it intends to support.
14. As an administrator, I want new configuration using an unsupported protocol to be rejected,
    so that removed protocol support cannot be reintroduced through configuration import.
15. As an operator with existing Anthropic records, I want to migrate them manually before rollout,
    so that persisted production data is not automatically deleted or rewritten.
16. As a Gateway maintainer, I want each Protocol Adapter to parse its own upstream Usage shape,
    so that protocol-specific field differences remain explicit.
17. As a Gateway maintainer, I want normalized Usage to pass through one shared pricing calculation,
    so that token cost formulas do not diverge between protocol and request accounting paths.
18. As a Tenant, I want day/month monetary budgets, reservations, refunds, and recovery to retain
    their current behavior, so that routing and orchestration cleanup does not change spending limits.
19. As an operator, I want Attempt and request-level Usage records to retain their current
    persistence requirements, so that accounting remains auditable.
20. As a caller, I want request authorization, admission, routing, Provider Attempts, auditing,
    and finalization to be coordinated with coroutines, so that the main request flow is easier to
    follow than a distributed callback state machine.
21. As a caller, I want streaming chunks to keep using the existing callback and backpressure
    behavior, so that responses remain incremental and slow clients do not cause event loss.
22. As a caller, I want disconnect cancellation to cancel the in-flight Provider operation,
    so that abandoned work does not continue consuming upstream resources.
23. As a Gateway operator, I want blocking database calls to remain on bounded workers while
    coroutine orchestration awaits their results, so that database work does not block the network
    executor or create unbounded work queues.
24. As a Gateway operator, I want required audit writes to finish before Provider invocation and
    finalization writes to finish before capacity release, so that audit and capacity guarantees
    remain intact.
25. As a Gateway operator, I want distributed RPM and concurrency limits and graceful drain to
    remain in force, so that the simplification preserves cluster-level admission and shutdown behavior.

## Implementation Decisions

- Routing uses priority plus deterministic rendezvous hashing for load-balanced/session-related
  requests; fixed-order routing remains available for primary/backup deployments.
- Redis Session Affinity bindings and their TTL are removed. Stable Candidate identity is used for
  hash inputs. Cache-related upstream request fields remain unchanged. Routing does not promise a
  Provider cache hit or retain a successful fallback as the next request's Candidate.
- Remove health-score state and ordering, scheduled Provider Health Checks and their scheduling
  configuration, and half-open probe leases. Keep the Circuit Breaker failure count and cooldown;
  after cooldown, requests are directly eligible and real request feedback updates circuit state.
- Protocol Adapters continue translating protocol-specific usage fields into normalized Usage.
  One shared pricing calculation consumes normalized Usage. Monetary budgets, reservations,
  refunds, Attempt accounting, and request-level Usage semantics remain unchanged.
- Coroutines orchestrate authorization, admission, route planning, Candidate Attempts, and
  finalization. Request-stage callbacks are adapted to awaitables where useful.
- Streaming chunk delivery remains on the existing callback/backpressure interface. Blocking
  database work remains on bounded workers, with coroutine code awaiting completion.
- Preserve audit-before-call, failure compensation, cancellation, pre-commit-only failover,
  execution plus downstream-response completion before Node Capacity release, distributed
  RPM/concurrency admission, and graceful drain.
- Remove native Anthropic Messages support, including request recognition and validation, Provider
  headers, SSE classification, and protocol-specific errors. Keep Responses and Chat Completions.
- Require operators to migrate existing Anthropic configuration manually before rollout. Do not
  automatically delete or rewrite existing database records; reject new unsupported protocol
  configuration.

## Testing Decisions

- Verify externally visible routing, cooldown, request accounting, cancellation, streaming, and
  protocol behavior. Tests should assert request/response outcomes, selected upstream behavior,
  persisted accounting, and configuration acceptance or rejection rather than private coroutine
  structure or Redis key names.
- Verify that Responses and Chat Completions remain supported and that `/v1/messages` and new
  Anthropic configuration are rejected.
- Preserve regression coverage for budget reservation/refund/recovery, audit-before-call, Usage
  persistence, pre-commit failover, streaming backpressure, and graceful drain.

## Out of Scope

- Removing monetary budgets, Usage accounting, Attempt persistence, audit requirements, distributed
  RPM/concurrency controls, or graceful drain.
- Moving audit or Usage persistence to best-effort background recording.
- Replacing the database driver or removing the bounded database worker queue.
- Rewriting streaming transport chunk delivery as an awaitable stream API.
- Automatically migrating, deleting, or rewriting existing Anthropic database records.
- Converting between Anthropic Messages, Responses, and Chat Completions wire formats.
- Guaranteeing Provider-side prompt-cache hits or persisting successful fallback choices.

## Further Notes

- This spec records the confirmed plan and the implementation boundaries used for the current
  change. Existing Anthropic records remain subject to manual migration.
