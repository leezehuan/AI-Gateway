# AI Gateway

The AI Gateway context controls who may access an AI model and which upstream resource fulfills
that access.

## Language

**Tenant**:
An organization-level security boundary that owns policies, models, and Provider resources.
_Avoid_: User, account, customer

**API Key**:
A bearer credential issued to a Tenant and bound to exactly one Access Policy. It identifies
machine access, not an interactive user account.
_Avoid_: Password, Provider key

**Access Policy**:
A reusable set of protocol, Logical Model, and Provider grants within one Tenant.
_Avoid_: Role, ACL

**Logical Model**:
A Tenant-scoped model name exposed to callers for one protocol. The same name may resolve
differently in another Tenant.
_Avoid_: Upstream model, deployment

**Provider**:
A Tenant-owned external AI service that can fulfill gateway requests.
_Avoid_: Logical Model, vendor key

**Endpoint**:
A protocol-specific HTTP destination belonging to a Provider.
_Avoid_: Route, Mapping

**Credential**:
A reference to secret material used to authenticate the Gateway to a Provider.
_Avoid_: API Key, plaintext secret

**Model Mapping**:
A candidate association from one Logical Model to an Endpoint, Credential, and upstream model
name.
_Avoid_: Logical Model, route

**Auth Snapshot**:
An immutable authorization decision containing the Tenant, API Key, Access Policy, grants, and
authorized Candidates for one point in time.
_Avoid_: Session, live configuration

**Route Policy**:
The Tenant-owned selection rule and attempt limit for one Logical Model.
_Avoid_: Access Policy, Model Mapping

**Candidate**:
An authorized Model Mapping that is eligible to fulfill a request.
_Avoid_: Provider, Logical Model

**Route Plan**:
The ordered Candidates available to one request after authorization and current routing state are
applied.
_Avoid_: Route Policy, retry list

**Attempt**:
One invocation of one Candidate for a request. A request may have several Attempts, but a Candidate
appears at most once.
_Avoid_: Retry, request

**Session Affinity**:
A preference that keeps related successful requests on the same Candidate without making that
Candidate mandatory.
_Avoid_: Authentication session, sticky connection

**Circuit Breaker**:
Shared temporary exclusion of an unhealthy Candidate, followed by a single trial before it becomes
generally eligible again.
_Avoid_: permanent disablement, Access Policy

**Quota Policy**:
A Tenant-owned reusable set of optional RPM, concurrency, budget, and reservation limits that can
be bound independently to a Tenant, API Key, or Credential.
_Avoid_: Access Policy, price

**Request Permit**:
The distributed admission result for one client request, containing the Tenant and API Key RPM
decision plus any active Concurrency Leases.
_Avoid_: API Key, Auth Snapshot

**Concurrency Lease**:
A Redis entry with a renewable TTL that represents one active request or Provider operation across
all Gateway nodes.
_Avoid_: database lock, permanent allocation

**Budget Reservation**:
A MySQL transaction record that temporarily reserves worst-case Attempt cost from one UTC day or
month and is later settled or released exactly once.
_Avoid_: invoice, Usage Record

**Model Price**:
An immutable, UTC-effective Provider/upstream-model price version expressed as integer micro-USD
per million input, cached input, and output tokens.
_Avoid_: quota, floating-point rate

**Usage Record**:
The unique request-level accounting terminal that aggregates Attempts, normalized token Usage,
cost, quality, and latency without storing request or response content.
_Avoid_: Attempt audit, Prompt log

**Health Check**:
An explicitly configured, non-Usage Provider GET or HEAD operation protected by a cross-node probe
lease and Credential quota whose result updates shared Candidate health.
_Avoid_: `/healthz`, billable model request

**Gateway Node**:
One `AiGateway` process with local ingress, upstream connection, and lifecycle state. Nodes share
identity, routing, and governance state but do not share local capacity counters.
_Avoid_: Tenant, Provider, cluster

**Node Capacity**:
The local maximum of admitted proxy requests and streams, held until both durable finalization and
actual downstream response completion. It is independent of distributed Concurrency Leases.
_Avoid_: Quota Policy, readiness, libcurl queue

**Drain**:
The node state entered after a termination signal in which new valid proxy requests are rejected
while already admitted work is allowed a bounded natural-completion window.
_Avoid_: dependency outage, capacity saturation, process crash

**Protocol Adapter**:
A Gateway module that recognizes one client protocol, validates its request, supplies the native
Provider headers, and classifies its JSON or SSE response. It does not convert another protocol.
_Avoid_: HTTP Handler, Provider

**Chat Completions**:
The OpenAI-compatible `/v1/chat/completions` protocol using a `messages` array and optional native
data-only SSE.
_Avoid_: Responses request, unrelated message protocol

**Anthropic Messages**:
The native `/v1/messages` protocol using Anthropic request fields and named SSE events.
_Avoid_: Chat Completions, protocol conversion

**Native Pass-through**:
A request or response remains in the selected protocol's wire shape while the Gateway only changes
the logical model, credentials, and controlled metadata.
_Avoid_: translation, normalization across providers

**Protocol Error**:
A client-visible error encoded by the selected Protocol Adapter, including its status, type,
parameter and code semantics.
_Avoid_: Provider Secret, implementation exception
