# AI Gateway

The AI Gateway context controls who may access an AI model and which upstream resource fulfills
that access. It is separate from the legacy cluster-chat context.

## Language

**Tenant**:
An organization-level security boundary that owns policies, models, and Provider resources.
_Avoid_: User, account, customer

**API Key**:
A bearer credential issued to a Tenant and bound to exactly one Access Policy. It identifies
machine access, not a legacy chat User.
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
