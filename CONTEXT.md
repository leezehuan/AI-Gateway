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
resolved Model Mapping observed at one configuration version.
_Avoid_: Session, live configuration
