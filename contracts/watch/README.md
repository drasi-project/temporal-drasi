# Watch API v1

Standalone contract module:
`github.com/drasi-project/temporal-drasi/contracts/watch` (Go 1.23+).
Neither consumers nor the companion UI server import the Temporal server module.
The Go dependency on `go.temporal.io/api` supplies standard workflow execution,
input payload, and workflow ID policy types.

## Imports and generated artifacts

| Artifact | Location |
|---|---|
| Authoritative protobuf | `proto/drasi/watch/v1/service.proto` |
| Protobuf package | `drasi.watch.v1` |
| Go messages, clients, servers, gateway | `github.com/drasi-project/temporal-drasi/contracts/watch/v1`, package `watchpb` |
| Boundary validation and method policy | `github.com/drasi-project/temporal-drasi/contracts/watch/validation` |
| REST OpenAPI 2 | `openapi/watch.swagger.json` |
| TypeScript messages and JSON conversion | `typescript/drasi/watch/v1/service.ts` |
| Node package subpath | `@drasi-project/temporal-watch-contracts/v1` |

TypeScript represents protobuf oneofs as discriminated unions:
`{ operation: { $case: 'start', value: start } }`.
Use the generated message's `toJSON` before sending to the HTTP gateway and
`fromJSON` after receiving a response. Wire JSON uses flattened protobuf JSON
oneofs, camelCase fields, base64 bytes, enum names, and **string** uint64 values.
Do not pass the in-memory union directly to `JSON.stringify`.
Imported Temporal and Google TypeScript files are generated from pinned inputs;
applications need no runtime dependency on the Temporal server or Node APIs.

## Unpublished development pin

Before a public contract revision is available, create a local content-addressed
artifact from this module:

```sh
python3 scripts/package-development.py
```

`.tools/distribution/watch-contract-dev-<sha256>.tgz` is an ordinary npm package
containing the standalone Go module, generated gateway/validation, authoritative
proto, OpenAPI and generated TypeScript, including imported types. Its adjacent
JSON manifest records every packaged file, npm integrity, and the archive SHA256.
The explicit package allowlist excludes tool caches and unrelated local files,
including outside a Git checkout. Repeating the command with unchanged inputs
reuses the identical pin and refuses conflicting existing artifacts.

The UI may retain the artifact inside its own repository, verify the SHA256,
and install it as a local npm dependency. Extract `package/` into a repository-local
directory for the Go gateway's relative module replacement; do not reference a
different checkout or the Temporal server module. Use the same archive for both
languages. This is unsigned local development input, not a published module,
trusted plugin artifact, signed release or authorization bypass. The npm lockfile
and build tools are not in the consumer archive; regenerate in the source module,
not the vendored consumer copy.

`make test-package-development` packages twice, checks the pin and every archive
member, runs standalone Go tests and TypeScript JSON tests from an isolated
extraction outside the source checkout, and repackages that consumer after build
caches exist. It also checks refusal to overwrite corrupt artifacts or symlinks.
It uses the source module's installed development compiler, not a packaged tool
cache. No publishing or plugin signature verification is performed.

## Method and message map

Namespace routes begin with `/api/v1/namespaces/{namespace}/drasi`.
`WatchService` provides:

| Methods | Request | Response |
|---|---|---|
| `GetCapabilities` | `GetCapabilitiesRequest` | `Capabilities` |
| `CreateSource`, `UpdateSource` | `CreateSourceRequest`, `UpdateSourceRequest` | `Source` |
| `GetSource`, `ListSources` | `GetSourceRequest`, `ListSourcesRequest` | `Source`, `ListSourcesResponse` |
| `DeleteSource`, `StartSource`, `StopSource` | `SourceOperationRequest` | `Source` |
| `CreateQuery`, `UpdateQuery` | `CreateQueryRequest`, `UpdateQueryRequest` | `Query` |
| `GetQuery`, `ListQueries` | `GetQueryRequest`, `ListQueriesRequest` | `Query`, `ListQueriesResponse` |
| `DeleteQuery`, `StartQuery`, `StopQuery` | `QueryOperationRequest` | `Query` |
| `ValidateQuery`, `PreviewQuery` | `ValidateQueryRequest`, `PreviewQueryRequest` | Corresponding response |
| `CreateWatch`, `UpdateWatch` | `CreateWatchRequest`, `UpdateWatchRequest` | `Watch` |
| `GetWatch`, `ListWatches` | `GetWatchRequest`, `ListWatchesRequest` | `Watch`, `ListWatchesResponse` |
| `DeleteWatch`, `PauseWatch`, `ResumeWatch`, `CancelWatch` | `WatchOperationRequest` | `Watch` |
| `ResetWatch` | `ResetWatchRequest` | `Watch` |
| `ListDeliveries`, `RetryDelivery` | `ListDeliveriesRequest`, `RetryDeliveryRequest` | `ListDeliveriesResponse`, `Delivery` |

Resources contain `namespace`, `id`, `revision`, `spec`, accepted `desired_state`,
and separately observed `status`. A successful Start/Pause/Stop response changes
desired state; it must not fabricate observed readiness. Protobuf JSON exposes
`desiredState` with the `DESIRED_STATE_*` enum names.
Lifecycle intent and observed health are resource metadata, not part of an
immutable Watch execution definition's identity.

The current server enriches Get/List responses from the cooperative Watch owner.
Observation is optional: an unhosted namespace has no status; unavailable, stale,
or mismatched runtime evidence produces UNSPECIFIED with a redacted diagnostic.
Definition reads remain available during runtime failure. `observed_revision`
names the public revision whose desired goal the runtime has actually observed,
not the accepted revision or a native activation. Zero means no such confirmation.
`updated_at` is the oldest contributing runtime sample, not the Get/List time.
Watch RUNNING requires ready inputs and a completed initial snapshot.
`pending_deliveries` is sampled; `accepted_output` remains absent until a complete
immutable output identity is available. Terminal one-shot conditions report
COMPLETED, CANCELED, or EXPIRED without inferring an owner-closure reason.

Create requests contain `namespace`, `id`, `spec`, and `request_id`.
Updates add `expected_revision`; lifecycle operations carry `namespace`, `id`,
`expected_revision`, and `request_id`. Definitions fully replace their prior
revision. `ResourceRef` pins an ID and nonzero revision in the same namespace.
`SourceSpec` selects the built-in Temporal source or an installed external source,
with an optional separate external bootstrap component. `QuerySpec` pins sources,
language, query text, and an administrator-configured persistent state store.

New Query Source pins and new Watch Query pins must reference the current resource
incarnation. An update may retain its existing exact pins, including superseded
definitions; lifecycle controls and Watch reset also preserve those pins. A new
Watch may pin a current Query that itself retains older Source pins. Changing a
direct pin cannot revive a superseded incarnation, including after deletion and
recreation. This policy permits permanent retirement of unreferenced native nodes.
Deploy it to every frontend and History registry owner before enabling retirement.

`SecretReference` contains an administrator provider ID, key and immutable
`version`, never a secret value. Fresh Source definitions and replacements require
versions for both source and bootstrap references. The writer selects the exact
provider generation; Temporal does not select or fall back to a moving alias.
Changing credentials requires an explicit Source replacement. Queries pinned to
older Source revisions retain those original references.

`validation.ValidateSourceDefinition` applies this acceptance policy after the
server's idempotency receipt lookup. `ValidateRequest` remains structural so
legacy provider/key-only requests can reach their original stored receipts.
Legacy definitions remain readable and are not rewritten; their missing versions
cannot authorize credential recovery. Existing lifecycle controls do not replace
their definitions. Fresh semantic rejections are also bound to their request IDs.
Deploy the policy to every frontend and History registry owner before enabling
writers that rely on it. The added field is wire-compatible, but older owners
cannot enforce the fresh-definition policy.

Ordinary component configuration must contain literal JSON with at most 32 value
levels. Full `${NAME}` / `${NAME:-default}` strings and structured
`kind: Secret` / `kind: EnvironmentVariable` objects are reserved SDK references
and are rejected recursively. Embedded text such as `prefix${NAME}suffix` remains
literal. Configuration cannot also supply fields listed in `secrets`. These
checks do not validate descriptor schemas, authorize providers or prove Source
readiness. Plugin-backed production reconciliation and exact-version provider
integration remain unsupported until their separate runtime gates are satisfied.

Cluster routes begin with `/api/v1/drasi/plugins`. `PluginService` exposes:
`GetCapabilities`, `ListCatalogs`, `ListPlugins` (including versions), `GetPlugin`, `InstallPlugin`,
`ListInstallations`, `GetInstallation`, and `LoadPlugin`. Each method takes its
matching `*Request` message, except `GetCapabilities`, which takes the empty
`GetPluginCapabilitiesRequest` and returns `PluginCapabilities`. Discovery returns catalogs or `Plugin` metadata;
installation/loading returns `PluginInstallation` with per-host observed states.
No method accepts a registry URL, file path, native binary, or trust override.
Catalog and signer configuration belongs to administrators outside this API.
Descriptor schemas are untrusted data, not executable UI code or remote URLs to fetch.

`GET /api/v1/drasi/plugins/capabilities` uses `ACCESS_CLUSTER_READ` without
requiring a namespace or probing writes. `PluginCapabilities` contains `enabled`,
`api_version`, `allowed_methods`, `max_request_bytes`, and `message`.
The backend filters `allowed_methods` to supported PluginService RPCs authorized
for the current caller; install/load controls require their exact full method
names to appear. Capability responses do not prove signature verification or
native loading, and permissions are rechecked on every subsequent request.
The namespace `WatchService.GetCapabilities(GetCapabilitiesRequest) -> Capabilities`
is unchanged and cannot substitute for this cluster authorization scope.

## Behavioral contract

- Mutations acknowledge **durable control-plane acceptance**, never implied
  runtime readiness, restored continuity, or successful workflow effects.
  Unsupported persistence/recovery must fail rather than synthesize success.
- A `request_id` identifies a mutation and its original contents. Retry lookup
  precedes revision/deadline validation and returns its original result.
  Reusing the ID with different contents fails. Updates/lifecycle changes use
  nonzero compare-and-swap `expected_revision`; conflicts must not overwrite edits.
  Revisions must not be reused for a namespace/resource ID after deletion and recreation.
- `Trigger` describes result-set ENTER, UPDATE, or LEAVE, not database operations.
  IGNORE is the default for recurring bootstrap. INCLUDE opts in existing results.
  One-shot conditions are ENTER-only and satisfy an already-true condition after
  consistent bootstrap even under IGNORE. Their owner includes an exact run and
  their action must signal that same run before the absolute deadline.
- Pause disables new acceptance while accepted intents remain visible and drain.
  Delete disables new acceptance and schedules idempotent cleanup; the response
  can remain DELETING. It cannot retract an RPC already sent. One-shot cancel,
  satisfaction, expiry, and owner closure converge on one terminal state.
- A reset is explicit, selects IGNORE/INCLUDE for bootstrap, and establishes a
  new output incarnation. Missing replay never silently resets or skips.
- Accepted progress is **per Watch/reaction**, not a shared query acknowledgment.
  Sources and query engines may be shared. Stable action identity includes
  namespace, Watch ID/revision, query incarnation, sequence, row identity, and
  diff ordinal; ownership epochs do not change it. Accepted intents retain
  their original definition, action, payload, and resolved target.
- Per-Watch bootstrap snapshots carry the query's current as-of sequence, which
  can be zero for a new query or greater than zero for an existing query. Live
  deltas follow that sequence; the first output need not be sequence one.
  Intake tracks snapshot completion separately from sequence values. A present
  `accepted_output` with sequence zero is not an absent cursor. Protobuf JSON
  may omit the zero-valued `sequence` field; decoders restore its zero default.
- Start IDs may include the sole supported `{action_id}` token. Standard
  workflow ID reuse/conflict policies apply, except destructive termination is
  rejected. Same-ID retries use the original action/request identity.
  A run-targeted signal never follows continue-as-new or workflow ID reuse.
  A current-workflow target is resolved once and pinned before dispatch.
- APPEND adds a protobuf-JSON `ResultChange` encoded as a standard `json/plain`
  Temporal payload after static input. Signal-with-start appends it to signal
  input only; start input stays unchanged. OMIT sends only static input.
  Inputs preserve the caller's data-converter encoding and metadata.
- Delivery is at least once with scoped deduplication, not permanent end-to-end
  exactly-once effects. DELIVERED requires a recorded successful workflow RPC.
  A lost response outside verified deduplication scope is AMBIGUOUS, not success.
  Retrying an ambiguous effect requires explicit `acknowledge_ambiguous`.
- Delivery listing includes retained lifetimes after edits, reset, deletion, and
  recreation. Each result keeps the original immutable Watch definition revision,
  query output identity, resolved workflow target, attempt count, and recorded
  transition time even after the request payload is discarded. Listings are not
  permanent audit logs. Pending and recent metadata are byte-bounded, completed
  controls follow Temporal retention, and evicted details are not reconstructed.
  Pagination uses immutable output positions rather than mutable pending-list
  offsets. Tokens are bound to a namespace UUID, Watch ID, and registry generation;
  concurrent definition changes require restarting the list. Delivery outcomes
  can change between pages. A bounded scan may return an empty page with a
  continuation token. `next_attempt_at` is absent when the History task scheduler
  has not published an authoritative next-attempt time.
- `RetryDelivery.expected_revision` checks the current public Watch revision,
  not the accepting revision of an older action or the private control revision.
  Accepted retries remain valid after subsequent edits, pause, or deletion.
  A permanent namespace-registry receipt pins the original target before the
  control atomically records its own retry receipt and schedules dispatch.
  Replaying the same command returns the original retry decision, not a later
  delivery outcome, and cannot renew a consumed ambiguous-effect acknowledgment.
  Changing any caller content under the same `request_id` is rejected. Semantic
  rejections are retained too. Receipt capacity exhaustion fails closed without
  evicting identities; each control retains at most 1,024 retry commands within a
  1 MiB control-state budget, for the lifetime of that retained execution.
- Discovery signer metadata is not verification. VERIFIED/LOADED host states
  require digest, signature, trusted identity, ABI, SDK/core, and platform checks
  before native initialization. Loading is desired state, not immediate success.
  In-use replacements need drain/checkpoint/restart; no live unload is offered.

## Backend and UI gateway registration

Register gRPC services using generated `RegisterWatchServiceServer` and
`RegisterPluginServiceServer`. Add `validation.UnaryServerInterceptor` to the
serving interceptor chain, or register `validation.WrapWatchService(server)` and
`validation.WrapPluginService(server)`.

Both Temporal frontend and the companion Go UI gateway can use generated
`RegisterWatchServiceHandler`, `RegisterWatchServiceHandlerFromEndpoint`, and
the equivalent Plugin methods to forward to an authenticated gRPC connection.
For in-process dispatch use **validation**'s
`RegisterWatchServiceHandlerServer` / `RegisterPluginServiceHandlerServer`.
Generated direct-dispatch helpers alone do not validate or run interceptors.

`validation.Policy(fullMethod)` exposes descriptor-derived namespace read/write
and cluster read/admin access. `validation.Namespace(request)` extracts scope.
Applications must enforce authorization, rate limits, credentials propagation,
and write-disable controls themselves; validation does not grant access.
ValidateQuery and PreviewQuery are read-only despite using HTTP POST.
Apply HTTP body and gRPC receive limits **before decoding**, then the structural
1 MiB request, 200-item page, and 200-row preview caps. Lower deployment limits
and capabilities take precedence. Check referenced revisions, runtime schemas,
secret references/redaction, query languages, deadlines against server time,
resource state transitions, and catalog trust in the serving implementation.

The Temporal frontend implements `ValidateQuery` for the native Cypher/GQL
compiler and built-in Temporal source. It resolves every exact Source revision
atomically, then compiles without registering a Query, subscribing, opening a
runtime or acquiring namespace ownership. Compiler acceptance does not prove
source readiness, property types or successful evaluation against future data.
Invalid compilation returns a fixed diagnostic; operational failures remain
errors. Unsupported external source schemas are rejected explicitly.

Native compilation is advertised only when a Watch host answers the capability
probe. Each advertised method is still authorized by the frontend, and a
definition-only deployment does not claim native support. Query text is limited
to 65,536 bytes, with two admitted native compiler jobs per process and no waiting
queue. The two-second internode budget cannot preempt synchronous parser work.
`PreviewQuery` remains unavailable and is not advertised.

## Reproducible generation and verification

From this directory, with Go 1.23+, Node 20+, Python 3, and network access:

```sh
npm ci --no-audit --no-fund
make generate
make check-generated
make test
make lint-code-fast
```

`scripts/generate.sh` installs exact Go plugin versions into ignored `.tools/bin`.
The npm lock pins Buf's compiler, ts-proto, and TypeScript. Public proto inputs
are pinned to immutable commits **and SHA-256 hashes** in
`proto-dependencies.lock.json`; Google well-known types come from the pinned Buf
compiler. Generation verifies field comments and derives all transport adapters
and authorization policies from service descriptors. The check regenerates and
compares bytes, including untracked files, restoring originals if it finds drift.
For intentional proto dependency changes, edit the revision pins in
`scripts/fetch-protos.py` and explicitly refresh the lock with `--update-lock`
after removing the corresponding cached inputs.

`make test` runs `go test -tags test_dep ./...`, TypeScript type checking, and
the generated TypeScript JSON round-trip test.
`make lint-code-fast` runs this independent module's tagged `go vet`; it does not
replace server linting. Transport tests exercise real generated gRPC and gateway
dispatch with an in-memory gRPC listener, union rejection, payload preservation,
method metadata, and boundary constraints. They are contract tests, **not**
native Drasi, PostgreSQL, durability, authorization, or full-system E2E evidence.
