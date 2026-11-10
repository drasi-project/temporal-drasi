# Watch resource registries

This library persists reusable Sources, Queries, and Watches in one CHASM root
per namespace. It uses the public `contracts/watch/v1` protobuf messages directly.
It performs no native, network, secret-provider, or workflow RPC I/O inside a
CHASM transaction.

## Integration

1. Add `watchregistry.Module` to the server's CHASM library FX modules, wherever
   the other CHASM libraries are registered. `Library.RegisterServices` registers
   the private `RegistryService` on History's gRPC server. The library must be
   registered on every process that deserializes this component.
2. `watchregistry.FrontendModule` is included in Temporal's frontend FX module.
   It registers the public WatchService and PluginService on the existing gRPC server, with contract
   validation and the standard Temporal authorization, namespace, rate-limit,
   metrics, and error interceptors. Capabilities report only supported methods
   authorized for the current caller. Native capability flags remain false until
   the runtime provides the corresponding behavior.

   Provide `registrypb.NewRegistryServiceLayeredClient` in the dedicated Watch
   role's FX graph. It returns `registrypb.RegistryServiceClient` and uses the
   existing History shard redirector, ownership caching, metrics, and retry
   machinery. Import:

   ```go
   "go.temporal.io/server/chasm/lib/watchregistry"
   registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
   ```

3. Resolve the authorized public namespace name to its Temporal namespace UUID.
   Every private request must carry that UUID and
   `ExecutionId: watchregistry.BusinessID`. For example:

   ```go
   result, err := registry.Mutate(ctx, &registrypb.MutateRequest{
       NamespaceId: namespaceID,
       ExecutionId: watchregistry.BusinessID,
       Operation: &registrypb.MutateRequest_CreateSource{
           CreateSource: publicRequest,
       },
   })
   // On success, result.Record.GetSource() is the ORIGINAL accepted response.
   ```

   Other CRUD/lifecycle methods use the corresponding typed oneof variants.
   Reads similarly wrap public Get/List requests in `registrypb.ReadRequest`.
   `ReadResponse.Records` contains typed public resources and their separate
   desired state; its page token maps to the public list response's token.
   `ReadRequest_Revision` resolves a public `ResourceRef` to its exact retained
   definition. No bytes decoding, JSON translation, or server-module types enter
   the public API.
4. Reconcile committed desired records asynchronously. Resume discovery with
   complete paginated lists; if pagination reports a changed generation,
   restart the list instead of using a partial scan for reconciliation/dependency
   decisions. Lists include deleted/canceled records so cleanup intent is not
   lost. New Sources/Queries desire STOPPED; new Watches desire RUNNING.
   Definition updates preserve the prior desired state.
5. Public resources expose accepted lifecycle intent as `desired_state`, matching
   private `Record.Desired`. This field is persisted in new revisions and
   projected for retained older records without modifying their stored bytes.
   Synthesize observed public `status` using the native runtime and the
   per-Watch `watch.ControlService`. Registry resource statuses are deliberately
   nil. `Record.Desired` is not an observed `LifecycleState`: a deleted or paused
   Watch may still have accepted deliveries draining. Never label an accepted
   effect retracted merely because desired state changed.

The private RPCs are **not** an authorization boundary for browser clients.
The frontend's standard claim mapper and authorizer enforce public method and
namespace permissions on every RPC. The Watch runtime must enforce native
plugin/schema/language/state-store policy. This registry
enforces authoritative structural contract validation, namespace-ID/name
consistency, the default-off `dynamicconfig.EnableDrasiWatches` setting, and
global-namespace rejection for new acceptance. Reads and disabling cleanup
(stop/pause/delete/cancel) remain available while acceptance is disabled. Neither
this flag nor successful registration claims native runtime readiness.

ValidateQuery, PreviewQuery, deliveries, retrying deliveries, and runtime
assignment are not resource registry operations. They remain with the native
runtime and per-Watch delivery controller. Do not make durable acceptance depend
on a subsequent synchronous runtime RPC: replay reconciliation after a crash or
lost response.

## Cluster plugin installation intent

A separate `clusterPlugins` CHASM component retains cluster installation intent.
It never adds records to namespace Source/Query/Watch pages. The private
`ClusterPluginService` accepts only the registered local `temporal-system`
namespace UUID and `ExecutionId: watchregistry.ClusterPluginBusinessID`. Its
generated layered client uses the normal History routing and retry machinery.
These internal RPCs do not authorize browser access.

Each installation selects an exact catalog, plugin ID, version, and digest.
Server-resolved metadata is detached and retained once; subsequent resolution
cannot overwrite the selection or enrich its schemas. Runtime-dependent
`compatible`, diagnostic `message`, and descriptor `supported` claims are
cleared. Persisted metadata is not evidence that an artifact was verified,
installed, or loaded on any host.

`LoadClusterPlugin` selects one desired digest per native plugin ID, including
across catalogs. A fresh request for another retained version supersedes that
desired selection; it does not replace resident factories, stop existing queries,
or claim loading succeeded. The native process still requires its controlled
restart/drain policy for a different resident artifact. Snapshot `desired_loads`
is separate from immutable installation metadata. Its generation changes only
when installation or desired selection changes.

Permanent compact receipts fingerprint the original public InstallPlugin or
LoadPlugin request, including the method, not current resolution or host
observations. Both methods share one permanent request-ID domain. Matching
retries recover the accepted digest even after catalog removal or a later desired
selection. An old load retry does not reselect an obsolete version. Reusing an
identity with changed method/content fails. A load requested before installation
retains NotFound even if the artifact is subsequently installed; use a new
request ID. Semantic rejections remain rejected on retry; transient failures are
not retained. Installation, receipt, and complete-state bounds are checked
atomically. Defaults are 128 installations, 1,024 requests, and 1 MiB; constructor
maxima are 256 installations, 4,096 requests, and 2 MiB. Full request/state
capacity refuses unknown identities without evicting old receipts. Termination
cannot erase them.

Snapshot reads return detached, digest-sorted metadata and a generation; an absent
root has an empty generation-zero snapshot. Receipt reads distinguish an unknown
identity from an empty snapshot. Transactions perform no native, catalog, or
secret-provider I/O.

The public PluginService replays these receipts before catalog resolution, then
projects current desired state and complete per-host observations. Namespace
roles do not authorize cluster plugin mutations. Both public services use the
standard frontend interceptor chain over gRPC and the generated HTTP routes.
Accepted installation or load intent is not evidence that native verification
or initialization succeeded.

## Consistency, pins, and retries

All changes serialize through the same namespace CHASM root. Each non-create
mutation compares the current revision with the public `expected_revision`.
Successful lifecycle operations also allocate a new revision. Deleted records
remain high-water tombstones, so recreation never reuses a revision.

Every revision remains readable. Updating a Source or Query does not retarget
an older Query or Watch. Query and Watch registration verifies every exact pin,
including the sources of a historically pinned Query revision. Source deletion
checks both current Query definitions and the historical Query revisions pinned
by current Watches. Query deletion checks current Watches. Paused/stopped
dependents still pin resources. Deleted/canceled Watches release their runtime
dependency checks, but all definitions remain retained for inspection and accepted
delivery reconciliation. Stopping a resource is explicit control, not deletion
of a pin; the runtime must apply current desired control to all consumed revisions
of that resource ID.

`request_id` has **namespace-wide** scope across every mutation method. The
deterministic typed request, including the method oneof, is fingerprinted.
Receipt lookup runs before feature-enable, stale-revision, dependency, and
server-time deadline checks. Exact retries return the original resource and
revision, not today's state. Content or method reuse is rejected. Semantic
rejections (including stale CAS) are also retained, so retrying a rejected
command cannot turn into a different mutation after state changes. A caller
must use a fresh request ID for a corrected command.

Malformed structural requests and requests rejected before capacity admission do
not reserve identities and do not mutate resources. Retention never expires or
evicts entries. At capacity, unknown request IDs fail with `ResourceExhausted`;
known exact retries continue to work. There is no bounded-ring claim of
permanent idempotency, nor an expired unknown identity that can be executed again.
Do not delete/recreate the namespace registry execution to recover capacity.

One-shot owners, exact signal runs, and absolute deadlines are validated using
the public contract; owner and deadline are immutable for that resource lifetime.
New accepting changes require a future deadline according to CHASM server time.
Retries and disabling cleanup do not. Creation and definition replacement allocate
a new `Record.Incarnation` for Sources, Queries, and Watches. Lifecycle-only
revisions preserve it, so start/stop and pause/resume do not replace native
evaluation identity. An explicit Watch reset allocates a new incarnation and
freezes the requested IGNORE/INCLUDE bootstrap policy in its revision.

Older retained Source/Query records can have an empty incarnation. Reads and
exact retries preserve those records; they must not invent a lifetime by using
the current lifecycle revision. Native admission must reject missing definition
identity until an explicit definition replacement establishes one.

List order is resource ID order. Continuation tokens bind the namespace, kind,
owner filter, and committed generation. Concurrent mutation fails continuation
explicitly rather than silently skipping or duplicating resources. Individual
reads and every dependency check observe one CHASM transaction. Byte-limited
pages may contain fewer resources than requested without omitting a continuation.

## Finite bounds

`ModuleWithLimits(*registrypb.Limits)` replaces `Module` for administrator-chosen
limits. Limits are validated and fixed in the persisted root at creation;
changing FX configuration cannot weaken existing roots' guarantees.

| Limit | Default | Maximum allowed |
|---|---:|---:|
| Distinct resource kind/ID pairs, including tombstones | 128 | 512 |
| Permanent request receipts | 1,024 | 4,096 |
| Entire serialized registry state | 1 MiB | 2 MiB |
| Individual public request | 64 KiB | 128 KiB |
| Read response page | 256 KiB | 512 KiB |

State includes current definitions, all historical revisions, original responses,
fingerprints, and rejected-request receipts. Successful revisions cannot outgrow
the receipt count. The byte limit can be reached before a count limit.
Pagination additionally observes the public 200-item cap; Query source references
observe the public 64-source cap. Every transaction scans/clones at most the
bounded retained state and performs no I/O. Resource capacity is not reclaimed by
deletion. Capacity increases require an explicit future migration of the existing
root, not a new business ID, eviction, or namespace registry termination.
The registry rejects termination to protect these guarantees.

This is a deliberately bounded first registry, not an unlimited tenant catalog.
It has no automated compaction, sharding, runtime observation persistence, or
cleanup-completion acknowledgment. Deleted records remain discoverable as desired
cleanup intent until overwritten by a higher-revision recreation.

## Generation and verification

The standard `make proto` pipeline reproduces the protobuf, internal gRPC, and
CHASM layered client. Its dependency image includes the generated public Watch
contract and its transitive imports. CHASM output preserves the declared package
name and API version, and uses the server's standard enum constant names.

```sh
make proto
go test -tags test_dep ./chasm/lib/watchregistry/...
.bin/golangci-lint-v2.13.0 run --allow-parallel-runners \
  --config=.github/.golangci.yml --build-tags=test_dep \
  ./chasm/lib/watchregistry/...
CGO_ENABLED=0 go test -tags test_dep,integration \
  -run '^(TestDrasiDefinitionAPIsWithoutNativeRuntime|TestDrasiClusterPluginIntentRPC)$' ./tests
CGO_ENABLED=1 go test -race -tags test_dep,integration \
  -run '^TestDrasiClusterPluginIntentRPC$' ./tests
```

Tests exercise the CHASM engine, reload serialized CHASM snapshots after every
successful transition, and invoke the registered private service over real gRPC.
Public-handler tests cover all typed definition methods, pagination and owner
filters, desired-state projection without invented observations, malformed
backend responses, and real gRPC authorization denial before History mutation.
Cluster installation tests reconstruct persisted CHASM snapshots, cover immutable
receipts and atomic limits, and check fixed-key/read-selector boundaries.
`TestDrasiClusterPluginIntentRPC` uses a dedicated actual History cluster to
exercise concurrent initial installation, exact retries, a lost response after
commit, concurrent version selections, load supersession without old-retry
rollback, changed request content, and foreign namespace refusal. It deliberately
does not concurrently invoke the single-threaded `chasmtest.Engine`.

`tests/TestDrasiNativeOutputStartsWorkflow` uses the real frontend WatchService,
SQLite-backed History registry, native result intake and SDK workflow. It also
checks original creation retries and disabling cleanup after the namespace gate
is turned off. This is not a production runtime reconciler, PostgreSQL recovery,
UI gateway, or browser proof.
The no-cgo integration fixture exercises the same actual frontend/History APIs
with the namespace gate initially disabled. It verifies immutable rejection and
acceptance receipts across gate changes, and stop/delete cleanup without native
dependencies.
