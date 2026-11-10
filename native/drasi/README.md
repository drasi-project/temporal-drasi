# Native Drasi host

This crate embeds unmodified published Drasi packages pinned in `Cargo.toml`.
`Cargo.lock` retains their exact registry versions and checksums. Temporal owns
the query orchestration and foreign-source process boundary. The Go adapter is
`service/watch/native`. The ordinary server
build does not link Rust; native hosting requires both `temporal_with_drasi` and
cgo.

```sh
bash native/drasi/build.sh
CGO_ENABLED=1 go test -tags test_dep,temporal_with_drasi ./service/watch/native
make build-tags-check
bash native/drasi/verify.sh
```

The build-tag check verifies native file selection requires both cgo and
`temporal_with_drasi`, including with `temporal_minimal`. Nested Go modules run
their tests from their own module roots, not through the server's test selection.
Source inventories exclude ignored native build artifacts and upstream caches.
The native verifier includes regression checks for these boundaries.

## Real-service UI verification fixture

The opt-in fixture starts an isolated SQLite-backed Temporal cluster, a separate
native Watch service, and an actual Go SDK worker. It is not a stock development
server, mock API, or production deployment:

The full default browser suite also needs an administrator-reviewed plugin policy
with a nonempty pinned catalog for its incompatibility/refusal scenario. Set
`TEMPORAL_DRASI_PLUGIN_CONFIG` to that policy's JSON path on the fixture command.
Without a policy, the native workflow fixture still works, but the browser
catalog scenario fails its setup guard before exercising discovery or refusal.

```sh
bash native/drasi/build.sh
TEMPORAL_DRASI_UI_FIXTURE="$PWD/native/drasi/.verification/ui-fixture.json" \
  GOMAXPROCS=2 CGO_ENABLED=1 RUST_LOG=error TEMPORAL_TEST_LOG_LEVEL=error \
  go test -count=1 -tags test_dep,integration,temporal_with_drasi \
    ./temporal -run '^TestDrasiUIFixture$' -v -timeout 25m
```

The manifest path must be unused. Once the cluster, namespace, assignment and
worker are ready, the fixture atomically publishes a mode-0600 manifest containing
the frontend gRPC/HTTP endpoints, namespace/task queue, registered workflow types,
a built-in execution-source query, and protected test controls. Configure the
Temporal UI gateway against its `temporalAddress`. The fixture uses a test-only
default-allow authorizer; production authorization is not established by this
fixture.

With the fixture running, use the retained acceptance runner from the Temporal UI
checkout, using its documented Node and pnpm versions:

```sh
DRASI_UI_PORT=8086 \
  DRASI_FIXTURE_MANIFEST=/absolute/path/to/temporal-drasi/native/drasi/.verification/ui-fixture.json \
  pnpm test:drasi
```

The runner builds the production UI and canonical Go gateway. It exercises native
Source, Query and Watch forms against this fixture, then checks real SDK workflow
effects for Start, exact-run once Signal and SignalWithStart. Queries isolate each
run's producer, and once Watches use fresh exact owner executions. Plugin discovery,
installation and loading are checked against actual host observations; an
incompatible library must remain a visible refusal, not Loaded. Sanitized screenshots
are retained in the UI checkout's `audits` directory.

This borrowed-fixture mode stops its own gateway, not this server fixture. Send the
controlled stop below after browser verification, before running slower unrelated
checks. See the UI checkout's `docs/drasi-ui.md` for its runner and cleanup contract.
This does not establish positive PostgreSQL browser acceptance.

The worker registers `DrasiUIProducer`, `DrasiUIConsumer`, `DrasiUISignalTarget`
and `DrasiUICondition`. The manifest's signal target is already running.
Authenticated `POST /produce` with `{"workflowId":"a-unique-test-id"}` starts the
producer, signals it to finish, and waits for real SDK completion. Consumer and
signal workflows return the received Drasi result JSON. Use the manifest's
`controlURL` and `Authorization: Bearer <controlToken>` only for these loopback
test controls; do not expose or log the token.

After browser verification, send an authenticated `POST /stop`. The test must
then exit successfully and remove its manifest; exceeding its 20-minute lifetime
without a controlled stop is a failure, not successful verification. For example:

```sh
python3 - native/drasi/.verification/ui-fixture.json <<'PY'
import json
import sys
import urllib.request

with open(sys.argv[1]) as source:
    fixture = json.load(source)
request = urllib.request.Request(
    fixture["controlURL"] + "/stop",
    data=b"",
    headers={"Authorization": "Bearer " + fixture["controlToken"]},
)
with urllib.request.urlopen(request, timeout=10) as response:
    assert response.status == 204
PY
```

`TEMPORAL_DRASI_PLUGIN_CONFIG` accepts a native plugin-policy JSON file containing
the administrator-configured catalogs and preload/managed pins.
Signatures are not required for this proof of concept. Removing signing policy
does not make incompatible public PostgreSQL libraries compatible or establish
PostgreSQL browser acceptance. External-source
capability remains false without a credential reader, an explicitly reviewed
managed source/bootstrap pair, and fresh Loaded observations for both
artifacts in management and every active namespace runtime. The capability is
not authorization for a particular credential or a database-readiness lease.

### Opt-in PostgreSQL browser fixture

Set `TEMPORAL_DRASI_UI_POSTGRES=1` on the same `TestDrasiUIFixture` command to add
an owned, digest-pinned PostgreSQL 17 container. The supplied plugin policy must
already contain exactly one reviewed PostgreSQL source/bootstrap pair and
unambiguous immutable catalog selections. Missing review fails before Docker
startup; the fixture does not select unconfigured plugins or bypass loading.
The default fixture starts no PostgreSQL container.

The private manifest adds `postgres.source` and `postgres.bootstrap`, each with
`catalogId`, `id`, `version`, `digest`, `component`, nonsecret `configuration`,
and immutable `{provider,key,version}` `secrets` references. A generated password
stays private. The Watch reader authorizes the actual registered namespace UUID
and exact reference and returns caller-owned bytes.

Authenticated `POST /postgres/prepare` accepts only `{"runId":"<canonical UUID>"}`.
It transactionally creates a per-run publication and seeds a bootstrap row before
the browser starts its Source. The response is exactly
`{runId,source,bootstrap,queryText,bootstrapRow,insertRow,updatedRow}`. Returned
components have authoritative per-run configuration and references; their pins
must match the manifest. Each run has distinct publication/slot names, and the
Source owns creation of its replication slot. Rows project
`{item_id,name,amount}`; the query filters the run UUID and `amount >= 10`.

Authenticated `POST /postgres/mutate` accepts only
`{runId,operation:"insert"|"update"|"delete"}` and responds only with
`{runId,operation}` after committing. The ordered operations insert amount 18,
update the bootstrap row from 12 to 24, then delete the inserted row. Database
row locks and transactionally committed phases make every accepted retry a
no-op, including after later phases. Repeated preparation never reseeds a run.
Future out-of-order operations fail, and the fixture accepts at most 32 runs.
There is no arbitrary SQL control.

From the UI checkout, explicitly select the positive-only browser scenario:

```sh
DRASI_POSTGRES_E2E=1 \
  DRASI_FIXTURE_MANIFEST=/absolute/path/to/temporal-drasi/native/drasi/.verification/ui-fixture.json \
  pnpm test:drasi
```

Manifest presence is not readiness. The browser must observe the actual
pair Loaded with supported descriptors and then the external-source capability.
It compares real bootstrap/insert/update/delete ResultChange payloads to actual
Start/exact-run Signal delivery and SDK completion, not SQL acknowledgments.
Delivered payloads use `json/plain` canonical protobuf JSON; the fixture's SDK
completed result is an outer JSON string containing that ResultChange JSON.
The default builtin/refusal browser scenarios remain separate.

The same authenticated `/stop` and joined fixture exit are required. Controls
drain before workers and Watch stop; the owned PostgreSQL container is removed
last. A failed service/control shutdown retains the container and fails cleanup
rather than removing PostgreSQL beneath live native work.

Real SQL-control tests can run independently of compatible plugins:

```sh
TEMPORAL_DRASI_UI_POSTGRES_SQL=1 CGO_ENABLED=1 \
  go test -race -count=1 -tags test_dep,integration,temporal_with_drasi \
    ./temporal -run '^TestDrasiUIPostgresSQLControls$' -timeout 5m
```

These tests verify committed rows, rollback, concurrent/lost-response retries,
and absence of extra PostgreSQL mutations. They do not load a Source, exercise
CDC, or establish PostgreSQL browser acceptance. Compatible loaded artifacts
are still required for that positive proof; signatures and publication are not.

## Native build and verification

The full verifier requires Python 3.12+ for safe package extraction.

The build first links and runs a minimal C ABI client against its actual output.
An invalid image or ABI mismatch fails before replacing the published library.
It publishes the shared library into the ignored `native/drasi/lib`
directory using a sibling staging file and atomic rename. Concurrent processes
retain their previously mapped inode rather than reading a truncated library.
On macOS, the library's install name is `@rpath/libtemporal_drasi.dylib`, so
executables resolve the published library instead of Cargo's mutable build output.
Both debug and optimized native builds disable stripping to avoid the Rust
toolchain's
[misaligned Mach-O string-table regression](https://github.com/rust-lang/rust/issues/157750).
A deployment must package that library, its matching
`temporal-drasi-plugin-worker` companion, and their runtime dependencies;
the Go test's development library path is not a deployment mechanism.

## Relocatable server package

Build a native-enabled server and its matching shared library on the destination
platform with Go, Rust, Python 3.12+, and the normal native build prerequisites:

```sh
GOMAXPROCS=2 python3 native/drasi/package.py --profile debug /existing/directory/temporal-drasi.tar.gz
mkdir /existing/directory/temporal-drasi
tar -xzf /existing/directory/temporal-drasi.tar.gz -C /existing/directory/temporal-drasi
/existing/directory/temporal-drasi/bin/temporal-server --version
```

Use `--profile release` for an optimized native library; it is the packaging
default. Both profiles remain **unpublished development builds**, not
release or compatible-plugin evidence. Existing output archives are
refused rather than replaced. The archive has `bin/temporal-server`, the matching
`lib/libtemporal_drasi` shared library, the matching
`lib/temporal-drasi-plugin-worker` executable, Temporal/Drasi licenses, documentation,
and a manifest
with artifact hashes, native ABI, exact crates.io dependency checksums
and actual Rust compiler/Go build metadata. Archive timestamps and ownership are
normalized.

The packaging command adds an executable-relative `bin/../lib` linker path.
Its package-only `temporal_drasi_packaged` build tag omits the checkout fallback.
Ordinary native development builds retain their existing checkout path. The
bundle can be moved as a unit; moving only the executable is not supported.
Native Linux/macOS amd64 and arm64 builds are supported by the command;
cross-target builds and redirected `CARGO_TARGET_DIR` output are refused rather
than publishing stale artifacts from the expected native output directory.
Linux packages require a compatible glibc/C++ runtime from the build platform,
not the repository's ordinary no-cgo Alpine server image. macOS packages require
a compatible macOS system runtime. Arbitrary plugin dependencies are not bundled.

The retained relocation proof builds an archive, verifies every manifest hash,
moves its contents to another directory, runs the server and actual native query
evaluation with durable replay, and constructs, starts and stops a copied SDK
Source fixture through the relocated companion. It requires missing bundled
worker/library failures. The library-loader check runs despite the development
library remaining in this checkout:

```sh
GOMAXPROCS=2 python3 native/drasi/tests/test-package.py
GOMAXPROCS=2 python3 native/drasi/tests/test-package.py --profile release
GOMAXPROCS=2 python3 native/drasi/tests/test-package.py --profile release /existing/directory/temporal-drasi.tar.gz
```

Provision Temporal persistence, service listeners, dynamic configuration and the
stable Watch volume separately as documented below. This package does not create
schemas, start services, configure a credential provider, install PostgreSQL
plugins, include the Temporal UI, or establish PostgreSQL/browser acceptance.

## Published Drasi dependencies

The integration pins unmodified crates.io packages in `Cargo.toml` and
`Cargo.lock`. It uses `drasi-lib` 0.9.3, host/plugin SDK 0.11.3 and core 0.5.10,
matching the public PostgreSQL source 0.2.12 and bootstrap 0.2.15 release family.
No Drasi Git dependencies, patches, path overrides, vendored forks, or upstream
changes are required. The index, state-store and WAL packages are also pinned
to published releases rather than unpublished workspace version bumps.

Release-family alignment is not complete runtime compatibility. The published
SDK's dedicated bootstrap path does not durably write its initial result rows,
and failed-stop cleanup and foreign provider ownership differ from the former
development snapshot. The dependency switch alone does not repair these gaps.
Native cold-recovery, failed-shutdown, plugin reopen and PostgreSQL gates
must pass before this build can be treated as a supported integration.

Query creation registers a Temporal-owned implementation of the public SDK
`Query` trait. It uses public Core evaluators and persistent indexes, not the
SDK's `DrasiQuery` bootstrap processor. Complete bounded source snapshots, source
cuts, keyed result rows and a versioned query seal commit in one index session.
The initial snapshot has sequence zero and an empty outbox. Future evaluation
starts only after that baseline commits. Restart validates the seal, rows,
configuration, generation and retained outbox before publishing readiness.
Missing or corrupt committed state is refused, not reset or silently bootstrapped.
A separate native control-store witness fences the index commit and survives loss
of the entire query index directory, including future-only query state with no
Watch. Bootstrap failures before that fence remain retryable. An uncertain commit
without its sealed indexes requires explicit recovery rather than reseeding.
The host preserves its separate admission identity, pending handoffs and receipts,
and checks output generation and contiguous positions before accepting output.
Permanent query retirement uses ordinary `remove_query`, which reclaims the
retired query's SDK indexes, checkpoints and output. Host tombstones and accepted
delivery records remain durable.

The selected foreign-source boundary uses one supervised companion process per
Source, with child-local state files included in the native checkpoint tree.
Only that child loads foreign libraries. It uses `src/plugins/native.rs` and the
public FFI registration/function tables, without private SDK layout casts.
Host WAL injection is disabled: SDK0.11.3's WAL bridge passes Rust-layout values
across library boundaries. PostgreSQL0.2.12 supports native LSN replay without it;
other sources must satisfy their administrator-reviewed native replay contract.
Opaque positions that the SDK cannot preserve are refused before acknowledgement.
Source stop requires an actual child reap and joined IPC owners. Final close also
prevents a retained Source reference from starting another process before
credentials are revoked. The companion is resolved beside the actual loaded
native library, not from `PATH`, the Go executable, or a checkout fallback.
The unsigned native PostgreSQL fixture exercises bootstrap, live CDC, offline
changes, cold recovery and query stop/restart through this boundary.
Configured-plugin installation, complete package and browser acceptance remain separate gates.

This is an unpublished Temporal development build. The unmodified dependency uses SDK/host
`0.11.3`, binary FFI ABI `0.14.0`, core `0.5.10`, lib `0.9.3`, PostgreSQL source
`0.2.12`, and bootstrap `0.2.15`. The native C ABI remains version 1.

`build.sh` and `verify.sh` run `verify-dependencies.py`. It checks that every
resolved Drasi package has exactly one registry version, matches its locked
crate checksum, and has unmodified source bytes matching the downloaded
archive. Changed sources, additional files and source symlinks fail verification
rather than being reset or silently repaired. Package provenance records the
actual registry versions, checksums and licenses; it does not claim a Git tree.

Before invoking Cargo directly, run:

```sh
python3 native/drasi/verify-dependencies.py
```

Cargo, Python 3.11 or newer and initial registry access are required; populated
Cargo caches can be reused offline. Checkpoint `upstream` and the persisted
native runtime definition identify the complete locked Drasi package set by
its version/checksum digest, followed by `+temporal-native-<package version>` for
Temporal's owned storage/runtime format. Format changes require a native package
version change. Restore, warm open, dormant admission, and retirement
refuse other or missing build identities before opening query indexes or
rewriting retained admission state. There is no automatic cross-build storage
migration, including from the previous unpublished Git build and its newer
RocksDB format.

Older Git caches and evidence remain historical and do not verify this registry
build. They are not selected or modified by production dependencies or
verification. Existing runtime storage is not silently retagged.

The verifier also runs the published SDK's query, bootstrap and recovery tests
from an isolated copy of the checksummed crate:

```sh
CARGO_BUILD_JOBS=2 python3 native/drasi/tests/test-sdk-recovery.py
```

The driver leaves the published source and manifest unchanged. A separate
verification manifest points at that source and restores the registry-only
development dependencies omitted from the published manifests. Independent
registry test locks in `tests/fixtures` freeze those verification dependencies;
the isolated workspace contains no dependency patches.
The registry cache remains unchanged; generated files are confined to the
disposable workspace and the existing native Cargo target directory. Missing
retained test selections are errors, not successful zero-test runs.

The same driver can run the existing SDK's real PostgreSQL container tests:

```sh
CARGO_BUILD_JOBS=2 RUST_LOG=error python3 native/drasi/tests/test-sdk-recovery.py --postgres
```

This requires Docker and exercises schema discovery, CRUD, bootstrap/CDC
handover, source positions, LSN replay, and persistent multi-query restart.
Its containers are owned and cleaned up by the SDK test fixtures. It uses the
PostgreSQL source and bootstrap libraries directly; it does not install native
artifacts or prove configured-plugin installation, Temporal workflow, or
browser acceptance.

To exercise those components across the actual native plugin ABI and managed
Source/Query host, run:

```sh
CARGO_BUILD_JOBS=2 RUST_LOG=error python3 native/drasi/tests/test-sdk-recovery.py --native-postgres
```

This builds Temporal's `plugins/postgres` library from unmodified, published
PostgreSQL Source and bootstrap descriptors, runs adapter Clippy,
and executes an exactly selected native test against an owned PostgreSQL 17
container. The packaging adapter exports the real linked core/lib versions and
supplies complete, typed schemas for the public PostgreSQL DTO fields instead
of their unresolved generic schema references. It accepts literal values and
host-bound Secret references, not arbitrary environment lookups. Bootstrap TLS
configuration resolves through the public SDK mapper before
delegating to the unchanged bootstrap descriptor. Before each snapshot, the
adapter uses the released Source's public replication API to create or retain
the Source configuration's slot. WAL is therefore retained before the snapshot
boundary is chosen, not created asynchronously after that boundary is needed
for replay. Neither native fixture precreates the slot. The adapter does not replace the
PostgreSQL connector or bypass compatibility checks. The driver also runs the adapter's
schema and literal/referenced TLS regressions, including unresolved and invalid
TLS refusal. The fixture covers credential-reference binding for password and
TLS mode, strict port/enum schema validation, initial query results, SQL CRUD
and filtering, durable unacknowledged output replay, and CDC committed while the
host is closed. A second query also consumes a real insert while the first query
is stopped; resuming the first must replay that insert exactly once through its
retained Watch subscription. Recovery must preserve the generation and semantic config hash
and advance the sequence exactly once for each result change. Persisted native
files must not contain the database password. The test-only loader uses private
resident handles; it does not install artifacts or claim public plugin Loaded
status. The container is removed on exit. Docker and Python 3.11 or newer are required.

The configured-plugin and PostgreSQL browser fixtures also accept a single
artifact exporting both PostgreSQL descriptors. Source and bootstrap selections
may share its immutable digest; both descriptor categories still have to load
and pass their schema and credential checks. Distinct artifacts remain supported.

### Preparing the Temporal PostgreSQL plugin

Build an unsigned native bundle in a new directory whose parent already exists:

```sh
CARGO_BUILD_JOBS=2 python3 native/drasi/plugins/postgres/prepare.py \
  --profile debug /existing/directory/temporal-postgres-bundle
CARGO_BUILD_JOBS=2 python3 native/drasi/tests/test-postgres-package.py
```

The default profile is `release`. The preparer calls the actual exported
`drasi_plugin_metadata()` without initializing the plugin. It checks the native
target, linked core/lib/plugin versions, and public FFI ABI against the exact
unmodified SDK and workspace lock. Registry `sdk_version` is the SDK crate
version, not the distinct binary FFI ABI version. The bundle contains the
library, `metadata.json`, `provenance.json` with hashes, source revision, dirty
state and exact registry package provenance, and a ready-to-use `catalog.json`.
The catalog pins the actual binary as `local@sha256:<binary hash>`, references its
absolute final path, and includes its Source/bootstrap managed pair.
Existing bundles and cross-target or redirected Cargo builds are refused.

To run the retained PostgreSQL bootstrap/CRUD/replay test against the actual
optimized bundle instead of the development library, use:

```sh
CARGO_BUILD_JOBS=2 python3 native/drasi/plugins/postgres/prepare.py \
  --profile release /existing/directory/temporal-postgres-release
CARGO_BUILD_JOBS=2 RUST_LOG=error python3 native/drasi/tests/test-sdk-recovery.py \
  --native-postgres --postgres-bundle /existing/directory/temporal-postgres-release
```

The driver checks recorded hashes, the exact SDK origin and the library's
actual exported ABI before using its private test-only resident handles.
This remains library/data-path acceptance, not installation or browser proof.

No signing, registry upload, or publication workflow is required. Install and
load the prepared bundle through the normal configured-catalog APIs:

```sh
TEMPORAL_DRASI_PLUGIN_CONFIG=/existing/directory/temporal-postgres-bundle/catalog.json \
  bash native/drasi/verify-plugins.sh
TEMPORAL_DRASI_PLUGIN_CONFIG=/existing/directory/temporal-postgres-bundle/catalog.json \
  bash native/drasi/verify-postgres.sh
```

The PostgreSQL verifier runs the actual install/load/idempotency gate
before creating its database container. An incompatible or corrupted artifact
fails there without starting PostgreSQL. Passing that gate still does not prove
the subsequent bootstrap, SQL mutation or browser behavior. After the query
and SQL-mutation assertions pass, the verifier checks the owned database's
command log to prove slot creation preceded the bootstrap snapshot.

The positive PostgreSQL browser fixture above is a further, separate gate.
The metadata inspector checks exact manifest/binary pins and metadata integrity
for public OCI artifacts or local bundles; it never requires signatures.
Its optional `--verify-release-tags` flag checks the upstream version-platform
tag convention for public artifacts. Local bundles use their binary digests
directly and have no release tags.

The `All Tests` workflow is configured to run the native verifier and both PostgreSQL
container tests in its `Native Drasi and PostgreSQL SDK` Linux arm64 job. This
keeps the data-path checks in CI without substituting them for the separate
configured-plugin and browser acceptance gates.

To run the full native verifier on Linux from a macOS worktree, use
`python3 native/drasi/tests/test-linux.py`. It requires Git and Docker, snapshots
the actual revision plus tracked and untracked source changes into an owned
disposable checkout, and runs `verify.sh` with two CPUs and a 6 GiB memory limit.
Recorded submodule pins are preserved without initializing their checkouts.
The container receives only a read-only snapshot, not the original worktree,
prepared caches, Docker socket, or host credentials. Its build tools use Go
1.27.0, Rust 1.96.1 and Python 3.12.13 on Debian Bookworm. This checks Linux native execution and
package relocation, not the Ubuntu GitHub runner or PostgreSQL/browser
acceptance. The owned container and checkout are removed on exit. Docker retains
the tool image and a worktree-keyed build cache, not runtime or checkpoint state.
Runs sharing that cache serialize through its verification lock.

## Host boundary

The versioned C ABI uses opaque runtime and cancellation handles, bounded
caller-owned output buffers, explicit free operations, and deadlines. Go never
passes a Go pointer into asynchronous Rust work. Native calls must remain
outside workflow replay and History/CHASM locks.

Ordinary calls share a runtime lifetime lease. All plugin management operations,
including discovery and installation probes, reserve one native flight before
scheduling or acquiring that lease. A runtime admits only one flight, and the
process admits at most eight. A canceled legacy caller does not release its
flight or permit another request to queue behind it.

The Go adapter's `Runtime.Plugin(command)` creates an immutable `PluginAttempt`
without starting work. Its first `Wait(ctx)` reserves one of eight Go completion
owners and executes the joined management-only C call. A canceled wait leaves
that owner attached until actual native completion. Reusing the same attempt
returns its original detached outcome without another call; an unstarted older
attempt cannot execute after a newer admitted attempt. Completed outcomes are
not fresh runtime observations. `Apply` keeps its existing bounded-wait
convenience API through the same lane. No native job ledger, receipt eviction,
explicit result acknowledgment, or immutable `Config` mutation is involved.

Plugin operations serialize on their own lock, not the output lock. Poll, acknowledgment, and subscription
mutations serialize their persisted read/modify/write operations; building a new
subscription's native snapshot happens outside that lock, with the binding
checked again before publication. Each Temporal source separately serializes its
feed journal commit and dispatch. A blocked plugin request therefore does not
block existing output or feeds, even after its caller cancels. Cancellation does
not abort an already-started operation. Close and checkpoint seal plugin admission and drain admitted flights, including
work not yet requesting Host access, before taking an exclusive lifetime lease.
The Go adapter also joins its published completion owner before entering native
teardown. They cannot tear down the graph or copy its stores while a joined
plugin call remains outstanding. An unwound flight retains native capacity,
quarantines its handle and prohibits checkpoint success; ordinary operation
errors remain terminal outcomes, not proof that no effects occurred. Close first
stops and joins each actual runtime owner. It does not treat the SDK's best-effort
shutdown as cleanup proof. These are host-operation guarantees, not a sandbox for
arbitrary native plugin threads.

A failed or interrupted native shutdown permanently disqualifies that Host from
publishing checkpoints. Repeated close attempts can clean up remaining work but
still report the original failure; a later no-op close is not shutdown proof.
The C ABI retains failed handles and their directory locks rather than turning
the next close into `NotFound`. They remain quarantined until process exit.
Successful close remains idempotent. Caller cancellation still does not abort an
already-started ABI operation; a hosting controller must retain its own ambiguous
outcome rather than infer a safe handoff from a later no-op.

The internal [namespace assignment](../../chasm/lib/watchassignment/README.md)
retains non-expiring owner grants and same-host sealed recovery cuts. The actual
workflow fixture now uses this authority around native close/checkpoint and
isolated restore, including a lost post-commit Seal response. This is a
cooperative built-in-source proof, not a production hosting controller or an
external plugin shutdown guarantee.

`service/watch.OpenGraph` now owns the Go startup/shutdown boundary. It obtains
the grant through the actual assignment client; callers cannot supply an epoch
or restore cut. The state root must already exist. Namespace/epoch directories
are derived beneath its resolved absolute path. Recovery verifies the exact
authoritative manifest digest before invoking native restore, which checks the
checkpoint's build and file integrity. Native open is always dormant with an
empty fixed source/query envelope.

`Graph.Close` durably records a close intent for the owned epoch, latches one
physical close, checkpoints the native graph, compares the actual manifest with
the native result, and seals that exact cut before releasing its ABI handle and
directory lock. A physical close/checkpoint error is
sticky even if fresh-context cleanup succeeds. A failed Seal RPC can retry the
same already-closed cut; a lost committed response is resolved through Describe.
The same grant cannot reopen after close has begun, including after failed or
cancelled close and successful cleanup. Otherwise new writes could invalidate a
checkpoint awaiting publication. The local close intent only prevents reuse; it
does not grant ownership, prove physical shutdown, or permit takeover.
Concurrent Close calls share one physical checkpoint, and a confirmed close
remains successful after a successor starts. The actual History fixture covers
these cooperative transitions and rejects a semantically equivalent manifest
whose bytes no longer match the authoritative digest.

### Same-volume process-crash recovery

When a different opening request encounters an active assignment, `OpenGraph`
attempts storage-only recovery of that exact predecessor on its original local
volume. It opens the **existing** `owner.lock` without creating or replacing it
and takes the same nonblocking exclusive OS lock used by ordinary runtimes.
A live, suspended, or quarantined native owner therefore prevents recovery.
Membership, elapsed time, process boot IDs, and assignment RPCs are not physical
death evidence. Missing directories, lock files, control databases, or admission
identity are errors, never permission to initialize empty state.

The mandatory native `checkpoint_recovery` mode never constructs DrasiLib,
Sources, Queries, plugin loaders, or credential readers. It validates retained
host admission metadata, catalog, bindings, and regular-file storage. Under the
lock, an atomic durable definition envelope retires the old directory. Existing
older bridges compare that definition before plugin or SDK startup and reject
it, including an old opener delayed between its assignment check and native
open. Successful cooperative checkpoints use the same retirement barrier.
Only a verified isolated immediate-successor restore unwraps the envelope.
An older bridge rejects the new mode and retired cuts; rollback requires a
compatible binary, not a fallback to ordinary dormant startup.
The standalone compatibility check accepts a frozen pre-retirement ABI library,
the rebuilt library, and an existing scratch directory:

```sh
RUST_LOG=error python3 native/drasi/tests/test-recovery-compatibility.py \
  /absolute/path/to/previous-library \
  native/drasi/lib/libtemporal_drasi.dylib native/drasi/.verification
```

Use the platform's `.so` library on Linux. The check creates retained state with
the old binary, recovers it with the new one, and requires the old opener and
unknown-mode fallback to be refused before admitting an isolated successor.

Recovery preserves engine indexes, WAL, durable results, and pending deliveries
without opening or repairing them. A successful snapshot is **not** certification
of engine semantic validity or external connector replay. Normal managed query
admission uses stock `Strict` startup. The handoff protocol
checks retained output generation and contiguous positions separately; this is
not a promise that SDK startup never initializes or repairs native state.

The immutable recovered cut is `recoveries/<epoch>`, separate from
`checkpoints/<epoch>`, which a delayed cooperative Seal may still name. Publication
copies and fsyncs a retirement-specific staging slot, validates the complete
manifest and file set, and atomically renames without replacement. Retrying
reuses the exact completed cut and Seal identity; invalid completed cuts are not
overwritten. Only the current retirement's nonauthoritative staging slot may be
rebuilt. Successor restore similarly uses an isolated stage and a durable origin
receipt, so an interrupted copy does not leave a partially published owner
directory or grant permission to replace unknown files.

This requires a stable, administratively provisioned local Linux or macOS volume
with correct exclusive-lock, atomic-rename, and fsync semantics. Copied,
rolled-back, or reassigned volumes, network/shared-filesystem takeover, and
cross-host migration are unsupported. The controller checks the recorded storage
host, but that identifier is not mount attestation.

The real subprocess fixture in `service/watch/graph_crash_test.go` suspends and
kills a native owner, refuses a live contender and a different storage host,
preserves an incomplete cooperative cut, and restores the exact pending event
with contiguous subsequent output. It includes lost and uncommitted Seal replies
and exact-cut retries. This is native-process/assignment-client acceptance, not
PostgreSQL CDC or browser acceptance.
`tests/drasi_graph_crash_test.go` additionally kills a separate native process
whose grant is retained by actual History/CHASM, loses the committed recovery
Seal response, and verifies ordinary successor admission and shutdown.

`Graph.Reconcile` installs complete, nonregressing desired views and returns
detached observations while convergence runs asynchronously. Repeating the same
generation retries native drift; it does not mark intent as applied merely
because a job was scheduled. Immutable retained Source/Query recipes resume
through native admission, while current public-ID stop gates cover every
retained lifetime. Source stop waits for its dependent queries to stop.

Current nondeleted Source and Query definitions retain their native lifetimes,
even when stopped. Current nonterminal Watches, including paused Watches, retain
their exact Query and transitive Source pins. Unpinned lifetimes stop after their
History acceptance barriers, then permanently retire after their native bindings
are deleted. Queries retire before their Sources. Already accepted
History deliveries continue draining independently of native retirement.

Retirement persists and syncs irreversible intent before stopped-only SDK removal,
reclaims the retired Query's SDK indexes, checkpoints and output state, then
syncs completion. Host admission tombstones and accepted delivery records remain.
Retired lifetimes cannot be readmitted and are not reconstructed by a successor.
Completion releases the 64-Source/256-Query runtime slots; full immutable admission
recipes remain within the existing 8 MiB catalog bound. The first retirement
upgrades the catalog to version 2, which older binaries reject before runtime
construction. This is not general disk garbage collection or unlimited definition
churn: retained host catalog and delivery state are not pruned. Any uncertain
cleanup fences admission and checkpoint publication.

Each Watch has independent control and intake workers. Its durable native binding
precedes History control creation. Recovery retrieves the original Watch
definition, claims the current control revision under the granted owner epoch,
and only then permits intake. A replaced Watch lifetime is canceled in History
before its native tail is discarded; accepted actions continue draining there.
Runtime pause/cancel mutations carry the owner epoch as well as the revision,
so delayed operations cannot modify a successor whose Claim left that revision
unchanged. One-shot control created after asynchronous provisioning has already
crossed its deadline starts timed out rather than becoming impossible to create.

Desired changes cancel affected acceptance calls immediately. Physical node stop
waits for the corresponding History pause/cancel barriers. Native topology jobs
are serialized independently of Watch control and intake; timed-out work stays
uncertain until native topology observation confirms completion. Duplicate native
binding attempts cannot accumulate snapshot futures. Native observations also
retain a failed-stop cleanup error even after the underlying node reaches
Stopped; such a graph cannot report convergence or publish a checkpoint.
Close cancels and joins Graph workers before the existing checkpoint/seal path.

The actual Graph fixture exercises retained Source/Query lifetimes, a blocked
History pause while another Watch completes an SDK workflow, managed stop/resume,
original definition recovery after public lifecycle changes, a lost committed
acceptance response, sealed successor recovery, and Watch replacement without
restarting the old lifetime or duplicating its workflow effect.
Namespace-scoped RPC faults also hold an accepted workflow start pending across
public Watch edit and deletion. The canceled old control retains its frozen
request and drains it after the fault is removed, with its original definition
revision and workflow identity. A subsequent independent Watch confirms that
new output still progresses without being accepted by any retired lifetime.

The opt-in server role connects this Graph to the owning-History execution feed.
`GraphOptions.TemporalInput` must explicitly select `HistoryTemporalInput` or
`ManualTemporalInput`; production supplies the History-routed client and the
cluster's immutable shard count. Manual feeding is reserved for explicit local
fixtures. A managed source rejects `FeedTemporal`. External recipes use the
separate sensitive plugin admission path below; configuration alone does not
advertise verified external-source capability.

### Managed History input

Each retained built-in Source has one joined Go transport worker and a native
scope containing its namespace UUID and shard count. For every shard, the worker
begins or resumes `BootstrapExecutionFeed`, preserves its opaque continuation
token and capture epoch, and starts replay at the certified reconstruction
position, never journal zero. It discovers the durable published cutoff before
requesting a bounded immutable read. Other namespaces' filtered-out records can
advance that shard's scan position without inventing graph events.

The worker formats complete payload-free `Workflow`, `Activity`, and `Timer`
nodes, `HAS_ACTIVITY`/`HAS_TIMER` relationships, and immutable `Fact` nodes.
Every node carries `namespace_id`, `workflow_id`, `run_id`, and a complete nested
`data` object derived from the versioned History projection. Search attributes
retain their declared type and exact decoded value, including 64-bit integers.
Inputs, outputs, failure messages, stacks and heartbeat payloads are not exposed.
Replacing `data` removes omitted fields; an omitted child removes that child and
its relationship, without deleting/reinserting an unchanged execution identity.

For example:

```cypher
MATCH (w:Workflow)-[:HAS_ACTIVITY]->(a:Activity)
WHERE a.data.status = 'STARTED'
RETURN w.workflow_id AS workflow_id, a.data.attempt AS attempt
```

Native redb commits the derived events, source head, shard successor cursor and
immutable acceptance receipt in one transaction. Go has no second durable cursor
or intent log. A receipt includes a canonical request UUID and a native
canonical-content digest. Lost acceptance replies are resolved against actual
native progress before another History read; conflicting retries fail.
Dispatch follows durable acceptance and does not constitute a query-evaluation
receipt. Individual graph changes are not a transactional SDK evaluation of the
whole upstream execution record.

SDK Running alone is not input readiness. Every shard must finish enumeration
and accept a fresh read caught up to both the bootstrap and published cutoffs.
Cold and warm starts clear this readiness; old receipt replay cannot restore it.
Query admission, snapshots, subscriptions and pending-output polling wait for
the source acceptance fence and require ready inputs. Interrupted acceptance
remains quarantined until joined stop and existing-state recovery.

The adapter measures the complete encoded native command against its 1 MiB
limit. It can accept a smaller prefix of an unaccepted History page, leaving the
cursor at the first omitted record. A single oversized projection is rejected
without advancing. Reads retain the RPC's 100-record/3 MiB limits, tokens are
bounded to 16 KiB before base64 encoding, and native control state is bounded to
8 MiB over at most 16384 configured shards.

Temporary capture disablement or an unavailable owner suspends native readiness;
dependent Watches cross their History pause barrier, queries stop, and the input
worker joins before source stop. Ordinary restart uses retained cursors and stock `Strict` query recovery. A structured capture-discontinuity error or invalid
projection permanently blocks that source incarnation instead of rebuilding it
silently. A new source incarnation and updated dependent pins are required to
reconstruct after an actual gap.

Enable the global execution-feed queue at History startup and enable execution
capture for each participating namespace:

```yaml
history.enableExecutionFeedQueue:
  - value: true
history.enableExecutionFeed:
  - value: true
    constraints:
      namespace: your-namespace
```

See the [History feed contract](../../service/history/executionfeed/README.md)
for durable capture fencing, force-deletion restrictions and retention limits.

The low-level Temporal feed preserves JSON objects and arrays as typed Drasi
properties, rather than JSON-encoded strings. Updates preserve omitted top-level
properties. Explicit `null` clears a nullable property; a supplied nested object
or array replaces that complete value, including removed keys or list entries.
Full-state adapters must supply a complete fixed property schema or replace a
complete nested state value. They must not simulate replacement by deleting and
reinserting an unchanged entity identity, which can invent Watch exit/entry
transitions. Earlier development journals that encoded objects or arrays as
strings retain those stored values; an incompatible retry at an occupied feed
position fails rather than silently rewriting accepted input.

The configured storage-host UUID does not prove volume continuity;
unsealed owners and unusable cuts remain blocked, with no expiry-based takeover.
These fixtures use actual native nodes and pending output, but do not establish
third-party plugin shutdown or PostgreSQL/UI E2E.

## Opt-in Watch server role

The `watch` service is available only in a cgo build with the
`temporal_with_drasi` tag and its matching native library. It is not in the
default service list. Requesting it in an ordinary/no-cgo build fails explicitly.
Build and publish the development native library as described above before
building the server.

Configure an RPC/membership listener and persistent storage:

```yaml
services:
  watch:
    rpc:
      grpcPort: 7243
      membershipPort: 6943
      bindOnLocalHost: true
    drasi:
      stateRoot: /var/lib/temporal/drasi
      storageHostID: 10000000-0000-4000-8000-000000000001
      maxNamespaces: 16
```

The administrator must create `stateRoot` and assign a unique, stable UUID to
that storage; the example UUID is not a deployment default. Keep the same
storage and mount path across cooperative restarts. `maxNamespaces` defaults
to 16 and accepts 1..256, including hosts whose close remains blocked. Capacity
exhaustion is reported rather than silently dropping an existing owner.
Configure all services with the Watch listener information so their membership
monitors can discover its ring. Then run `temporal-server start --service watch`
alongside the ordinary Temporal services. The same role may also be explicitly
included in a development server's service list.

The role uses Temporal's common service dependencies, internode TLS/listener,
namespace cache, History-routed generated clients, membership and Fx lifecycle.
It periodically discovers namespace-filtered `drasi.enabled` local registered
namespaces. Membership chooses placement but cannot grant ownership. Each
namespace has independent reconciliation and Graph workers; a blocked namespace
does not serialize unrelated namespaces. An uncertain membership lookup retains
an existing owner without admitting a new one. Global, disabled, deleted or
deprecated namespaces do not admit new work.

Disablement, reassignment and service shutdown cancel intake, join workers and
close/checkpoint/seal before releasing ownership. Uncertain opening outcomes
retain their original request identity. Shutdown may reopen only an
authority-confirmed grant belonging to that same request/process/storage to
finish its dormant close, never acquire a new owner merely to clean up.
Transient unavailable Seal calls retry the identical cut. Failed physical
close remains blocked and is not automatically reopened or stolen.
Each service instance generates one process boot UUID; namespace disable/re-enable
retains it, while a new service instance must recover under a new boot UUID and
owner epoch. `watch.persistenceMaxQPS` and `watch.throttledLogRPS` bound its
namespace-cache persistence and throttled logging. Standard gRPC health uses
service name `temporal.server.watch`; it reports the service listener, not
readiness of every namespace, source or query. Native resource failures remain
visible in Graph observations and structured service logs.

Public Source/Query/Watch Get/List reads enrich the accepted registry page through
one read-only internode Watch observation. The frontend checks the durable owner
before and after the call. Membership supplies only a candidate endpoint; the host
must match the namespace, owner epoch, storage/boot identity, registry generation
and exact kind/ID/public-revision/incarnation. A different owner, failed/closing
Graph, or samples older than five seconds cannot certify readiness. The public
response then preserves definitions with an unknown, redacted observation.
A never-hosted or sealed namespace has no current observation. Reads neither
acquire ownership nor reconcile, and native/network I/O stays outside Graph locks.

The optional observation budget is two seconds for at most 200 resources and
1 MiB per internode request/response. Registry pages reserve bytes for attached
status. Runtime samples retain their actual receipt time; reading status never
refreshes it. A query also requires its exact source bindings Running, and a
Watch requires its current acceptance generation and completed initial snapshot.
The existing managed History-source status already includes all-shard readiness.
These are as-of observations, not a linearizable cross-resource health snapshot.
Lifecycle mutation responses still acknowledge only accepted intent.

`TestDrasiWatchServerRole` starts the actual core server and a separate Watch
server, verifies membership and health, and connects public definitions through
owning-History bootstrap/live RPCs, native query evaluation and durable Watch
dispatch to an actual SDK worker. It proves that committed workflow completions
start consumer workflows with the expected results, without manual feeding.
Completions committed while the namespace is disabled and while the Watch role
is stopped are delivered after sealed recovery, with contiguous delivery counts.
The fixture verifies authoritative sealed cuts across both restarts and checks
public Get/List readiness for all three resource kinds before and after recovery.
It also exercises public Watch pause/resume, edit, explicit reset and deletion
against the running History/native/SDK pipeline. A second Watch confirms that
new output exists while the first is paused; resuming delivers that retained
output exactly once. Editing retires the old lifetime and uses the new action
without replaying existing matches. Reset with Include delivers the complete
current snapshot under a new Watch lifetime. Deletion stops future acceptance
while another Watch continues delivering, and delivery history retains each
action's original definition revision across all these changes.
The [optional Go helper](../../sdk/go/README.md) runs in that same fixture.
It receives an already-true condition on its exact owning run even when the
create response is lost, completes before registration confirmation, replays
the captured real workflow history, and cancels a timed-out wait through the
public API. Its local timeout does not claim that server acceptance lost.
PostgreSQL/UI acceptance remains separate.

`ValidateQuery` resolves all pinned Source revisions in one read-only registry
snapshot and compiles an arbitrary, unregistered Cypher or GQL specification
through the native language parser, matching function registry and
`QueryBuilder.try_build`. It neither creates a query nor opens a runtime,
subscribes to a source, acquires namespace ownership or changes persistent state.
The host checks the authoritative namespace UUID and feature policy independently
of the frontend. The frontend forwards every exact Source definition from that
same registry cut; the private host rejects missing, extra, reordered, or
cross-namespace bindings.

External and mixed bindings use the private `ValidateBoundQuery` operation.
An older Watch host cannot approve these requests by ignoring the Source
definitions: an unsupported operation returns Unavailable, without retrying as
compilation-only validation. Built-in and legacy compiler requests retain their
existing operation. Upgrade Watch hosts before frontends to avoid external
validation unavailability during a mixed-version rollout.

After compilation, external definitions are checked against the actual loaded
Source and bootstrap descriptors in the existing management runtime. One bounded
management command validates all external recipes using the same schema checker
and administrator-reviewed lifecycle pairs as admission. It never constructs a
Source, reads a secret provider, opens a namespace Graph, reserves admission
state, or loads a plugin. Immutable credential references are checked using
schema placeholders, not resolved values. Built-in and older internal
compiler-only requests do not need a management runtime.

A schema mismatch returns a fixed diagnostic identifying the public Source
index, without native errors, configuration values, or credential references.
Missing policy, unloaded factories, and unsupported schema constructs are
FailedPrecondition errors, not invalid configurations. Capacity refusal remains
ResourceExhausted. Missing, partial, or unknown native verdicts fail closed, and
an operational failure cannot be hidden by an earlier schema mismatch.
The management command retains its actual completion owner after a caller stops
waiting. This schema check does not advertise external-source capability or
prove PostgreSQL/bootstrap recovery; those gates still require compatible
artifacts and positive lifecycle verification.

Compiler acceptance is not execution, a static property-type check, or a claim
that a source is ready. Functions and properties can still fail when evaluated
against actual values. Invalid compilation returns a fixed diagnostic without
echoing engine errors or query text. Operational failures retain error status
rather than becoming an invalid-query result.

Compilation accepts at most 65,536 bytes of query text and admits at most two
native jobs per process before scheduling, with no waiting queue. Internode
calls have a two-second budget and 1 MiB wire bounds. Cancellation and deadlines
stop waiting; they cannot preempt synchronous parser work, which retains its
permit until it exits. These limits are not a hard CPU-time guarantee.

`GetCapabilities` advertises native languages, persistent state, the built-in
Temporal source, one-shot conditions and `ValidateQuery` only after a bounded
native host probe succeeds. The frontend authorizes every advertised method;
host capabilities cannot grant access. A missing native role preserves
definition-only capabilities. `PreviewQuery` is not implemented or advertised:
bounded output alone would not bound intermediate evaluation work or memory.
The real-server fixture also proves compilation in both languages, aggregate
queries, fixed invalid-query diagnostics, unchanged registry generation, and
subsequent workflow dispatch.

The same real-worker fixture verifies that an already-matching one-shot ENTER
condition signals its exact owner once even with default IGNORE bootstrap.
Before accepting new one-shot output, the Graph checks the exact owner's
workflow status through the normal Temporal frontend. A closed or missing run
cancels the condition; lookup failures pause intake rather than imply closure,
and mismatched runs or unknown statuses fail explicitly. A condition waiting
on an empty query is also cancelled when its owner closes. Accepted intents
remain independent and continue draining; this check cannot retract an RPC
already sent or make owner closure atomic with a concurrent acceptance.

The startup configuration is an immutable envelope for one isolated generation.
Its initial Sources and Queries cannot be replaced. The admission catalog below
adds immutable nodes without reopening that envelope. Sources are
either managed committed History projections, explicit application-fed test
fixtures, or verified native source and bootstrap plugins. The managed adapter
supplies certified History progress and contiguous native positions.
Configuration must come from the server, not arbitrary browser JSON or file
uploads.

### Managed plugin admission

The native Rust host has a separate `Host::admit_plugin_source` entry point for
ephemeral credential values. Ordinary declarative `AdmitNode` commands reject
plugin Sources. Go `Runtime.AdmitPluginSource` uses a separate C entry point,
with public definition JSON and a bounded private binary credential packet.
Resolved values never enter declarative command JSON. The native decoder owns
its copies before scheduling; cancellation or a deadline cannot leave a
background job borrowing freed caller memory.

The Go boundary reserves one of eight process-wide transfers before provider
reads and imposes a 30-second operation deadline. Native transfers have a
separate eight-job limit retained until each scheduled job finishes, even when
its caller stops waiting. Values are at most 16 KiB, groups at most 256 KiB,
and each packet at most 1 MiB. Provider-returned byte slices transfer ownership
to the host and are cleared on success, error or cancellation. The C packet
is cleared before freeing on every exit. Native copies use the credential
router's zeroing ownership. This does not promise to clear SDK-created string
copies or a provider's private copies.

The production Graph translates pinned external Source/bootstrap recipes
without I/O. Its joined topology worker performs native preflight and provider
reads outside the Graph mutex. Preflight checks the actual ledger, reviewed
pair and verified descriptors without reserving state or calling factories.
Resident stop/start retries do not reread credentials. Sensitive admission
revalidates the definition and native creation phase after provider reads;
preflight is not a second runtime identity authority.

Custom server builds configure administrator policy with
`temporal.WithDrasiPluginPolicy(watch.PluginPolicy{...})` and their provider
adapter with `temporal.WithDrasiCredentialReader(reader)`. The policy contains
`Preload`, `Catalogs` and exact `ManagedPlugins` descriptor pairs;
it contains no resolved secrets. Graphs detach its slices before use. No policy
or reader is installed by default. These options affect only the opt-in Watch
role and perform no credential reads in ordinary/no-cgo builds.

`watch.CredentialReader.ReadSecret(ctx, namespace.ID, native.SecretReference)`
must authorize the authoritative namespace UUID and the complete immutable
`provider/key/version` tuple, honor cancellation, return caller-owned bytes,
and retain old generations needed by pinned definitions and recovery. It must
not select latest, normalize aliases into hidden bindings or log private
values. The Graph supplies its owned UUID, never a request-supplied namespace
name. Provider errors reaching observations are replaced by a static message.
There is no environment, filesystem-path or latest-version fallback provider.
Without an adapter, recipes with credential references fail explicitly.

`Config.managed_plugins` defaults empty. An administrator must review and allow
the exact pinned Source/bootstrap descriptor pair's replay, stop/drop and
bootstrap-completion contract. This is separate from content and compatibility
checks and is not a claim supplied by a plugin or namespace writer.
Only already-loaded, exactly pinned descriptors can construct instances.

Persisted recipes contain ordinary configuration and immutable
`provider/key/version` references, never resolved values or resolver
capabilities. Before construction, a bounded validator checks the exact verified
descriptor's declared fields and supported schema vocabulary. It supports
objects, arrays, primitive types, enum/const, conjunctions/alternatives,
size limits, integral ranges and local component-schema references. Unsupported
assertions and recursive/excessive schemas fail closed. UI hints do not authorize
secret fields, and `format` remains descriptive rather than an assertion.
This is not a general JSON Schema implementation.

Managed Sources expose only the public recipe through Source properties. Their
companions sanitize factory, lifecycle, subscription and asynchronous bootstrap
errors. Credential callbacks are bound to the exact lease and binary audience;
plaintext values travel only in transient resolver replies, never arguments,
environment variables, state files or public properties. Source data remains
application data, not a sanitized diagnostic channel. Native code is trusted,
not sandboxed. Source and bootstrap capabilities remain distinct and survive
normal stop/start. An explicit clean stop permits restarting the same graph
Source and lease with a fresh child epoch and the same source-local state. It
does not authorize a new managed admission or clear an unproved-cleanup fault.

Dormant reopen constructs no retained plugin Sources and deliberately defers
their dependent Queries. These exact skipped entries report `Deferred`;
unexplained disappearance or a retained fault reports recovery required.
Persisted pending handoffs remain pollable and acknowledgeable while deferred.
Further output is explicitly unavailable until dependencies are materialized and
the retained Query validates its owned seal and satisfies handoff continuity checks.
Warm restart uses the public `start_query`, without replacing the runtime.
The Go planner admits Deferred nodes only when desired state and dependency
readiness permit it. Deferred dependencies do not block stopping their Source.
Public status reports deferred desired-running nodes as Starting, never Running;
a deliberately stopped deferred node can confirm Stopped without construction.
An operator-requested Source stop can invoke native materialization-fault
cleanup; a running desired state never silently clears that fault.

Close joins owned query epochs and source processes before destroying managed
instances and revoking their capabilities. A retained safe Source reference does
not prevent cleanup after final close has joined its child and prohibited restart.
Failed joins and failed stops quarantine the instance and credential allocation,
block checkpoint publication, and retain directory ownership. A later reference
drop or no-op close does not undo a retained cleanup failure.

The actual-library fixture verifies managed admission, warm/cold recovery,
nonempty output and contiguous delivery, pending handoff recovery, sanitized
errors, graph-identity-preserving child retries and retained-reference/failed-stop
fencing. Owned query recovery validates hashes, keyed rows, checkpoints and the
sealed outbox rather than using SDK reset-marker recovery. The plugin fixture itself emits an
empty positional bootstrap; nonempty changes come from a second built-in test
Source. These checks do not prove PostgreSQL CDC, arbitrary plugin lifecycle
contracts, provider-adapter authorization or browser acceptance. The C-boundary
fixture also proves that eight timed-out jobs survive cleared caller buffers,
retain their native transfer permits, and construct only one resident instance.
Go tests cover byte limits, exact references and component slots, partial-error
clearing, redacted provider failures, no-cgo refusal, namespace UUID routing,
deep observation copies and real native refusal before provider reads.
Signatures are not required. Durable install/load intent and its
private runtime reconciliation and public plugin management are described below.
A compatible PostgreSQL acceptance run remains a separate gate; it is not
advertised by this wiring.
The verifier also rejects credential/capability fixture markers in captured
native test diagnostics, redacting those markers before printing a failure.

Indexes, source journal positions, WAL, subscription handoffs, and receipts are
persistent. Each Watch has a separate durable subscription and initial keyed
snapshot. The snapshot sequence is the query's current sequence, including zero
when no live output has committed. Following deltas start at that sequence plus one.
A consumer must preserve this boundary rather than assuming that every Watch
starts at output sequence one.

For a new query, an application-fed Source reconstructs its current graph at
accepted journal head H. It folds inserts, partial updates, and deletions before
selecting the requested labels. Nodes precede relations in deterministic identity
order. Both bootstrap channels are prepared and closed before live registration;
an empty image still supplies the query's bootstrap barrier and exact position H.
Historical state populates the initial snapshot without adding historical live
outbox entries. A per-subscription floor excludes retried positions at or below H.
Existing checkpoints, including a zero framework sequence with an opaque nonzero
source position, use replay instead of a new bootstrap.

Subscription, feed acceptance/dispatch, and stop share one fence. Bootstrap reads
one immutable journal record at a time without a detached source reader. The
finite scan holds that fence and bounds every intermediate, unfiltered projection
by `snapshot_rows` and `pending_bytes`; each raw journal record must also fit the
byte bound. A selective or finally-small image can therefore fail if earlier
state exceeds those bounds. Corruption, missing records, and resource exhaustion
fail explicitly rather than truncating the image. These are encoded-state bounds,
not a hard allocator ceiling: storage returns an owned raw value before its size
can be checked. Existing replay still uses the SDK's historical replay allocation.

This boundary covers only changes already accepted into native storage. It does
not prove upstream History enumeration completeness, durable shard-vector intake,
or uninterrupted capture. The owned query actor evaluates complete source
snapshots through unmodified Core, commits keyed initial output with all source
cuts, and replays from that sealed boundary. It does not certify a legacy empty
image without a successful positional bootstrap. A live event with no result change
can advance its source checkpoint while the result sequence remains zero.
Bootstrap or storage errors block output. Uncertain cleanup retains host ownership
and credentials; a successful foreign SDK stop call alone is not completion
proof without child reap and joined IPC owners. These boundaries do not establish
upstream History baseline completeness.

The outer output protocol normalizes native row and aggregation diffs into
`ADD`, `UPDATE`, and `DELETE`. Drasi aggregation diffs can contain a default
`before` value even when their group did not previously exist. Each subscription
therefore retains bounded keyed results with its pending handoff.
An aggregation creates an entry only for a new signature, updates an existing
signature only when its value changes, and removes a signature when no after row
remains. Results and replayable pending output commit together before exposure;
repeated polling cannot reinterpret an entry as an update. Keyed results are retained
across acknowledgments, restarts, and checkpoints. Older subscriptions without
those results require an explicit reset rather than guessing their prior rows.
The retained result set is bounded by both `snapshot_rows` and `pending_bytes`,
not only by the size of one change. Exceeding either limit fails polling before
committing the next handoff or advancing its cursor; it never skips that output.

The pinned engine retains an aggregate group's identity-valued row after its last
input is deleted. The handoff mirrors the engine's materialized snapshot rather
than inventing a group deletion. Applications that require empty-group removal
must account for this engine limitation in their query.

Polls replay an unacknowledged handoff. Acknowledge only after Temporal durably
accepts all its chunks and corresponding immutable action intents. A filtered
or empty snapshot still needs a durable acceptance. Revisions, incarnations,
and contiguous positions are checked; a gap requires explicit recovery, not an
automatic reset.

New initialization is permitted only in an empty owned directory (apart from
`owner.lock`). The immutable definition and identity reserve that creation before
SDK I/O. Reopen validates that host identity before owned-query startup.
Missing control identity/definition in a used directory fails. Owned query
recovery refuses unsealed, missing or inconsistent committed state instead of
adopting an SDK reset marker. Retained Watch handoffs
still refuse changed output generations or noncontiguous output. Such a fault
requires explicit operator recovery/reset, not cursor advancement.

The SDK's whole-graph start is best-effort for queries. The outer host additionally
checks each query's actual runtime status, rather than claiming success for a
failed admission or relying on asynchronously updated graph status. A failed
query admission uses the same full teardown as close, including releasing native graph
reference cycles. Query bootstrap may still be in progress when open returns;
open is not an assertion that all sources and queries are healthy.

## Incremental native admission

The private `admit_node` command accepts a `NodeDefinition` containing `lifetime`,
`activation`, and a closed `recipe`. The lifetime contains the canonical Temporal
`namespace_id`, public `resource_id`, and the registry's opaque definition
`incarnation`. An incarnation is not a UUID or a lifecycle revision: legacy empty
incarnations are rejected. Activation is a nonzero, stable admission identity,
not the current owner epoch. Changing immutable semantics requires a new
definition lifetime or an explicit new activation.
Public resource IDs follow the API's 1..128-character rule: an initial ASCII
letter/digit followed by ASCII letters/digits, `.`, `-` or `_`. Dotted IDs are
retained in lifetime identity, never used as native storage paths. Hashed native
IDs retain the stricter path-safe identifier rules.

Supported recipes are `temporal_source` and `query`. Query recipes contain
`language`, `text`, and ordered native `sources`. Each dependency must be an
admitted source in the same namespace; anonymous startup sources cannot supply
this ownership proof. Source and query IDs are `s-` or `q-` followed by the
SHA256 of a domain-separated, typed native serialization of the complete
definition. Callers use the returned ID rather than hashing their transport JSON.
Separate public Query IDs do not share evaluations merely because their text
matches. Lifecycle-only public revisions reuse the same node. The immutable
native evaluation revision is `1`, independent of the exact public Query pin.

Go callers use `AdmitNodeCommand` with `TemporalSourceRecipe` or `QueryRecipe`,
and `ObserveNodesCommand`. Results contain typed `Node` or `Nodes` observations
with complete detached immutable definitions, including original public lifetimes
and dependency recipes. The bounded catalog remains within the ABI output limit.
Observations read actual native runtime status, not a persisted ready flag.
Managed stopping/error transitions remain visible until the SDK graph processes
their completion, rather than advertising a premature restart opportunity.
The authoritative namespace registry and its accepted desired state are unchanged.

The catalog persists `intended` before SDK admission. A query is provisioned
without auto-start; `provisioned` consumes its one creation permission before
starting it. A retry inspects the existing runtime rather than removing or
replacing it. An unconsumed intent can still create its query; a crash after
consumption uses stock `Strict` startup; it can initialize missing native state.
Recovery reconstructs admitted sources and queries from the retained catalog
before the SDK's normal startup and subscription-completion sequence. A failed
admitted query remains visible without preventing healthy admitted queries from
running. Pending Watch bytes and output incarnations survive checkpoint restore.

`start_mode: "dormant"` (Go `StartModeDormant`) reconstructs retained catalog
nodes without starting their sources or queries. It requires an empty immutable
source/query envelope; fixed definitions and their whole-envelope creation
reservation do not support deferred activation. Omitted or `"automatic"` mode
preserves normal startup. Startup mode is not part of the immutable definition.
The host still starts the SDK lifecycle, but catalog nodes have auto-start
disabled and do not participate in its running-source subscription-completion
callback. Replaying a retained pending handoff does not require query activation.
After restoring original Watch bindings and checking current desired-state gates,
the hosting controller re-admits only the permitted source and query definitions.
This explicitly starts existing stopped sources and uses managed existing-state
query recovery; unrelated retained nodes remain stopped. This is a mechanism,
not the production controller or a plugin feedback/shutdown proof.

A first-created query retains the SDK's `Strict` configuration in memory.
Consumed admissions start stopped runtimes through the public `start_query`
operation, without changing that policy or replacing the query runtime.
Failed or stopping runtimes still require normal stop cleanup before retry.
Real native tests preserve the same runtime, pending bytes, generation and
contiguous output across warm restart. An injected source-position gap fails
without advancing the Handoff; repairing that position permits a contiguous
retry.

`StopNodeCommand` stops one admitted node through the real SDK lifecycle. It is
idempotent for stopped, stopping and unprovisioned intended nodes; unknown IDs
are rejected. Stop dependent queries before their source: a running, starting
or failed dependent blocks source shutdown until its own cleanup completes.
Observe `NodeStopped` before re-admitting the exact retained recipe. Neither
stop nor re-admission removes/recreates a query or grants consumed creation
permission. Other queries continue running, and pending handoffs remain intact.
Built-in source stop joins its feed critical section before publishing Stopped.
A managed node-stop failure permanently disqualifies this Host from checkpoint
publication, even if a later cleanup retry succeeds.
These commands operate on catalog nodes, not the immutable initial envelope.
Production desired-state reconciliation remains a separate hosting responsibility.

The catalog retains at most 64 sources and 256 queries, including immutable
startup nodes, and at most 8 MiB of serialized admission state. Query text is
bounded to 64 KiB with at most 64 distinct source bindings. Exhaustion never
prunes nodes. This version has no retirement API, external-source/secret-version
recipe, distributed assignment, or public
frontend reconciliation. The startup envelope includes catalog format version 1;
older development state is rejected rather than silently upgraded.

Native/public intake uses an explicit `NativeBinding`. It verifies the native
subscription, query ID, and evaluation revision before translating to the
Watch's exact public Query pin. Receipts and unsubscribe operations retain the
original native identities. Legacy anonymous subscriptions can use independent
control execution IDs; native Query IDs remain independent of public Query IDs.

## Durable public Watch bindings

The private `bind_watch` command accepts a `WatchBinding` and an admitted native
`query_id`. The binding retains the Watch and Query definition lifetimes and their
exact public revisions. It does not duplicate Watch payloads or retain an owner
epoch. Both lifetimes must belong to the same namespace; the native query must
prove that exact Query lifetime through the admission catalog. Anonymous startup
queries cannot provide this proof.

The subscription ID is `w-` plus a domain-separated SHA256 of the Watch lifetime.
This ID is also the CHASM control execution ID. Lifecycle-only revisions and owner
changes do not allocate another identity. The binding, keyed result baseline, and
initial pending snapshot publish in one durable Handoff write. A successful retry
returns the same registration without replacing its pending bytes. Another Query
pin, Watch revision, native evaluation, or binding cannot reuse that identity.
A reset/replacement requires a new Watch lifetime. Raw `subscribe` cannot create
or downgrade one of these reserved IDs.

Publish the native binding before creating control. After a lost registration
response or process restart, use `list_subscriptions` rather than rebuilding the
binding from current resource revisions. Pages contain at most 64 immutable
definitions/bindings and their deletion flags, with an exclusive `after` cursor
and `next_page_token`. Tombstones retain their original metadata and cannot be
resubscribed. Recovery validates bindings, keys and catalog dependencies before
starting sources or queries; missing or inconsistent public binding metadata
fails closed rather than becoming an anonymous subscription.

Use the private `poll_watch` command with one bound `subscription_id` for
production intake. It performs one bounded check and returns an `event`, or
`null` when the healthy subscription is caught up. Missing, anonymous, or deleted
subscriptions return explicit errors. A query gap or fetch failure is reported
only by that Watch's call; callers can keep polling unrelated bindings without
skipping or resetting the failed Watch. A real bounded-outbox test preserves the
failed Handoff byte-for-byte while another bound query publishes its snapshot
and next contiguous delta. The legacy aggregate C ABI poll remains for anonymous
fixtures and still fails the whole call if any selected subscription fails.
This isolation covers independent call failures, not an indefinitely blocked
shared storage operation or complete production supervision.

`DesiredView.NativeWatchBinding` constructs new metadata from retained registry
records. `DesiredView.RestoreNativeBinding` recovers the original immutable Watch
revision and exact Query pin from that metadata, even after pause/resume changed
the current Watch revision. A required historical read is fenced to the view's
registry generation. Returned definitions are detached, and the view is not
mutated. The current owner supplies its epoch separately; control still checks
the actual ownership fence before acceptance. The control's mutable revision
fences acceptance against concurrent pause/resume, while its immutable
`creation_revision` pins the Watch definition and action IDs. Intake keeps the
original public Watch revision when control lifecycle changes. These helpers do not grant
ownership, apply current lifecycle gates, supervise a role, or authorize cleanup.
The registry must retain every bound public revision, and native retirement must
retain catalog dependencies until their bindings/tombstones can be collected.

The retained workflow fixture publishes a binding, reopens before control
creation, changes public lifecycle revisions, recovers the original definition,
and creates real History control. It then loses a committed acceptance response,
restores an isolated native checkpoint under a new epoch, rejects the stale
owner, replays the same pending output, and completes one actual SDK workflow
without duplicating delivery. This is a bounded recovery proof, not a distributed
assignment or production reconciliation implementation.

## Registry desired-view projection

`watch.ReadDesiredView` reads all current namespace Sources, Queries and Watches,
including paused/deleted cleanup intent, then resolves the exact retained Queries
and Sources referenced by those definitions. Every page and retained lookup must
observe the same registry generation. A concurrent mutation returns
`FailedPrecondition` and no partial view; the caller must retry the complete read.
Unchanged generations establish one consistent cut without an unbounded snapshot
RPC or a long-held History lock.

The view keeps latest resource lifecycle gates separately from historical pinned
definitions. A retained revision's desired state is historical, not permission to
ignore a current stop/delete. The snapshot is not the complete revision archive
and does not authorize garbage collection of existing native nodes or bindings.
Accessors return detached copies. Empty legacy definition incarnations remain
empty; native admission still requires explicit definition replacement.

Malformed scope, records, pin responses, ordering, pagination and unknown desired
states fail explicitly. Record/page/byte bounds share the namespace registry's
hard ceilings; retained bytes are tested at exactly 2 MiB and one byte above it.
This reader does not resolve credentials, mutate native nodes, implement retirement,
or replace the production assignment/reconciliation loop.

The actual frontend/History/native/SDK workflow fixture changes Source and Query
lifecycle revisions between real registry reads. It observes the mixed-view
refusal, retries, resolves the older exact pins, and admits both Query lifecycle
aliases to the same native query ID/evaluation revision before delivering output.
The normal verifier runs these reader checks in native and both no-cgo modes.

## Trust and recovery

Plugins require configured catalogs, pinned digests, binary hashes and exact
release/ABI/core/lib/platform compatibility. Signatures and signer identities
are not required. The host checks content and actual exported metadata before
loading descriptors. External sources require an explicit bootstrap
descriptor. Libraries remain loaded until process exit; there is no live unload
or replacement operation.

### Native plugin management

`Config.catalogs` is an administrator-authored list of catalogs and immutable
releases. A catalog has `id`, `display_name`, and `plugins`; each release has
`version` and an `artifact` using the existing `Plugin` configuration
(`id`, digest-pinned `reference`, `binary_sha256`, `filename`, and optional
absolute `local_catalog_path`). There is no implicit default catalog. Discovery
fetches metadata only for these configured releases using `drasi-host-sdk`;
it never accepts registry prefixes, trust configuration, file paths, or URLs
from an operation request. Catalog IDs and release pins are not advertised as
cryptographically verified by discovery.

The existing `Config.plugins` startup list still installs and loads before
source construction. It is exposed as the reserved `startup` catalog.
The legacy configuration has no release-version field, so this catalog uses the
manifest digest as its version selector, not an asserted upstream release.
Adding catalogs does not change the durable query-generation fingerprint; they are
host policy, not query state or authoritative Temporal installation records.
Existing sources and queries remain immutable. An explicit load registers
descriptors in that runtime, not a new source or query. External-source
definitions still use startup configuration; plugin-backed incremental admission
requires the immutable credential-version recipe that is not implemented yet.
The public Source contract now carries explicit provider/key/version references,
and registry acceptance enforces their presence on fresh definitions after
idempotency lookup. It also rejects SDK resolver forms in ordinary configuration.
The native credential router now accepts ephemeral values only for exact
declared field/reference tuples and verified resident artifact/component pins.
Each resident library gets one fixed resolver; random per-instance capabilities
separate source and bootstrap audiences, including the Source configuration
passed to the bootstrap factory. Values are not inserted into declarative recipes.
Router-owned values are limited to 16 KiB per field, 256 KiB per construction,
16 MiB per graph and 64 MiB per process. Group/token counts are also bounded.
Revocation retains all allocation quotas until the last callback borrow ends.
Unproved cleanup quarantines the credentials and graph reservation rather than
revoking credentials needed by surviving work. Callers must explicitly prove
joined teardown before releasing a lease; a dropped lease is not shutdown proof.
Owned byte buffers are cleared on destruction, but SDK string/FFI copies are not
promised to be zeroized. Native code is trusted, not a confidentiality sandbox.

These primitives are not yet wired to Graph source admission, provider
authorization, descriptor-schema validation or checkpoint/seal fencing. The
declarative catalog does not accept external recipes yet. Global plugin
log/lifecycle callbacks discard plugin-provided text; credential-bearing Source
wrappers must additionally select the opt-in per-instance SDK callback path and
sanitize properties, synchronous errors and asynchronous bootstrap failures.

The C signature is unchanged:

```c
temporal_drasi_v1_status temporal_drasi_v1_apply(
    uint64_t runtime, const uint8_t *data, size_t length,
    uint64_t cancel, uint32_t timeout_ms);
```

Its new closed JSON command variants are `list_catalogs`, `discover_plugins`
(`catalog_id`, `search`), `inspect_plugin` (`selection`), `install_plugin`
(`selection`, `digest`), `load_plugin` (`digest`),
`list_plugin_installations`, and `get_plugin_installation` (`digest`).
A selection contains only `catalog_id`, `plugin_id`, and `version`.
Unknown fields are rejected, including nested fields. Unknown selections and
digests fail before accessing a registry or filesystem location.

Rust exposes these through
`pub async fn Host::apply(&self, command: Command) -> Result<Value>`.
Checkpoint uses `Host::checkpoint(&mut self, destination: &str)` so Rust
callers also require exclusive ownership. The ABI's `Request` routes the
unchanged checkpoint JSON command to that exclusive path; its remaining
`Command` variants use shared lifetime leases.
The typed Rust DTOs are in `src/protocol.rs`.
Go exposes the existing exact signature
`func (r *Runtime) Apply(ctx context.Context, command Command) (Result, error)`.
The new Go commands are `ListCatalogsCommand`, `DiscoverPluginsCommand`,
`InspectPluginCommand`, `InstallPluginCommand`, `LoadPluginCommand`,
`ListPluginInstallationsCommand`, and `GetPluginInstallationCommand`.
`Result` has typed `Catalogs`, `DiscoveredPlugins`, `Plugin`, `Installation`,
and `Installations` fields. The existing `Result.Plugins` raw descriptor JSON
is preserved for compatibility; the new exact signature
`func (r *Runtime) Descriptors(ctx context.Context) (PluginDescriptors, error)`
provides a typed descriptor response.

For example, after the administrator has supplied the catalog release in
`Config` and opened the runtime:

```go
selection := native.PluginSelection{
    CatalogID: catalogID, PluginID: pluginID, Version: version,
}
inspected, err := runtime.Apply(ctx, native.InspectPluginCommand{Selection: selection})
if err != nil {
    return err
}
if !inspected.Plugin.Compatible {
    return fmt.Errorf("incompatible plugin: %s", inspected.Plugin.Message)
}
installed, err := runtime.Apply(ctx, native.InstallPluginCommand{
    Selection: selection, Digest: inspected.Plugin.Digest,
})
if err != nil {
    return err
}
loaded, err := runtime.Apply(ctx, native.LoadPluginCommand{
    Digest: installed.Installation.Plugin.Digest,
})
if err != nil {
    return err
}
// loaded.Installation.State is PluginLoaded only after actual SDK initialization.
// loaded.Installation.Plugin.Descriptors contains the SDK's real component schemas.
```

Install checks the manifest digest for OCI artifacts, exact release/SDK version,
core/lib versions, platform, and configured binary hash. It uses the
unmodified SDK's OCI downloader with its default disabled signature verifier.
For local bundles, `reference` is `local@sha256:<binary hash>` and
`local_catalog_path` is the absolute library path beside `metadata.json`.
Local content is checked before the SDK's install/copy operation and again
after copying. The host atomically publishes a read-only binary and content
receipt under `plugins/<artifact-digest>/<binary-sha256>/`. Existing entries are
revalidated, never overwritten or silently repaired. Interrupted staging
directories are not installations. Retries converge on the same immutable
artifact; no request ID or authoritative desired state is stored here.

Load requires an installed, intact cache entry. It rechecks cached content and
checks the binary's embedded metadata before
`drasi_plugin_init`, then uses the SDK loader. Process-wide per-identity
initialization slots retain the SDK handle and reject replacement pins with
`restart_required`, even after a runtime closes. Concurrent attempts to load the
same artifact share initialization; library I/O occurs outside the process-map
mutex. Rejected libraries that have been opened also remain resident.

`PluginObservation` is host-local, with a typed `PluginHostState`, runtime
`host_id`, observation time, and redacted diagnostic. Legacy optional signer
fields remain empty. `installed` means content-checked cache bytes exist; `loaded`
means descriptors were registered in this runtime. Failed install/load calls
return an error and retain a queryable failed observation rather than returning
success. A subsequent observation checks cache integrity again. No status
claims Temporal durable acceptance, assignment, caller authorization, or
successful external database connectivity. ABI deadlines/cancellation still
allow an already-started operation to finish; retry or inspect its observation.

#### Durable cluster intent and runtime propagation

The private `watchregistry` cluster root retains immutable installation metadata,
permanent method-bound request receipts, and one desired digest per native plugin
identity. It lives at the fixed local `temporal-system` business key, separately
from namespace resource registries. A superseded Load retry preserves its original
accepted digest but cannot restore an older desired selection. Host compatibility,
support flags, diagnostics and Loaded claims are never persisted as cluster truth.

Each Watch process reads complete bounded cuts independently of namespace feed
and topology reconciliation. Missing-root reads are explicit empty snapshots;
unavailable, malformed, regressing or inconsistent History reads never erase the
last accepted cut. Retained metadata and desired native identities cannot
disappear in later cuts. The process opens a dedicated dormant management Runtime
under `stateRoot/cluster-plugins` using the administrator's startup policy.
Its native definition, including preloads, remains immutable; a preload-definition
change requires an explicit isolated store rather than resetting that store.

The management worker reconciles all retained installations. Each actual
namespace Graph independently installs and loads its current desired pins,
including Graphs opened after the cut was read. Workers neither change native
Config nor grant managed Source/bootstrap pairs or credential access. Managed
materialization selects exact loaded pins from that Graph's native plugin map;
loading the management Runtime is not factory registration in another Runtime.
The plugin worker has a separate lifecycle from first `Graph.Reconcile`, and is
canceled and joined before Graph checkpoint. Native completion ownership still
drains an operation whose shutdown waiter has canceled.

One immutable `PluginAttempt` remains attached through each operation's actual
completion; only shutdown cancels its wait. Errors can follow effects, so a fresh
native observation, not an error's absence/presence, determines observed state.
New intent can arrive while old work completes, but old-generation completions
cannot publish evidence for the new cut. Workers yield between artifacts, jitter
capacity retries, and refresh successful native cache/resident observations once
per minute rather than hashing every artifact on every control-plane tick.
Native and Go process-wide eight-operation bounds remain in force. Slow or stuck
trusted native operations can delay convergence; they do not justify false
readiness or release capacity early.

The private `WatchService.ObservePlugins` reduces management and every actual
local namespace Graph for a requested complete generation. Starting, failed,
closing, wrong-boot, wrong-volume and unobserved Graphs remain unknown rather than
being omitted. Loaded requires matching fresh evidence from every applicable
Runtime. A nonselected artifact is reported at most Installed, even if an old
factory remains resident. Root observations expire after five seconds; native
evidence expires after two minutes and retains its actual probe timestamp, not
the latest control-plane tick. Responses identify the storage host and process
boot, and recheck the sampled Graph set and lifecycle before returning.
This private endpoint does not authorize public requests. The public frontend
projects its evidence after the normal cluster-scoped authorization checks.

The cluster observation client enumerates all Watch membership entries, including
draining members, rather than looking up a namespace owner. It validates complete
generation-matched replies, canonical storage/boot identities, exact requested
digests, known states, signer evidence and timestamps. Public host identifiers are
membership identities, including for unreachable members; private storage UUIDs
do not replace those identifiers. Unreachable or invalid members remain explicit
unknowns with redacted diagnostics. Conflicting storage/boot identities invalidate
both members' evidence. A changed membership cut returns the bounded union of old
and new entries as unknown; an oversized union fails rather than dropping hosts.
No atomic cluster-wide snapshot or future Graph readiness is implied.

The client accepts at most 64 members and 200 digests per internal page, reserves
at most eight RPCs across concurrent calls on each frontend observer, and joins
its dispatchers under a two-second work budget. Native evidence is checked again
after all replies complete, so waiting for another member cannot keep expired
evidence healthy. Each projected host status is bounded to 4096 serialized bytes,
including its membership identity. Public pagination must reserve metadata plus
these per-host bounds; the internal digest count is not a public page byte budget.
The public PluginService uses this client without treating empty membership as
installation or loading readiness.

#### Private catalog reads

The private Watch API exposes typed `ListCatalogs`, `ListPlugins`, and `GetPlugin`
reads over the actual dormant management Runtime. Requests contain only configured
catalog identities, release identities, search text, and pagination; they cannot
introduce registry locations, paths, credentials, trust policy, or native commands.
These reads do not install, load, or materialize components. Metadata compatibility
is not signature verification. Descriptors still require an actual verified load.

Discovery sorts configured selections and chooses at most 16 releases before
fetching any registry metadata. Individual projected plugin metadata is bounded
to 64 KiB. Public-shaped pages reserve continuation space and remain within 1 MiB;
byte-limited continuations resume after the last returned release, not after
fetched-but-unreturned metadata. Catalogs are sorted and bounded to 32 configured
catalogs plus the optional startup catalog. The older native discovery command's
omitted limit retains its unpaged behavior.

Continuation tokens bind the method, filters, and Watch process boot. Changing
hosts, restarting a process, or changing filters requires restarting pagination.
Tokens are cursors, not authorization grants or cryptographic provenance.
The configured selection set is immutable within that Runtime; discovery does
not promise a transactional snapshot of changing cache or resident metadata.

The frontend client chooses the first available member by sorted membership
identity, without namespace placement or namespace assignment. Empty membership
is explicitly unavailable. Catalog RPCs share the observer's eight-slot budget.
Local catalog listing and plugin status RPCs have a two-second deadline;
registry-backed discovery and inspection allow 30 seconds, bounded by any earlier
caller deadline. Native completion ownership is
separate: canceling a catalog caller does not release the runtime's actual
operation, bypass its busy state, or let close pass unfinished native work.
Private reads and their client do not authorize public requests. The public
frontend handlers below provide that boundary. External-source capability remains
separate from catalog discovery and desired installation.

Public descriptors expose the selected configuration schema at the JSON root.
The complete named SDK schema map is retained under `components.schemas`, so
original `#/components/schemas/Name` references resolve within that document.
Clients do not infer a root from component names or map ordering. For local
validation only, compose non-secret configuration with canonical structural
Secret references for the separately bound top-level fields. Send the original
configuration and immutable reference map separately; UI hints cannot authorize
references, and canonical SDK references are not accepted as literal configuration.

#### Public plugin management and HTTP routes

The frontend registers the contract's complete `PluginService` alongside
`WatchService`. Both services' generated HTTP routes use the same in-process
client connection and gRPC interceptor chain as the standard Temporal HTTP API.
The validating service wrappers apply to both transports. Authorization headers
are forwarded through the standard gateway; no direct handler-server gateway
bypasses authentication or authorization.

Plugin discovery, capabilities and installation observations require cluster-read
access. Installing an immutable catalog selection and requesting loading require
cluster-admin access. Namespace administration does not grant those permissions.
`GET /api/v1/drasi/plugins/capabilities` filters its fully qualified allowed
methods using the configured authorizer. Enabled means a native management
Runtime is available, not that any artifact has passed verification.

Installation resolves only the configured catalog, plugin ID and version and
requires the advertised digest to match. Neither RPC accepts a binary, a registry
location, a filesystem path or credentials. Permanent method-bound receipts are
checked before current catalog resolution. Semantic refusals are retained;
transient catalog failures do not become permanent request outcomes. Replaying a
superseded Load returns that artifact's current `load_requested` value and does
not restore the old desired selection.

Installation and Load responses describe durable desired state plus fresh,
per-member observations. They are not synchronous attestations of successful
native verification or initialization. Retained metadata deliberately omits
host-dependent compatibility and descriptor support claims; inspect the catalog
release for current discovery metadata and each host for actual verification.
Signer identity is not a readiness condition. Empty membership is explicitly unavailable.

`GET /api/v1/drasi/plugins/installations` returns digest-ordered, generation-fenced
pages. Every selected artifact reserves space for its metadata and all 64
possible 4096-byte host observations before requesting runtime status. The
complete response, including continuation, is bounded to 1 MiB. A changed intent
generation requires restarting pagination; host evidence can change independently
and does not constitute a transactional cluster-wide snapshot.

The actual Watch-role integration fixture exercises public catalog/capability
reads, installation and Load receipt replay, and HTTP installation reads before
and after a Watch-process restart. Its unconfigured artifact must remain Unknown.
It is not positive configured-plugin, PostgreSQL CDC, or browser acceptance evidence.

#### Pinned SDK limitations

* OCI metadata's `sdk_version` is the SDK **crate release** (`0.11.3`), not
  the binary FFI ABI (`0.14.0`). Discovery exposes the crate release as
  `sdk_version`; `abi_version` remains empty until actual verified load.
  The host uses the SDK's exported crate version rather than duplicating it.
  Core and lib must be exactly `0.5.10` and `0.9.3`; the native target must match exactly.
  Plugins whose published metadata incorrectly reports the plugin's own
  release as core/lib are rejected, not allowed through.
* OCI metadata has no component schemas. Descriptors remain empty until actual
  verified load. Thereafter the real source/bootstrap/reaction/identity/secret
  store schemas are exposed, preserving the SDK schema map and `x-ui` hints.
  Only source/bootstrap factories are supported. Schema JSON must be an object
  and is bounded; invalid schemas cause load failure, not a fabricated schema.
  The descriptor JSON preserves `kind`, `schema`, and `instantiable` field names.
* The unmodified SDK's default registry configuration disables signature
  verification. This proof of concept uses that configuration as-is.
* The SDK's local-directory discovery opens binaries to inspect them, so this
  adapter does **not** use it to discover unchecked directories. Local catalogs
  select exact configured files; the SDK's local install/copy operation is used
  only after checking their pins. Downloaded and copied bytes must match the
  configured hash. Local-only installation and cached loading work offline.
* The SDK's metadata fetch currently pulls artifact layers, including the
  binary. These operations are not a lightweight registry index service.
  Native policy caps configured releases, schemas and binary file sizes; the
  SDK does not expose a streaming network download size limiter here.

Checkpointing quiesces and closes the runtime, then publishes a manifest with
file hashes. Restore uses an isolated destination and preserves query/source
state, output incarnation, pending handoffs, and receipts. Directory locking
prevents concurrent local owners. A distributed coordinator must additionally
fence runtime assignments in Temporal; a local file lock cannot fence another
host.

## Verification scope

`verify.sh` runs Rust formatting, tests, and Clippy; a compiled C ABI client;
and native-enabled and disabled Go tests. The tests execute a real Drasi query,
replay unaccepted output after restart, preserve separate Watch boundaries,
reject gaps, and restore an isolated checkpoint.

The verifier also exercises the actual Temporal frontend WatchService, durable
SQLite-backed History registry, native output acceptance and a real SDK workflow.
It injects a lost post-commit acceptance response and requires one recorded
workflow effect. A separate no-cgo fixture proves default-off capabilities,
stable accepted/rejected mutation retries and disabling cleanup. These fixtures
do not start a production Watch reconciler or prove PostgreSQL/UI integration.

Private SDK-premise tests also add a second query to an already-running shared
application-fed source, preserve the first Watch's pending bytes, attach another
Watch, stop/start queries, and reopen/re-admit the second query without changing
the first query's output identity. Failed query startup does not prevent the
existing queries from processing later changes. Stop completion is observed from
the actual component graph before restart; a successful stop call alone does not
mean its asynchronous lifecycle transition has completed. Another real-store
case changes output-generation metadata and confirms an existing subscription
rejects the new generation rather than advancing its old cursor.

The private admission API additionally proves late source/query admission,
immutable lifetime binding, creation-permission crash cuts, exact byte limits,
and isolated checkpoint recovery with unchanged pending bytes. The actual
frontend/History/SDK workflow fixture now uses these admissions and real registry
incarnations rather than a preconfigured native graph. Its public and native
IDs/revisions deliberately differ. A real blocked TLS metadata fetch also proves
poll/ack, feed dispatch, and subsequent output remain serviceable before and
after the provisioning caller cancels; configured-catalog restrictions remain.
These are not production reconciliation or PostgreSQL/browser verification.

Stock `Strict` recovery is not mutation-free existing-only admission. The host
retains pending Watch handoffs and refuses incompatible output generations or
gaps, rather than assuming that every successful SDK startup preserves its old
output. Missing control identity/definition or admission catalogs fail closed.
The dependency-integrity tests reject non-registry Drasi packages, modified
crate archives, modified source bytes and extra source files. Concurrent PostgreSQL
bootstrap/feedback and complete production graph-reconciliation crash recovery remain
required gates before exposing production graph mutation.

Plugin tests cover configured-catalog isolation and request-field rejection,
unsigned local installation, compatibility boundaries, cache corruption,
offline cache observations, and a
real SDK-built descriptor fixture loaded through the resident native loader.
The same fixture constructs real Source and bootstrap instances across the
plugin ABI. It verifies an empty positional bootstrap, configuration properties,
and a managed Source remaining stopped while the actual SDK starts. Explicit
Source start/stop and joined SDK shutdown are also exercised. The factory forwards
the requested auto-start setting; automatic startup retains its existing setting.
This fixture is not a CDC source or plugin-backed Graph reconciliation proof.
The unsigned fixture additionally exercises normal configured local install/load
and content-tamper refusal, without seeded cache receipts or signer evidence.

Credential tests load two independently linked fixture libraries through the
actual loader, interleave two namespace/graph constructions, and exercise delayed
resolution, stop/restart, bootstrap-owned credentials and bootstrap-audience
Source configuration. A same-image alias does not replace the resolver; a
different library cannot resolve the first library's token. Values containing
literal `${...}` remain literal. Deliberate premature revocation makes a later
start fail. A diagnostic fixture emits a resolved value and capability through
both log and lifecycle callbacks; the redacted per-instance path preserves the
host identity/error status without retaining that text. The SDK's own tests also
exercise forged callback identities. `tests/test-sdk-diagnostics.py` rebuilds the two native diagnostic fixture
libraries and runs the exactly selected native callback/loader regression,
verifying registry dependency integrity before and after the run. This is
primitive-level coverage, not provider
authorization, safe arbitrary-plugin teardown or persisted Graph continuity.

For real PostgreSQL Source/bootstrap plugins, use a configured catalog with
compatible immutable pins, build the host, then run:

```sh
TEMPORAL_DRASI_PLUGIN_CONFIG=/absolute/prepared-postgres-bundle/catalog.json \
    bash native/drasi/verify-plugins.sh
```

That optional fixture uses the public Go/C ABI, asserts real installation and
load plus both PostgreSQL descriptor categories, and checks retries. It does
not bypass verification or make a database connection. Missing configuration
makes the script fail; directly running the integration test without it reports
a skip, not a verified installation.

### Configured PostgreSQL database fixture

```sh
TEMPORAL_DRASI_PLUGIN_CONFIG=/absolute/prepared-postgres-bundle/catalog.json \
  bash native/drasi/verify-postgres.sh
```

Without an explicit configuration, the verifier uses
[`tests/postgres-catalog.json`](tests/postgres-catalog.json). Its immutable
Darwin arm64 releases are the public
[PostgreSQL source 0.2.12](https://github.com/drasi-project/drasi-core/releases/tag/drasi-source-postgres-v0.2.12)
and separate
[PostgreSQL bootstrap 0.2.15](https://github.com/drasi-project/drasi-core/releases/tag/drasi-bootstrap-postgres-v0.2.15).
Manifest/binary digests are confined to this fixture, not generic runtime code.
These particular binaries have incompatible exported metadata and remain a
negative compatibility fixture, not the positive PostgreSQL configuration.

The script inspects public manifest/metadata pins or the configured local
bundle, then performs actual digest, installation, and load checks.
To also reproduce the retained upstream release-tag binding, run
`tests/inspect-postgres-catalog.py` with `--verify-release-tags`; that convention
is not required of configured local artifacts.
After plugins load, the script starts its own digest-pinned PostgreSQL 17 container with logical
replication, seeds matching/nonmatching rows, and creates a publication. It
attempts both plugins independently. If both load, the Go/C ABI test constructs a
generation with the real source and separate bootstrap, checks initial query
output `{"name":"initial","doubled":24}`, executes an SQL update, and requires
actual output `{"name":"initial","doubled":36}` at a later sequence.
`TEMPORAL_DRASI_PLUGIN_CONFIG` can select an administrator-reviewed replacement
catalog; it cannot bypass content or compatibility checks. The script
removes only its own container and writes evidence under `.verification`.

This retained catalog uses the selected SDK 0.11.3/ABI 0.14 release family.
Release-family alignment does not prove that the binaries' exported metadata,
schemas, loading, database behavior or recovery satisfy the host contract.

**Public artifact incompatibility:** the retained public Darwin arm64 binaries
contradict their OCI metadata:

| Artifact | Advertised core / lib | Embedded core / lib | Embedded ABI |
| --- | --- | --- | --- |
| Source 0.2.12 | `0.5.10` / `0.9.3` | `0.2.12` / `0.2.12` | `0.14.0` |
| Bootstrap 0.2.15 | `0.5.10` / `0.9.3` | `0.2.15` / `0.2.15` | `0.14.0` |

Both therefore returned `incompatible plugin binary metadata`, not `loaded`.
The published 0.9.3/SDK 0.11.3 host reproduces the source refusal: its required
ABI and the binary ABI both equal 0.14.0, but the embedded core/lib values are
wrong. Switching the host release family or removing signature requirements
does not repair that artifact.
The published release tag commit
`22125bf1d66062533b832a166fe4a51079a23d6e` contains the cause in the
[source export](https://github.com/drasi-project/drasi-core/blob/22125bf1d66062533b832a166fe4a51079a23d6e/components/sources/postgres/src/lib.rs)
and [bootstrap export](https://github.com/drasi-project/drasi-core/blob/22125bf1d66062533b832a166fe4a51079a23d6e/components/bootstrappers/postgres/src/lib.rs):
both pass their own `CARGO_PKG_VERSION` as the core and lib versions.
The [SDK export macro](https://github.com/drasi-project/drasi-core/blob/207ef6f3c4cd12d854dbe512c9b670282f996284/components/plugin-sdk/src/lib.rs)
copies those values into binary metadata. The Temporal-owned packaging adapter
exports the actual linked versions while using the unchanged released Source
and bootstrap crates. Its prepared local bundle exercises normal install/load
without requiring an upstream change, publication, or signing.

A historical nightly Source attempt was refused by the former signing policy.
After removing that policy, its actual unsigned bytes installed successfully
but loading still failed because its embedded core/lib values were `0.2.12`.
Neither this result nor the stable catalog classifies all GHCR versions or
platforms. Mutable tags must be selected by their actual immutable digest.

These PostgreSQL checks do not prove plugin-backed production Graph recovery or
browser-to-workflow behavior. The separate built-in-source Watch-role and
same-volume History/native crash fixtures described above remain distinct proofs.
