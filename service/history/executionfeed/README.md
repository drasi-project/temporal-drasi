# Committed execution feed

The feed uses Temporal's primary persistence abstraction. It does not tail
History or poll Visibility. A versioned, payload-free `ExecutionFeedTask` is
included in each relevant mutable-state mutation/snapshot. History append alone
cannot publish a record. Retention deletion atomically inserts a tombstone with
the mutable-state deletion, using the shard ownership fence on SQL and Cassandra.

## Integration

Set `history.enableExecutionFeedQueue: true` before starting the services. This
registers category 8 in the shared startup task registry. Restart all services
when changing this setting. Once every relevant History writer supports capture,
set the namespace-filtered `history.enableExecutionFeed` setting for the desired
namespace. Both settings default to false. History does not enable capture when
its startup registry lacks the ingestion queue, even if the namespace setting
is true.

1. Register `tasks.CategoryExecutionFeed` (immediate category 8) at History
   startup when the cluster feature is enabled. The History engine creates its
   serial ingestion queue when this category is registered.
2. Set `configs.Config.ExecutionFeedEnabled`, a namespace-filtered boolean.
   Nil/false leaves ordinary execution capture unchanged for namespaces without
   an activated capture session. An activated session persists gap evidence for
   relevant uncaptured writes, including while its ingestion queue is disabled.
3. Keep `tasks.CategoryExecutionFeedJournal` (category 9) **out of ordinary queue
   registration and cleanup**. These rows are retained journal data.
4. Bootstrap through the owning History shard. Retain its capture epoch and
   reconstruction start. Pin managed reads with `Reader.CaptureEpoch`; namespace
   filtering does not change the partition cursor.
5. Complete bootstrap before declaring the source healthy. The deployment must
   establish that every relevant writer supports capture; a local configuration
   flag alone is not evidence that a mixed-version cluster is ready.

No schema migration is necessary on current schemas: both SQL and Cassandra
already store arbitrary immediate task categories. SQL primary-key enumeration
is implemented for MySQL, PostgreSQL, and SQLite. An older/custom SQL plugin
without enumeration support returns `Unimplemented`, not an empty bootstrap.

## Bootstrap and replay

Activate capture **before** enumerating. The first `BootstrapExecutionFeed`
request has epoch zero and no token. The owning shard drains in-flight writes,
persists a new shard range with the namespace capture epoch, and returns a fresh
reconstruction start in the new task-ID range. Range fencing resolves old writes
whose commit responses were lost. Transactions prepared before activation are
rejected before primary persistence and reload through normal conflict handling.
The namespace epoch survives owner replacement. A second source receives a fresh
reconstruction start without changing the existing source's epoch.

Run bootstrap on every shard, persisting the returned capture epoch, initial
reconstruction start, and primary-key page token in the source incarnation's
control state. Subsequent requests retain that epoch. `BootstrapPage` enumerates
primary keys, acquires the normal execution lock,
and commits a complete baseline with the normal mutable-state write. Retrying
a page captures current state again; it never reuses an unlocked scan's object.
Concurrent creates have capture tasks even if their keys precede the scan
cursor. Concurrent deletions have atomic tombstones.

After all pages complete, obtain a safe cutoff from each shard after its baseline
commits. Wait for `PublishedCut` to cover that cutoff. `Processor.Drain` can also
produce a cut directly, but only when `caughtUp` is true. Do not run a second
processor beside the History-owned queue for the same shard.

Managed sources reconstruct from their certified **reconstruction starts**, not
journal zero. This excludes images of executions deleted during an earlier
capture interruption. A scoped read refuses unscoped records, another epoch, or
a committed capture gap without advancing its cursor. A triggering source must
stop on a gap; only an explicit fresh reconstruction may discard that history.

The low-level, unscoped `Reader.Snapshot(ctx, vector, limit)` reconstructs an immutable execution
projection from journal versions strictly below that vector. Repeat reads
produce the same result while writers continue. Resume each partition with
`Reader.Read(ctx, shard, vector[shard], nextPublishedCut, limit)`. Persist the
returned exclusive cursor only after accepting the batch durably. A filtered
page can have zero records and still advance its cursor.

The vector is not a global atomic snapshot, a wall-clock boundary, or a
transaction shared with an external CDC source. The projection contains current
retained execution state. It cannot invent activity attempts/timers removed
before activation. The source control state must retain bootstrap completion
and the vector together; an empty/uninitialized vector is rejected.

## Record contract

`FeedRecord` identifies its shard and embeds a versioned `ExecutionFeedTask`.
Schema 1 is the legacy unscoped format. Schema 2 carries a positive capture epoch.
A schema-2 `capture_gap` has no state, facts, baseline, or tombstone and cannot be
interpreted as a projection replacement. A relevant write while namespace
capture is disabled commits that marker with the primary mutation. Passive
local-state rewrites likewise leave gap evidence instead of silently omitting
input. Ordinary unrelated CHASM updates and heartbeat-only changes do not
become execution-state records.

If a disabled projection cannot be decoded, History logs the failure and commits
a gap instead of blocking ordinary workflow progress. Because relevance cannot
be established from an unreadable projection, that gap is conservative even for
an otherwise unrelated update. Enabled capture still rejects projection errors;
it never substitutes an empty execution image.
Namespace/workflow/run identify an execution. Workflow activities use the
scheduled event ID; attempts add the attempt number. Standalone activities are
explicitly marked and use their own execution identity. Timers use the started
event ID in addition to the user timer ID, so a reused timer ID is not confused
with its predecessor.

`State` completely replaces the execution projection. `Facts` preserve committed
transitions, including retry-policy activity starts/failures/timeouts that have
no History events. A subsequently materialized transient Started event does not
produce a second Started fact. Workflow metadata includes typed decoded search
attributes, lineage, parent identity, and create-request identity for correlation
with Watch-originated starts. Inputs, results, failure messages/details, and
heartbeat details are never serialized.

Buffered timer firing is `FIRE_BUFFERED`, not a finalized `FIRED` fact.
Cancellation removes the buffered firing; only finalized event batches produce
`FIRED`. A buffered timer's deadline is retained from the preceding state or
reconstructed from its committed start event when loading a legacy baseline.

The stable delivery identity is `(shard, task ID, fact ordinal)`. Journal append
checks immutable content on retries. Source-task acknowledgment follows durable
journal acceptance. Projection reconstruction needs no separate mutable
checkpoint: the immutable journal record is the ingestion receipt.

## Explicit limits

- Global namespaces/passive-cluster action evaluation are unsupported. Bootstrap
  fails explicitly; enabling capture for such a namespace rejects active writes.
- Administrative force deletion is rejected while namespace capture is enabled
  or a durable capture session exists, even after its flag is disabled. Outside
  capture it reads the persisted shard range without requiring a live History
  owner and fences primary deletion against later capture activation. An
  initialized shard range is required; orphaned executions without initialized
  shard metadata cannot use this path. Normal deletion/retention uses atomic tombstones.
  When namespace metadata has already been removed, a registered source retains
  tombstones without guessing the namespace's former capture setting.
- Only workflows and standalone activity roots participate. Other CHASM
  archetypes are intentionally not execution graph nodes.
- Journal GC is not enabled. Records are retained indefinitely. Adding bounded
  retention requires replay/bootstrap leases and an explicit gap contract first.
- Capture metadata is bounded to 1,024 namespace epochs per shard. Existing
  sessions can reconstruct at that limit; new namespaces fail explicitly.
  Epochs are not automatically retired or pruned while retained sources can
  still resume.
- Snapshot reconstruction is bounded by the supplied execution count. Capture
  still obeys the existing Temporal transaction-size limit; enabling capture
  increases transaction size.
- Disabling capture while a source is active breaks continuity. A new bootstrap
  incarnation is required after capture interruption; callers must not present
  the old cursor as a continuous source.

  ## Internal transport

  The History service exposes shard-routed `BootstrapExecutionFeed` and
  `GetExecutionFeed` RPCs. Both require a local namespace with capture enabled and
  the startup ingestion category registered. They wait for the owning shard's
  engine rather than opening an independent ingestion processor.

  `BootstrapExecutionFeed` returns a capture epoch, first-page reconstruction
  start, primary enumeration page token, and an allocation-aware
  `bootstrap_cutoff` after the page's baseline commits. Continuation tokens
  require a positive capture epoch. After the
  last page, wait for the shard's `published_cutoff` to cover that cutoff.
  `GetExecutionFeed` scans `[start_position, end_position)` below a durably
  published cutoff and requires the established epoch; reading an unpublished suffix fails. A zero-length interval
  can discover the published cutoff without consuming records.

  Both APIs accept page sizes from 1 to 100. Bootstrap tokens are bounded to 16 KiB.
  Journal responses contain versioned `FeedRecord` JSON with at most 3 MiB of
  combined record bytes. A record that cannot fit alone fails explicitly. If a
  later record crosses the response bound, `next_position` leaves it unread.
  Persist the next position only after durable downstream acceptance, including
  empty namespace-filtered pages that advance it.

  The persistence metrics wrapper permits ingestion and journal reads and
  individual ingestion acknowledgment. It does not permit journal cleanup.
  These RPCs provide transport, not native source bootstrap readiness or durable
  downstream cursor storage.

  ## Verification

  All commands use `test_dep`; the SQLite persistence test also uses `integration`.
`TestExecutionFeedPrimaryCommitAndReplay` exercises real SQL mutable-state
transactions, History-append-before-commit failure, retry, primary enumeration,
durable replay, ownership fencing, and atomic deletion/tombstones. Unit tests
cover state-only activity transitions, transient-start deduplication, buffered
timer cancellation, disabled capture, heartbeat suppression, standalone payload
exclusion, lost commit/ack responses, namespace filtering, and a baseline/update
serialization race.
`TestExecutionFeedBootstrapUsesExecutionCache` exercises the real shard/engine
cache path for workflow and standalone-activity archetypes. The pre-lock primary
read discovers only the immutable archetype; baseline state is loaded again
under the normal execution cache lock.

`TestExecutionFeedHistoryRPC` starts a real SQLite-backed server and exercises
primary bootstrap, persisted capture epochs, History-owned journal ingestion,
namespace-isolated paging, immutable replay across a later workflow termination,
and payload exclusion through gRPC. It disables capture, creates and deletes
executions through public APIs, proves the old cursor encounters a durable gap,
replaces the History owner, and proves fresh reconstruction retains the epoch
without resurrecting the deleted execution.
`TestExecutionFeedCaptureWaitsForEmptyTaskWriteThenFencesUncertainty` covers
in-flight writes with no queue tasks and an unknown commit outcome. Shard tests
also cover stale create/update/reset/new-run/set transactions, activation failure,
metadata isolation, bounds, and deletion gaps without a registered queue.
`TestExecutionFeedPersistenceMetrics` verifies the decorated
persistence path used by that server; journal cleanup remains rejected.

Regenerate/check the three SQL primary-key readers with:

```sh
python3 common/persistence/sql/sqlplugin/generate_execution_enumeration.py
python3 common/persistence/sql/sqlplugin/generate_execution_enumeration.py --check
```

MySQL/PostgreSQL/Cassandra deployment verification is separate from the SQLite
contract test. No external CDC, native Drasi, UI, or end-to-end success is implied.
