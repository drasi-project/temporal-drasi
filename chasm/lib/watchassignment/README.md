# Cooperative native ownership

One nonterminating CHASM execution per namespace retains a native owner's epoch
independently of History shard ownership, membership, health checks, and Watch
control epochs. This is an internal History API, not a public management API.
New ownership is disabled by the existing namespace Drasi feature gate.

`Begin` atomically creates the namespace assignment through
`UpdateWithStartExecution`. An exact retry returns the active grant before the
version and enablement checks. Changed content under the current request ID is
rejected. No other Begin request can displace an active grant. A crashed or
unreachable owner blocks successors until a hosting controller establishes a
physical fence and publishes a verified retained-state checkpoint.

`Seal` accepts the exact active grant and version and retains one exact local
checkpoint locator and manifest SHA256. An identical retry is idempotent;
`Describe` resolves a lost response. Once sealed, the old Begin request cannot
reactivate its grant. A fresh Begin with the sealed version advances the epoch,
retains the recovery cut, and requires the same storage host and volume identity.
Versions reserve room for sealing and never wrap. Receipts cover the current
opening and sealing, not an unbounded request history.

**The record is not a physical fence or proof of native shutdown.** Only a trusted
hosting controller may call Seal, after either successfully stopping and joining
its native graph, or exclusively acquiring the dead owner's existing native
directory lock and completing storage-only recovery. Both paths close persistent
handles, durably retire the predecessor, publish a checkpoint, and verify its
exact manifest bytes. Any uncertain or failed shutdown must
permanently disqualify that graph from sealing; a later no-op close is not proof.
The controller must verify a recovery cut before opening a fresh isolated
epoch directory. These runtime controller operations are not implemented by
this CHASM library.

`service/watch.OpenGraph` implements cooperative handoff and process-crash
recovery on the original stable local host and volume. The existing OS lock,
not the assignment record or process boot ID, excludes live and suspended owners.
Durable native retirement prevents a delayed opener from writing after that lock
is eventually released. Snapshot-only recovery does not start Sources or plugins.
The CHASM transitions remain ordinary Seal and Begin; there is no timer, lease
expiry, force-clear operation, checkpoint transport, or cross-host recovery.

Regenerate the internal protocol and History-routed client with:

```sh
make proto
```
