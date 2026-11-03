# Go workflow conditions

This optional module provides workflow-owned, one-shot Drasi conditions. It uses
the existing public Watch API through ordinary activities and receives normal
Temporal workflow signals. It does not import the Temporal server or create
Sources or Queries. The shared Query must already exist and be running.

The module requires Go 1.26 and the existing Temporal Go SDK 1.49. The separate
[`contracts/watch`](../../contracts/watch) module remains SDK-free and supports
Go 1.23. Local replacements in this repository connect these development modules;
they are not published module versions.

Register the activities on the worker that runs the waiting workflows, using an
already authenticated public Watch client:

```go
import "github.com/drasi-project/temporal-drasi/sdk/go/condition"

if err := condition.RegisterActivities(worker, watchClient); err != nil {
    return err
}
```

In workflow code, pin the existing Query revision and supply its acceptance
deadline:

```go
result, err := condition.Await(ctx, condition.Condition{
    QueryID: queryID,
    QueryRevision: queryRevision,
    Deadline: workflow.Now(ctx).Add(time.Hour),
})
if err != nil {
    return err
}
return result.Match.After, nil
```

An already-true query satisfies the condition after consistent bootstrap.
Concurrent and repeated `Await` calls have separate marker-recorded identities
and signal receivers. Repeated calls are separate conditions, not subscriptions
to the next change. Registration retries reuse the complete frozen create
request, including its exact owning run and deadline. A matching signal can
complete the wait before a lost registration response is recovered.

`WaitTimeout`, when positive, bounds local observation to the shorter of that
duration and the acceptance deadline. Zero waits until the deadline. The
`DrasiConditionWaitTimeout` application error means observation stopped, **not**
that the condition was false or that acceptance did not occur. Accepted signals
can arrive after the deadline and cannot be retracted.

After an unsuccessful wait, the helper allows at most 30 additional seconds for
disconnected cleanup. Cleanup recovers the original create receipt before
canceling its original revision; it never treats a `GetWatch` absence as proof
that an in-flight create cannot commit. `Result.Cleanup` distinguishes no cleanup
needed, acknowledged cancellation, and unconfirmed cleanup. Acknowledged
cancellation does not retract accepted actions. Missing receipts after expiry,
revision conflicts, and exhausted cleanup remain explicit errors and are logged.
For primary cancellation, cleanup uncertainty is retained in canceled-error
details; other cleanup failures retain the original error as their cause.

The helper preserves caller activity routing and retry options. Missing
registration timeouts default to 10 seconds per attempt and one minute overall.
Cleanup uses 10-second attempts and a 30-second overall budget. Inherited retry
policies still apply; choose retry intervals that fit those budgets.

Normal replay within one exact run is supported. Reset-spanning waits are not:
recorded requests never retarget the reset run, and newly executing activities
reject an owner mismatch. A completed historical registration may instead wait
until its local timeout. Continue-as-new requires new waits in the new run.
Custom data converters must retain support for the server's standard
`json/plain` protobuf-JSON signal envelope.

## Verification

```sh
cd sdk/go
GOMAXPROCS=2 CGO_ENABLED=1 go test -race -count=2 -tags test_dep ./...
```

The root `TestDrasiWatchServerRole` fixture also executes this helper through
the actual separate Watch role, owning-History source, native query, public API,
and SDK worker. It verifies already-true exact-run delivery after an intentionally
lost create response, completion before registration confirmation, replay of the
captured real history, and timeout cleanup. This is not PostgreSQL or browser/UI
acceptance evidence.

The test workflow runs both standalone Go modules with their own declared
toolchains and includes their results in `Test Status`.
