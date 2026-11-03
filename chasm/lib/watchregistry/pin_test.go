package watchregistry

import (
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
)

func TestRegistryFreshPinsRejectSupersededIncarnations(t *testing.T) {
	t.Parallel()
	for _, kind := range []registrypb.ResourceKind{registrypb.RESOURCE_KIND_QUERY, registrypb.RESOURCE_KIND_WATCH} {
		t.Run(kind.String(), func(t *testing.T) {
			t.Parallel()
			for _, action := range []string{"create", "update", "recreate", "recreated dependency"} {
				t.Run(action, func(t *testing.T) {
					t.Parallel()
					env := newEnvironment(t, DefaultLimits())
					_, err := env.mutate(env.createSource("source", "source"))
					require.NoError(t, err)
					_, err = env.mutate(env.createSource("other", "other"))
					require.NoError(t, err)
					_, err = env.mutate(env.createQuery("query", "source", 1))
					require.NoError(t, err)
					_, err = env.mutate(env.createQuery("other-query", "other", 1))
					require.NoError(t, err)
					if kind == registrypb.RESOURCE_KIND_WATCH {
						_, err = env.mutate(env.createWatch("existing", watchSpec("other-query", 1)))
						require.NoError(t, err)
					}
					if action == "recreate" {
						if kind == registrypb.RESOURCE_KIND_QUERY {
							_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteQuery{
								DeleteQuery: env.queryOperation("other-query", 1, "delete"),
							}})
						} else {
							_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteWatch{
								DeleteWatch: env.watchOperation("existing", 1, "delete"),
							}})
						}
						require.NoError(t, err)
					}
					if action == "recreated dependency" {
						_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteQuery{
							DeleteQuery: env.queryOperation("query", 1, "delete-dependency-query"),
						}})
						require.NoError(t, err)
						if kind == registrypb.RESOURCE_KIND_QUERY {
							_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteSource{
								DeleteSource: env.sourceOperation("source", 1, "delete-dependency-source"),
							}})
							require.NoError(t, err)
							_, err = env.mutate(env.createSource("source", "recreate-source"))
						} else {
							request := env.createQuery("query", "source", 1)
							request.GetCreateQuery().RequestId = "recreate-query"
							_, err = env.mutate(request)
						}
					} else if kind == registrypb.RESOURCE_KIND_QUERY {
						_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateSource{
							UpdateSource: &contractpb.UpdateSourceRequest{
								Namespace: env.namespace, Id: "source", ExpectedRevision: 1, RequestId: "replace", Spec: sourceSpec(),
							},
						}})
					} else {
						_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateQuery{
							UpdateQuery: &contractpb.UpdateQueryRequest{
								Namespace: env.namespace, Id: "query", ExpectedRevision: 1, RequestId: "replace", Spec: querySpec("source", 1),
							},
						}})
					}
					require.NoError(t, err)
					var request *registrypb.MutateRequest
					if kind == registrypb.RESOURCE_KIND_QUERY {
						request = env.createQuery("new", "source", 1)
						switch action {
						case "update":
							request = &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateQuery{
								UpdateQuery: &contractpb.UpdateQueryRequest{
									Namespace: env.namespace, Id: "other-query", ExpectedRevision: 1, RequestId: "fresh-pin", Spec: querySpec("source", 1),
								},
							}}
						case "recreate":
							request.GetCreateQuery().Id, request.GetCreateQuery().RequestId = "other-query", "recreate"
						default:
						}
					} else {
						request = env.createWatch("new", watchSpec("query", 1))
						switch action {
						case "update":
							request = &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateWatch{
								UpdateWatch: &contractpb.UpdateWatchRequest{
									Namespace: env.namespace, Id: "existing", ExpectedRevision: 1, RequestId: "fresh-pin", Spec: watchSpec("query", 1),
								},
							}}
						case "recreate":
							request.GetCreateWatch().Id, request.GetCreateWatch().RequestId = "existing", "recreate"
						default:
						}
					}
					before := env.state(t)
					_, err = env.mutate(request)
					require.ErrorContains(t, err, "superseded resource definition")
					require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
					after := env.state(t)
					require.Len(t, after.Receipts, len(before.Receipts)+1)
					after.Receipts = before.Receipts
					protorequire.ProtoEqual(t, before, after)
					_, err = env.mutate(request)
					require.ErrorContains(t, err, "superseded resource definition")
				})
			}
		})
	}
}

func TestRegistryHistoricalPinsSurviveReplacementLifecycleAndReset(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	_, err := env.mutate(env.createSource("source", "source"))
	require.NoError(t, err)
	original := env.createQuery("query", "source", 1)
	created, err := env.mutate(original)
	require.NoError(t, err)
	_, err = env.mutate(env.createWatch("watch", watchSpec("query", 1)))
	require.NoError(t, err)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateSource{
		UpdateSource: &contractpb.UpdateSourceRequest{
			Namespace: env.namespace, Id: "source", ExpectedRevision: 1, RequestId: "replace-source", Spec: sourceSpec(),
		},
	}})
	require.NoError(t, err)
	for _, request := range []*registrypb.MutateRequest{
		{Operation: &registrypb.MutateRequest_UpdateQuery{UpdateQuery: &contractpb.UpdateQueryRequest{
			Namespace: env.namespace, Id: "query", ExpectedRevision: 1, RequestId: "replace-query", Spec: querySpec("source", 1),
		}}},
		{Operation: &registrypb.MutateRequest_UpdateWatch{UpdateWatch: &contractpb.UpdateWatchRequest{
			Namespace: env.namespace, Id: "watch", ExpectedRevision: 1, RequestId: "replace-watch", Spec: watchSpec("query", 1),
		}}},
		{Operation: &registrypb.MutateRequest_ResetWatch{ResetWatch: &contractpb.ResetWatchRequest{
			Namespace: env.namespace, Id: "watch", ExpectedRevision: 2, RequestId: "reset",
			InitialResults: contractpb.InitialResults_INITIAL_RESULTS_INCLUDE,
		}}},
		{Operation: &registrypb.MutateRequest_PauseWatch{PauseWatch: env.watchOperation("watch", 3, "pause")}},
		{Operation: &registrypb.MutateRequest_ResumeWatch{ResumeWatch: env.watchOperation("watch", 4, "resume")}},
		{Operation: &registrypb.MutateRequest_StartQuery{StartQuery: env.queryOperation("query", 2, "start-query")}},
		{Operation: &registrypb.MutateRequest_StopQuery{StopQuery: env.queryOperation("query", 3, "stop-query")}},
		env.createWatch("new", watchSpec("query", 4)),
	} {
		_, err = env.mutate(request)
		require.NoError(t, err)
	}
	retried, err := env.mutate(original)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, created, retried)
}

func TestRegistryFreshPinsAcceptEarlierRevisionsOfCurrentIncarnation(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	_, err := env.mutate(env.createSource("source", "source"))
	require.NoError(t, err)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StartSource{
		StartSource: env.sourceOperation("source", 1, "start-source"),
	}})
	require.NoError(t, err)
	_, err = env.mutate(env.createQuery("query", "source", 1))
	require.NoError(t, err)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StartQuery{
		StartQuery: env.queryOperation("query", 1, "start-query"),
	}})
	require.NoError(t, err)
	_, err = env.mutate(env.createWatch("watch", watchSpec("query", 1)))
	require.NoError(t, err)
}
