package watchregistry

import (
	"context"
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type environment struct {
	ctx       context.Context
	handler   *handler
	namespace string
	id        string
	ref       chasm.ComponentRef
	clock     *clock.EventTimeSource
	registry  *chasm.Registry
	engine    *chasmtest.Engine
	enabled   bool
}

func newEnvironment(t *testing.T, limits *registrypb.Limits) *environment {
	t.Helper()
	tv := testvars.New(t)
	name := strings.ReplaceAll(tv.NamespaceName().String(), "/", "-")
	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{
		Id: tv.NamespaceID().String(), Name: name,
	}, nil, "active")
	namespaces := namespace.NewMockRegistry(gomock.NewController(t))
	namespaces.EXPECT().GetNamespaceByID(namespace.ID(tv.NamespaceID().String())).Return(ns, nil).AnyTimes()
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly))
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&Library{}))
	ts := clock.NewEventTimeSource().Update(time.Unix(1000, 0).UTC())
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts),
		chasmtest.WithInvariantCheck(func(t *testing.T, node *chasm.Node, root chasm.RootComponent) {
			reloaded, err := chasm.NewTreeFromDB(node.Snapshot(nil).Nodes, registry, &chasm.MockNodeBackend{},
				chasm.DefaultPathEncoder, testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly), metrics.NoopMetricsHandler)
			require.NoError(t, err)
			persisted, err := reloaded.Component(chasm.NewContext(testcontext.For(t), reloaded), chasm.ComponentRef{})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, root.(*Component).State, persisted.(*Component).State)
		}),
	)
	env := &environment{
		namespace: name, id: tv.NamespaceID().String(), clock: ts, enabled: true, registry: registry, engine: engine,
		ctx: chasm.NewEngineContext(testcontext.For(t), engine),
		ref: chasm.NewComponentRef[*Component](chasm.ExecutionKey{NamespaceID: tv.NamespaceID().String(), BusinessID: BusinessID}),
	}
	env.handler = &handler{namespaces: namespaces, enabled: func(string) bool { return env.enabled }, limits: limits}
	return env
}

func (e *environment) mutate(request *registrypb.MutateRequest) (*registrypb.MutationResult, error) {
	request.NamespaceId, request.ExecutionId = e.id, BusinessID
	return e.handler.Mutate(e.ctx, request)
}

func (e *environment) read(request *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
	request.NamespaceId, request.ExecutionId = e.id, BusinessID
	return e.handler.Read(e.ctx, request)
}

func (e *environment) state(t *testing.T) *registrypb.State {
	t.Helper()
	state, err := chasm.ReadComponent(e.ctx, e.ref,
		func(c *Component, _ chasm.Context, _ chasm.NoValue) (*registrypb.State, error) {
			return common.CloneProto(c.State), nil
		}, nil)
	require.NoError(t, err)
	return state
}

func sourceSpec() *contractpb.SourceSpec {
	return &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}}
}

func (e *environment) createSource(id, requestID string) *registrypb.MutateRequest {
	return &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CreateSource{
		CreateSource: &contractpb.CreateSourceRequest{Namespace: e.namespace, Id: id, Spec: sourceSpec(), RequestId: requestID},
	}}
}

func querySpec(source string, revision uint64) *contractpb.QuerySpec {
	return &contractpb.QuerySpec{
		Language: "cypher", Text: "MATCH (n) RETURN n", StateStore: "persistent",
		Sources: []*contractpb.ResourceRef{{Id: source, Revision: revision}},
	}
}

func (e *environment) createQuery(id, source string, revision uint64) *registrypb.MutateRequest {
	return &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CreateQuery{
		CreateQuery: &contractpb.CreateQueryRequest{Namespace: e.namespace, Id: id, RequestId: "create-" + id, Spec: querySpec(source, revision)},
	}}
}

func watchSpec(query string, revision uint64) *contractpb.WatchSpec {
	return &contractpb.WatchSpec{
		Query: &contractpb.ResourceRef{Id: query, Revision: revision}, Trigger: contractpb.Trigger_TRIGGER_ENTER,
		Lifetime: &contractpb.WatchSpec_Persistent{Persistent: &emptypb.Empty{}},
		Action: &contractpb.WorkflowAction{Operation: &contractpb.WorkflowAction_Start{Start: &contractpb.WorkflowStart{
			WorkflowId: "target-{action_id}", WorkflowType: "workflow", TaskQueue: "queue",
			ReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		}}},
	}
}

func (e *environment) createWatch(id string, spec *contractpb.WatchSpec) *registrypb.MutateRequest {
	return &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CreateWatch{
		CreateWatch: &contractpb.CreateWatchRequest{Namespace: e.namespace, Id: id, RequestId: "create-" + id, Spec: spec},
	}}
}

func (e *environment) sourceOperation(id string, revision uint64, requestID string) *contractpb.SourceOperationRequest {
	return &contractpb.SourceOperationRequest{Namespace: e.namespace, Id: id, ExpectedRevision: revision, RequestId: requestID}
}

func (e *environment) queryOperation(id string, revision uint64, requestID string) *contractpb.QueryOperationRequest {
	return &contractpb.QueryOperationRequest{Namespace: e.namespace, Id: id, ExpectedRevision: revision, RequestId: requestID}
}

func (e *environment) watchOperation(id string, revision uint64, requestID string) *contractpb.WatchOperationRequest {
	return &contractpb.WatchOperationRequest{Namespace: e.namespace, Id: id, ExpectedRevision: revision, RequestId: requestID}
}

func TestRegistryOriginalRetryResultsAndMonotonicRecreation(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	createRequest := env.createSource("source", "create")
	original, err := env.mutate(createRequest)
	require.NoError(t, err)
	expected := &registrypb.MutationResult{Record: &registrypb.Record{
		Resource: &registrypb.Record_Source{Source: &contractpb.Source{
			Namespace: env.namespace, Id: "source", Revision: 1, Spec: sourceSpec(),
			DesiredState: contractpb.DesiredState_DESIRED_STATE_STOPPED,
		}},
		Desired: registrypb.DESIRED_STATE_STOPPED, Incarnation: "1",
	}}
	protorequire.ProtoEqual(t, expected, original)
	startRequest := &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StartSource{
		StartSource: env.sourceOperation("source", 1, "start"),
	}}
	started, err := env.mutate(startRequest)
	require.NoError(t, err)
	require.Equal(t, "1", started.Record.Incarnation)
	stopRequest := &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StopSource{
		StopSource: env.sourceOperation("source", 2, "stop"),
	}}
	stopped, err := env.mutate(stopRequest)
	require.NoError(t, err)
	require.Equal(t, "1", stopped.Record.Incarnation)
	retry, err := env.mutate(startRequest)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, started, retry)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteSource{
		DeleteSource: env.sourceOperation("source", 3, "delete"),
	}})
	require.NoError(t, err)
	recreated, err := env.mutate(env.createSource("source", "recreate"))
	require.NoError(t, err)
	require.Equal(t, uint64(5), recreated.Record.GetSource().Revision)
	require.Equal(t, "5", recreated.Record.Incarnation)
	env.enabled = false
	retry, err = env.mutate(createRequest)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, retry)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StopSource{
		StopSource: env.sourceOperation("source", 5, "start"),
	}})
	require.ErrorContains(t, err, "different method or content")
	changed := common.CloneProto(createRequest)
	changed.GetCreateSource().Id = "different"
	_, err = env.mutate(changed)
	require.ErrorContains(t, err, "different method or content")
	response, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_GetSource{
		GetSource: &contractpb.GetSourceRequest{Namespace: env.namespace, Id: "source"},
	}})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, recreated.Record, response.Records[0])
	require.Equal(t, uint64(5), response.Generation)
}

func TestRegistryRejectedCASIsBoundToOriginalIdentity(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	_, err := env.mutate(env.createSource("source", "create"))
	require.NoError(t, err)
	stale := &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StartSource{
		StartSource: env.sourceOperation("source", 2, "rejected"),
	}}
	_, err = env.mutate(stale)
	require.ErrorContains(t, err, "revision changed")
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StartSource{
		StartSource: env.sourceOperation("source", 1, "accepted"),
	}})
	require.NoError(t, err)
	_, err = env.mutate(stale)
	require.ErrorContains(t, err, "revision changed")
	require.Equal(t, uint64(2), env.state(t).Generation)
	fixed := common.CloneProto(stale)
	fixed.GetStartSource().ExpectedRevision = 1
	_, err = env.mutate(fixed)
	require.ErrorContains(t, err, "different method or content")
}

func TestRegistryPinsRetainedRevisionsAndChecksTransitiveDependents(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	source1, err := env.mutate(env.createSource("source", "create-source"))
	require.NoError(t, err)
	_, err = env.mutate(env.createSource("other", "create-other"))
	require.NoError(t, err)
	query1, err := env.mutate(env.createQuery("query", "source", 1))
	require.NoError(t, err)
	watch1, err := env.mutate(env.createWatch("watch", watchSpec("query", 1)))
	require.NoError(t, err)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateSource{
		UpdateSource: &contractpb.UpdateSourceRequest{
			Namespace: env.namespace, Id: "source", ExpectedRevision: 1, RequestId: "update-source", Spec: sourceSpec(),
		},
	}})
	require.NoError(t, err)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateQuery{
		UpdateQuery: &contractpb.UpdateQueryRequest{
			Namespace: env.namespace, Id: "query", ExpectedRevision: 1, RequestId: "update-query", Spec: querySpec("other", 1),
		},
	}})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		kind registrypb.ResourceKind
		id   string
		want *registrypb.Record
	}{
		{"source", registrypb.RESOURCE_KIND_SOURCE, "source", source1.Record},
		{"query", registrypb.RESOURCE_KIND_QUERY, "query", query1.Record},
		{"watch", registrypb.RESOURCE_KIND_WATCH, "watch", watch1.Record},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The engine is intentionally shared with the enclosing test's sequential mutations.
			response, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_Revision{
				Revision: &registrypb.RevisionLookup{Kind: tc.kind, Ref: &contractpb.ResourceRef{Id: tc.id, Revision: 1}},
			}})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, tc.want, response.Records[0])
		})
	}
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteSource{
		DeleteSource: env.sourceOperation("source", 2, "delete-source-blocked"),
	}})
	require.ErrorContains(t, err, "pinned by a watch query revision")
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteQuery{
		DeleteQuery: env.queryOperation("query", 2, "delete-query-blocked"),
	}})
	require.ErrorContains(t, err, "pinned by a watch")
	deleted, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteWatch{
		DeleteWatch: env.watchOperation("watch", 1, "delete-watch"),
	}})
	require.NoError(t, err)
	require.Equal(t, registrypb.DESIRED_STATE_DELETED, deleted.Record.Desired)
	require.Nil(t, deleted.Record.GetWatch().Status)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteSource{
		DeleteSource: env.sourceOperation("source", 2, "delete-source"),
	}})
	require.NoError(t, err)
	_, err = env.mutate(env.createWatch("late-watch", watchSpec("query", 1)))
	require.ErrorContains(t, err, "dependency does not exist or is deleted")
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteSource{
		DeleteSource: env.sourceOperation("other", 1, "delete-current-source"),
	}})
	require.ErrorContains(t, err, "pinned by a query")
}

func TestRegistryOnceDeadlineDeduplicationAndOwnerPinning(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	_, err := env.mutate(env.createSource("source", "source"))
	require.NoError(t, err)
	_, err = env.mutate(env.createQuery("query", "source", 1))
	require.NoError(t, err)
	tv := testvars.New(t).WithRunID("00000000-0000-4000-8000-000000000001")
	owner := &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: tv.RunID()}
	spec := watchSpec("query", 1)
	spec.Lifetime = &contractpb.WatchSpec_Once{Once: &contractpb.Once{
		Owner: owner, Deadline: timestamppb.New(time.Unix(2000, 0)),
	}}
	spec.Action.Operation = &contractpb.WorkflowAction_Signal{Signal: &contractpb.WorkflowSignal{
		Target: &contractpb.WorkflowSignal_Run{Run: common.CloneProto(owner)}, SignalName: "ready",
	}}
	request := env.createWatch("once", spec)
	original, err := env.mutate(request)
	require.NoError(t, err)
	env.clock.Update(time.Unix(3000, 0))
	retry, err := env.mutate(request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, retry)
	_, err = env.mutate(env.createWatch("expired", spec))
	require.ErrorContains(t, err, "deadline must be in the future")
	paused, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_PauseWatch{
		PauseWatch: env.watchOperation("once", 1, "pause"),
	}})
	require.NoError(t, err)
	require.Equal(t, registrypb.DESIRED_STATE_PAUSED, paused.Record.Desired)
	require.Nil(t, paused.Record.GetWatch().Status)
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_ResumeWatch{
		ResumeWatch: env.watchOperation("once", 2, "resume"),
	}})
	require.ErrorContains(t, err, "deadline must be in the future")
	changed := common.CloneProto(spec)
	changed.GetOnce().Owner.RunId = "00000000-0000-4000-8000-000000000002"
	changed.Action.GetSignal().GetRun().RunId = changed.GetOnce().Owner.RunId
	changed.GetOnce().Deadline = timestamppb.New(time.Unix(4000, 0))
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateWatch{
		UpdateWatch: &contractpb.UpdateWatchRequest{
			Namespace: env.namespace, Id: "once", ExpectedRevision: 2, RequestId: "retarget", Spec: changed,
		},
	}})
	require.ErrorContains(t, err, "owner and deadline are immutable")
	env.enabled = false
	canceled, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CancelWatch{
		CancelWatch: env.watchOperation("once", 2, "cancel"),
	}})
	require.NoError(t, err)
	require.Equal(t, registrypb.DESIRED_STATE_CANCELED, canceled.Record.Desired)
}

func TestRegistryFiniteRetentionNeverEvictsIdentities(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		limits *registrypb.Limits
	}{
		{"request identities", &registrypb.Limits{ResourceIds: 2, Mutations: 1, StateBytes: 4096, RequestBytes: 4096, PageBytes: 8192}},
		{"resource tombstones", &registrypb.Limits{ResourceIds: 1, Mutations: 10, StateBytes: 8192, RequestBytes: 4096, PageBytes: 8192}},
		{"retained bytes", &registrypb.Limits{ResourceIds: 10, Mutations: 100, StateBytes: 4096, RequestBytes: 4096, PageBytes: 8192}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t, tc.limits)
			request := env.createSource("original", "original")
			original, err := env.mutate(request)
			require.NoError(t, err)
			if tc.name == "resource tombstones" {
				_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteSource{
					DeleteSource: env.sourceOperation("original", 1, "delete"),
				}})
				require.NoError(t, err)
			}
			var lastErr error
			for i := range 50 {
				id := "new-" + strings.Repeat("x", i+1)
				_, lastErr = env.mutate(env.createSource(id, id))
				if lastErr != nil {
					break
				}
			}
			require.Error(t, lastErr)
			require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(lastErr).Code())
			state := env.state(t)
			retry, err := env.mutate(request)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, original, retry)
			protorequire.ProtoEqual(t, state, env.state(t))
			require.LessOrEqual(t, proto.Size(state), int(tc.limits.StateBytes))
			require.LessOrEqual(t, len(state.Receipts), int(tc.limits.Mutations))
			require.LessOrEqual(t, len(state.Current), int(tc.limits.ResourceIds))
		})
	}
}
