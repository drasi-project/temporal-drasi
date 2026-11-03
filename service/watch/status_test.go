package watch

import (
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/watchservice/v1"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	watchclient "go.temporal.io/server/client/watch"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/watch/native"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func statusGraph(t *testing.T) (*Graph, *watchservice.ObserveRequest, time.Time) {
	t.Helper()
	source, query, watch := desiredRecords()
	source.Desired, query.Desired, watch.Desired = registrypb.DESIRED_STATE_RUNNING, registrypb.DESIRED_STATE_RUNNING, registrypb.DESIRED_STATE_RUNNING
	// Public lifecycle revisions need not equal native activations or control creation revisions.
	source.GetSource().Revision, query.GetQuery().Revision, watch.GetWatch().Revision = 3, 5, 9
	query.GetQuery().Spec.Sources[0].Revision, watch.GetWatch().Spec.Query.Revision = 3, 5
	view, err := ReadDesiredView(t.Context(), &desiredTestClient{t: t, steps: desiredSteps(source, query, watch)},
		"00000000-0000-4000-8000-000000000001", "ns")
	require.NoError(t, err)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	graph := &Graph{
		options: GraphOptions{NamespaceID: view.namespaceID, Namespace: "ns", TemporalInput: ManualTemporalInput{}},
		owner: &assignmentpb.Active{Grant: &assignmentpb.Grant{
			NamespaceId: view.namespaceID, InstanceId: view.namespaceID, Epoch: 2,
			StorageHostId: "10000000-0000-4000-8000-000000000001", ProcessBootId: "20000000-0000-4000-8000-000000000001",
		}},
		view: view, nodeGeneration: view.generation, nodeObservedAt: now.Add(-time.Second),
		nodes: make(map[string]native.NodeObservation), problems: make(map[string]string),
		watches: make(map[native.ResourceLifetime]*graphWatch),
	}
	for _, record := range []*registrypb.Record{source, query} {
		definition, ready, err := desiredNode(view, record, graph.nodes, graph.temporalRecipe())
		require.NoError(t, err)
		require.True(t, ready)
		id := "native-" + definition.Lifetime.ResourceID
		graph.nodes[id] = native.NodeObservation{NativeID: id, Definition: definition, Status: native.NodeRunning}
	}
	binding, err := view.NativeWatchBinding("watch")
	require.NoError(t, err)
	graph.watches[binding.Watch] = &graphWatch{
		lifetime: binding.Watch, generation: view.generation,
		status: controlpb.STATUS_RUNNING, accepting: true, live: true,
		observedAt: now.Add(-2 * time.Second), pending: 2,
		binding: &native.SubscriptionObservation{
			Definition: native.Subscription{QueryID: "native-query"}, Binding: &binding,
		},
	}
	request := &watchservice.ObserveRequest{
		NamespaceId: view.namespaceID, Generation: view.generation,
		Owner: &watchservice.Owner{Epoch: 2, StorageHostId: graph.owner.Grant.StorageHostId, ProcessBootId: graph.owner.Grant.ProcessBootId},
	}
	for _, record := range []*registrypb.Record{source, query, watch} {
		target, err := watchclient.RecordTarget(record)
		require.NoError(t, err)
		request.Targets = append(request.Targets, target)
	}
	return graph, request, now
}

func TestRuntimeStatusExactIdentityAndEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Graph, *watchservice.ObserveRequest)
		code   codes.Code
	}{
		{name: "matching public revisions"},
		{name: "namespace", code: codes.Unavailable, change: func(_ *Graph, r *watchservice.ObserveRequest) { r.NamespaceId = "other" }},
		{name: "owner epoch", code: codes.Unavailable, change: func(_ *Graph, r *watchservice.ObserveRequest) { r.Owner.Epoch++ }},
		{name: "storage volume", code: codes.Unavailable, change: func(_ *Graph, r *watchservice.ObserveRequest) { r.Owner.StorageHostId = "other" }},
		{name: "process boot", code: codes.Unavailable, change: func(_ *Graph, r *watchservice.ObserveRequest) { r.Owner.ProcessBootId = "other" }},
		{name: "registry cut", code: codes.Unavailable, change: func(_ *Graph, r *watchservice.ObserveRequest) { r.Generation++ }},
		{name: "native sample cut", code: codes.Unavailable, change: func(g *Graph, _ *watchservice.ObserveRequest) { g.nodeGeneration-- }},
		{name: "expired sample", code: codes.Unavailable, change: func(g *Graph, _ *watchservice.ObserveRequest) {
			g.nodeObservedAt = g.nodeObservedAt.Add(-5 * time.Second)
		}},
		{name: "failed sample", code: codes.Unavailable, change: func(g *Graph, _ *watchservice.ObserveRequest) { g.problems["observation"] = "password=secret" }},
		{name: "failed native cleanup", code: codes.Unavailable, change: func(g *Graph, _ *watchservice.ObserveRequest) { g.problems["cleanup"] = "password=secret" }},
		{name: "closing", code: codes.Unavailable, change: func(g *Graph, _ *watchservice.ObserveRequest) { g.closing = make(chan struct{}) }},
		{name: "incarnation", code: codes.FailedPrecondition, change: func(_ *Graph, r *watchservice.ObserveRequest) { r.Targets[0].Incarnation = "next" }},
		{name: "public revision", code: codes.FailedPrecondition, change: func(_ *Graph, r *watchservice.ObserveRequest) { r.Targets[0].Revision++ }},
		{name: "kind collision", code: codes.FailedPrecondition, change: func(_ *Graph, r *watchservice.ObserveRequest) { r.Targets[0].Kind = watchservice.RESOURCE_KIND_QUERY }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			graph, request, now := statusGraph(t)
			if tc.change != nil {
				tc.change(graph, request)
			}
			observations, err := graph.observeStatus(request, now)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			if tc.code != codes.OK {
				require.Nil(t, observations)
				require.NotContains(t, err.Error(), "password")
				return
			}
			require.Len(t, observations, 3)
			for i, entry := range observations {
				at, pending := graph.nodeObservedAt, uint64(0)
				if i == 2 {
					at, pending = now.Add(-2*time.Second), 2
				}
				protorequire.ProtoEqual(t, &watchservice.ResourceObservation{
					Target: request.Targets[i],
					Status: &contractpb.ResourceStatus{State: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING,
						ObservedRevision: request.Targets[i].Revision, UpdatedAt: timestamppb.New(at), PendingDeliveries: pending},
				}, entry)
			}
			observations[0].Target.Id = "changed"
			require.Equal(t, "source", request.Targets[0].Id)
		})
	}
}

func TestRuntimeStatusReadinessAndTerminalProjection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Graph, *graphWatch)
		source contractpb.LifecycleState
		watch  contractpb.LifecycleState
	}{
		{name: "live", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, watch: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING},
		{name: "input bootstrap", source: contractpb.LifecycleState_LIFECYCLE_STATE_STARTING, change: func(g *Graph, _ *graphWatch) {
			node := g.nodes["native-source"]
			node.Status = native.NodeStarting
			g.nodes[node.NativeID] = node
		}},
		{name: "deferred input", source: contractpb.LifecycleState_LIFECYCLE_STATE_STARTING, change: func(g *Graph, _ *graphWatch) {
			node := g.nodes["native-source"]
			node.Status = native.NodeDeferred
			g.nodes[node.NativeID] = node
		}},
		{name: "incomplete initial snapshot", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, change: func(_ *Graph, w *graphWatch) { w.live = false }},
		{name: "not accepting", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, change: func(_ *Graph, w *graphWatch) { w.accepting = false }},
		{name: "stale control", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, change: func(_ *Graph, w *graphWatch) { w.observedAt = w.observedAt.Add(-5 * time.Second) }},
		{name: "old control generation", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, change: func(_ *Graph, w *graphWatch) { w.generation-- }},
		{name: "wrong query binding", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, change: func(_ *Graph, w *graphWatch) { w.binding.Binding.Query.Incarnation = "other" }},
		{name: "completed", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, watch: contractpb.LifecycleState_LIFECYCLE_STATE_COMPLETED,
			change: func(_ *Graph, w *graphWatch) { w.status, w.accepting = controlpb.STATUS_SATISFIED, false }},
		{name: "canceled", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, watch: contractpb.LifecycleState_LIFECYCLE_STATE_CANCELED,
			change: func(_ *Graph, w *graphWatch) { w.status, w.accepting = controlpb.STATUS_CANCELLED, false }},
		{name: "expired", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, watch: contractpb.LifecycleState_LIFECYCLE_STATE_EXPIRED,
			change: func(_ *Graph, w *graphWatch) { w.status, w.accepting = controlpb.STATUS_TIMED_OUT, false }},
		{name: "redacted failure", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, watch: contractpb.LifecycleState_LIFECYCLE_STATE_FAILED,
			change: func(_ *Graph, w *graphWatch) { w.problem = "password=secret /private/path" }},
		{name: "terminal with observation failure", source: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, watch: contractpb.LifecycleState_LIFECYCLE_STATE_FAILED,
			change: func(_ *Graph, w *graphWatch) { w.status, w.problem = controlpb.STATUS_SATISFIED, "password=secret" }},
		{name: "recovery required", source: contractpb.LifecycleState_LIFECYCLE_STATE_RECOVERY_REQUIRED, change: func(g *Graph, _ *graphWatch) {
			node := g.nodes["native-source"]
			node.Status, node.HistoryBlocked = native.NodeFailed, true
			g.nodes[node.NativeID] = node
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			graph, request, now := statusGraph(t)
			for _, watch := range graph.watches {
				if tc.change != nil {
					tc.change(graph, watch)
				}
			}
			response, err := graph.observeStatus(request, now)
			require.NoError(t, err)
			require.Equal(t, tc.source, response[0].Status.GetState())
			require.Equal(t, tc.watch, response[2].Status.GetState())
			if tc.watch == contractpb.LifecycleState_LIFECYCLE_STATE_FAILED {
				require.Zero(t, response[2].Status.ObservedRevision)
			}
			if tc.source != contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING {
				require.Nil(t, response[1].Status, "an SDK query cannot certify incomplete inputs")
				require.Zero(t, response[0].Status.ObservedRevision)
			}
			for _, entry := range response {
				require.NotContains(t, entry.Status.GetMessage(), "secret")
				require.NotContains(t, entry.Status.GetMessage(), "/private")
				require.Nil(t, entry.Status.GetAcceptedOutput())
			}
		})
	}
}

func TestRuntimeStatusDeferredKeepsDesiredState(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		desired  registrypb.DesiredState
		state    contractpb.LifecycleState
		revision uint64
		message  string
	}{
		{
			name: "waiting to run", desired: registrypb.DESIRED_STATE_RUNNING,
			state:   contractpb.LifecycleState_LIFECYCLE_STATE_STARTING,
			message: "Runtime dependencies await plugin materialization",
		},
		{
			name: "deliberately stopped", desired: registrypb.DESIRED_STATE_STOPPED,
			state: contractpb.LifecycleState_LIFECYCLE_STATE_STOPPED, revision: 5,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			graph, request, now := statusGraph(t)
			record := graph.view.current[resourceKey{registrypb.RESOURCE_KIND_QUERY, "query"}]
			record.Desired = tc.desired
			node := graph.nodes["native-query"]
			node.Status = native.NodeDeferred
			graph.nodes[node.NativeID] = node
			request.Targets = request.Targets[1:2]
			response, err := graph.observeStatus(request, now)
			require.NoError(t, err)
			require.Len(t, response, 1)
			protorequire.ProtoEqual(t, &watchservice.ResourceObservation{
				Target: request.Targets[0],
				Status: &contractpb.ResourceStatus{
					State: tc.state, ObservedRevision: tc.revision, Message: tc.message,
					UpdatedAt: timestamppb.New(graph.nodeObservedAt),
				},
			}, response[0])
		})
	}
}

func TestRuntimeStatusRetiredDefinitionsNeedNoRunningDependencies(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"source", "query"} {
		t.Run(id, func(t *testing.T) {
			t.Parallel()
			graph, _, _ := statusGraph(t)
			node := graph.nodes["native-"+id]
			node.Status = native.NodeRetired
			graph.nodes[node.NativeID] = node
			source := graph.view.current[resourceKey{registrypb.RESOURCE_KIND_SOURCE, "source"}]
			query := graph.view.current[resourceKey{registrypb.RESOURCE_KIND_QUERY, "query"}]
			source.Desired = registrypb.DESIRED_STATE_DELETED
			query.Desired = registrypb.DESIRED_STATE_DELETED
			record := source
			if id == "query" {
				record = query
			}
			protorequire.ProtoEqual(t, &contractpb.ResourceStatus{
				State: contractpb.LifecycleState_LIFECYCLE_STATE_STOPPED, ObservedRevision: 10,
				UpdatedAt: timestamppb.New(graph.nodeObservedAt),
			}, graph.nodeStatusLocked(record, 10))
		})
	}
}

func TestRuntimeStatusHostRejectsInvalidOrUnobservableRequests(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		change  func(*watchservice.ObserveRequest, *hostedNamespace)
		invalid bool
	}{
		{name: "closed graph", change: func(_ *watchservice.ObserveRequest, h *hostedNamespace) { h.closed = true }},
		{name: "host problem", change: func(_ *watchservice.ObserveRequest, h *hostedNamespace) { h.problem = "secret" }},
		{name: "no graph", change: func(_ *watchservice.ObserveRequest, h *hostedNamespace) { h.graph = nil }},
		{name: "duplicate", invalid: true, change: func(r *watchservice.ObserveRequest, _ *hostedNamespace) {
			r.Targets = append(r.Targets, common.CloneProto(r.Targets[0]))
		}},
		{name: "unknown kind", invalid: true, change: func(r *watchservice.ObserveRequest, _ *hostedNamespace) { r.Targets[0].Kind = 99 }},
		{name: "invalid ID", invalid: true, change: func(r *watchservice.ObserveRequest, _ *hostedNamespace) { r.Targets[0].Id = "../private" }},
		{name: "missing target", invalid: true, change: func(r *watchservice.ObserveRequest, _ *hostedNamespace) { r.Targets[0] = nil }},
		{name: "oversized request", invalid: true, change: func(r *watchservice.ObserveRequest, _ *hostedNamespace) {
			r.Owner.StorageHostId = strings.Repeat("x", 1<<20)
		}},
		{name: "oversized page", invalid: true, change: func(r *watchservice.ObserveRequest, _ *hostedNamespace) {
			for len(r.Targets) <= 200 {
				r.Targets = append(r.Targets, common.CloneProto(r.Targets[0]))
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			graph, request, _ := statusGraph(t)
			hosted := &hostedNamespace{graph: graph}
			tc.change(request, hosted)
			ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: request.NamespaceId, Name: "ns"}, nil, "cluster")
			namespaces := namespace.NewMockRegistry(gomock.NewController(t))
			if len(request.Targets) <= 200 && len(request.Owner.StorageHostId) < 1<<20 {
				namespaces.EXPECT().GetNamespaceByID(ns.ID()).Return(ns, nil)
			}
			service := &Service{params: ServiceParams{Namespaces: namespaces}, namespaces: map[namespace.ID]*hostedNamespace{ns.ID(): hosted}}
			response, err := service.Observe(t.Context(), request)
			want := codes.Unavailable
			if tc.invalid {
				want = codes.InvalidArgument
			}
			require.Equal(t, want, serviceerror.ToStatus(err).Code())
			require.Nil(t, response)
			require.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestRuntimeStatusDistinguishesSameSourceAndQueryID(t *testing.T) {
	t.Parallel()
	graph, request, now := statusGraph(t)
	key := resourceKey{registrypb.RESOURCE_KIND_QUERY, "query"}
	query := graph.view.current[key]
	delete(graph.view.current, key)
	delete(graph.view.revisions, definitionPin{key, 5})
	key.id = "source"
	query.GetQuery().Id = "source"
	graph.view.current[key] = query
	graph.view.revisions[definitionPin{key, 5}] = query
	graph.view.current[resourceKey{registrypb.RESOURCE_KIND_WATCH, "watch"}].GetWatch().Spec.Query.Id = "source"
	node := graph.nodes["native-query"]
	node.Definition.Lifetime.ResourceID = "source"
	graph.nodes[node.NativeID] = node
	for _, watch := range graph.watches {
		watch.binding.Binding.Query.ResourceID = "source"
	}
	request.Targets[1].Id = "source"
	response, err := graph.observeStatus(request, now)
	require.NoError(t, err)
	require.Equal(t, uint64(3), response[0].Status.ObservedRevision)
	require.Equal(t, uint64(5), response[1].Status.ObservedRevision)
	require.Equal(t, uint64(9), response[2].Status.ObservedRevision)
}

func TestRuntimeStatusHostUsesCurrentGraphInsteadOfCachedSuccess(t *testing.T) {
	t.Parallel()
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh graph", true: "stale cached success"}[stale], func(t *testing.T) {
			t.Parallel()
			graph, request, _ := statusGraph(t)
			graph.nodeObservedAt = time.Now().Add(-time.Second)
			for _, watch := range graph.watches {
				watch.observedAt = graph.nodeObservedAt
			}
			if stale {
				graph.nodeGeneration--
			}
			ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: request.NamespaceId, Name: "ns"}, nil, "cluster")
			namespaces := namespace.NewMockRegistry(gomock.NewController(t))
			namespaces.EXPECT().GetNamespaceByID(ns.ID()).Return(ns, nil)
			service := &Service{
				params: ServiceParams{Namespaces: namespaces},
				namespaces: map[namespace.ID]*hostedNamespace{ns.ID(): {
					graph: graph, observed: GraphObservation{Generation: request.Generation, Converged: true},
				}},
			}
			response, err := service.Observe(t.Context(), request)
			if stale {
				require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
				require.Nil(t, response)
			} else {
				require.NoError(t, err)
				require.Equal(t, contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, response.Observations[2].Status.State)
			}
		})
	}
}
