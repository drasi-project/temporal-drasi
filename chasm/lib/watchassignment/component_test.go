package watchassignment

import (
	"context"
	"crypto/sha256"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
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
)

type environment struct {
	ctx       context.Context
	handler   *handler
	namespace string
	id        string
	ref       chasm.ComponentRef
	clock     *clock.EventTimeSource
	engine    *chasmtest.Engine
	enabled   bool
}

func newEnvironment(t *testing.T) *environment {
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
			require.Equal(t, chasm.LifecycleStateRunning, persisted.(*Component).LifecycleState(nil))
		}),
	)
	env := &environment{
		namespace: name, id: tv.NamespaceID().String(), clock: ts, enabled: true, engine: engine,
		ctx: chasm.NewEngineContext(testcontext.For(t), engine),
		ref: chasm.NewComponentRef[*Component](chasm.ExecutionKey{NamespaceID: tv.NamespaceID().String(), BusinessID: BusinessID}),
	}
	env.handler = &handler{namespaces: namespaces, enabled: func(string) bool { return env.enabled }}
	return env
}

func (e *environment) request() *assignmentpb.BeginRequest {
	return &assignmentpb.BeginRequest{
		NamespaceId: e.id, ExecutionId: BusinessID, RequestId: "begin-1",
		StorageHostId: "10000000-0000-4000-8000-000000000001",
		ProcessBootId: "20000000-0000-4000-8000-000000000001",
	}
}

func (e *environment) closing(state *assignmentpb.State) *assignmentpb.SealRequest {
	return &assignmentpb.SealRequest{
		NamespaceId: e.id, ExecutionId: BusinessID, RequestId: "seal-1", ExpectedVersion: state.Version,
		Grant: common.CloneProto(state.GetActive().Grant),
		Checkpoint: &assignmentpb.Cut{
			Directory: "/checkpoints/epoch-1", ManifestSha256: strings.Repeat("a", 64),
			InstanceId: state.GetActive().Grant.InstanceId, OwnerEpoch: state.GetActive().Grant.Epoch,
		},
	}
}

func (e *environment) state(t *testing.T) *assignmentpb.State {
	t.Helper()
	state, err := e.handler.Describe(e.ctx, &assignmentpb.DescribeRequest{NamespaceId: e.id, ExecutionId: BusinessID})
	require.NoError(t, err)
	return state
}

func expectedFingerprint(t *testing.T, request proto.Message) []byte {
	t.Helper()
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)
	return digest[:]
}

func TestAssignmentCooperativeHandoff(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t)
	request := env.request()
	active, err := env.handler.Begin(env.ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &assignmentpb.State{Version: 1, Phase: &assignmentpb.State_Active{
		Active: &assignmentpb.Active{
			Grant: &assignmentpb.Grant{
				NamespaceId: env.id, InstanceId: env.id, Epoch: 1,
				StorageHostId: request.StorageHostId, ProcessBootId: request.ProcessBootId,
			},
			RequestId: request.RequestId, Fingerprint: expectedFingerprint(t, request),
		},
	}}, active)
	retry, err := env.handler.Begin(env.ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, active, retry)

	closing := env.closing(active)
	sealed, err := env.handler.Seal(env.ctx, closing)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &assignmentpb.State{Version: 2, Phase: &assignmentpb.State_Sealed{
		Sealed: &assignmentpb.Sealed{
			Predecessor: active.GetActive(), RequestId: closing.RequestId,
			Fingerprint: expectedFingerprint(t, closing), Checkpoint: closing.Checkpoint,
		},
	}}, sealed)
	protorequire.ProtoEqual(t, sealed, env.state(t))
	retry, err = env.handler.Seal(env.ctx, closing)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, sealed, retry)
	_, err = env.handler.Begin(env.ctx, request)
	require.ErrorContains(t, err, "has been sealed")

	next := common.CloneProto(request)
	next.ExpectedVersion, next.RequestId = sealed.Version, "begin-2"
	next.ProcessBootId = "20000000-0000-4000-8000-000000000002"
	successor, err := env.handler.Begin(env.ctx, next)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &assignmentpb.State{Version: 3, Phase: &assignmentpb.State_Active{
		Active: &assignmentpb.Active{
			Grant: &assignmentpb.Grant{
				NamespaceId: env.id, InstanceId: env.id, Epoch: 2,
				StorageHostId: request.StorageHostId, ProcessBootId: next.ProcessBootId,
			},
			RequestId: next.RequestId, Fingerprint: expectedFingerprint(t, next), RecoveryCut: closing.Checkpoint,
		},
	}}, successor)
	retry, err = env.handler.Begin(env.ctx, next)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, successor, retry)
	_, err = env.handler.Begin(env.ctx, request)
	require.ErrorContains(t, err, "active owner")
	_, err = env.handler.Seal(env.ctx, closing)
	require.ErrorContains(t, err, "active owner")
	protorequire.ProtoEqual(t, successor, env.state(t))
}

func TestAssignmentRefusesUnsealedTakeover(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*assignmentpb.BeginRequest)
	}{
		{"same process new request", func(r *assignmentpb.BeginRequest) { r.RequestId = "another-request" }},
		{"new process", func(r *assignmentpb.BeginRequest) {
			r.RequestId, r.ProcessBootId = "another-request", "20000000-0000-4000-8000-000000000002"
		}},
		{"another host", func(r *assignmentpb.BeginRequest) {
			r.RequestId, r.StorageHostId = "another-request", "10000000-0000-4000-8000-000000000002"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t)
			request := env.request()
			active, err := env.handler.Begin(env.ctx, request)
			require.NoError(t, err)
			env.clock.Advance(100 * 365 * 24 * time.Hour)
			contender := common.CloneProto(request)
			tc.change(contender)
			contender.ExpectedVersion = active.Version
			_, err = env.handler.Begin(env.ctx, contender)
			require.ErrorContains(t, err, "active owner")
			protorequire.ProtoEqual(t, active, env.state(t))
		})
	}
}

func TestAssignmentChangedRetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*assignmentpb.BeginRequest)
	}{
		{"expected version", func(r *assignmentpb.BeginRequest) { r.ExpectedVersion = 1 }},
		{"process boot", func(r *assignmentpb.BeginRequest) { r.ProcessBootId = "20000000-0000-4000-8000-000000000002" }},
		{"storage host", func(r *assignmentpb.BeginRequest) { r.StorageHostId = "10000000-0000-4000-8000-000000000002" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t)
			request := env.request()
			active, err := env.handler.Begin(env.ctx, request)
			require.NoError(t, err)
			changed := common.CloneProto(request)
			tc.change(changed)
			_, err = env.handler.Begin(env.ctx, changed)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, active, env.state(t))
			sealed, err := env.handler.Seal(env.ctx, env.closing(active))
			require.NoError(t, err)
			_, err = env.handler.Begin(env.ctx, changed)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, sealed, env.state(t))
		})
	}
}

func TestAssignmentSealGuards(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*assignmentpb.SealRequest)
		text   string
	}{
		{"wrong version", func(r *assignmentpb.SealRequest) { r.ExpectedVersion++ }, "version"},
		{"wrong process", func(r *assignmentpb.SealRequest) {
			r.Grant.ProcessBootId = "20000000-0000-4000-8000-000000000002"
		}, "active owner"},
		{"wrong host", func(r *assignmentpb.SealRequest) {
			r.Grant.StorageHostId = "10000000-0000-4000-8000-000000000002"
		}, "active owner"},
		{"wrong epoch", func(r *assignmentpb.SealRequest) { r.Grant.Epoch++; r.Checkpoint.OwnerEpoch++ }, "active owner"},
		{"missing grant", func(r *assignmentpb.SealRequest) { r.Grant = nil }, "grant"},
		{"wrong namespace", func(r *assignmentpb.SealRequest) { r.Grant.NamespaceId = "other" }, "grant"},
		{"wrong instance", func(r *assignmentpb.SealRequest) { r.Grant.InstanceId = "other" }, "grant"},
		{"missing cut", func(r *assignmentpb.SealRequest) { r.Checkpoint = nil }, "checkpoint"},
		{"cut epoch", func(r *assignmentpb.SealRequest) { r.Checkpoint.OwnerEpoch++ }, "checkpoint"},
		{"cut instance", func(r *assignmentpb.SealRequest) { r.Checkpoint.InstanceId = "other" }, "checkpoint"},
		{"cut relative path", func(r *assignmentpb.SealRequest) { r.Checkpoint.Directory = "relative" }, "directory"},
		{"cut traversal", func(r *assignmentpb.SealRequest) { r.Checkpoint.Directory = "/cuts/../other" }, "directory"},
		{"cut root", func(r *assignmentpb.SealRequest) { r.Checkpoint.Directory = "/" }, "directory"},
		{"missing digest", func(r *assignmentpb.SealRequest) { r.Checkpoint.ManifestSha256 = "" }, "SHA256"},
		{"invalid digest", func(r *assignmentpb.SealRequest) { r.Checkpoint.ManifestSha256 = strings.Repeat("z", 64) }, "SHA256"},
		{"noncanonical digest", func(r *assignmentpb.SealRequest) { r.Checkpoint.ManifestSha256 = strings.Repeat("A", 64) }, "SHA256"},
		{"missing request", func(r *assignmentpb.SealRequest) { r.RequestId = "" }, "request_id"},
		{"Begin request reused", func(r *assignmentpb.SealRequest) { r.RequestId = "begin-1" }, "new request_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t)
			active, err := env.handler.Begin(env.ctx, env.request())
			require.NoError(t, err)
			closing := env.closing(active)
			tc.change(closing)
			_, err = env.handler.Seal(env.ctx, closing)
			require.ErrorContains(t, err, tc.text)
			protorequire.ProtoEqual(t, active, env.state(t))
		})
	}
}

func TestAssignmentSealedRetryAndRecoveryGuards(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*assignmentpb.SealRequest)
		text   string
	}{
		{"changed manifest", func(r *assignmentpb.SealRequest) { r.Checkpoint.ManifestSha256 = strings.Repeat("b", 64) }, "different Seal content"},
		{"changed directory", func(r *assignmentpb.SealRequest) { r.Checkpoint.Directory += "-other" }, "different Seal content"},
		{"changed version", func(r *assignmentpb.SealRequest) { r.ExpectedVersion++ }, "different Seal content"},
		{"another request", func(r *assignmentpb.SealRequest) { r.RequestId = "another-seal" }, "already sealed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t)
			active, err := env.handler.Begin(env.ctx, env.request())
			require.NoError(t, err)
			closing := env.closing(active)
			sealed, err := env.handler.Seal(env.ctx, closing)
			require.NoError(t, err)
			tc.change(closing)
			_, err = env.handler.Seal(env.ctx, closing)
			require.ErrorContains(t, err, tc.text)
			protorequire.ProtoEqual(t, sealed, env.state(t))
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*assignmentpb.BeginRequest)
		text   string
	}{
		{"old version", func(r *assignmentpb.BeginRequest) { r.ExpectedVersion = 0 }, "version"},
		{"different volume", func(r *assignmentpb.BeginRequest) {
			r.StorageHostId = "10000000-0000-4000-8000-000000000002"
		}, "same storage host"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t)
			active, err := env.handler.Begin(env.ctx, env.request())
			require.NoError(t, err)
			sealed, err := env.handler.Seal(env.ctx, env.closing(active))
			require.NoError(t, err)
			next := env.request()
			next.RequestId, next.ExpectedVersion = "begin-2", sealed.Version
			tc.change(next)
			_, err = env.handler.Begin(env.ctx, next)
			require.ErrorContains(t, err, tc.text)
			protorequire.ProtoEqual(t, sealed, env.state(t))
		})
	}
}

func TestAssignmentDetachedResponses(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t)
	request := env.request()
	active, err := env.handler.Begin(env.ctx, request)
	require.NoError(t, err)
	saved := common.CloneProto(active)
	active.GetActive().Grant.StorageHostId = "mutated-response"
	request.StorageHostId = "mutated-request"
	protorequire.ProtoEqual(t, saved, env.state(t))
	closing := env.closing(saved)
	sealed, err := env.handler.Seal(env.ctx, closing)
	require.NoError(t, err)
	saved = common.CloneProto(sealed)
	sealed.GetSealed().Checkpoint.Directory = "mutated-response"
	closing.Checkpoint.Directory = "mutated-request"
	protorequire.ProtoEqual(t, saved, env.state(t))
	next := env.request()
	next.RequestId, next.ExpectedVersion = "begin-2", saved.Version
	successor, err := env.handler.Begin(env.ctx, next)
	require.NoError(t, err)
	saved = common.CloneProto(successor)
	successor.GetActive().RecoveryCut.Directory = "mutated-response"
	protorequire.ProtoEqual(t, saved, env.state(t))
	read := env.state(t)
	read.GetActive().Grant.Epoch++
	protorequire.ProtoEqual(t, saved, env.state(t))
}

func TestAssignmentExhaustionAndUnknownPhase(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*assignmentpb.State)
		text   string
	}{
		{"epoch exhaustion", func(s *assignmentpb.State) { s.GetSealed().Predecessor.Grant.Epoch = math.MaxUint64 }, "epoch exhausted"},
		{"version exhaustion", func(s *assignmentpb.State) { s.Version = math.MaxUint64 - 1 }, "version exhausted"},
		{"missing phase", func(s *assignmentpb.State) { s.Phase = nil }, "compatible ownership phase"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t)
			active, err := env.handler.Begin(env.ctx, env.request())
			require.NoError(t, err)
			sealed, err := env.handler.Seal(env.ctx, env.closing(active))
			require.NoError(t, err)
			tc.change(sealed)
			_, _, err = chasm.UpdateComponent(env.ctx, env.ref,
				func(c *Component, _ chasm.MutableContext, input *assignmentpb.State) (chasm.NoValue, error) {
					c.State = common.CloneProto(input)
					return nil, nil
				}, sealed)
			require.NoError(t, err)
			next := env.request()
			next.RequestId, next.ExpectedVersion = "begin-2", sealed.Version
			_, err = env.handler.Begin(env.ctx, next)
			require.ErrorContains(t, err, tc.text)
			protorequire.ProtoEqual(t, sealed, env.state(t))
		})
	}
}
