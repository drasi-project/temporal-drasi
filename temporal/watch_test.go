//go:build integration && temporal_with_drasi && cgo

package temporal_test

import (
	"fmt"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/freeport"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	watchservice "go.temporal.io/server/service/watch"
	"go.temporal.io/server/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestDrasiWatchServerRole(t *testing.T) {
	t.Parallel()
	ctx := testcontext.For(t)
	tv := testvars.New(t)
	cfg := loadConfig(t)
	cfg.Persistence.NumHistoryShards = 1
	role := cfg.Services[string(primitives.WorkerService)]
	role.RPC.GRPCPort = freeport.MustGetFreePort()
	role.RPC.MembershipPort = freeport.MustGetFreePort()
	role.RPC.HTTPPort = 0
	role.Drasi = config.Watch{
		StateRoot: t.TempDir(), StorageHostID: "10000000-0000-4000-8000-000000000001",
	}
	cfg.Services[string(primitives.WatchService)] = role
	dc := dynamicconfig.NewMemoryClient()
	dc.OverrideSetting(dynamicconfig.NamespaceCacheRefreshInterval, 50*time.Millisecond)
	dc.OverrideSetting(dynamicconfig.EnableExecutionFeedQueue, true)
	dc.OverrideSetting(dynamicconfig.EnableExecutionFeed, true)
	dc.OverrideSetting(dynamicconfig.TransferProcessorMaxPollInterval, 100*time.Millisecond)
	dc.OverrideSetting(dynamicconfig.TransferProcessorUpdateAckInterval, 100*time.Millisecond)
	dc.OverrideSetting(dynamicconfig.EnableDrasiWatches, []dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{Namespace: tv.NamespaceName().String()}, Value: true},
	})
	server, err := temporal.NewServer(
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithConfig(cfg), temporal.WithDynamicConfigClient(dc),
		temporal.WithLogger(log.NewTestLogger()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Stop()) })
	require.NoError(t, server.Start())
	roleConfig := *cfg
	roleConfig.Global.PProf.Port = 0
	startRole := func() temporal.Server {
		roleServer, err := temporal.NewServer(
			temporal.ForServices([]string{string(primitives.WatchService)}),
			temporal.WithConfig(&roleConfig), temporal.WithDynamicConfigClient(dc),
			temporal.WithLogger(log.NewTestLogger()),
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, roleServer.Stop()) })
		require.NoError(t, roleServer.Start())
		return roleServer
	}
	roleServer := startRole()
	connect := func(service string) *grpc.ClientConn {
		conn, err := grpc.NewClient(
			fmt.Sprintf("127.0.0.1:%d", cfg.Services[service].RPC.GRPCPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		return conn
	}
	frontendConn := connect(string(primitives.FrontendService))
	frontend := workflowservice.NewWorkflowServiceClient(frontendConn)
	_, err = frontend.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: tv.NamespaceName().String(), WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	require.NoError(t, err)
	description, err := frontend.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: tv.NamespaceName().String(),
	})
	require.NoError(t, err)
	assignments := assignmentpb.NewAssignmentServiceClient(connect(string(primitives.HistoryService)))
	request := &assignmentpb.DescribeRequest{
		NamespaceId: description.NamespaceInfo.Id, ExecutionId: watchassignment.BusinessID,
	}
	var owner *assignmentpb.State
	await.Require(ctx, t, func(t *await.T) {
		var err error
		owner, err = assignments.Describe(t.Context(), request)
		require.NoError(t, err)
		require.NotNil(t, owner.GetActive())
		require.Equal(t, role.Drasi.StorageHostID, owner.GetActive().Grant.StorageHostId)
	}, 30*time.Second, 100*time.Millisecond)
	firstBoot := owner.GetActive().Grant.ProcessBootId
	watchConn := connect(string(primitives.WatchService))
	health := healthpb.NewHealthClient(watchConn)
	response, err := health.Check(ctx, &healthpb.HealthCheckRequest{Service: watchservice.ServiceName})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.Status)
	admin := adminservice.NewAdminServiceClient(frontendConn)
	await.Require(ctx, t, func(t *await.T) {
		response, err := admin.DescribeCluster(t.Context(), &adminservice.DescribeClusterRequest{})
		require.NoError(t, err)
		for _, ring := range response.MembershipInfo.Rings {
			if ring.Role == string(primitives.WatchService) {
				require.Equal(t, int32(1), ring.MemberCount)
				return
			}
		}
		t.Fatal("Watch membership ring is missing")
	}, 30*time.Second, 100*time.Millisecond)
	definitions := contractpb.NewWatchServiceClient(frontendConn)
	source, err := definitions.CreateSource(ctx, &contractpb.CreateSourceRequest{
		Namespace: tv.NamespaceName().String(), Id: "runtime.source", RequestId: tv.Any().String(),
		Spec: &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}},
	})
	require.NoError(t, err)
	source, err = definitions.StartSource(ctx, &contractpb.SourceOperationRequest{
		Namespace: source.Namespace, Id: source.Id, ExpectedRevision: source.Revision, RequestId: tv.Any().String(),
	})
	require.NoError(t, err)
	pipeline := verifyWatchHistoryPipeline(t, frontendConn, connect(string(primitives.HistoryService)), source, description.NamespaceInfo.Id)
	pluginIntent := installWatchRolePluginIntent(t, connect(string(primitives.HistoryService)))
	requireWatchRolePluginObservation(t, watchConn, pluginIntent, role.Drasi.StorageHostID, firstBoot)
	requireWatchRolePublicPluginObservation(t, frontendConn, pluginIntent, cfg.Services[string(primitives.FrontendService)].RPC.HTTPPort)
	disable := dc.OverrideSetting(dynamicconfig.EnableDrasiWatches, false)
	await.Require(ctx, t, func(t *await.T) {
		state, err := assignments.Describe(t.Context(), request)
		require.NoError(t, err)
		require.NotNil(t, state.GetSealed())
		require.Equal(t, uint64(1), state.GetSealed().Checkpoint.OwnerEpoch)
		require.NotEmpty(t, state.GetSealed().Checkpoint.ManifestSha256)
	}, 30*time.Second, 100*time.Millisecond)
	secondProducer := pipeline.completeProducer(t, tv.WithWorkflowIDNumber(3).WorkflowID())
	disable()
	await.Require(ctx, t, func(t *await.T) {
		state, err := assignments.Describe(t.Context(), request)
		require.NoError(t, err)
		require.Equal(t, uint64(2), state.GetActive().GetGrant().GetEpoch())
		require.Equal(t, firstBoot, state.GetActive().Grant.ProcessBootId)
		require.NotNil(t, state.GetActive().RecoveryCut)
	}, 30*time.Second, 100*time.Millisecond)
	pipeline.requireDelivery(t, 2, secondProducer)
	pipeline.requireRunningResources(t, source)
	require.NoError(t, roleServer.Stop())
	sealed, err := assignments.Describe(ctx, request)
	require.NoError(t, err)
	require.Equal(t, uint64(2), sealed.GetSealed().GetCheckpoint().GetOwnerEpoch())
	thirdProducer := pipeline.completeProducer(t, tv.WithWorkflowIDNumber(4).WorkflowID())
	successor := startRole()
	var successorBoot string
	await.Require(ctx, t, func(t *await.T) {
		state, err := assignments.Describe(t.Context(), request)
		require.NoError(t, err)
		require.Equal(t, uint64(3), state.GetActive().GetGrant().GetEpoch())
		require.NotEqual(t, firstBoot, state.GetActive().Grant.ProcessBootId)
		require.Equal(t, role.Drasi.StorageHostID, state.GetActive().Grant.StorageHostId)
		require.Equal(t, uint64(2), state.GetActive().RecoveryCut.OwnerEpoch)
		successorBoot = state.GetActive().Grant.ProcessBootId
	}, 30*time.Second, 100*time.Millisecond)
	requireWatchRolePluginObservation(t, watchConn, pluginIntent, role.Drasi.StorageHostID, successorBoot)
	requireWatchRolePublicPluginObservation(t, frontendConn, pluginIntent, cfg.Services[string(primitives.FrontendService)].RPC.HTTPPort)
	pipeline.requireDelivery(t, 3, thirdProducer)
	pipeline.requireRunningResources(t, source)
	pipeline.verifyExternalQueryValidation(t, source)
	pipeline.verifyWatchLifecycle(t, []string{tv.WorkflowID(), secondProducer, thirdProducer})
	require.NoError(t, successor.Stop())
	sealed, err = assignments.Describe(ctx, request)
	require.NoError(t, err)
	require.Equal(t, uint64(3), sealed.GetSealed().GetCheckpoint().GetOwnerEpoch())
}
