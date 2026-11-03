//go:build integration && temporal_with_drasi && cgo

package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	watchservice "go.temporal.io/server/service/watch"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type graphUnavailableSealClient struct {
	assignmentpb.AssignmentServiceClient
	failed bool
}

func (c *graphUnavailableSealClient) Seal(
	ctx context.Context,
	request *assignmentpb.SealRequest,
	options ...grpc.CallOption,
) (*assignmentpb.State, error) {
	if !c.failed {
		c.failed = true
		return nil, serviceerror.NewUnavailable("Seal failed before commit")
	}
	return c.AssignmentServiceClient.Seal(ctx, request, options...)
}

func TestDrasiGraphCooperativeOwnership(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableDrasiWatches, true))
	ctx := testcontext.For(t)
	lost := &assignmentLostSealClient{AssignmentServiceClient: env.GetTestCluster().WatchAssignmentClient()}
	client := &graphUnavailableSealClient{AssignmentServiceClient: lost}
	options := watchservice.GraphOptions{
		TemporalInput: watchservice.ManualTemporalInput{},
		NamespaceID:   env.NamespaceID().String(), Namespace: env.Namespace().String(), StateRoot: t.TempDir(),
		StorageHostID: "10000000-0000-4000-8000-000000000001",
		ProcessBootID: "20000000-0000-4000-8000-000000000001",
		RequestID:     "graph-owner-1", Assignments: client,
	}
	first, err := watchservice.OpenGraph(ctx, options)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, first.Close(shutdown))
	})
	describe := &assignmentpb.DescribeRequest{NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID}
	owner, err := client.Describe(ctx, describe)
	require.NoError(t, err)
	require.Equal(t, uint64(1), owner.GetActive().Grant.Epoch)
	next := options
	next.ProcessBootID, next.RequestID = "20000000-0000-4000-8000-000000000002", "graph-owner-2"
	contender, err := watchservice.OpenGraph(ctx, next)
	require.Nil(t, contender)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	require.ErrorContains(t, first.Close(ctx), "Seal failed before commit")
	reopened, err := watchservice.OpenGraph(ctx, options)
	require.Nil(t, reopened)
	require.ErrorContains(t, err, "close has begun")
	stillOwned, err := client.Describe(ctx, describe)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, owner, stillOwned)
	require.NoError(t, first.Close(ctx))
	require.True(t, lost.dropped)
	sealed, err := client.Describe(ctx, describe)
	require.NoError(t, err)
	require.NotNil(t, sealed.GetSealed())
	require.NotEmpty(t, sealed.GetSealed().Checkpoint.ManifestSha256)
	require.NoError(t, first.Close(ctx))
	successor, err := watchservice.OpenGraph(ctx, next)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, successor.Close(shutdown))
	})
	owner, err = client.Describe(ctx, describe)
	require.NoError(t, err)
	require.Equal(t, uint64(2), owner.GetActive().Grant.Epoch)
	protorequire.ProtoEqual(t, sealed.GetSealed().Checkpoint, owner.GetActive().RecoveryCut)
	require.NoError(t, first.Close(ctx), "confirmed close remains idempotent after a successor starts")
	closed := make(chan error, 2)
	for range 2 {
		go func() { closed <- successor.Close(ctx) }()
	}
	for range 2 {
		require.NoError(t, await.Rcv(t, closed))
	}
	sealed, err = client.Describe(ctx, describe)
	require.NoError(t, err)
	require.Equal(t, uint64(2), sealed.GetSealed().Checkpoint.OwnerEpoch)
	manifest := filepath.Join(sealed.GetSealed().Checkpoint.Directory, "checkpoint-v1.json")
	file, err := os.OpenFile(manifest, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteString("\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	next.RequestID, next.ProcessBootID = "graph-owner-3", "20000000-0000-4000-8000-000000000003"
	damaged, err := watchservice.OpenGraph(ctx, next)
	require.Nil(t, damaged)
	require.ErrorContains(t, err, "authoritative manifest digest")
	owner, err = client.Describe(ctx, describe)
	require.NoError(t, err)
	require.Equal(t, uint64(3), owner.GetActive().Grant.Epoch)
	require.NoDirExists(t, filepath.Join(filepath.Dir(filepath.Dir(sealed.GetSealed().Checkpoint.Directory)), "owners", "3"))
}
