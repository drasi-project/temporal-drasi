//go:build integration

package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type assignmentLostSealClient struct {
	assignmentpb.AssignmentServiceClient
	dropped bool
}

func (c *assignmentLostSealClient) Seal(
	ctx context.Context,
	request *assignmentpb.SealRequest,
	options ...grpc.CallOption,
) (*assignmentpb.State, error) {
	response, err := c.AssignmentServiceClient.Seal(ctx, request, options...)
	if err == nil && !c.dropped {
		c.dropped = true
		return nil, serviceerror.NewUnavailable("seal response lost after History commit")
	}
	return response, err
}

func TestDrasiNamespaceAssignmentRPC(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableDrasiWatches, true))
	ctx := testcontext.For(t)
	client := &assignmentLostSealClient{AssignmentServiceClient: env.GetTestCluster().WatchAssignmentClient()}
	first := &assignmentpb.BeginRequest{
		NamespaceId: env.NamespaceID().String(), ExecutionId: watchassignment.BusinessID, RequestId: "begin-1",
		StorageHostId: "10000000-0000-4000-8000-000000000001",
		ProcessBootId: "20000000-0000-4000-8000-000000000001",
	}
	second := common.CloneProto(first)
	second.ProcessBootId, second.RequestId = "20000000-0000-4000-8000-000000000002", "begin-2"
	type result struct {
		request *assignmentpb.BeginRequest
		state   *assignmentpb.State
		err     error
	}
	completed := make(chan result, 2)
	for _, request := range []*assignmentpb.BeginRequest{first, second} {
		go func() {
			state, err := client.Begin(ctx, request)
			completed <- result{request, state, err}
		}()
	}
	var winner result
	failures := 0
	for range 2 {
		response := await.Rcv(t, completed)
		if response.err != nil {
			require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(response.err).Code())
			failures++
		} else {
			winner = response
		}
	}
	require.Equal(t, 1, failures)
	require.NotNil(t, winner.state)
	protorequire.ProtoEqual(t, &assignmentpb.Grant{
		NamespaceId: env.NamespaceID().String(), InstanceId: env.NamespaceID().String(), Epoch: 1,
		StorageHostId: first.StorageHostId, ProcessBootId: winner.request.ProcessBootId,
	}, winner.state.GetActive().Grant)
	retry, err := client.Begin(ctx, winner.request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, winner.state, retry)
	contender := common.CloneProto(second)
	contender.RequestId, contender.ExpectedVersion = "another-process", winner.state.Version
	_, err = client.Begin(ctx, contender)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())

	restore := env.OverrideDynamicConfig(dynamicconfig.EnableDrasiWatches, false)
	defer restore()
	closing := &assignmentpb.SealRequest{
		NamespaceId: first.NamespaceId, ExecutionId: first.ExecutionId, RequestId: "seal-1",
		ExpectedVersion: winner.state.Version, Grant: common.CloneProto(winner.state.GetActive().Grant),
		Checkpoint: &assignmentpb.Cut{
			Directory: "/checkpoints/fixture/epoch-1", ManifestSha256: strings.Repeat("a", 64),
			InstanceId: env.NamespaceID().String(), OwnerEpoch: 1,
		},
	}
	_, err = client.Seal(ctx, closing)
	require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
	sealed, err := client.Describe(ctx, &assignmentpb.DescribeRequest{NamespaceId: first.NamespaceId, ExecutionId: first.ExecutionId})
	require.NoError(t, err)
	require.Equal(t, uint64(2), sealed.Version)
	protorequire.ProtoEqual(t, closing.Checkpoint, sealed.GetSealed().Checkpoint)
	retry, err = client.Seal(ctx, closing)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, sealed, retry)
	_, err = client.Begin(ctx, winner.request)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())

	next := common.CloneProto(first)
	next.RequestId, next.ExpectedVersion = "begin-3", sealed.Version
	next.ProcessBootId = "20000000-0000-4000-8000-000000000003"
	_, err = client.Begin(ctx, next)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	restore()
	successor, err := client.Begin(ctx, next)
	require.NoError(t, err)
	require.Equal(t, uint64(3), successor.Version)
	expected := common.CloneProto(winner.state.GetActive().Grant)
	expected.Epoch, expected.ProcessBootId = 2, next.ProcessBootId
	protorequire.ProtoEqual(t, expected, successor.GetActive().Grant)
	protorequire.ProtoEqual(t, closing.Checkpoint, successor.GetActive().RecoveryCut)
	_, err = client.Seal(ctx, closing)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
}
