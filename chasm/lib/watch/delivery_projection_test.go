package watch

import (
	"math"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestDeliveryProjectionStatesAndIntegrity(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	receipt, err := env.accept(testBatch())
	require.NoError(t, err)
	intent, state := env.intent(t, receipt.ActionIDs[0]), env.state(t)
	for _, tc := range []struct {
		name     string
		status   watchpb.DeliveryStatus
		attempts uint32
		inFlight bool
		expected contractpb.DeliveryState
	}{
		{"new", watchpb.DELIVERY_STATUS_PENDING, 0, false, contractpb.DeliveryState_DELIVERY_STATE_PENDING},
		{"first dispatch", watchpb.DELIVERY_STATUS_PENDING, 1, true, contractpb.DeliveryState_DELIVERY_STATE_PENDING},
		{"retry authorized", watchpb.DELIVERY_STATUS_PENDING, 1, false, contractpb.DeliveryState_DELIVERY_STATE_RETRYING},
		{"retry dispatch", watchpb.DELIVERY_STATUS_PENDING, 2, true, contractpb.DeliveryState_DELIVERY_STATE_RETRYING},
		{"delivered", watchpb.DELIVERY_STATUS_DELIVERED, 1, false, contractpb.DeliveryState_DELIVERY_STATE_DELIVERED},
		{"blocked", watchpb.DELIVERY_STATUS_FAILED, 1, false, contractpb.DeliveryState_DELIVERY_STATE_BLOCKED},
		{"ambiguous", watchpb.DELIVERY_STATUS_AMBIGUOUS, 1, false, contractpb.DeliveryState_DELIVERY_STATE_AMBIGUOUS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := common.CloneProto(intent)
			input.Status, input.Attempts, input.InFlight = tc.status, tc.attempts, tc.inFlight
			result, err := DescribeDelivery(state, input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result.State)
			require.Nil(t, result.NextAttemptAt)
			result.Output.RowIdentity = "modified-response"
			protorequire.ProtoEqual(t, intent.Output, input.Output)
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*watchpb.Intent)
	}{
		{"missing output", func(input *watchpb.Intent) { input.Output = nil }},
		{"different query", func(input *watchpb.Intent) { input.Output.QueryRevision++ }},
		{"different incarnation", func(input *watchpb.Intent) { input.Output.Incarnation = "another" }},
		{"different row", func(input *watchpb.Intent) { input.Output.RowIdentity = "another" }},
		{"different action", func(input *watchpb.Intent) { input.ActionId = "another" }},
		{"unknown status", func(input *watchpb.Intent) { input.Status = watchpb.DeliveryStatus(100) }},
		{"missing timestamp", func(input *watchpb.Intent) { input.UpdatedAt = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := common.CloneProto(intent)
			tc.change(input)
			_, err := DescribeDelivery(state, input)
			require.Equal(t, codes.DataLoss, serviceerror.ToStatus(err).Code())
		})
	}
}

func TestRecentDeliveryMetadataIsByteBounded(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t, func(input *Create) { input.Limits.Recent = 4 })
	batch := testBatch()
	batch.Actions[0].RowKey = strings.Repeat("x", int(env.create.Limits.PendingBytes/2))
	for sequence := uint64(1); sequence <= 2; sequence++ {
		batch.Sequence = sequence
		receipt, err := env.accept(batch)
		require.NoError(t, err)
		require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
			return c.Complete(ctx, receipt.ActionIDs[0], 0, "delivered-run", "")
		}))
	}
	state := env.state(t)
	require.Len(t, state.RecentIds, 1)
	require.LessOrEqual(t, state.RecentBytes, state.MaxPendingBytes)
	require.Equal(t, uint64(proto.Size(env.intent(t, state.RecentIds[0]).Output)), state.RecentBytes)
	require.Zero(t, state.PendingBytes)
}

func TestDeliveryOrdinalCannotOverflowPublicIdentity(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	batch := testBatch()
	batch.Complete, batch.FirstOrdinal, batch.ItemCount = false, 0, math.MaxUint32+1
	batch.Actions = nil
	_, err := env.accept(batch)
	require.NoError(t, err)
	batch.FirstOrdinal, batch.ItemCount, batch.Complete = math.MaxUint32+1, 1, true
	batch.Actions = testBatch().Actions
	batch.Actions[0].Ordinal = batch.FirstOrdinal
	before := env.state(t)
	_, err = env.accept(batch)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	protorequire.ProtoEqual(t, before, env.state(t))
}
