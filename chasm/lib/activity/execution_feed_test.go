package activity

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecutionFeedStandaloneActivity(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	attempt := &activitypb.ActivityAttemptState{Count: 2,
		DispatchTime: timestamppb.New(time.Unix(110, 0)),
		LastFailureDetails: &activitypb.ActivityAttemptState_LastFailureDetails{
			Time: timestamppb.New(time.Unix(100, 0)), Failure: &failurepb.Failure{
				Message: "SECRET_FAILURE", FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
					TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE},
				},
			},
		},
	}
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			ActivityType: &commonpb.ActivityType{Name: "type"}, ScheduleTime: timestamppb.New(time.Unix(90, 0))},
		LastAttempt: chasm.NewDataField(ctx, attempt),
		LastHeartbeat: chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
			Details: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("SECRET_HEARTBEAT")}}},
		}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("SECRET_INPUT")}}},
		}),
	}
	state, err := activity.ExecutionFeedState(ctx)
	require.NoError(t, err)
	require.True(t, state.Standalone)
	require.Equal(t, "SCHEDULED", state.Status)
	require.Equal(t, int32(2), state.Attempt)
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, state.LastTimeoutType)
	data, err := json.Marshal(state)
	require.NoError(t, err)
	require.NotContains(t, string(data), "SECRET")
	attempt.Count = 3
	require.Equal(t, int32(2), state.Attempt)
}
