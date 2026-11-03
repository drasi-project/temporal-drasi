package history

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

func TestExecutionFeedBootstrapUsesExecutionCache(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		archetypeID chasm.ArchetypeID
	}{
		{name: "workflow", archetypeID: chasm.WorkflowArchetypeID},
		{name: "standalone activity", archetypeID: activity.ArchetypeID},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			s := shard.NewTestContext(controller, &persistencespb.ShardInfo{ShardId: 1, RangeId: 1}, tests.NewDynamicConfig())
			cache := wcache.NewHostLevelCache(s.GetConfig(), s.GetLogger(), metrics.NoopMetricsHandler, testhooks.TestHooks{})
			engine := &historyEngineImpl{shardContext: s, workflowCache: cache}
			s.SetEngineForTesting(engine)
			state := &persistencespb.WorkflowMutableState{}
			if test.archetypeID != chasm.WorkflowArchetypeID {
				state.ChasmNodes = map[string]*persistencespb.ChasmNode{"": {
					Metadata: &persistencespb.ChasmNodeMetadata{
						Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
							ComponentAttributes: &persistencespb.ChasmComponentAttributes{TypeId: test.archetypeID},
						},
					},
				}}
			}
			execution := &commonpb.WorkflowExecution{WorkflowId: "execution-feed", RunId: "10000000-0000-0000-0000-000000000001"}
			s.Resource.ExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
				ShardID: 1, NamespaceID: tests.NamespaceID.String(), WorkflowID: execution.WorkflowId, RunID: execution.RunId, ArchetypeID: chasm.WorkflowArchetypeID,
			}).Return(&persistence.GetWorkflowExecutionResponse{State: state}, nil)
			acquired, release, err := s.GetCachedWorkflowContext(t.Context(), tests.NamespaceID, execution, locks.PriorityLow)
			require.NoError(t, err)
			require.Equal(t, test.archetypeID, acquired.GetArchetypeID())
			release(nil)
			regular, release, err := cache.GetOrCreateChasmExecution(t.Context(), s, tests.NamespaceID, execution, test.archetypeID, locks.PriorityHigh)
			require.NoError(t, err)
			require.Same(t, acquired, regular)
			release(nil)
		})
	}
}
