package history

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/tasks"
)

func TestExecutionFeedRequiresStartupQueueAndNamespaceEnablement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		queueRegistered bool
	}{
		{"capture disabled without startup queue", false},
		{"capture follows namespace setting with startup queue", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := dynamicconfig.NewMemoryClient()
			t.Cleanup(client.OverrideValue(dynamicconfig.EnableExecutionFeed.Key(), []dynamicconfig.ConstrainedValue{{
				Constraints: dynamicconfig.Constraints{Namespace: "enabled-namespace"},
				Value:       true,
			}}))
			dc := dynamicconfig.NewCollection(client, log.NewNoopLogger())
			registry := tasks.NewDefaultTaskCategoryRegistry()
			if tc.queueRegistered {
				registry.AddCategory(tasks.CategoryExecutionFeed)
			}
			cfg := ConfigProvider(dc, config.Persistence{NumHistoryShards: 4}, registry)
			if !tc.queueRegistered {
				require.Nil(t, cfg.ExecutionFeedEnabled)
				return
			}
			require.NotNil(t, cfg.ExecutionFeedEnabled)
			require.True(t, cfg.ExecutionFeedEnabled("enabled-namespace"))
			require.False(t, cfg.ExecutionFeedEnabled("disabled-namespace"))
		})
	}
}
