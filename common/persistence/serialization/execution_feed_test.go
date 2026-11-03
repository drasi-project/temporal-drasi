package serialization

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
)

func TestExecutionFeedTaskSerialization(t *testing.T) {
	t.Parallel()
	for _, journal := range []bool{false, true} {
		name := "source"
		if journal {
			name = "journal"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			task := &tasks.ExecutionFeedTask{SchemaVersion: 1, TaskID: 123, Journal: journal,
				WorkflowKey: definition.NewWorkflowKey("namespace", "workflow", "run"),
				State:       tasks.FeedState{Activities: []tasks.FeedActivity{{ID: "activity", Attempt: 2, Status: "STARTED"}}},
			}
			serializer := NewSerializer()
			blob, err := serializer.SerializeTask(task)
			require.NoError(t, err)
			result, err := serializer.DeserializeTask(task.GetCategory(), blob)
			require.NoError(t, err)
			require.Equal(t, task, result)
			_, err = serializer.DeserializeTask(task.GetCategory(), &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_JSON, Data: []byte(`{"schema_version":2}`)})
			require.ErrorContains(t, err, "unsupported execution feed schema")
			_, err = serializer.DeserializeTask(task.GetCategory(), &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_JSON, Data: []byte(`{`)})
			require.ErrorContains(t, err, "invalid execution feed")
		})
	}
}

func TestExecutionFeedCaptureTaskSerialization(t *testing.T) {
	t.Parallel()
	for _, gap := range []bool{false, true} {
		name := "state"
		if gap {
			name = "gap"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			task := &tasks.ExecutionFeedTask{
				SchemaVersion: 2, TaskID: 123, CaptureEpoch: 7, CaptureGap: gap,
				WorkflowKey: definition.NewWorkflowKey("namespace", "workflow", "run"),
			}
			if !gap {
				task.State = tasks.FeedState{Activities: []tasks.FeedActivity{{ID: "activity", Attempt: 2, Status: "STARTED"}}}
			}
			serializer := NewSerializer()
			blob, err := serializer.SerializeTask(task)
			require.NoError(t, err)
			result, err := serializer.DeserializeTask(task.GetCategory(), blob)
			require.NoError(t, err)
			require.Equal(t, task, result)
		})
	}
}
