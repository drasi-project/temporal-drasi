//go:build integration && temporal_with_drasi && cgo

package temporal_test

import (
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testvars"
)

func (pipeline *watchHistoryPipeline) verifyWatchLifecycle(t *testing.T, initialProducers []string) {
	t.Helper()
	tv := testvars.New(t)
	ctx := t.Context()
	observe := func(id string, status controlpb.Status, producers []string) *controlpb.ExecutionResponse {
		var response *controlpb.ExecutionResponse
		await.Require(ctx, t, func(t *await.T) {
			var err error
			response, err = pipeline.control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
				NamespaceId: pipeline.namespaceID, ExecutionId: id,
			})
			require.NoError(t, err)
			require.NotNil(t, response.GetState())
			require.Equal(t, status, response.State.Status)
			if status == controlpb.STATUS_RUNNING {
				require.Equal(t, controlpb.PROGRESS_PHASE_LIVE, response.State.ProgressPhase)
			}
			require.Equal(t, uint64(len(producers)), response.State.Delivered)
			require.Zero(t, response.State.Failed)
			require.Empty(t, response.State.PendingIds)
			require.Len(t, response.Deliveries, len(producers))
		}, 30*time.Second, 100*time.Millisecond)
		var results []string
		for _, delivery := range response.Deliveries {
			var result string
			require.NoError(t, pipeline.client.GetWorkflow(ctx, delivery.WorkflowId, "").Get(ctx, &result))
			results = append(results, result)
		}
		require.ElementsMatch(t, producers, results)
		return response
	}
	observePublic := func(resource *contractpb.Watch, status contractpb.LifecycleState) {
		await.Require(ctx, t, func(t *await.T) {
			current, err := pipeline.definitions.GetWatch(t.Context(), &contractpb.GetWatchRequest{
				Namespace: resource.Namespace, Id: resource.Id,
			})
			require.NoError(t, err)
			require.NotNil(t, current.Status)
			require.Equal(t, status, current.Status.State)
			require.Equal(t, resource.Revision, current.Status.ObservedRevision)
			require.Zero(t, current.Status.PendingDeliveries)
		}, 30*time.Second, 100*time.Millisecond)
	}
	create := func(id string) *contractpb.Watch {
		spec := common.CloneProto(pipeline.watch.Spec)
		spec.Action.GetStart().WorkflowId = "{action_id}"
		spec.Action.GetStart().ReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		resource, err := pipeline.definitions.CreateWatch(ctx, &contractpb.CreateWatchRequest{
			Namespace: pipeline.watch.Namespace, Id: id, RequestId: tv.Any().String(), Spec: spec,
		})
		require.NoError(t, err)
		return resource
	}
	resource := create("runtime.lifecycle")
	originalRevision := resource.Revision
	originalID := pipeline.executionID(t, resource)
	observe(originalID, controlpb.STATUS_RUNNING, nil)
	observePublic(resource, contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING)
	probe := create("runtime.lifecycle.probe")
	probeID := pipeline.executionID(t, probe)
	observe(probeID, controlpb.STATUS_RUNNING, nil)
	first := pipeline.completeProducer(t, tv.WithWorkflowIDNumber(5).WorkflowID())
	observe(originalID, controlpb.STATUS_RUNNING, []string{first})
	observe(probeID, controlpb.STATUS_RUNNING, []string{first})
	resource, err := pipeline.definitions.PauseWatch(ctx, &contractpb.WatchOperationRequest{
		Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: resource.Revision, RequestId: tv.Any().String(),
	})
	require.NoError(t, err)
	observe(originalID, controlpb.STATUS_PAUSED, []string{first})
	observePublic(resource, contractpb.LifecycleState_LIFECYCLE_STATE_PAUSED)
	second := pipeline.completeProducer(t, tv.WithWorkflowIDNumber(6).WorkflowID())
	// The independent subscriber proves the paused Watch's next output exists.
	observe(probeID, controlpb.STATUS_RUNNING, []string{first, second})
	observe(originalID, controlpb.STATUS_PAUSED, []string{first})
	resource, err = pipeline.definitions.ResumeWatch(ctx, &contractpb.WatchOperationRequest{
		Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: resource.Revision, RequestId: tv.Any().String(),
	})
	require.NoError(t, err)
	require.Equal(t, originalID, pipeline.executionID(t, resource))
	observe(originalID, controlpb.STATUS_RUNNING, []string{first, second})
	observePublic(resource, contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING)
	spec := common.CloneProto(resource.Spec)
	spec.Action.GetStart().WorkflowId = "updated-{action_id}"
	resource, err = pipeline.definitions.UpdateWatch(ctx, &contractpb.UpdateWatchRequest{
		Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: resource.Revision, RequestId: tv.Any().String(), Spec: spec,
	})
	require.NoError(t, err)
	updatedRevision := resource.Revision
	updatedID := pipeline.executionID(t, resource)
	require.NotEqual(t, originalID, updatedID)
	observe(originalID, controlpb.STATUS_CANCELLED, []string{first, second})
	observe(updatedID, controlpb.STATUS_RUNNING, nil)
	observePublic(resource, contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING)
	third := pipeline.completeProducer(t, tv.WithWorkflowIDNumber(7).WorkflowID())
	updated := observe(updatedID, controlpb.STATUS_RUNNING, []string{third})
	require.True(t, strings.HasPrefix(updated.Deliveries[0].WorkflowId, "updated-"))
	observe(probeID, controlpb.STATUS_RUNNING, []string{first, second, third})
	observe(originalID, controlpb.STATUS_CANCELLED, []string{first, second})
	resource, err = pipeline.definitions.ResetWatch(ctx, &contractpb.ResetWatchRequest{
		Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: resource.Revision, RequestId: tv.Any().String(),
		InitialResults: contractpb.InitialResults_INITIAL_RESULTS_INCLUDE,
	})
	require.NoError(t, err)
	resetRevision := resource.Revision
	resetID := pipeline.executionID(t, resource)
	require.NotEqual(t, originalID, resetID)
	require.NotEqual(t, updatedID, resetID)
	observe(updatedID, controlpb.STATUS_CANCELLED, []string{third})
	snapshot := append(append([]string(nil), initialProducers...), first, second, third)
	observe(resetID, controlpb.STATUS_RUNNING, snapshot)
	observePublic(resource, contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING)
	deleted, err := pipeline.definitions.DeleteWatch(ctx, &contractpb.WatchOperationRequest{
		Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: resource.Revision, RequestId: tv.Any().String(),
	})
	require.NoError(t, err)
	observe(resetID, controlpb.STATUS_CANCELLED, snapshot)
	observePublic(deleted, contractpb.LifecycleState_LIFECYCLE_STATE_CANCELED)
	fourth := pipeline.completeProducer(t, tv.WithWorkflowIDNumber(8).WorkflowID())
	observe(probeID, controlpb.STATUS_RUNNING, []string{first, second, third, fourth})
	observe(originalID, controlpb.STATUS_CANCELLED, []string{first, second})
	observe(updatedID, controlpb.STATUS_CANCELLED, []string{third})
	observe(resetID, controlpb.STATUS_CANCELLED, snapshot)
	deliveries, err := pipeline.definitions.ListDeliveries(ctx, &contractpb.ListDeliveriesRequest{
		Namespace: resource.Namespace, Id: resource.Id,
	})
	require.NoError(t, err)
	require.Empty(t, deliveries.NextPageToken)
	revisions := make(map[uint64]int)
	for _, delivery := range deliveries.Deliveries {
		require.Equal(t, contractpb.DeliveryState_DELIVERY_STATE_DELIVERED, delivery.State)
		revisions[delivery.Watch.Revision]++
	}
	require.Equal(t, map[uint64]int{
		originalRevision: 2, updatedRevision: 1, resetRevision: len(snapshot),
	}, revisions)
}
