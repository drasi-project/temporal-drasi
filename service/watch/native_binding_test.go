package watch

import (
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/watch/native"
)

func TestNativeBindingRestoresOriginalWatchRevision(t *testing.T) {
	t.Parallel()
	ctx := testcontext.For(t)
	source, query, watch := desiredRecords()
	current := common.CloneProto(watch)
	current.GetWatch().Revision = 2
	client := &desiredTestClient{t: t, steps: desiredSteps(source, query, current)}
	view, err := ReadDesiredView(ctx, client, "00000000-0000-4000-8000-000000000001", "ns")
	require.NoError(t, err)
	metadata, err := view.NativeWatchBinding("watch")
	require.NoError(t, err)
	expected := native.WatchBinding{
		Watch: native.ResourceLifetime{
			NamespaceID: view.namespaceID, ResourceID: "watch", Incarnation: "1",
		},
		WatchRevision: 2,
		Query: native.ResourceLifetime{
			NamespaceID: view.namespaceID, ResourceID: "query", Incarnation: "1",
		},
		QueryRevision: 1,
	}
	require.Equal(t, expected, metadata)
	metadata.WatchRevision = 1
	observation := native.SubscriptionObservation{
		Definition: native.Subscription{ID: "native-watch", QueryID: "native-query", QueryRevision: 1},
		Binding:    &metadata,
	}
	client.steps = append(client.steps, desiredReadStep{
		request: desiredRevision(registrypb.RESOURCE_KIND_WATCH, &contractpb.ResourceRef{Id: "watch", Revision: 1}),
		response: &registrypb.ReadResponse{
			Generation: 7, Records: []*registrypb.Record{watch},
		},
	})
	binding, err := view.RestoreNativeBinding(ctx, client, observation, 2)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, watch.GetWatch(), binding.Target.Watch)
	require.Equal(t, view.namespaceID, binding.Target.NamespaceID)
	require.Equal(t, observation.Definition.ID, binding.Target.ExecutionID)
	require.Equal(t, uint64(2), binding.Target.OwnerEpoch)
	require.Equal(t, observation.Definition, binding.Subscription)
	require.Equal(t, len(client.steps), client.calls)
	watch.GetWatch().Spec.Query.Id = "changed"
	protorequire.ProtoEqual(t, &contractpb.ResourceRef{Id: "query", Revision: 1}, binding.Target.Watch.Spec.Query)
	_, err = view.pinned(registrypb.RESOURCE_KIND_WATCH, &contractpb.ResourceRef{Id: "watch", Revision: 1})
	require.ErrorContains(t, err, "not in the desired view")
}

func TestNativeBindingRejectsDifferentMetadata(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*native.SubscriptionObservation, *DesiredView, *uint64)
		error  string
	}{
		{"missing binding", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) { o.Binding = nil }, "incomplete"},
		{"watch namespace", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) {
			o.Binding.Watch.NamespaceID = "other"
		}, "scope"},
		{"query namespace", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) {
			o.Binding.Query.NamespaceID = "other"
		}, "scope"},
		{"watch lifetime", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) { o.Binding.Watch.Incarnation = "2" }, "lifetime"},
		{"missing watch lifetime", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) { o.Binding.Watch.Incarnation = "" }, "incomplete"},
		{"query lifetime", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) { o.Binding.Query.Incarnation = "2" }, "query lifetime"},
		{"missing query lifetime", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) { o.Binding.Query.Incarnation = "" }, "incomplete"},
		{"future watch pin", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) { o.Binding.WatchRevision++ }, "lifetime"},
		{"query pin", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) { o.Binding.QueryRevision++ }, "query pin"},
		{"native identity", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) { o.Definition.ID = "" }, "incomplete"},
		{"native revision", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) { o.Definition.QueryRevision++ }, "incomplete"},
		{"deleted", func(o *native.SubscriptionObservation, _ *DesiredView, _ *uint64) { o.Deleted = true }, "tombstone"},
		{"owner grant", func(_ *native.SubscriptionObservation, _ *DesiredView, epoch *uint64) { *epoch = 0 }, "owner epoch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source, query, watch := desiredRecords()
			client := &desiredTestClient{t: t, steps: desiredSteps(source, query, watch)}
			view, err := ReadDesiredView(testcontext.For(t), client, "00000000-0000-4000-8000-000000000001", "ns")
			require.NoError(t, err)
			metadata, err := view.NativeWatchBinding("watch")
			require.NoError(t, err)
			observation := native.SubscriptionObservation{
				Definition: native.Subscription{ID: "native-watch", QueryID: "native-query", QueryRevision: 1},
				Binding:    &metadata,
			}
			epoch := uint64(1)
			tc.change(&observation, view, &epoch)
			actual, err := view.RestoreNativeBinding(testcontext.For(t), client, observation, epoch)
			require.ErrorContains(t, err, tc.error)
			require.Equal(t, NativeBinding{}, actual)
			require.Equal(t, len(client.steps), client.calls)
		})
	}
}

func TestNativeBindingRejectsMixedGenerationRecovery(t *testing.T) {
	t.Parallel()
	source, query, watch := desiredRecords()
	current := common.CloneProto(watch)
	current.GetWatch().Revision = 2
	client := &desiredTestClient{t: t, steps: desiredSteps(source, query, current)}
	view, err := ReadDesiredView(testcontext.For(t), client, "00000000-0000-4000-8000-000000000001", "ns")
	require.NoError(t, err)
	metadata, err := view.NativeWatchBinding("watch")
	require.NoError(t, err)
	metadata.WatchRevision = 1
	client.steps = append(client.steps, desiredReadStep{
		request: desiredRevision(registrypb.RESOURCE_KIND_WATCH, &contractpb.ResourceRef{Id: "watch", Revision: 1}),
		response: &registrypb.ReadResponse{
			Generation: 8, Records: []*registrypb.Record{watch},
		},
	})
	actual, err := view.RestoreNativeBinding(testcontext.For(t), client, native.SubscriptionObservation{
		Definition: native.Subscription{ID: "native-watch", QueryID: "native-query", QueryRevision: 1},
		Binding:    &metadata,
	}, 1)
	require.ErrorContains(t, err, "registry changed")
	require.Equal(t, NativeBinding{}, actual)
}
