package watch

import (
	"encoding/json"
	"math"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNativeBindingPreservesIndependentPublicAndNativeIdentities(t *testing.T) {
	t.Parallel()
	binding := NativeBinding{
		Target: IntakeTarget{
			NamespaceID: "namespace-uuid", ExecutionID: "control-execution", OwnerEpoch: 7,
			Watch: &contractpb.Watch{
				Namespace: "namespace", Id: "public-watch", Revision: 9,
				Spec: &contractpb.WatchSpec{Query: &contractpb.ResourceRef{Id: "public-query", Revision: 5}},
			},
		},
		Subscription: native.Subscription{ID: "native-subscription", QueryID: "native-query", QueryRevision: 3},
	}
	event := native.Event{
		SubscriptionID: "native-subscription", QueryID: "native-query", QueryRevision: 3,
		Incarnation: "native-incarnation", Sequence: 0, Kind: native.EventSnapshot,
	}
	output, err := binding.decode(event)
	require.NoError(t, err)
	require.Equal(t, QueryOutput{
		QueryID: "public-query", Revision: 5, Incarnation: "native-incarnation",
		Sequence: 0, Initial: true,
	}, output)
	require.NoError(t, validateOutput(binding.Target, output))
	require.Equal(t, native.Receipt{
		SubscriptionID: "native-subscription", QueryRevision: 3, Incarnation: "native-incarnation",
		Sequence: 0, Kind: native.EventSnapshot, AcceptanceID: "accepted",
	}, event.Receipt("accepted"))
	for _, tc := range []struct {
		name    string
		change  func(*NativeBinding, *native.Event)
		message string
	}{
		{"subscription mismatch", func(_ *NativeBinding, event *native.Event) { event.SubscriptionID = "another" }, "does not match"},
		{"query mismatch", func(_ *NativeBinding, event *native.Event) { event.QueryID = "another" }, "does not match"},
		{"native revision mismatch", func(_ *NativeBinding, event *native.Event) { event.QueryRevision++ }, "does not match"},
		{"missing native subscription", func(binding *NativeBinding, _ *native.Event) { binding.Subscription.ID = "" }, "incomplete"},
		{"missing native query", func(binding *NativeBinding, _ *native.Event) { binding.Subscription.QueryID = "" }, "incomplete"},
		{"missing native revision", func(binding *NativeBinding, _ *native.Event) { binding.Subscription.QueryRevision = 0 }, "incomplete"},
		{"missing public query", func(binding *NativeBinding, _ *native.Event) { binding.Target.Watch.Spec.Query = nil }, "incomplete"},
		{"missing public revision", func(binding *NativeBinding, _ *native.Event) { binding.Target.Watch.Spec.Query.Revision = 0 }, "incomplete"},
		{"malformed bound event", func(_ *NativeBinding, event *native.Event) { event.Kind = "unsupported" }, "unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bound := binding
			bound.Target.Watch = proto.Clone(binding.Target.Watch).(*contractpb.Watch)
			changed := event
			tc.change(&bound, &changed)
			_, err := bound.decode(changed)
			require.ErrorContains(t, err, tc.message)
		})
	}
}

func TestDecodeNativeOutput(t *testing.T) {
	t.Parallel()
	before, after := json.RawMessage(`{"value":1}`), json.RawMessage(`{"value":2}`)
	for _, tc := range []struct {
		name    string
		event   native.Event
		trigger contractpb.Trigger
		key     string
		old     bool
		message string
	}{
		{"initial row", native.Event{Kind: native.EventSnapshot, Rows: []native.Row{{Key: "18446744073709551615", Value: after}}}, contractpb.Trigger_TRIGGER_ENTER, "18446744073709551615", false, ""},
		{"entry", native.Event{Kind: native.EventDelta, Changes: []native.Diff{{Type: "ADD", RowSignature: math.MaxUint64, Data: after}}}, contractpb.Trigger_TRIGGER_ENTER, "18446744073709551615", false, ""},
		{"update", native.Event{Kind: native.EventDelta, Changes: []native.Diff{{Type: "UPDATE", RowSignature: 7, Before: before, After: after}}}, contractpb.Trigger_TRIGGER_UPDATE, "7", true, ""},
		{"leave", native.Event{Kind: native.EventDelta, Changes: []native.Diff{{Type: "DELETE", RowSignature: 7, Data: before}}}, contractpb.Trigger_TRIGGER_LEAVE, "7", true, ""},
		{"malformed row", native.Event{Kind: native.EventSnapshot, Rows: []native.Row{{Key: "row", Value: json.RawMessage(`[]`)}}}, 0, "", false, "JSON object"},
		{"unknown diff", native.Event{Kind: native.EventDelta, Changes: []native.Diff{{Type: "unknown"}}}, 0, "", false, "unsupported"},
		{"unknown event", native.Event{Kind: "unknown"}, 0, "", false, "unsupported"},
		{"mixed snapshot", native.Event{Kind: native.EventSnapshot, Changes: []native.Diff{{Type: "ADD", Data: after}}}, 0, "", false, "delta changes"},
		{"mixed delta", native.Event{Kind: native.EventDelta, Rows: []native.Row{{Key: "row", Value: after}}}, 0, "", false, "snapshot rows"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			output, err := DecodeNativeOutput(tc.event)
			if tc.message != "" {
				require.ErrorContains(t, err, tc.message)
				return
			}
			require.NoError(t, err)
			require.Len(t, output.Changes, 1)
			require.Equal(t, tc.event.Kind == native.EventSnapshot, output.Initial)
			expected := RowChange{Identity: tc.key, Trigger: tc.trigger}
			if tc.old {
				expected.Before = &structpb.Struct{Fields: map[string]*structpb.Value{"value": structpb.NewNumberValue(1)}}
			}
			if tc.trigger != contractpb.Trigger_TRIGGER_LEAVE {
				expected.After = &structpb.Struct{Fields: map[string]*structpb.Value{"value": structpb.NewNumberValue(2)}}
			}
			require.Empty(t, cmp.Diff(expected, output.Changes[0], protocmp.Transform()))
		})
	}
}
