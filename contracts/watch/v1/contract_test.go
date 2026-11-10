package watchpb_test

import (
	"testing"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestPayloadAndOutputIdentityRoundTrip(t *testing.T) {
	t.Parallel()
	original := &watchpb.WorkflowAction{Operation: &watchpb.WorkflowAction_Start{Start: &watchpb.WorkflowStart{
		WorkflowId: "watch-{action_id}", WorkflowType: "workflow", TaskQueue: "queue",
		Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
			Metadata: map[string][]byte{"encoding": []byte("json/plain"), "custom": {0, 1, 255}},
			Data:     []byte(`{"amount":7}`),
		}}},
	}}}
	binary, err := proto.Marshal(original)
	require.NoError(t, err)
	var binaryResult watchpb.WorkflowAction
	require.NoError(t, proto.Unmarshal(binary, &binaryResult))
	require.True(t, proto.Equal(original, &binaryResult))

	json, err := protojson.Marshal(original)
	require.NoError(t, err)
	var jsonResult watchpb.WorkflowAction
	require.NoError(t, protojson.Unmarshal(json, &jsonResult))
	require.True(t, proto.Equal(original, &jsonResult))

	identity := &watchpb.OutputIdentity{
		QueryRevision: 9007199254740993, Incarnation: "incarnation", Sequence: 9007199254740995,
		RowIdentity: "row", DiffOrdinal: 2,
	}
	json, err = protojson.Marshal(identity)
	require.NoError(t, err)
	require.JSONEq(t, `{"queryRevision":"9007199254740993","incarnation":"incarnation","sequence":"9007199254740995","rowIdentity":"row","diffOrdinal":2}`, string(json))
}

func TestWireDefaultsPreserveRecurringBootstrapPolicy(t *testing.T) {
	t.Parallel()
	var spec watchpb.WatchSpec
	require.NoError(t, protojson.Unmarshal([]byte(`{}`), &spec))
	require.Equal(t, watchpb.InitialResults_INITIAL_RESULTS_IGNORE, spec.InitialResults)
	require.Equal(t, watchpb.Trigger_TRIGGER_UNSPECIFIED, spec.Trigger)
	require.Equal(t, watchpb.ResultPayload_RESULT_PAYLOAD_APPEND, (&watchpb.WorkflowAction{}).ResultPayload)
}

func TestSnapshotOutputSequenceRoundTrip(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		sequence uint64
	}{
		{"new query snapshot", 0},
		{"existing query snapshot", 37},
		{"live delta after existing query snapshot", 38},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			expected := &watchpb.ResourceStatus{AcceptedOutput: &watchpb.OutputIdentity{
				QueryRevision: 1, Incarnation: "incarnation", Sequence: test.sequence, RowIdentity: "row",
			}}
			for _, codec := range []struct {
				name      string
				marshal   func(proto.Message) ([]byte, error)
				unmarshal func([]byte, proto.Message) error
			}{
				{"protobuf", proto.Marshal, proto.Unmarshal},
				{"json", protojson.Marshal, protojson.Unmarshal},
			} {
				t.Run(codec.name, func(t *testing.T) {
					t.Parallel()
					encoded, err := codec.marshal(expected)
					require.NoError(t, err)
					var actual watchpb.ResourceStatus
					require.NoError(t, codec.unmarshal(encoded, &actual))
					require.True(t, proto.Equal(expected, &actual))
				})
			}
		})
	}
}
