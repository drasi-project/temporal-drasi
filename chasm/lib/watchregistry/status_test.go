package watchregistry

import (
	"context"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	watchclient "go.temporal.io/server/client/watch"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc"
)

type frontendAssignmentClient struct {
	assignmentpb.AssignmentServiceClient
	err error
}

func (c frontendAssignmentClient) Describe(context.Context, *assignmentpb.DescribeRequest, ...grpc.CallOption) (*assignmentpb.State, error) {
	return nil, c.err
}

func TestFrontendRuntimeFailurePreservesDefinitionsAndRedactsDiagnostics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		err    error
		status *contractpb.ResourceStatus
	}{
		{name: "never hosted", err: serviceerror.NewNotFound("no runtime")},
		{name: "runtime unavailable", err: serviceerror.NewUnavailable("password=secret /private/path"),
			status: &contractpb.ResourceStatus{Message: "Runtime observation unavailable"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := &contractpb.Source{Namespace: "test", Id: "source", Revision: 3, Spec: sourceSpec()}
			record := &registrypb.Record{
				Resource: &registrypb.Record_Source{Source: source}, Incarnation: "1", Desired: registrypb.DESIRED_STATE_RUNNING,
			}
			snapshot := &registrypb.ReadResponse{Generation: 4, Records: []*registrypb.Record{record}}
			before := common.CloneProto(snapshot)
			handler := frontendForTest(t, frontendRegistryClient{read: func(context.Context, *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
				return snapshot, nil
			}})
			handler.observer = watchclient.NewObserver(frontendAssignmentClient{err: tc.err}, nil, nil)
			got, err := handler.GetSource(t.Context(), &contractpb.GetSourceRequest{Namespace: "test", Id: "source"})
			require.NoError(t, err)
			expected := common.CloneProto(source)
			expected.DesiredState, expected.Status = contractpb.DesiredState_DESIRED_STATE_RUNNING, tc.status
			protorequire.ProtoEqual(t, expected, got)
			protorequire.ProtoEqual(t, before, snapshot)
		})
	}
}
