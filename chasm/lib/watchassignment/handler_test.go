package watchassignment

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
)

func TestAssignmentEnablementAndCleanup(t *testing.T) {
	t.Parallel()
	require.False(t, newHandler(nil, dynamicconfig.NewNoopCollection()).enabled("namespace"))
	for _, tc := range []struct {
		name   string
		global bool
	}{
		{"disabled namespace", false},
		{"global namespace", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t)
			request := env.request()
			active, err := env.handler.Begin(env.ctx, request)
			require.NoError(t, err)
			ns := namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Id: env.id, Name: env.namespace}, nil, tc.global, nil, 0)
			namespaces := namespace.NewMockRegistry(gomock.NewController(t))
			namespaces.EXPECT().GetNamespaceByID(namespace.ID(env.id)).Return(ns, nil).AnyTimes()
			env.handler.namespaces, env.enabled = namespaces, false
			retry, err := env.handler.Begin(env.ctx, request)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, active, retry)
			sealed, err := env.handler.Seal(env.ctx, env.closing(active))
			require.NoError(t, err)
			next := env.request()
			next.RequestId, next.ExpectedVersion = "begin-2", sealed.Version
			_, err = env.handler.Begin(env.ctx, next)
			require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, sealed, env.state(t))
		})
	}
	env := newEnvironment(t)
	env.enabled = false
	_, err := env.handler.Begin(env.ctx, env.request())
	require.ErrorContains(t, err, "disabled")
	_, err = env.handler.Describe(env.ctx, &assignmentpb.DescribeRequest{NamespaceId: env.id, ExecutionId: BusinessID})
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
}

func TestAssignmentBoundaryValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*assignmentpb.BeginRequest)
		text   string
	}{
		{"invalid namespace", func(r *assignmentpb.BeginRequest) { r.NamespaceId = "not-a-uuid" }, "canonical namespace UUID"},
		{"noncanonical namespace", func(r *assignmentpb.BeginRequest) {
			r.NamespaceId = strings.ReplaceAll(r.NamespaceId, "-", "")
		}, "canonical namespace UUID"},
		{"other business ID", func(r *assignmentpb.BeginRequest) { r.ExecutionId = "other" }, "execution ID"},
		{"missing host", func(r *assignmentpb.BeginRequest) { r.StorageHostId = "" }, "canonical UUIDs"},
		{"invalid boot", func(r *assignmentpb.BeginRequest) { r.ProcessBootId = "boot" }, "canonical UUIDs"},
		{"noncanonical host", func(r *assignmentpb.BeginRequest) {
			r.StorageHostId = strings.ReplaceAll(r.StorageHostId, "-", "")
		}, "canonical UUIDs"},
		{"missing request", func(r *assignmentpb.BeginRequest) { r.RequestId = "" }, "request_id"},
		{"large request ID", func(r *assignmentpb.BeginRequest) { r.RequestId = strings.Repeat("x", 129) }, "request_id"},
		{"oversized request", func(r *assignmentpb.BeginRequest) { r.RequestId = strings.Repeat("x", 17<<10) }, "size limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t)
			request := env.request()
			tc.change(request)
			_, err := env.handler.Begin(env.ctx, request)
			require.ErrorContains(t, err, tc.text)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
		})
	}
	for _, request := range []keyedRequest{
		nil, (*assignmentpb.BeginRequest)(nil), (*assignmentpb.DescribeRequest)(nil), (*assignmentpb.SealRequest)(nil),
	} {
		_, _, err := (&handler{}).resolve(request)
		require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	}
}
