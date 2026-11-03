//go:build integration

package tests

import (
	"strings"
	"testing"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestDrasiDefinitionAPIsWithoutNativeRuntime(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t)
	ctx := testcontext.For(t)
	tv := testvars.New(t)
	client := env.GetTestCluster().DrasiClient()
	capabilities, err := client.GetCapabilities(ctx, &watchpb.GetCapabilitiesRequest{Namespace: env.Namespace().String()})
	require.NoError(t, err)
	require.False(t, capabilities.Enabled)
	require.Empty(t, capabilities.QueryLanguages)
	require.False(t, capabilities.TemporalSource)
	require.False(t, capabilities.ExternalSources)
	sources, err := client.ListSources(ctx, &watchpb.ListSourcesRequest{Namespace: env.Namespace().String()})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &watchpb.ListSourcesResponse{}, sources)
	rejected := &watchpb.CreateSourceRequest{
		Namespace: env.Namespace().String(), Id: "feed", RequestId: tv.RequestID(),
		Spec: &watchpb.SourceSpec{Kind: &watchpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}},
	}
	_, err = client.CreateSource(ctx, rejected)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	restore := env.OverrideDynamicConfig(dynamicconfig.EnableDrasiWatches, true)
	defer restore()
	_, err = client.CreateSource(ctx, rejected)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	request := common.CloneProto(rejected)
	request.RequestId += "-enabled"
	created, err := client.CreateSource(ctx, request)
	require.NoError(t, err)
	expected := &watchpb.Source{
		Namespace: request.Namespace, Id: request.Id, Revision: 1, Spec: request.Spec,
		DesiredState: watchpb.DesiredState_DESIRED_STATE_STOPPED,
	}
	protorequire.ProtoEqual(t, expected, created)
	restore()
	retried, err := client.CreateSource(ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, retried)
	stopped, err := client.StopSource(ctx, &watchpb.SourceOperationRequest{
		Namespace: request.Namespace, Id: request.Id, ExpectedRevision: 1, RequestId: tv.RequestID() + "-stop",
	})
	require.NoError(t, err)
	expected.Revision = 2
	protorequire.ProtoEqual(t, expected, stopped)
	deleted, err := client.DeleteSource(ctx, &watchpb.SourceOperationRequest{
		Namespace: request.Namespace, Id: request.Id, ExpectedRevision: 2, RequestId: tv.RequestID() + "-delete",
	})
	require.NoError(t, err)
	expected.Revision = 3
	expected.DesiredState = watchpb.DesiredState_DESIRED_STATE_DELETED
	protorequire.ProtoEqual(t, expected, deleted)
	sources, err = client.ListSources(ctx, &watchpb.ListSourcesRequest{Namespace: env.Namespace().String()})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &watchpb.ListSourcesResponse{Sources: []*watchpb.Source{expected}}, sources)
}

func TestDrasiSourceCredentialAPIs(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t)
	ctx := testcontext.For(t)
	tv := testvars.New(t)
	client := env.GetTestCluster().DrasiClient()
	restore := env.OverrideDynamicConfig(dynamicconfig.EnableDrasiWatches, true)
	defer restore()
	legacy := &watchpb.CreateSourceRequest{
		Namespace: env.Namespace().String(), Id: "external", RequestId: tv.RequestID(),
		Spec: &watchpb.SourceSpec{Kind: &watchpb.SourceSpec_External{External: &watchpb.ComponentConfiguration{
			Plugin: &watchpb.PluginComponentRef{Digest: "sha256:" + strings.Repeat("a", 64), Component: "fixture-source"},
			Secrets: map[string]*watchpb.SecretReference{
				"connection": {Provider: "credentials", Key: "reader"},
			},
		}}},
	}
	_, rejected := client.CreateSource(ctx, legacy)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(rejected).Code())
	_, err := client.GetSource(ctx, &watchpb.GetSourceRequest{Namespace: legacy.Namespace, Id: legacy.Id})
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
	_, err = client.CreateSource(ctx, legacy)
	require.EqualError(t, err, rejected.Error())
	request := common.CloneProto(legacy)
	request.Spec.GetExternal().Secrets["connection"].Version = "generation-17"
	request.Spec.Bootstrap = common.CloneProto(request.Spec.GetExternal())
	request.Spec.Bootstrap.Plugin.Component = "fixture-bootstrap"
	request.Spec.Bootstrap.Secrets["connection"].Version = "bootstrap-18"
	_, err = client.CreateSource(ctx, request)
	require.ErrorContains(t, err, "different method or content")
	request.RequestId += "-accepted"
	created, err := client.CreateSource(ctx, request)
	require.NoError(t, err)
	expected := &watchpb.Source{
		Namespace: request.Namespace, Id: request.Id, Revision: 1, Spec: request.Spec,
		DesiredState: watchpb.DesiredState_DESIRED_STATE_STOPPED,
	}
	protorequire.ProtoEqual(t, expected, created)
	updated := common.CloneProto(expected)
	updated.Revision = 2
	updated.Spec.GetExternal().Secrets["connection"].Version = "generation-19"
	updated.Spec.Bootstrap.Secrets["connection"].Version = "bootstrap-20"
	actual, err := client.UpdateSource(ctx, &watchpb.UpdateSourceRequest{
		Namespace: request.Namespace, Id: request.Id, ExpectedRevision: 1, RequestId: tv.RequestID() + "-rotate",
		Spec: updated.Spec,
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, updated, actual)
	invalidUpdate := &watchpb.UpdateSourceRequest{
		Namespace: request.Namespace, Id: request.Id, ExpectedRevision: 2, RequestId: tv.RequestID() + "-reject-update",
		Spec: common.CloneProto(updated.Spec),
	}
	invalidUpdate.Spec.Bootstrap.Secrets["connection"].Version = ""
	_, rejectedUpdate := client.UpdateSource(ctx, invalidUpdate)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(rejectedUpdate).Code())
	actual, err = client.GetSource(ctx, &watchpb.GetSourceRequest{Namespace: request.Namespace, Id: request.Id})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, updated, actual)
	sources, err := client.ListSources(ctx, &watchpb.ListSourcesRequest{Namespace: request.Namespace})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &watchpb.ListSourcesResponse{Sources: []*watchpb.Source{updated}}, sources)
	restore()
	actual, err = client.CreateSource(ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, actual)
	_, err = client.UpdateSource(ctx, invalidUpdate)
	require.EqualError(t, err, rejectedUpdate.Error())
}
