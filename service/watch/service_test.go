package watch

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/watch/native"
	"go.uber.org/mock/gomock"
)

func TestWatchServiceConfig(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*config.Watch)
		valid  bool
	}{
		{name: "default capacity", valid: true},
		{name: "explicit capacity", valid: true, change: func(c *config.Watch) { c.MaxNamespaces = 256 }},
		{name: "missing storage host", change: func(c *config.Watch) { c.StorageHostID = "" }},
		{name: "zero storage host", change: func(c *config.Watch) { c.StorageHostID = "00000000-0000-0000-0000-000000000000" }},
		{name: "noncanonical storage host", change: func(c *config.Watch) { c.StorageHostID = "10000000000040008000000000000001" }},
		{name: "relative root", change: func(c *config.Watch) { c.StateRoot = "state" }},
		{name: "unclean root", change: func(c *config.Watch) { c.StateRoot += "/../state" }},
		{name: "filesystem root", change: func(c *config.Watch) { c.StateRoot = "/" }},
		{name: "oversized root", change: func(c *config.Watch) { c.StateRoot = "/" + strings.Repeat("x", 3500) }},
		{name: "missing directory", change: func(c *config.Watch) { c.StateRoot = filepath.Join(c.StateRoot, "missing") }},
		{name: "negative capacity", change: func(c *config.Watch) { c.MaxNamespaces = -1 }},
		{name: "oversized capacity", change: func(c *config.Watch) { c.MaxNamespaces = 257 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			input := config.Watch{
				StateRoot: t.TempDir(), StorageHostID: "10000000-0000-4000-8000-000000000001",
			}
			if test.change != nil {
				test.change(&input)
			}
			output, err := validateServiceConfig(input)
			if !test.valid {
				require.Error(t, err)
				require.Equal(t, config.Watch{}, output)
				return
			}
			require.NoError(t, err)
			root, err := filepath.EvalSymlinks(input.StateRoot)
			require.NoError(t, err)
			input.StateRoot = root
			if input.MaxNamespaces == 0 {
				input.MaxNamespaces = 16
			}
			require.Equal(t, input, output)
		})
	}
	t.Run("regular file is not a storage directory", func(t *testing.T) {
		t.Parallel()
		file := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(file, nil, 0o600))
		_, err := validateServiceConfig(config.Watch{
			StateRoot: file, StorageHostID: "10000000-0000-4000-8000-000000000001",
		})
		require.ErrorContains(t, err, "existing non-root directory")
	})
}

func TestWatchServiceNamespacePlacement(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		global     bool
		disabled   bool
		state      enumspb.NamespaceState
		remote     bool
		lookupErr  bool
		wantCancel bool
	}{
		{name: "enabled local namespace"},
		{name: "global namespace", global: true, wantCancel: true},
		{name: "disabled namespace", disabled: true, wantCancel: true},
		{name: "deleted namespace", state: enumspb.NAMESPACE_STATE_DELETED, wantCancel: true},
		{name: "deprecated namespace", state: enumspb.NAMESPACE_STATE_DEPRECATED, wantCancel: true},
		{name: "other host selected", remote: true, wantCancel: true},
		{name: "uncertain placement retains current owner", lookupErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			registry := namespace.NewMockRegistry(controller)
			resolver := membership.NewMockServiceResolver(controller)
			state := test.state
			if state == enumspb.NAMESPACE_STATE_UNSPECIFIED {
				state = enumspb.NAMESPACE_STATE_REGISTERED
			}
			info := &persistencespb.NamespaceInfo{Id: "00000000-0000-4000-8000-000000000001", Name: "ns", State: state}
			ns := namespace.NewLocalNamespaceForTest(info, nil, "cluster")
			if test.global {
				ns = namespace.NewGlobalNamespaceForTest(info, nil, &persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: "cluster", Clusters: []string{"cluster"},
				}, 1)
			}
			registry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{ns})
			if !test.global && !test.disabled && state == enumspb.NAMESPACE_STATE_REGISTERED {
				address, err := "self", error(nil)
				if test.remote {
					address = "other"
				}
				if test.lookupErr {
					err = serviceerror.NewUnavailable("placement temporarily unavailable")
				}
				resolver.EXPECT().Lookup(ns.ID().String()).Return(membership.NewHostInfoFromAddress(address), err)
			}
			cancelled := false
			hosted := &hostedNamespace{cancel: func() { cancelled = true }}
			service := &Service{
				params: ServiceParams{
					Namespaces: registry, Resolver: resolver, Logger: log.NewNoopLogger(),
					Host: membership.NewHostInfoProvider(membership.NewHostInfoFromAddress("self")),
				},
				enabled:    func(string) bool { return !test.disabled },
				namespaces: map[namespace.ID]*hostedNamespace{ns.ID(): hosted},
			}
			service.discover(testcontext.For(t))
			require.Equal(t, test.wantCancel, cancelled)
			require.Same(t, hosted, service.namespaces[ns.ID()])
		})
	}
}

func TestWatchServiceDoesNotReopenBlockedClose(t *testing.T) {
	t.Parallel()
	controller := gomock.NewController(t)
	registry := namespace.NewMockRegistry(controller)
	registry.EXPECT().GetAllNamespaces().Return(nil)
	failure := serviceerror.NewFailedPrecondition("physical close failed")
	cancelled := false
	hosted := &hostedNamespace{
		cancel: func() { cancelled = true }, closed: true, closeErr: failure,
	}
	service := &Service{
		params:     ServiceParams{Namespaces: registry},
		namespaces: map[namespace.ID]*hostedNamespace{"ns": hosted},
	}
	service.discover(testcontext.For(t))
	require.True(t, cancelled)
	require.Same(t, hosted, service.namespaces["ns"])
}

func TestWatchServiceNoNativeBuild(t *testing.T) {
	t.Parallel()
	if native.Enabled() {
		t.Skip("requires a build without native support")
	}
	service, err := NewService(ServiceParams{})
	require.Nil(t, service)
	require.ErrorIs(t, err, native.ErrDisabled)
}

func TestWatchServiceCloseDoesNotAcquireNewOwnership(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		state *assignmentpb.State
		err   error
	}{
		{name: "never created", err: serviceerror.NewNotFound("no owner")},
		{name: "another owner", state: &assignmentpb.State{Version: 1, Phase: &assignmentpb.State_Active{
			Active: &assignmentpb.Active{RequestId: "another-request", Grant: &assignmentpb.Grant{
				ProcessBootId: "another-process", StorageHostId: "another-storage",
			}},
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			begins := 0
			client := &graphAssignmentClient{
				describe: func(context.Context, *assignmentpb.DescribeRequest) (*assignmentpb.State, error) {
					return test.state, test.err
				},
				begin: func(context.Context, *assignmentpb.BeginRequest) (*assignmentpb.State, error) {
					begins++
					return nil, serviceerror.NewInternal("unexpected Begin")
				},
			}
			service := &Service{}
			hosted := &hostedNamespace{options: graphTestOptions(t, client)}
			require.NoError(t, service.closeNamespace(testcontext.For(t), hosted, nil))
			require.Zero(t, begins)
		})
	}
}
