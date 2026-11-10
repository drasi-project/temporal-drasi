package watch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type graphAssignmentClient struct {
	assignmentpb.AssignmentServiceClient
	describe func(context.Context, *assignmentpb.DescribeRequest) (*assignmentpb.State, error)
	begin    func(context.Context, *assignmentpb.BeginRequest) (*assignmentpb.State, error)
	seal     func(context.Context, *assignmentpb.SealRequest) (*assignmentpb.State, error)
}

func (c *graphAssignmentClient) Describe(ctx context.Context, request *assignmentpb.DescribeRequest, _ ...grpc.CallOption) (*assignmentpb.State, error) {
	return c.describe(ctx, request)
}

func (c *graphAssignmentClient) Begin(ctx context.Context, request *assignmentpb.BeginRequest, _ ...grpc.CallOption) (*assignmentpb.State, error) {
	return c.begin(ctx, request)
}

func (c *graphAssignmentClient) Seal(ctx context.Context, request *assignmentpb.SealRequest, _ ...grpc.CallOption) (*assignmentpb.State, error) {
	return c.seal(ctx, request)
}

func graphTestOptions(t *testing.T, client assignmentpb.AssignmentServiceClient) GraphOptions {
	t.Helper()
	return GraphOptions{
		TemporalInput: ManualTemporalInput{},
		NamespaceID:   "00000000-0000-4000-8000-000000000001", Namespace: "ns", StateRoot: t.TempDir(),
		StorageHostID: "10000000-0000-4000-8000-000000000001",
		ProcessBootID: "20000000-0000-4000-8000-000000000001",
		RequestID:     "graph-begin", Assignments: client,
	}
}

func graphActiveState(t *testing.T, request *assignmentpb.BeginRequest) *assignmentpb.State {
	t.Helper()
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)
	return &assignmentpb.State{Version: request.ExpectedVersion + 1, Phase: &assignmentpb.State_Active{
		Active: &assignmentpb.Active{
			RequestId: request.RequestId, Fingerprint: digest[:],
			Grant: &assignmentpb.Grant{
				NamespaceId: request.NamespaceId, InstanceId: request.NamespaceId,
				StorageHostId: request.StorageHostId, ProcessBootId: request.ProcessBootId, Epoch: 1,
			},
		},
	}}
}

func graphSealedState(t *testing.T, active *assignmentpb.Active, request *assignmentpb.SealRequest) *assignmentpb.State {
	t.Helper()
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)
	return &assignmentpb.State{Version: request.ExpectedVersion + 1, Phase: &assignmentpb.State_Sealed{
		Sealed: &assignmentpb.Sealed{
			Predecessor: common.CloneProto(active), Checkpoint: common.CloneProto(request.Checkpoint),
			RequestId: request.RequestId, Fingerprint: digest[:],
		},
	}}
}

func TestGraphOptions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*GraphOptions)
	}{
		{"missing namespace", func(o *GraphOptions) { o.Namespace = "" }},
		{"noncanonical namespace ID", func(o *GraphOptions) { o.NamespaceID = strings.ToUpper(o.NamespaceID) + "A" }},
		{"zero storage host", func(o *GraphOptions) { o.StorageHostID = "00000000-0000-0000-0000-000000000000" }},
		{"invalid process boot", func(o *GraphOptions) { o.ProcessBootID = "boot" }},
		{"relative root", func(o *GraphOptions) { o.StateRoot = "state" }},
		{"filesystem root", func(o *GraphOptions) { o.StateRoot = "/" }},
		{"unclean root", func(o *GraphOptions) { o.StateRoot += "/../state" }},
		{"missing request ID", func(o *GraphOptions) { o.RequestID = "" }},
		{"oversized request ID", func(o *GraphOptions) { o.RequestID = strings.Repeat("x", 121) }},
		{"missing assignments", func(o *GraphOptions) { o.Assignments = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options := graphTestOptions(t, &graphAssignmentClient{})
			require.NoError(t, validateGraphOptions(options))
			test.change(&options)
			require.Error(t, validateGraphOptions(options))
		})
	}
}

func TestGraphOwnershipResolvesLostBeginWithoutInventingGrant(t *testing.T) {
	t.Parallel()
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary reply", true: "committed reply lost"}[lost], func(t *testing.T) {
			t.Parallel()
			var committed *assignmentpb.State
			var opening *assignmentpb.BeginRequest
			client := &graphAssignmentClient{
				describe: func(context.Context, *assignmentpb.DescribeRequest) (*assignmentpb.State, error) {
					if committed == nil {
						return nil, serviceerror.NewNotFound("no assignment")
					}
					return common.CloneProto(committed), nil
				},
				begin: func(_ context.Context, request *assignmentpb.BeginRequest) (*assignmentpb.State, error) {
					if opening != nil {
						protorequire.ProtoEqual(t, opening, request)
					}
					opening = common.CloneProto(request)
					committed = graphActiveState(t, request)
					if lost {
						return nil, serviceerror.NewUnavailable("committed Begin reply lost")
					}
					return common.CloneProto(committed), nil
				},
			}
			options := graphTestOptions(t, client)
			state, err := beginGraphOwnership(testcontext.For(t), options)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, committed, state)
			state.GetActive().Grant.Epoch = 99
			retry, err := beginGraphOwnership(testcontext.For(t), options)
			require.NoError(t, err)
			require.Equal(t, uint64(1), retry.GetActive().Grant.Epoch)
			require.Zero(t, opening.ExpectedVersion)
		})
	}
}

func TestGraphOwnershipRejectsMalformedConfirmation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*assignmentpb.State)
	}{
		{"wrong namespace", func(s *assignmentpb.State) { s.GetActive().Grant.NamespaceId = "other" }},
		{"wrong instance", func(s *assignmentpb.State) { s.GetActive().Grant.InstanceId = "other" }},
		{"wrong storage host", func(s *assignmentpb.State) { s.GetActive().Grant.StorageHostId = "other" }},
		{"wrong boot", func(s *assignmentpb.State) { s.GetActive().Grant.ProcessBootId = "other" }},
		{"zero epoch", func(s *assignmentpb.State) { s.GetActive().Grant.Epoch = 0 }},
		{"wrong version", func(s *assignmentpb.State) { s.Version++ }},
		{"missing fingerprint", func(s *assignmentpb.State) { s.GetActive().Fingerprint = nil }},
		{"wrong request ID", func(s *assignmentpb.State) { s.GetActive().RequestId = "other" }},
		{"missing grant", func(s *assignmentpb.State) { s.GetActive().Grant = nil }},
		{"missing phase", func(s *assignmentpb.State) { s.Phase = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options := graphTestOptions(t, &graphAssignmentClient{})
			request := &assignmentpb.BeginRequest{
				NamespaceId: options.NamespaceID, StorageHostId: options.StorageHostID,
				ProcessBootId: options.ProcessBootID, RequestId: options.RequestID,
			}
			state := graphActiveState(t, request)
			require.NoError(t, checkGraphOwnership(state, request))
			test.change(state)
			require.Error(t, checkGraphOwnership(state, request))
		})
	}
	require.Error(t, checkGraphOwnership(nil, &assignmentpb.BeginRequest{}))
}

func TestGraphCheckpointRequiresExactManifestAndIdentity(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*native.Checkpoint, *assignmentpb.Cut)
	}{
		{"valid", func(*native.Checkpoint, *assignmentpb.Cut) {}},
		{"wrong digest", func(_ *native.Checkpoint, c *assignmentpb.Cut) { c.ManifestSha256 = strings.Repeat("a", 64) }},
		{"wrong namespace", func(m *native.Checkpoint, _ *assignmentpb.Cut) { m.InstanceID = "other" }},
		{"wrong epoch", func(m *native.Checkpoint, _ *assignmentpb.Cut) { m.OwnerEpoch++ }},
		{"unsupported version", func(m *native.Checkpoint, _ *assignmentpb.Cut) { m.Version++ }},
		{"missing build", func(m *native.Checkpoint, _ *assignmentpb.Cut) { m.Upstream = "" }},
		{"missing files", func(m *native.Checkpoint, _ *assignmentpb.Cut) { m.Files = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			manifest := &native.Checkpoint{
				Version: native.ABIVersion, InstanceID: "instance", OwnerEpoch: 1, Upstream: "development-build",
				Files: map[string]string{"state.db": strings.Repeat("b", 64)},
			}
			cut := &assignmentpb.Cut{Directory: t.TempDir(), InstanceId: "instance", OwnerEpoch: 1}
			test.change(manifest, cut)
			encoded, err := json.Marshal(manifest)
			require.NoError(t, err)
			if cut.ManifestSha256 == "" {
				digest := sha256.Sum256(encoded)
				cut.ManifestSha256 = hex.EncodeToString(digest[:])
			}
			require.NoError(t, os.WriteFile(filepath.Join(cut.Directory, "checkpoint-v1.json"), encoded, 0o600))
			actual, err := readGraphCheckpoint(cut)
			if test.name == "valid" {
				require.NoError(t, err)
				require.Equal(t, manifest, actual)
			} else {
				require.Error(t, err)
				require.Nil(t, actual)
			}
		})
	}
}

func TestGraphCheckpointRejectsMissingOversizedAndLinkedManifests(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "checkpoint-v1.json")
	_, err := readCheckpointManifest(root)
	require.ErrorContains(t, err, "read authoritative Watch checkpoint")
	file, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(16<<20))
	require.NoError(t, file.Close())
	exact, err := readCheckpointManifest(root)
	require.NoError(t, err)
	require.Len(t, exact, 16<<20)
	require.NoError(t, os.Truncate(path, (16<<20)+1))
	_, err = readCheckpointManifest(root)
	require.ErrorContains(t, err, "bounded regular file")
	linkRoot := t.TempDir()
	require.NoError(t, os.Symlink(path, filepath.Join(linkRoot, "checkpoint-v1.json")))
	_, err = readCheckpointManifest(linkRoot)
	require.ErrorContains(t, err, "bounded regular file")
}
