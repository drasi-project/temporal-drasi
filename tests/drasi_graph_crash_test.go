//go:build integration && temporal_with_drasi && cgo && (darwin || linux)

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	watchservice "go.temporal.io/server/service/watch"
	"go.temporal.io/server/service/watch/native"
	"go.temporal.io/server/tests/testcore"
)

func TestDrasiGraphNativeOwnerProcess(t *testing.T) {
	t.Parallel()
	encoded := os.Getenv("TEMPORAL_DRASI_CRASH_CONFIG")
	if encoded == "" {
		t.Skip("subprocess fixture")
	}
	var config native.Config
	require.NoError(t, json.Unmarshal([]byte(encoded), &config))
	runtime, err := native.Open(t.Context(), config)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	ready := os.NewFile(3, "native-owner-ready")
	require.NotNil(t, ready)
	require.NoError(t, json.NewEncoder(ready).Encode(true))
	require.NoError(t, ready.Close())
	<-t.Context().Done()
}

func TestDrasiGraphProcessCrashRecovery(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableDrasiWatches, true))
	client := &assignmentLostSealClient{AssignmentServiceClient: env.GetTestCluster().WatchAssignmentClient()}
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	options := watchservice.GraphOptions{
		TemporalInput: watchservice.ManualTemporalInput{},
		NamespaceID:   env.NamespaceID().String(), Namespace: env.Namespace().String(), StateRoot: root,
		StorageHostID: "10000000-0000-4000-8000-000000000001",
		ProcessBootID: "20000000-0000-4000-8000-000000000001",
		RequestID:     "crashed-native-owner", Assignments: client,
	}
	owner, err := client.Begin(t.Context(), &assignmentpb.BeginRequest{
		NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID,
		StorageHostId: options.StorageHostID, ProcessBootId: options.ProcessBootID, RequestId: options.RequestID,
	})
	require.NoError(t, err)
	directory := filepath.Join(root, options.NamespaceID, "owners", "1")
	require.NoError(t, os.MkdirAll(filepath.Dir(directory), 0o700))
	config, err := json.Marshal(native.Config{
		InstanceID: options.NamespaceID, OwnerEpoch: owner.GetActive().Grant.Epoch,
		StateDir: directory, StartMode: native.StartModeDormant,
	})
	require.NoError(t, err)
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDrasiGraphNativeOwnerProcess$")
	command.Env = append(os.Environ(), "TEMPORAL_DRASI_CRASH_CONFIG="+string(config), "RUST_LOG=error")
	command.ExtraFiles = []*os.File{writer}
	outputPath := filepath.Join(t.TempDir(), "owner.log")
	output, err := os.Create(outputPath)
	require.NoError(t, err)
	command.Stdout, command.Stderr = output, output
	require.NoError(t, command.Start())
	require.NoError(t, writer.Close())
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	waited := false
	t.Cleanup(func() {
		cancel()
		if !waited {
			<-exited
		}
		require.NoError(t, output.Close())
		if t.Failed() {
			log, err := os.ReadFile(outputPath)
			require.NoError(t, err)
			t.Log(string(log))
		}
	})
	type startedOwner struct {
		started bool
		err     error
	}
	ready := make(chan startedOwner, 1)
	go func() {
		var result startedOwner
		result.err = json.NewDecoder(reader).Decode(&result.started)
		ready <- result
	}()
	started := await.Rcv(t, ready)
	require.NoError(t, started.err)
	require.True(t, started.started)
	options.ProcessBootID, options.RequestID = "20000000-0000-4000-8000-000000000002", "recover-native-owner"
	contender, err := watchservice.OpenGraph(t.Context(), options)
	require.Nil(t, contender)
	require.ErrorContains(t, err, "cannot be fenced")
	describe := &assignmentpb.DescribeRequest{NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID}
	unchanged, err := client.Describe(t.Context(), describe)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, owner, unchanged)
	require.NoError(t, command.Process.Kill())
	_, killed := errors.AsType[*exec.ExitError](await.Rcv(t, exited))
	waited = true
	require.True(t, killed)
	successor, err := watchservice.OpenGraph(t.Context(), options)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, successor.Close(ctx))
	})
	require.True(t, client.dropped, "the committed recovery Seal reply was lost")
	active, err := client.Describe(t.Context(), describe)
	require.NoError(t, err)
	require.Equal(t, uint64(2), active.GetActive().Grant.Epoch)
	require.Equal(t, filepath.Join(root, options.NamespaceID, "recoveries", "1"), active.GetActive().RecoveryCut.Directory)
	require.NotEmpty(t, active.GetActive().RecoveryCut.ManifestSha256)
	require.NoError(t, successor.Close(t.Context()))
}
