//go:build integration && temporal_with_drasi && cgo

package temporal_test

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/sdk/go/condition"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/freeport"
	"go.temporal.io/server/common/testing/testcontext"
	watchservice "go.temporal.io/server/service/watch"
	"go.temporal.io/server/service/watch/native"
	"go.temporal.io/server/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestDrasiUIFixture(t *testing.T) {
	manifestPath := os.Getenv("TEMPORAL_DRASI_UI_FIXTURE")
	if manifestPath == "" {
		t.Skip("set TEMPORAL_DRASI_UI_FIXTURE to an unused endpoint manifest path")
	}
	t.Parallel()
	postgresMode := os.Getenv("TEMPORAL_DRASI_UI_POSTGRES")
	require.True(t, postgresMode == "" || postgresMode == "1", "TEMPORAL_DRASI_UI_POSTGRES must be unset or 1")
	ctx := testcontext.For(t, testcontext.WithTimeout(22*time.Minute))
	namespaceName, taskQueue := "drasi-ui-e2e", "drasi-ui-e2e"
	producerType, consumerType, signalType := "DrasiUIProducer", "DrasiUIConsumer", "DrasiUISignalTarget"
	cfg := loadConfig(t)
	logger := log.NewTestLogger()
	cfg.Persistence.NumHistoryShards = 1
	role := cfg.Services[string(primitives.WorkerService)]
	role.RPC.GRPCPort = freeport.MustGetFreePort()
	role.RPC.MembershipPort = freeport.MustGetFreePort()
	role.RPC.HTTPPort = 0
	role.Drasi = config.Watch{StateRoot: t.TempDir(), StorageHostID: uuid.NewString()}
	cfg.Services[string(primitives.WatchService)] = role
	dc := dynamicconfig.NewMemoryClient()
	dc.OverrideSetting(dynamicconfig.NamespaceCacheRefreshInterval, 50*time.Millisecond)
	dc.OverrideSetting(dynamicconfig.EnableExecutionFeedQueue, true)
	dc.OverrideSetting(dynamicconfig.EnableExecutionFeed, true)
	dc.OverrideSetting(dynamicconfig.TransferProcessorMaxPollInterval, 100*time.Millisecond)
	dc.OverrideSetting(dynamicconfig.TransferProcessorUpdateAckInterval, 100*time.Millisecond)
	dc.OverrideSetting(dynamicconfig.EnableDrasiWatches, []dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{Namespace: namespaceName}, Value: true},
	})
	var policy watchservice.PluginPolicy
	if path := os.Getenv("TEMPORAL_DRASI_PLUGIN_CONFIG"); path != "" {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		var selection native.Config
		require.NoError(t, decoder.Decode(&selection))
		var trailing any
		require.ErrorIs(t, decoder.Decode(&trailing), io.EOF)
		policy = watchservice.PluginPolicy{
			Preload: selection.Plugins, Catalogs: selection.Catalogs,
			Trust: selection.Trust, ManagedPlugins: selection.ManagedPlugins,
		}
	}
	var postgres *drasiUIPostgres
	if postgresMode == "1" {
		postgres = startDrasiUIPostgres(ctx, t, policy)
	}
	server, err := temporal.NewServer(
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithConfig(cfg), temporal.WithDynamicConfigClient(dc),
		temporal.WithDrasiPluginPolicy(policy), temporal.WithLogger(logger),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := server.Stop()
		if postgres != nil && err != nil {
			postgres.retain = true
		}
		require.NoError(t, err)
	})
	require.NoError(t, server.Start())
	address := fmt.Sprintf("127.0.0.1:%d", cfg.Services[string(primitives.FrontendService)].RPC.GRPCPort)
	frontendConn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, frontendConn.Close()) })
	frontend := workflowservice.NewWorkflowServiceClient(frontendConn)
	_, err = frontend.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: namespaceName, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	require.NoError(t, err)
	description, err := frontend.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: namespaceName})
	require.NoError(t, err)
	roleConfig := *cfg
	roleConfig.Global.PProf.Port = 0
	roleOptions := []temporal.ServerOption{
		temporal.ForServices([]string{string(primitives.WatchService)}),
		temporal.WithConfig(&roleConfig), temporal.WithDynamicConfigClient(dc),
		temporal.WithDrasiPluginPolicy(policy), temporal.WithLogger(logger),
	}
	if postgres != nil {
		roleOptions = append(roleOptions, temporal.WithDrasiCredentialReader(drasiUICredentialReader{
			namespace: namespace.ID(description.NamespaceInfo.Id), reference: postgres.reference, password: postgres.password,
		}))
	}
	roleServer, err := temporal.NewServer(roleOptions...)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := roleServer.Stop()
		if postgres != nil && err != nil {
			postgres.retain = true
		}
		require.NoError(t, err)
	})
	require.NoError(t, roleServer.Start())
	historyConn, err := grpc.NewClient(
		fmt.Sprintf("127.0.0.1:%d", cfg.Services[string(primitives.HistoryService)].RPC.GRPCPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, historyConn.Close()) })
	assignments := assignmentpb.NewAssignmentServiceClient(historyConn)
	await.Require(ctx, t, func(t *await.T) {
		state, err := assignments.Describe(t.Context(), &assignmentpb.DescribeRequest{
			NamespaceId: description.NamespaceInfo.Id, ExecutionId: watchassignment.BusinessID,
		})
		require.NoError(t, err)
		require.Equal(t, role.Drasi.StorageHostID, state.GetActive().GetGrant().GetStorageHostId())
	}, 30*time.Second, 100*time.Millisecond)
	client, err := sdkclient.Dial(sdkclient.Options{HostPort: address, Namespace: namespaceName})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	worker := sdkworker.New(client, taskQueue, sdkworker.Options{})
	worker.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		var done bool
		workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &done)
		return nil
	}, workflow.RegisterOptions{Name: producerType})
	worker.RegisterWorkflowWithOptions(func(_ workflow.Context, result json.RawMessage) (string, error) {
		return string(result), nil
	}, workflow.RegisterOptions{Name: consumerType})
	worker.RegisterWorkflowWithOptions(func(ctx workflow.Context) (string, error) {
		var result json.RawMessage
		workflow.GetSignalChannel(ctx, "drasi-result").Receive(ctx, &result)
		return string(result), nil
	}, workflow.RegisterOptions{Name: signalType})
	worker.RegisterWorkflowWithOptions(watchConditionHelperWorkflow, workflow.RegisterOptions{Name: "DrasiUICondition"})
	require.NoError(t, condition.RegisterActivities(worker, contractpb.NewWatchServiceClient(frontendConn)))
	require.NoError(t, worker.Start())
	t.Cleanup(worker.Stop)
	targetID := "drasi-ui-signal-target"
	_, err = client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{ID: targetID, TaskQueue: taskQueue}, signalType)
	require.NoError(t, err)
	stop := make(chan struct{})
	var stopOnce sync.Once
	mux := http.NewServeMux()
	if postgres != nil {
		postgres.registerControls(mux, logger)
	}
	mux.HandleFunc("POST /produce", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		var request struct {
			WorkflowID string `json:"workflowId"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			http.Error(w, "invalid fixture workflow request", http.StatusBadRequest)
			return
		}
		var trailing any
		if request.WorkflowID == "" || len(request.WorkflowID) > 128 || decoder.Decode(&trailing) != io.EOF {
			http.Error(w, "one bounded workflowId is required", http.StatusBadRequest)
			return
		}
		run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID: request.WorkflowID, TaskQueue: taskQueue,
		}, producerType)
		if err == nil {
			err = client.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "finish", true)
		}
		if err == nil {
			err = run.Get(ctx, nil)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := fmt.Fprintf(w, `{"workflowId":%q,"runId":%q}`, run.GetID(), run.GetRunID()); err != nil {
			logger.Error("Writing UI fixture workflow result failed", tag.Error(err))
		}
	})
	mux.HandleFunc("POST /stop", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
		if err := http.NewResponseController(w).Flush(); err != nil {
			logger.Error("Flushing UI fixture stop response failed", tag.Error(err))
			return
		}
		stopOnce.Do(func() { close(stop) })
	})
	token := uuid.NewString()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	control := &http.Server{
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
				http.Error(w, "fixture authorization required", http.StatusUnauthorized)
				return
			}
			mux.ServeHTTP(w, r)
		}),
	}
	served := make(chan error, 1)
	go func() { served <- control.Serve(listener) }()
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := errors.Join(control.Shutdown(shutdown), control.Close())
		if postgres != nil && err != nil {
			postgres.retain = true
		}
		require.NoError(t, err)
		select {
		case err := <-served:
			require.ErrorIs(t, err, http.ErrServerClosed)
		case <-shutdown.Done():
			if postgres != nil {
				postgres.retain = true
			}
			t.Fatal("UI fixture control server did not stop")
		}
	})
	manifestValue := map[string]any{
		"temporalAddress": address,
		"frontendHttpURL": fmt.Sprintf("http://127.0.0.1:%d", cfg.Services[string(primitives.FrontendService)].RPC.HTTPPort),
		"namespace":       namespaceName, "taskQueue": taskQueue,
		"producerWorkflowType": producerType, "consumerWorkflowType": consumerType,
		"signalWorkflowId": targetID, "signalName": "drasi-result", "conditionWorkflowType": "DrasiUICondition",
		"queryText": fmt.Sprintf("MATCH (w:Workflow) WHERE w.data.type = '%s' AND w.data.status = '%s' RETURN w.workflow_id AS workflow_id",
			producerType, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED.String()),
		"controlURL": "http://" + listener.Addr().String(), "controlToken": token,
		"externalSourcesVerified": false, "authorizer": "test-only-default-allow-all",
	}
	if postgres != nil {
		manifestValue["postgres"] = postgres.manifest
	}
	manifest, err := json.MarshalIndent(manifestValue, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(manifestPath), 0o700))
	file, err := os.CreateTemp(filepath.Dir(manifestPath), ".drasi-ui-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.Remove(file.Name())) })
	_, err = file.Write(manifest)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NoError(t, os.Link(file.Name(), manifestPath))
	t.Cleanup(func() { require.NoError(t, os.Remove(manifestPath)) })
	t.Logf("Real Temporal/Watch/SDK UI fixture ready; endpoint manifest: %s", manifestPath)
	timer := time.NewTimer(20 * time.Minute)
	defer timer.Stop()
	select {
	case <-stop:
	case <-ctx.Done():
		t.Fatal("UI fixture context ended before a controlled stop")
	case <-timer.C:
		t.Fatal("UI fixture lifetime expired before a controlled stop")
	}
}
