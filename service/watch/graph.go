package watch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/proto"
)

type GraphOptions struct {
	Plugins       PluginPolicy
	Credentials   CredentialReader
	TemporalInput TemporalInput
	NamespaceID   string
	Namespace     string
	StateRoot     string
	StorageHostID string
	ProcessBootID string
	RequestID     string
	Assignments   assignmentpb.AssignmentServiceClient
	Registry      registrypb.RegistryServiceClient
	Control       controlpb.ControlServiceClient
	Workflows     workflowservice.WorkflowServiceClient
}

// Graph retains exclusive native ownership until a verified checkpoint is sealed.
type Graph struct {
	runtime     *native.Runtime
	options     GraphOptions
	owner       *assignmentpb.Active
	version     uint64
	destination string

	mu       sync.Mutex
	closing  chan struct{}
	closeErr error
	seal     *assignmentpb.SealRequest
	sealed   bool

	view           *DesiredView
	nodes          map[string]native.NodeObservation
	watches        map[native.ResourceLifetime]*graphWatch
	problems       map[string]string
	attempts       map[string]time.Time
	observed       bool
	nodeGeneration uint64
	nodeObservedAt time.Time
	topology       *graphOperation
	runContext     context.Context
	cancel         context.CancelFunc
	workers        sync.WaitGroup
	history        map[string]*historyInputWorker
	plugins        *runtimePlugins
}

// OpenGraph accepts no caller-supplied epoch or recovery cut.
func OpenGraph(ctx context.Context, options GraphOptions) (*Graph, error) {
	if !native.Enabled() {
		return nil, native.ErrDisabled
	}
	if err := validateGraphOptions(options); err != nil {
		return nil, err
	}
	root, err := filepath.EvalSymlinks(options.StateRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve Watch state root: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("read Watch state root: %w", err)
	}
	if !info.IsDir() || root == string(filepath.Separator) {
		return nil, serviceerror.NewInvalidArgument("Watch state root must be an existing non-root directory")
	}
	options.StateRoot = root
	options.Plugins = clonePluginPolicy(options.Plugins)
	state, err := beginGraphOwnership(ctx, options)
	if err != nil {
		return nil, err
	}
	owner := state.GetActive()
	closeIntent := graphDirectory(root, options.NamespaceID, "closing", owner.Grant.Epoch)
	if _, err := os.Lstat(closeIntent); err == nil {
		return nil, serviceerror.NewFailedPrecondition("Watch graph close has begun; this owner epoch cannot reopen")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read Watch graph close intent: %w", err)
	}
	configuration := graphConfiguration(options, owner.Grant.Epoch)
	if owner.RecoveryCut != nil {
		cut := owner.RecoveryCut
		if cut.InstanceId != options.NamespaceID || cut.OwnerEpoch == 0 ||
			cut.OwnerEpoch != owner.Grant.Epoch-1 ||
			(cut.Directory != graphDirectory(root, options.NamespaceID, "checkpoints", cut.OwnerEpoch) &&
				cut.Directory != graphDirectory(root, options.NamespaceID, "recoveries", cut.OwnerEpoch)) {
			return nil, serviceerror.NewDataLoss("assignment recovery cut does not match the owned storage location")
		}
		manifest, err := readGraphCheckpoint(cut)
		if err != nil {
			return nil, err
		}
		if cut.Directory == graphDirectory(root, options.NamespaceID, "recoveries", cut.OwnerEpoch) &&
			manifest.RetirementID == "" {
			return nil, serviceerror.NewDataLoss("recovered Watch checkpoint has no native retirement identity")
		}
		configuration.RestoreFrom = cut.Directory
	} else if owner.Grant.Epoch != 1 {
		return nil, serviceerror.NewDataLoss("successor assignment has no recovery cut")
	}
	for _, directory := range []string{
		filepath.Dir(configuration.StateDir),
		filepath.Dir(graphDirectory(root, options.NamespaceID, "checkpoints", owner.Grant.Epoch)),
		filepath.Dir(closeIntent),
	} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("prepare owned Watch storage: %w", err)
		}
	}
	runtime, err := native.Open(ctx, configuration)
	if err != nil {
		return nil, fmt.Errorf("open owned Watch graph: %w", err)
	}
	return &Graph{
		runtime: runtime, options: options, owner: common.CloneProto(owner), version: state.Version,
		destination: graphDirectory(root, options.NamespaceID, "checkpoints", owner.Grant.Epoch),
	}, nil
}

func validateGraphOptions(options GraphOptions) error {
	switch input := options.TemporalInput.(type) {
	case ManualTemporalInput:
	case HistoryTemporalInput:
		if input.Client == nil || input.ShardCount < 1 || input.ShardCount > 16_384 {
			return serviceerror.NewInvalidArgument("History input requires an owning-History client and between 1 and 16384 shards")
		}
	default:
		return serviceerror.NewInvalidArgument("Watch graph requires an explicit Temporal input")
	}
	for _, value := range []string{options.NamespaceID, options.StorageHostID, options.ProcessBootID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return serviceerror.NewInvalidArgument("Watch graph requires canonical nonzero namespace, storage host and process boot UUIDs")
		}
	}
	if err := validation.ValidateRequest(&contractpb.ListSourcesRequest{Namespace: options.Namespace}); err != nil {
		return err
	}
	if !filepath.IsAbs(options.StateRoot) || filepath.Clean(options.StateRoot) != options.StateRoot ||
		options.StateRoot == string(filepath.Separator) || len(options.StateRoot) > 3500 {
		return serviceerror.NewInvalidArgument("Watch state root must be a canonical absolute non-root path of at most 3500 bytes")
	}
	if options.RequestID == "" || len(options.RequestID) > 120 || options.Assignments == nil {
		return serviceerror.NewInvalidArgument("Watch graph requires an assignment client and a request ID of at most 120 bytes")
	}
	return nil
}

func graphDirectory(root, namespaceID, kind string, epoch uint64) string {
	return filepath.Join(root, namespaceID, kind, strconv.FormatUint(epoch, 10))
}

func graphConfiguration(options GraphOptions, epoch uint64) native.Config {
	return native.Config{
		InstanceID: options.NamespaceID, OwnerEpoch: epoch, StartMode: native.StartModeDormant,
		StateDir: graphDirectory(options.StateRoot, options.NamespaceID, "owners", epoch),
		Plugins:  options.Plugins.Preload, Catalogs: options.Plugins.Catalogs,
		ManagedPlugins: options.Plugins.ManagedPlugins,
	}
}

func beginGraphOwnership(ctx context.Context, options GraphOptions) (*assignmentpb.State, error) {
	request := &assignmentpb.BeginRequest{
		NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID,
		StorageHostId: options.StorageHostID, ProcessBootId: options.ProcessBootID, RequestId: options.RequestID,
	}
	state, err := options.Assignments.Describe(ctx, &assignmentpb.DescribeRequest{
		NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID,
	})
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); !ok {
			return nil, err
		}
	} else {
		if state == nil || state.Version == 0 || (state.GetActive() == nil && state.GetSealed() == nil) {
			return nil, serviceerror.NewDataLoss("assignment returned an invalid ownership state")
		}
		if active := state.GetActive(); active != nil && active.RequestId != request.RequestId {
			state, err = recoverGraphOwnership(ctx, options, state)
			if err != nil {
				return nil, err
			}
		}
		request.ExpectedVersion = state.Version
		if active := state.GetActive(); active != nil && active.RequestId == request.RequestId {
			request.ExpectedVersion--
		}
	}
	state, beginErr := options.Assignments.Begin(ctx, request)
	if beginErr != nil {
		observed, describeErr := options.Assignments.Describe(ctx, &assignmentpb.DescribeRequest{
			NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID,
		})
		if describeErr != nil || checkGraphOwnership(observed, request) != nil {
			return nil, beginErr
		}
		state = observed
	}
	if err := checkGraphOwnership(state, request); err != nil {
		return nil, err
	}
	return common.CloneProto(state), nil
}

func checkGraphOwnership(state *assignmentpb.State, request *assignmentpb.BeginRequest) error {
	active := state.GetActive()
	grant := active.GetGrant()
	if state.GetVersion() == 0 || state.GetVersion() != request.ExpectedVersion+1 || grant.GetEpoch() == 0 ||
		grant.GetNamespaceId() != request.NamespaceId || grant.GetInstanceId() != request.NamespaceId ||
		grant.GetStorageHostId() != request.StorageHostId || grant.GetProcessBootId() != request.ProcessBootId ||
		active.GetRequestId() != request.RequestId {
		return serviceerror.NewDataLoss("assignment did not confirm the requested graph ownership")
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	if !bytes.Equal(digest[:], active.Fingerprint) {
		return serviceerror.NewDataLoss("assignment ownership fingerprint does not match Begin")
	}
	return nil
}

// Close retries publication, but never turns a failed physical close into a seal.
func (g *Graph) Close(ctx context.Context) error {
	g.mu.Lock()
	if g.sealed {
		g.mu.Unlock()
		return g.releaseHandle(ctx)
	}
	first := g.closing == nil
	if first {
		g.closing = make(chan struct{})
		if g.cancel != nil {
			g.cancel()
		}
		if g.plugins != nil {
			g.plugins.cancel()
		}
	}
	done := g.closing
	g.mu.Unlock()
	if first {
		request, err := g.checkpoint(ctx)
		g.mu.Lock()
		g.seal, g.closeErr = request, err
		close(done)
		g.mu.Unlock()
	} else {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
	}
	g.mu.Lock()
	request, err := common.CloneProto(g.seal), g.closeErr
	g.mu.Unlock()
	if err != nil {
		return err
	}
	if _, err := readGraphCheckpoint(request.Checkpoint); err != nil {
		return err
	}
	state, sealErr := g.options.Assignments.Seal(ctx, request)
	if sealErr != nil {
		state, err = g.options.Assignments.Describe(ctx, &assignmentpb.DescribeRequest{
			NamespaceId: g.options.NamespaceID, ExecutionId: watchassignment.BusinessID,
		})
		if err != nil || !graphSealMatches(state, request) {
			g.mu.Lock()
			confirmed := g.sealed
			g.mu.Unlock()
			if confirmed {
				return g.releaseHandle(ctx)
			}
			return sealErr
		}
	}
	if !graphSealMatches(state, request) {
		return serviceerror.NewDataLoss("assignment did not confirm the exact graph checkpoint seal")
	}
	g.mu.Lock()
	g.sealed = true
	g.mu.Unlock()
	return g.releaseHandle(ctx)
}

func (g *Graph) releaseHandle(ctx context.Context) error {
	if err := g.runtime.Close(ctx); err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		return errors.Join(err, g.runtime.Close(cleanup))
	}
	return nil
}

func (g *Graph) checkpoint(ctx context.Context) (_ *assignmentpb.SealRequest, failure error) {
	defer func() {
		if failure != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			failure = errors.Join(failure, g.runtime.Close(cleanup))
		}
	}()
	if err := g.persistCloseIntent(); err != nil {
		return nil, err
	}
	if g.plugins != nil {
		if err := g.plugins.close(ctx); err != nil {
			return nil, err
		}
	}
	drained := make(chan struct{})
	go func() {
		g.workers.Wait()
		close(drained)
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-drained:
	}
	result, err := g.runtime.Apply(ctx, native.CheckpointCommand{Destination: g.destination})
	if err != nil {
		return nil, err
	}
	cut, err := graphCheckpointCut(g.destination, result.Checkpoint)
	if err != nil {
		return nil, err
	}
	if cut.InstanceId != g.owner.Grant.InstanceId || cut.OwnerEpoch != g.owner.Grant.Epoch {
		return nil, serviceerror.NewDataLoss("native checkpoint does not match the owned Watch grant")
	}
	return &assignmentpb.SealRequest{
		NamespaceId: g.options.NamespaceID, ExecutionId: watchassignment.BusinessID,
		Grant: common.CloneProto(g.owner.Grant), ExpectedVersion: g.version,
		RequestId: "seal/" + g.options.RequestID, Checkpoint: cut,
	}, nil
}

func graphCheckpointCut(destination string, expected *native.Checkpoint) (*assignmentpb.Cut, error) {
	if expected == nil {
		return nil, serviceerror.NewDataLoss("native close returned no Watch checkpoint")
	}
	manifest, err := readCheckpointManifest(destination)
	if err != nil {
		return nil, fmt.Errorf("read closed Watch checkpoint: %w", err)
	}
	digest := sha256.Sum256(manifest)
	cut := &assignmentpb.Cut{
		Directory: destination, ManifestSha256: hex.EncodeToString(digest[:]),
		InstanceId: expected.InstanceID, OwnerEpoch: expected.OwnerEpoch,
	}
	actual, err := readGraphCheckpoint(cut)
	if err != nil {
		return nil, err
	}
	if actual.Version != expected.Version || actual.Upstream != expected.Upstream ||
		actual.InstanceID != expected.InstanceID || actual.OwnerEpoch != expected.OwnerEpoch ||
		actual.RetirementID != expected.RetirementID || !maps.Equal(actual.Files, expected.Files) {
		return nil, serviceerror.NewDataLoss("published Watch checkpoint differs from the native close result")
	}
	return cut, nil
}

func (g *Graph) persistCloseIntent() error {
	path := graphDirectory(g.options.StateRoot, g.options.NamespaceID, "closing", g.owner.Grant.Epoch)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("reserve Watch graph close: %w", err)
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return fmt.Errorf("persist Watch graph close: %w", err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open Watch graph close directory: %w", err)
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return fmt.Errorf("persist Watch graph close directory: %w", err)
	}
	return nil
}

func readGraphCheckpoint(cut *assignmentpb.Cut) (*native.Checkpoint, error) {
	encoded, err := readCheckpointManifest(cut.Directory)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	if hex.EncodeToString(digest[:]) != cut.ManifestSha256 {
		return nil, serviceerror.NewDataLoss("Watch checkpoint does not match the authoritative manifest digest")
	}
	var manifest native.Checkpoint
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		return nil, serviceerror.NewDataLoss("Watch checkpoint manifest is invalid")
	}
	if manifest.Version != native.ABIVersion || manifest.InstanceID != cut.InstanceId ||
		manifest.OwnerEpoch != cut.OwnerEpoch || manifest.Upstream == "" || len(manifest.Files) == 0 {
		return nil, serviceerror.NewDataLoss("Watch checkpoint identity does not match the authoritative cut")
	}
	if manifest.RetirementID != "" {
		id, err := uuid.Parse(manifest.RetirementID)
		if err != nil || id == uuid.Nil || id.String() != manifest.RetirementID {
			return nil, serviceerror.NewDataLoss("Watch checkpoint retirement identity is invalid")
		}
	}
	return &manifest, nil
}

func readCheckpointManifest(directory string) ([]byte, error) {
	path := filepath.Join(directory, "checkpoint-v1.json")
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("read authoritative Watch checkpoint: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > 16<<20 {
		return nil, serviceerror.NewDataLoss("Watch checkpoint manifest is not a bounded regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open authoritative Watch checkpoint: %w", err)
	}
	encoded, readErr := io.ReadAll(io.LimitReader(file, (16<<20)+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nil, fmt.Errorf("read authoritative Watch checkpoint: %w", err)
	}
	if len(encoded) > 16<<20 {
		return nil, serviceerror.NewDataLoss("Watch checkpoint manifest exceeds the native response limit")
	}
	return encoded, nil
}

func graphSealMatches(state *assignmentpb.State, request *assignmentpb.SealRequest) bool {
	sealed := state.GetSealed()
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return false
	}
	digest := sha256.Sum256(encoded)
	return state.GetVersion() != 0 && state.GetVersion() == request.ExpectedVersion+1 &&
		sealed.GetRequestId() == request.RequestId && bytes.Equal(digest[:], sealed.GetFingerprint()) &&
		proto.Equal(sealed.GetPredecessor().GetGrant(), request.Grant) &&
		proto.Equal(sealed.GetCheckpoint(), request.Checkpoint)
}
