package watch

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/watchservice/v1"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/chasm/lib/watchregistry"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/watch/native"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const ServiceName = "temporal.server.watch"

type ServiceParams struct {
	fx.In

	Config         *config.Config
	Namespaces     namespace.Registry
	Membership     membership.Monitor
	Resolver       membership.ServiceResolver
	Host           membership.HostInfoProvider
	Logger         log.SnTaggedLogger
	Metrics        metrics.Handler
	Dynamic        *dynamicconfig.Collection
	Server         *grpc.Server
	Listener       net.Listener
	Health         *health.Server
	Assignments    assignmentpb.AssignmentServiceClient
	Registry       registrypb.RegistryServiceClient
	ClusterPlugins registrypb.ClusterPluginServiceClient
	Control        controlpb.ControlServiceClient
	Workflows      workflowservice.WorkflowServiceClient
	History        resource.HistoryClient
	PluginPolicy   *PluginPolicy    `optional:"true"`
	Credentials    CredentialReader `optional:"true"`
}

type Service struct {
	watchservice.UnimplementedWatchServiceServer
	params  ServiceParams
	config  config.Watch
	bootID  string
	enabled dynamicconfig.BoolPropertyFnWithNamespaceFilter

	mu               sync.Mutex
	cancel           context.CancelFunc
	done             chan struct{}
	serveDone        chan struct{}
	serveErr         error
	namespaces       map[namespace.ID]*hostedNamespace
	plugins          *runtimePlugins
	pluginView       *pluginIntent
	pluginObservedAt time.Time
	pluginProblem    string
	pluginDone       chan struct{}
	pluginCloseErr   error
	pluginClosing    bool
}

type hostedNamespace struct {
	options  GraphOptions
	cancel   context.CancelFunc
	done     chan struct{}
	graph    *Graph
	observed GraphObservation
	problem  string
	closeErr error
	closed   bool
}

func NewService(params ServiceParams) (*Service, error) {
	if !native.Enabled() {
		return nil, native.ErrDisabled
	}
	if params.History == nil || params.ClusterPlugins == nil || params.Config == nil ||
		params.Config.Persistence.NumHistoryShards < 1 || params.Config.Persistence.NumHistoryShards > 16_384 {
		return nil, serviceerror.NewInvalidArgument("Watch service requires owning-History input, cluster plugin intent and between 1 and 16384 History shards")
	}
	serviceConfig, err := validateServiceConfig(params.Config.Services[string(primitives.WatchService)].Drasi)
	if err != nil {
		return nil, err
	}
	if params.PluginPolicy != nil {
		policy := clonePluginPolicy(*params.PluginPolicy)
		params.PluginPolicy = &policy
	}
	healthpb.RegisterHealthServer(params.Server, params.Health)
	params.Health.SetServingStatus(ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	service := &Service{
		params: params, config: serviceConfig, bootID: uuid.NewString(),
		enabled:    dynamicconfig.EnableDrasiWatches.Get(params.Dynamic),
		namespaces: make(map[namespace.ID]*hostedNamespace),
	}
	watchservice.RegisterWatchServiceServer(params.Server, service)
	return service, nil
}

func validateServiceConfig(value config.Watch) (config.Watch, error) {
	id, err := uuid.Parse(value.StorageHostID)
	if err != nil || id == uuid.Nil || id.String() != value.StorageHostID {
		return config.Watch{}, serviceerror.NewInvalidArgument("Watch service requires a canonical nonzero storageHostID UUID")
	}
	if !filepath.IsAbs(value.StateRoot) || filepath.Clean(value.StateRoot) != value.StateRoot ||
		value.StateRoot == string(filepath.Separator) || len(value.StateRoot) > 3500 {
		return config.Watch{}, serviceerror.NewInvalidArgument("Watch stateRoot must be a canonical absolute non-root path of at most 3500 bytes")
	}
	root, err := filepath.EvalSymlinks(value.StateRoot)
	if err != nil {
		return config.Watch{}, fmt.Errorf("resolve Watch stateRoot: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return config.Watch{}, fmt.Errorf("read Watch stateRoot: %w", err)
	}
	if !info.IsDir() || root == string(filepath.Separator) {
		return config.Watch{}, serviceerror.NewInvalidArgument("Watch stateRoot must resolve to an existing non-root directory")
	}
	value.StateRoot = root
	value.MaxNamespaces = cmp.Or(value.MaxNamespaces, 16)
	if value.MaxNamespaces < 1 || value.MaxNamespaces > 256 {
		return config.Watch{}, serviceerror.NewInvalidArgument("Watch maxNamespaces must be between 1 and 256")
	}
	return value, nil
}

func (s *Service) Start(ctx context.Context) error {
	s.params.Membership.Start()
	if err := s.params.Membership.WaitUntilInitialized(ctx); err != nil {
		return err
	}
	var policy PluginPolicy
	if s.params.PluginPolicy != nil {
		policy = *s.params.PluginPolicy
	}
	management, err := native.Open(ctx, native.Config{
		InstanceID: watchregistry.ClusterPluginBusinessID, OwnerEpoch: 1, StartMode: native.StartModeDormant,
		StateDir: filepath.Join(s.config.StateRoot, "cluster-plugins"),
		Plugins:  policy.Preload, Catalogs: policy.Catalogs, Trust: policy.Trust, ManagedPlugins: policy.ManagedPlugins,
	})
	if err != nil {
		return fmt.Errorf("open Watch plugin management runtime: %w", err)
	}
	runContext, cancel := context.WithCancel(headers.SetCallerInfo(context.Background(), headers.SystemBackgroundHighCallerInfo))
	s.mu.Lock()
	s.cancel, s.done, s.serveDone = cancel, make(chan struct{}), make(chan struct{})
	s.plugins = newRuntimePlugins(management, watchregistry.ClusterPluginBusinessID, true, s.params.Logger)
	s.pluginDone = make(chan struct{})
	s.mu.Unlock()
	metrics.RestartCount.With(s.params.Metrics).Record(1)
	s.params.Health.SetServingStatus(ServiceName, healthpb.HealthCheckResponse_SERVING)
	go func() {
		defer close(s.serveDone)
		if err := s.params.Server.Serve(s.params.Listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.mu.Lock()
			s.serveErr = err
			s.mu.Unlock()
			s.params.Health.SetServingStatus(ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
			s.params.Logger.Error("Watch listener failed", tag.Error(err))
			cancel()
		}
	}()
	go s.run(runContext)
	go s.runPlugins(runContext)
	s.params.Logger.Info("Watch service started", tag.Address(s.params.Host.HostInfo().GetAddress()))
	return nil
}

func (s *Service) Stop(ctx context.Context) error {
	s.params.Health.SetServingStatus(ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	drainErr := s.params.Membership.SetDraining(true)
	if drainErr != nil {
		s.params.Logger.Error("Watch membership draining failed", tag.Error(drainErr))
	}
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.pluginClosing = true
	s.mu.Unlock()
	if cancel != nil {
		cancel()
		select {
		case <-done:
		case <-ctx.Done():
			s.params.Server.Stop()
			return errors.Join(drainErr, ctx.Err())
		}
	}
	stopped := make(chan struct{})
	go func() {
		s.params.Server.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		s.params.Server.Stop()
		<-stopped
		drainErr = errors.Join(drainErr, ctx.Err())
	}
	if s.serveDone != nil {
		<-s.serveDone
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := errors.Join(drainErr, s.serveErr, s.pluginCloseErr)
	for id, hosted := range s.namespaces {
		if hosted.closeErr != nil {
			result = errors.Join(result, fmt.Errorf("close Watch namespace %s: %w", id, hosted.closeErr))
		}
	}
	return result
}

func (s *Service) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		s.discover(ctx)
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	s.mu.Lock()
	hosts := make([]*hostedNamespace, 0, len(s.namespaces))
	for _, hosted := range s.namespaces {
		hosts = append(hosts, hosted)
	}
	s.mu.Unlock()
	for _, hosted := range hosts {
		<-hosted.done
	}
	<-s.pluginDone
}

func (s *Service) discover(ctx context.Context) {
	wanted := make(map[namespace.ID]*namespace.Namespace)
	for _, ns := range s.params.Namespaces.GetAllNamespaces() {
		if !s.enabled(ns.Name().String()) || ns.IsGlobalNamespace() ||
			ns.State() != enumspb.NAMESPACE_STATE_REGISTERED {
			continue
		}
		host, err := s.params.Resolver.Lookup(ns.ID().String())
		if err != nil {
			s.params.Logger.Error("Watch namespace placement failed", tag.WorkflowNamespaceID(ns.ID().String()), tag.Error(err))
			s.mu.Lock()
			hosted, retained := s.namespaces[ns.ID()]
			retained = retained && !hosted.closed
			s.mu.Unlock()
			if retained {
				wanted[ns.ID()] = ns
			}
			continue
		}
		if host.GetAddress() == s.params.Host.HostInfo().GetAddress() {
			wanted[ns.ID()] = ns
		}
	}
	s.mu.Lock()
	for id, hosted := range s.namespaces {
		if _, ok := wanted[id]; !ok {
			hosted.cancel()
		}
		if hosted.closed && hosted.closeErr == nil {
			delete(s.namespaces, id)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(wanted)) {
		ns := wanted[id]
		if _, ok := s.namespaces[id]; ok {
			continue
		}
		if len(s.namespaces) >= s.config.MaxNamespaces {
			s.mu.Unlock()
			s.params.Logger.Error("Watch host namespace capacity exceeded", tag.WorkflowNamespaceID(id.String()))
			s.mu.Lock()
			continue
		}
		runContext, cancel := context.WithCancel(ctx)
		hosted := &hostedNamespace{
			options: GraphOptions{
				TemporalInput: HistoryTemporalInput{Client: s.params.History, ShardCount: s.params.Config.Persistence.NumHistoryShards},
				NamespaceID:   id.String(), Namespace: ns.Name().String(), StateRoot: s.config.StateRoot,
				StorageHostID: s.config.StorageHostID, ProcessBootID: s.bootID, RequestID: uuid.NewString(),
				Assignments: s.params.Assignments, Registry: s.params.Registry,
				Control: s.params.Control, Workflows: s.params.Workflows,
				Credentials: s.params.Credentials,
			},
			cancel: cancel, done: make(chan struct{}),
		}
		if s.params.PluginPolicy != nil {
			hosted.options.Plugins = clonePluginPolicy(*s.params.PluginPolicy)
		}
		s.namespaces[id] = hosted
		go s.runNamespace(runContext, hosted)
	}
	s.mu.Unlock()
}

func (s *Service) runNamespace(ctx context.Context, hosted *hostedNamespace) {
	defer close(hosted.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var graph *Graph
	attempted := false
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		var err error
		var observation GraphObservation
		if graph == nil {
			attempted = true
			graph, err = OpenGraph(operation, hosted.options)
		}
		cancel()
		if err == nil && ctx.Err() == nil {
			operation, cancel = context.WithTimeout(ctx, 30*time.Second)
			observation, err = s.reconcileNamespace(operation, hosted, graph)
			cancel()
		}
		s.mu.Lock()
		hosted.graph = graph
		previous := hosted.problem
		hosted.problem = ""
		if err != nil {
			hosted.problem = err.Error()
		}
		changed := previous != hosted.problem
		problemsChanged := false
		if err == nil && ctx.Err() == nil {
			problemsChanged = !maps.Equal(hosted.observed.Problems, observation.Problems)
			hosted.observed = observation
		}
		s.mu.Unlock()
		if err != nil && changed {
			s.params.Logger.Error("Watch namespace reconciliation failed", tag.WorkflowNamespaceID(hosted.options.NamespaceID), tag.Error(err))
		}
		if problemsChanged && len(observation.Problems) != 0 {
			s.params.Logger.Warn("Watch graph resources are blocked", tag.WorkflowNamespaceID(hosted.options.NamespaceID),
				tag.NewAnyTag("problems", observation.Problems))
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	var err error
	if attempted {
		err = s.closeNamespace(shutdown, hosted, graph)
	}
	s.mu.Lock()
	hosted.closeErr, hosted.closed = err, true
	s.mu.Unlock()
	if err != nil {
		s.params.Logger.Error("Watch namespace close remains blocked", tag.WorkflowNamespaceID(hosted.options.NamespaceID), tag.Error(err))
	}
}

func (s *Service) reconcileNamespace(ctx context.Context, hosted *hostedNamespace, graph *Graph) (GraphObservation, error) {
	s.mu.Lock()
	hosted.graph = graph
	plugins := s.pluginView
	s.mu.Unlock()
	if plugins != nil {
		if err := graph.updatePlugins(plugins, s.params.Logger); err != nil {
			return GraphObservation{}, err
		}
	}
	view, err := ReadDesiredView(ctx, hosted.options.Registry, hosted.options.NamespaceID, hosted.options.Namespace)
	if err != nil {
		return GraphObservation{}, err
	}
	return graph.Reconcile(ctx, view)
}

func (s *Service) closeNamespace(ctx context.Context, hosted *hostedNamespace, graph *Graph) error {
	if graph == nil {
		state, err := hosted.options.Assignments.Describe(ctx, &assignmentpb.DescribeRequest{
			NamespaceId: hosted.options.NamespaceID, ExecutionId: watchassignment.BusinessID,
		})
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			return nil
		}
		if err != nil {
			return err
		}
		if state.GetVersion() == 0 || (state.GetActive() == nil && state.GetSealed() == nil) {
			return serviceerror.NewDataLoss("assignment returned an invalid ownership state during namespace close")
		}
		owner := state.GetActive()
		if owner.GetRequestId() != hosted.options.RequestID ||
			owner.GetGrant().GetProcessBootId() != hosted.options.ProcessBootID ||
			owner.GetGrant().GetStorageHostId() != hosted.options.StorageHostID {
			return nil
		}
		graph, err = OpenGraph(ctx, hosted.options)
		if err != nil {
			return err
		}
		s.mu.Lock()
		hosted.graph = graph
		s.mu.Unlock()
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		err := graph.Close(ctx)
		if err == nil {
			return nil
		}
		graph.mu.Lock()
		physicalFailure := graph.closeErr != nil
		graph.mu.Unlock()
		if physicalFailure || ctx.Err() != nil {
			return err
		}
		if _, ok := errors.AsType[*serviceerror.Unavailable](err); !ok {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-ticker.C:
		}
	}
}
