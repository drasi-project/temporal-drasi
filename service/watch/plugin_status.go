package watch

import (
	"context"
	"maps"
	"slices"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pluginGraphSample struct {
	hosted     *hostedNamespace
	graph      *Graph
	controller *runtimePlugins
	closed     bool
	closing    bool
	problem    string
}

func (s *Service) externalSourcesAvailable(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	policy := s.params.PluginPolicy
	if s.params.Credentials == nil || policy == nil || len(policy.ManagedPlugins) == 0 {
		return false, nil
	}
	s.mu.Lock()
	view := s.pluginView
	if !s.pluginIntentFreshLocked(time.Now()) {
		s.mu.Unlock()
		return false, nil
	}
	digests := make(map[string]struct{})
	for _, pair := range policy.ManagedPlugins {
		_, source := view.byDigest[pair.Source.Digest]
		_, bootstrap := view.byDigest[pair.Bootstrap.Digest]
		if source && bootstrap {
			digests[pair.Source.Digest] = struct{}{}
			digests[pair.Bootstrap.Digest] = struct{}{}
		}
	}
	s.mu.Unlock()
	if len(digests) == 0 {
		return false, nil
	}
	observed, err := s.ObservePlugins(ctx, &watchservice.ObservePluginsRequest{
		Generation: view.snapshot.Generation, Digests: slices.Sorted(maps.Keys(digests)),
	})
	if err != nil {
		switch serviceerror.ToStatus(err).Code() {
		case codes.Unavailable, codes.FailedPrecondition:
			return false, nil
		default:
			return false, err
		}
	}
	loaded := make(map[string]bool, len(observed.Observations))
	for _, observation := range observed.Observations {
		status := observation.Status
		loaded[observation.Digest] = status.State == contractpb.PluginHostState_PLUGIN_HOST_STATE_LOADED
	}
	for _, pair := range policy.ManagedPlugins {
		if loaded[pair.Source.Digest] && loaded[pair.Bootstrap.Digest] {
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) ObservePlugins(ctx context.Context, request *watchservice.ObservePluginsRequest) (*watchservice.ObservePluginsResponse, error) {
	if err := validatePluginObservationRequest(request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now()
	s.mu.Lock()
	view, management := s.pluginView, s.plugins
	if !s.pluginIntentFreshLocked(now) || management == nil || view.snapshot.Generation != request.Generation {
		s.mu.Unlock()
		return nil, serviceerror.NewUnavailable("Watch host has no fresh observation of the requested cluster plugin cut")
	}
	for _, digest := range request.Digests {
		if _, ok := view.byDigest[digest]; !ok {
			s.mu.Unlock()
			return nil, serviceerror.NewFailedPrecondition("plugin is not retained in the requested cluster intent cut")
		}
	}
	graphs := make(map[namespace.ID]pluginGraphSample, len(s.namespaces))
	for id, hosted := range s.namespaces {
		graphs[id] = pluginGraphSample{
			hosted: hosted, graph: hosted.graph, closed: hosted.closed, closing: hosted.closed, problem: hosted.problem,
		}
	}
	response := &watchservice.ObservePluginsResponse{
		Generation: request.Generation, StorageHostId: s.config.StorageHostID, ProcessBootId: s.bootID,
	}
	s.mu.Unlock()
	ordered := slices.Sorted(maps.Keys(graphs))
	select {
	case <-management.done:
		return nil, serviceerror.NewUnavailable("Watch plugin management runtime is closing")
	default:
	}
	for _, id := range ordered {
		sample := graphs[id]
		if sample.graph != nil {
			sample.graph.mu.Lock()
			sample.controller = sample.graph.plugins
			grant := sample.graph.owner.GetGrant()
			sample.closing = sample.closing || sample.graph.closing != nil ||
				grant.GetProcessBootId() != response.ProcessBootId || grant.GetStorageHostId() != response.StorageHostId ||
				grant.GetNamespaceId() != id.String() || grant.GetInstanceId() != id.String() || grant.GetEpoch() == 0
			sample.graph.mu.Unlock()
			graphs[id] = sample
		}
	}
	now = time.Now()
	for _, digest := range request.Digests {
		plugin := view.byDigest[digest]
		load := view.snapshot.DesiredLoads[plugin.Id] == digest
		samples := []runtimePluginObservation{management.observation(request.Generation, digest, now)}
		if load {
			for _, id := range ordered {
				samples = append(samples, graphs[id].observation(request.Generation, digest, now))
			}
		}
		response.Observations = append(response.Observations, &watchservice.PluginObservation{
			Digest: digest, Status: reducePluginObservations(response.StorageHostId, load, samples),
		})
	}
	if !s.pluginObservationCurrent(view, graphs) {
		return nil, serviceerror.NewUnavailable("Watch runtime cut changed or expired during plugin observation")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if proto.Size(response) > validation.MaxRequestBytes {
		return nil, serviceerror.NewDataLoss("Watch plugin observation exceeds its byte limit")
	}
	return response, nil
}

func validatePluginObservationRequest(request *watchservice.ObservePluginsRequest) error {
	if request == nil || proto.Size(request) > validation.MaxRequestBytes || len(request.Digests) > validation.MaxPageSize {
		return serviceerror.NewInvalidArgument("plugin observation requires a bounded cluster intent cut")
	}
	seen := make(map[string]struct{}, len(request.Digests))
	for _, digest := range request.Digests {
		if err := validation.ValidateRequest(&contractpb.GetInstallationRequest{Digest: digest}); err != nil {
			return err
		}
		if _, ok := seen[digest]; ok {
			return serviceerror.NewInvalidArgument("plugin observation contains duplicate digests")
		}
		seen[digest] = struct{}{}
	}
	return nil
}

func (s *Service) pluginIntentFreshLocked(now time.Time) bool {
	return s.pluginView != nil && !s.pluginClosing && s.pluginProblem == "" && !s.pluginObservedAt.IsZero() &&
		now.Sub(s.pluginObservedAt) <= 5*time.Second && !s.pluginObservedAt.After(now.Add(2*time.Second))
}

func (s *Service) pluginObservationCurrent(view *pluginIntent, graphs map[namespace.ID]pluginGraphSample) bool {
	s.mu.Lock()
	valid := s.pluginView == view && s.pluginIntentFreshLocked(time.Now()) && len(s.namespaces) == len(graphs)
	for id, sample := range graphs {
		current := s.namespaces[id]
		valid = valid && current == sample.hosted && current.graph == sample.graph &&
			current.closed == sample.closed && current.problem == sample.problem
	}
	s.mu.Unlock()
	if !valid {
		return false
	}
	for _, sample := range graphs {
		if sample.graph != nil && !sample.closing {
			sample.graph.mu.Lock()
			valid = sample.graph.closing == nil && sample.graph.plugins == sample.controller
			sample.graph.mu.Unlock()
			if !valid {
				return false
			}
		}
	}
	return true
}

func (sample pluginGraphSample) observation(generation uint64, digest string, now time.Time) runtimePluginObservation {
	if sample.graph == nil || sample.controller == nil || sample.closing || sample.problem != "" {
		return runtimePluginObservation{problem: "A namespace runtime is starting, closing or unavailable"}
	}
	return sample.controller.observation(generation, digest, now)
}

func (p *runtimePlugins) observation(generation uint64, digest string, now time.Time) runtimePluginObservation {
	p.mu.Lock()
	defer p.mu.Unlock()
	observed, ok := p.observations[digest]
	if p.view == nil || p.view.snapshot.Generation != generation || !ok || observed.generation != generation ||
		observed.observedAt.IsZero() || now.Sub(observed.observedAt) > 2*time.Minute ||
		observed.observedAt.After(now.Add(2*time.Second)) {
		return runtimePluginObservation{problem: "Runtime has no fresh evidence for this plugin intent"}
	}
	return observed
}

func reducePluginObservations(hostID string, load bool, samples []runtimePluginObservation) *contractpb.PluginHostStatus {
	status := &contractpb.PluginHostStatus{HostId: hostID}
	oldest := samples[0].observedAt
	loaded, known := true, true
	priority := 0
	for _, sample := range samples {
		if !sample.observedAt.IsZero() && (oldest.IsZero() || sample.observedAt.Before(oldest)) {
			oldest = sample.observedAt
		}
		loaded = loaded && sample.state == native.PluginLoaded
		known = known && sample.state != ""
		rank := 0
		state := contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED
		switch sample.state {
		case native.PluginRestartRequired:
			rank, state = 3, contractpb.PluginHostState_PLUGIN_HOST_STATE_RESTART_REQUIRED
		case native.PluginIncompatible:
			rank, state = 2, contractpb.PluginHostState_PLUGIN_HOST_STATE_INCOMPATIBLE
		case native.PluginFailed:
			rank, state = 1, contractpb.PluginHostState_PLUGIN_HOST_STATE_FAILED
		default:
			continue
		}
		if rank > priority {
			priority = rank
			status.Message = sample.problem
			status.State = state
		}
	}
	if !oldest.IsZero() {
		status.UpdatedAt = timestamppb.New(oldest)
	}
	if priority != 0 {
		return status
	}
	if !known {
		status.Message = "One or more local runtimes have no fresh plugin observation"
		return status
	}
	if load && loaded {
		status.State = contractpb.PluginHostState_PLUGIN_HOST_STATE_LOADED
	} else {
		switch samples[0].state {
		case native.PluginInstalled, native.PluginLoaded:
			status.State = contractpb.PluginHostState_PLUGIN_HOST_STATE_INSTALLED
		default:
			status.State = contractpb.PluginHostState_PLUGIN_HOST_STATE_AVAILABLE
		}
		if load {
			status.Message = "Plugin loading is pending in one or more local runtimes"
		}
	}
	return status
}
