package watch

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	// MaxPluginHosts bounds complete membership observations and public page reservations.
	MaxPluginHosts = 64
	// MaxPluginHostStatusBytes includes the public membership identity.
	MaxPluginHostStatusBytes = 4 << 10
)

type pluginMember struct {
	id      string
	address string
}

type pluginMemberObservation struct {
	storageHostID string
	processBootID string
	statuses      []*contractpb.PluginHostStatus
	problem       string
}

// ObservePlugins includes draining members and never uses namespace placement.
// Public host IDs identify membership entries; storage and boot UUIDs fence the
// private evidence rather than replacing unreachable members' identities.
func (o *Observer) ObservePlugins(ctx context.Context, generation uint64, digests []string) (map[string][]*contractpb.PluginHostStatus, error) {
	request := &watchservice.ObservePluginsRequest{Generation: generation, Digests: slices.Clone(digests)}
	if err := validatePluginObservationPage(request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(request.Digests) == 0 {
		return nil, nil
	}
	resolver, err := o.membership.GetResolver(primitives.WatchService)
	if err != nil {
		return nil, err
	}
	members, err := pluginMembers(resolver.Members())
	if err != nil {
		return nil, err
	}
	observations := o.observePluginMembers(ctx, request, members)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	current, err := pluginMembers(resolver.Members())
	if err != nil {
		return nil, err
	}
	if !slices.Equal(members, current) {
		members, err = changedPluginMembers(members, current)
		if err != nil {
			return nil, err
		}
		observations = make([]pluginMemberObservation, len(members))
		for i := range observations {
			observations[i].problem = "Watch membership changed during plugin observation"
		}
	}
	fencePluginMemberObservations(observations, time.Now())
	result := make(map[string][]*contractpb.PluginHostStatus, len(request.Digests))
	for i, digest := range request.Digests {
		for j, member := range members {
			observation := observations[j]
			status := &contractpb.PluginHostStatus{HostId: member.id, Message: observation.problem}
			if observation.problem == "" {
				status = observation.statuses[i]
			}
			result[digest] = append(result[digest], status)
		}
	}
	return result, nil
}

func validatePluginObservationPage(request *watchservice.ObservePluginsRequest) error {
	if len(request.Digests) > validation.MaxPageSize || proto.Size(request) > validation.MaxRequestBytes ||
		len(request.Digests) != 0 && request.Generation == 0 {
		return serviceerror.NewInvalidArgument("plugin observation requires a bounded nonzero intent cut")
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

func pluginMembers(hosts []membership.HostInfo) ([]pluginMember, error) {
	if len(hosts) > MaxPluginHosts {
		return nil, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "Watch membership exceeds the plugin observation host limit")
	}
	members := make([]pluginMember, 0, len(hosts))
	identities, addresses := make(map[string]struct{}, len(hosts)), make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		if host == nil {
			return nil, serviceerror.NewDataLoss("Watch membership contains a missing host")
		}
		member := pluginMember{id: host.Identity(), address: host.GetAddress()}
		for _, value := range []string{member.id, member.address} {
			if value == "" || len(value) > 512 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
				return nil, serviceerror.NewDataLoss("Watch membership contains an invalid host identity or address")
			}
		}
		if _, ok := identities[member.id]; ok {
			return nil, serviceerror.NewDataLoss("Watch membership contains duplicate host identities")
		}
		if _, ok := addresses[member.address]; ok {
			return nil, serviceerror.NewDataLoss("Watch membership contains duplicate host addresses")
		}
		identities[member.id], addresses[member.address] = struct{}{}, struct{}{}
		members = append(members, member)
	}
	slices.SortFunc(members, func(a, b pluginMember) int { return strings.Compare(a.id, b.id) })
	return members, nil
}

func (o *Observer) observePluginMembers(ctx context.Context, request *watchservice.ObservePluginsRequest, members []pluginMember) []pluginMemberObservation {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	observations := make([]pluginMemberObservation, len(members))
	jobs := make(chan int, len(members))
	for i := range members {
		observations[i].problem = "Watch member plugin observation is unavailable"
		jobs <- i
	}
	close(jobs)
	var workers sync.WaitGroup
	for range min(8, len(members)) {
		workers.Go(func() {
			for index := range jobs {
				if ctx.Err() != nil {
					return
				}
				observations[index] = o.observePluginMember(ctx, request, members[index])
			}
		})
	}
	workers.Wait()
	return observations
}

func (o *Observer) observePluginMember(ctx context.Context, request *watchservice.ObservePluginsRequest, member pluginMember) pluginMemberObservation {
	select {
	case o.pluginSlots <- struct{}{}:
		defer func() { <-o.pluginSlots }()
	case <-ctx.Done():
		return pluginMemberObservation{problem: "Watch member plugin observation is unavailable"}
	}
	client := watchservice.NewWatchServiceClient(o.rpc.CreateWatchGRPCConnection(member.address))
	response, err := client.ObservePlugins(ctx, request,
		grpc.MaxCallRecvMsgSize(validation.MaxRequestBytes), grpc.MaxCallSendMsgSize(validation.MaxRequestBytes))
	if err != nil {
		return pluginMemberObservation{problem: "Watch member plugin observation is unavailable"}
	}
	statuses, err := pluginObservationStatuses(request, response, member.id, time.Now())
	if err != nil {
		return pluginMemberObservation{problem: "Watch member returned invalid plugin observation evidence"}
	}
	return pluginMemberObservation{
		storageHostID: response.StorageHostId, processBootID: response.ProcessBootId, statuses: statuses,
	}
}

func pluginObservationStatuses(request *watchservice.ObservePluginsRequest, response *watchservice.ObservePluginsResponse, memberID string, now time.Time) ([]*contractpb.PluginHostStatus, error) {
	if response == nil || response.Generation != request.Generation || len(response.Observations) != len(request.Digests) ||
		proto.Size(response) > validation.MaxRequestBytes {
		return nil, serviceerror.NewDataLoss("Watch member returned an invalid plugin observation envelope")
	}
	for _, value := range []string{response.StorageHostId, response.ProcessBootId} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return nil, serviceerror.NewDataLoss("Watch member returned an invalid plugin runtime identity")
		}
	}
	statuses := make([]*contractpb.PluginHostStatus, len(request.Digests))
	for i, entry := range response.Observations {
		if entry.GetDigest() != request.Digests[i] || entry.GetStatus() == nil || entry.Status.HostId != response.StorageHostId {
			return nil, serviceerror.NewDataLoss("Watch member plugin observation does not match the requested artifact and host")
		}
		status := common.CloneProto(entry.Status)
		status.HostId = memberID
		if err := validatePluginHostStatus(status, now); err != nil {
			return nil, err
		}
		statuses[i] = status
	}
	return statuses, nil
}

func validatePluginHostStatus(status *contractpb.PluginHostStatus, now time.Time) error {
	_, known := contractpb.PluginHostState_name[int32(status.State)]
	if !known || proto.Size(status) > MaxPluginHostStatusBytes ||
		!utf8.ValidString(status.Message) || !utf8.ValidString(status.VerifiedSignerIdentity) {
		return serviceerror.NewDataLoss("Watch member returned invalid plugin status")
	}
	switch status.State {
	case contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED:
		if status.VerifiedSignerIdentity != "" || status.Message == "" {
			return serviceerror.NewDataLoss("Watch member returned unsupported plugin evidence")
		}
	case contractpb.PluginHostState_PLUGIN_HOST_STATE_VERIFIED, contractpb.PluginHostState_PLUGIN_HOST_STATE_INSTALLED,
		contractpb.PluginHostState_PLUGIN_HOST_STATE_LOADED:
		if status.VerifiedSignerIdentity == "" {
			return serviceerror.NewDataLoss("Watch member plugin success has no verified signer")
		}
	default:
	}
	if status.State == contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED && status.UpdatedAt == nil {
		return nil
	}
	if status.UpdatedAt.CheckValid() != nil || now.Sub(status.UpdatedAt.AsTime()) > 2*time.Minute ||
		status.UpdatedAt.AsTime().After(now.Add(2*time.Second)) {
		return serviceerror.NewDataLoss("Watch member returned stale or invalid plugin evidence")
	}
	return nil
}

func changedPluginMembers(previous, current []pluginMember) ([]pluginMember, error) {
	union := make(map[string]pluginMember, len(previous)+len(current))
	for _, member := range previous {
		union[member.id] = member
	}
	for _, member := range current {
		union[member.id] = member
	}
	if len(union) > MaxPluginHosts {
		return nil, serviceerror.NewUnavailable("Changed Watch membership exceeds the plugin observation host limit")
	}
	return slices.SortedFunc(maps.Values(union), func(a, b pluginMember) int { return strings.Compare(a.id, b.id) }), nil
}

func fencePluginMemberObservations(observations []pluginMemberObservation, now time.Time) {
	volumes, boots := make(map[string]int), make(map[string]int)
	for i, observation := range observations {
		for _, status := range observation.statuses {
			if err := validatePluginHostStatus(status, now); err != nil {
				observations[i].problem = "Watch member plugin evidence expired before observation completed"
				break
			}
		}
		if observation.storageHostID == "" || observation.processBootID == "" {
			continue
		}
		for j, identity := range []string{observation.storageHostID, observation.processBootID} {
			seen := volumes
			if j == 1 {
				seen = boots
			}
			if previous, ok := seen[identity]; ok {
				observations[previous].problem = "Watch members reported conflicting runtime identities"
				observations[i].problem = "Watch members reported conflicting runtime identities"
			} else {
				seen[identity] = i
			}
		}
	}
}
