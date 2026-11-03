// Package validation validates public Watch and Plugin API requests at transport boundaries.
package validation

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// MaxRequestBytes bounds decoded protobuf requests before dispatch.
	MaxRequestBytes = 1 << 20
	// MaxPageSize bounds public resource and delivery listings.
	MaxPageSize = 200
	// MaxPreviewRows bounds previews independently of any lower runtime limit.
	MaxPreviewRows = 200
)

var (
	resourceID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	digestID   = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

// MethodPolicy describes required backend authorization and write-disable enforcement.
// Serving applications must enforce it; validation is not authorization.
type MethodPolicy struct {
	Access   watchpb.Access
	Mutation bool
}

// Namespace extracts the namespace scope from a public request.
func Namespace(request any) string {
	if scoped, ok := request.(interface{ GetNamespace() string }); ok {
		return scoped.GetNamespace()
	}
	return ""
}

// UnaryServerInterceptor rejects malformed requests for this API before dispatch.
// It leaves unrelated services untouched so it can join a shared server chain.
func UnaryServerInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if _, ok := Policy(info.FullMethod); ok {
		message, ok := request.(proto.Message)
		if !ok {
			return nil, invalid("request", "must be a protobuf message")
		}
		if err := ValidateRequest(message); err != nil {
			return nil, err
		}
	}
	return handler(ctx, request)
}

// ValidateRequest applies structural, size, and union validation without I/O.
// Resource existence, revisions, deadlines relative to server time, native schemas,
// language support, trust, permissions, and lower deployment limits remain server checks.
func ValidateRequest(request proto.Message) error {
	if request == nil || !request.ProtoReflect().IsValid() {
		return invalid("request", "is required")
	}
	if proto.Size(request) > MaxRequestBytes {
		return invalid("request", "exceeds the maximum encoded size")
	}
	if scoped, ok := request.(interface{ GetNamespace() string }); ok {
		if err := text("namespace", scoped.GetNamespace(), 255); err != nil {
			return err
		}
		if strings.Contains(scoped.GetNamespace(), "/") {
			return invalid("namespace", "must not contain a slash")
		}
	}
	if identified, ok := request.(interface{ GetId() string }); ok {
		if err := id("id", identified.GetId()); err != nil {
			return err
		}
	}
	if mutation, ok := request.(interface{ GetRequestId() string }); ok {
		if err := text("request_id", mutation.GetRequestId(), 128); err != nil {
			return err
		}
	}
	if revisioned, ok := request.(interface{ GetExpectedRevision() uint64 }); ok && revisioned.GetExpectedRevision() == 0 {
		return invalid("expected_revision", "must be nonzero")
	}
	if paginated, ok := request.(interface{ GetPageSize() int32 }); ok {
		if paginated.GetPageSize() < 0 || paginated.GetPageSize() > MaxPageSize {
			return invalid("page_size", "must be between 0 and 200")
		}
	}
	if paginated, ok := request.(interface{ GetPageToken() string }); ok && len(paginated.GetPageToken()) > 4096 {
		return invalid("page_token", "exceeds the maximum length")
	}
	switch request := request.(type) {
	case *watchpb.CreateSourceRequest:
		return source(request.Spec)
	case *watchpb.UpdateSourceRequest:
		return source(request.Spec)
	case *watchpb.CreateQueryRequest:
		return query(request.Spec)
	case *watchpb.UpdateQueryRequest:
		return query(request.Spec)
	case *watchpb.ValidateQueryRequest:
		return query(request.Spec)
	case *watchpb.PreviewQueryRequest:
		if request.Limit == 0 || request.Limit > MaxPreviewRows {
			return invalid("limit", "must be between 1 and 200")
		}
		return query(request.Spec)
	case *watchpb.CreateWatchRequest:
		return watch(request.Spec)
	case *watchpb.UpdateWatchRequest:
		return watch(request.Spec)
	case *watchpb.ResetWatchRequest:
		return initialResults(request.InitialResults)
	case *watchpb.ListWatchesRequest:
		if request.Owner != nil {
			return execution("owner", request.Owner)
		}
	case *watchpb.RetryDeliveryRequest:
		return text("action_id", request.ActionId, 512)
	case *watchpb.GetPluginRequest:
		return plugin(request.CatalogId, request.PluginId, request.Version)
	case *watchpb.InstallPluginRequest:
		if err := plugin(request.CatalogId, request.PluginId, request.Version); err != nil {
			return err
		}
		return digest("digest", request.Digest)
	case *watchpb.GetInstallationRequest:
		return digest("digest", request.Digest)
	case *watchpb.LoadPluginRequest:
		return digest("digest", request.Digest)
	case *watchpb.ListPluginsRequest:
		if request.CatalogId != "" {
			if err := id("catalog_id", request.CatalogId); err != nil {
				return err
			}
		}
		if len(request.Search) > 256 {
			return invalid("search", "exceeds the maximum length")
		}
	case *watchpb.GetCapabilitiesRequest, *watchpb.GetSourceRequest, *watchpb.ListSourcesRequest,
		*watchpb.SourceOperationRequest, *watchpb.GetQueryRequest, *watchpb.ListQueriesRequest,
		*watchpb.QueryOperationRequest, *watchpb.GetWatchRequest, *watchpb.WatchOperationRequest,
		*watchpb.ListDeliveriesRequest, *watchpb.GetPluginCapabilitiesRequest,
		*watchpb.ListCatalogsRequest, *watchpb.ListInstallationsRequest:
	default:
		return invalid("request", "is not a public Watch or Plugin API request")
	}
	return nil
}

func source(spec *watchpb.SourceSpec) error {
	if spec == nil {
		return invalid("spec", "is required")
	}
	switch kind := spec.Kind.(type) {
	case *watchpb.SourceSpec_Temporal:
		if kind == nil || kind.Temporal == nil {
			return invalid("spec.temporal", "is required")
		}
		if spec.Bootstrap != nil {
			return invalid("spec.bootstrap", "is only supported for external sources")
		}
	case *watchpb.SourceSpec_External:
		if kind == nil {
			return invalid("spec.external", "is required")
		}
		if err := component("spec.external", kind.External); err != nil {
			return err
		}
		if spec.Bootstrap != nil {
			return component("spec.bootstrap", spec.Bootstrap)
		}
	default:
		return invalid("spec.kind", "must select temporal or external")
	}
	return nil
}

func component(field string, configuration *watchpb.ComponentConfiguration) error {
	if configuration == nil || configuration.Plugin == nil {
		return invalid(field+".plugin", "is required")
	}
	if err := digest(field+".plugin.digest", configuration.Plugin.Digest); err != nil {
		return err
	}
	if err := id(field+".plugin.component", configuration.Plugin.Component); err != nil {
		return err
	}
	if len(configuration.Secrets) > 64 {
		return invalid(field+".secrets", "exceeds the maximum number of references")
	}
	for name, secret := range configuration.Secrets {
		if err := text(field+".secrets field", name, 256); err != nil {
			return err
		}
		if secret == nil {
			return invalid(field+".secrets", "must contain references, not null values")
		}
		if err := id(field+".secrets.provider", secret.Provider); err != nil {
			return err
		}
		if err := text(field+".secrets.key", secret.Key, 1024); err != nil {
			return err
		}
		if secret.Version != "" {
			if err := text(field+".secrets.version", secret.Version, 1024); err != nil {
				return err
			}
		}
	}
	return nil
}

func query(spec *watchpb.QuerySpec) error {
	if spec == nil {
		return invalid("spec", "is required")
	}
	if err := text("spec.language", spec.Language, 64); err != nil {
		return err
	}
	if strings.TrimSpace(spec.Text) == "" || len(spec.Text) > 65536 {
		return invalid("spec.text", "must contain a query of at most 65536 bytes")
	}
	if err := id("spec.state_store", spec.StateStore); err != nil {
		return err
	}
	if len(spec.Sources) == 0 || len(spec.Sources) > 64 {
		return invalid("spec.sources", "must contain between 1 and 64 pinned sources")
	}
	seen := make(map[string]bool, len(spec.Sources))
	for _, source := range spec.Sources {
		if err := reference("spec.sources", source); err != nil {
			return err
		}
		if seen[source.Id] {
			return invalid("spec.sources", "must not contain duplicate source IDs")
		}
		seen[source.Id] = true
	}
	return nil
}

func watch(spec *watchpb.WatchSpec) error {
	if spec == nil {
		return invalid("spec", "is required")
	}
	if err := reference("spec.query", spec.Query); err != nil {
		return err
	}
	switch spec.Trigger {
	case watchpb.Trigger_TRIGGER_ENTER, watchpb.Trigger_TRIGGER_UPDATE, watchpb.Trigger_TRIGGER_LEAVE:
	default:
		return invalid("spec.trigger", "must select ENTER, UPDATE, or LEAVE")
	}
	if err := initialResults(spec.InitialResults); err != nil {
		return err
	}
	if err := action(spec.Action); err != nil {
		return err
	}
	switch lifetime := spec.Lifetime.(type) {
	case *watchpb.WatchSpec_Persistent:
		if lifetime == nil || lifetime.Persistent == nil {
			return invalid("spec.persistent", "is required")
		}
	case *watchpb.WatchSpec_Once:
		if lifetime == nil || lifetime.Once == nil {
			return invalid("spec.once", "is required")
		}
		once := lifetime.Once
		if err := execution("spec.once.owner", once.Owner); err != nil {
			return err
		}
		if once.Deadline == nil || once.Deadline.CheckValid() != nil {
			return invalid("spec.once.deadline", "must be a valid absolute timestamp")
		}
		if spec.Trigger != watchpb.Trigger_TRIGGER_ENTER {
			return invalid("spec.trigger", "must be ENTER for a one-shot condition")
		}
		signal := spec.Action.GetSignal()
		if signal == nil || !proto.Equal(signal.GetRun(), once.Owner) {
			return invalid("spec.action.signal.run", "must target the exact one-shot owner run")
		}
	default:
		return invalid("spec.lifetime", "must select persistent or once")
	}
	return nil
}

func action(action *watchpb.WorkflowAction) error {
	if action == nil {
		return invalid("spec.action", "is required")
	}
	switch action.ResultPayload {
	case watchpb.ResultPayload_RESULT_PAYLOAD_APPEND, watchpb.ResultPayload_RESULT_PAYLOAD_OMIT:
	default:
		return invalid("spec.action.result_payload", "is not supported")
	}
	switch operation := action.Operation.(type) {
	case *watchpb.WorkflowAction_Start:
		if operation == nil {
			return invalid("spec.action.start", "is required")
		}
		return start(operation.Start)
	case *watchpb.WorkflowAction_Signal:
		if operation == nil {
			return invalid("spec.action.signal", "is required")
		}
		return signal(operation.Signal)
	case *watchpb.WorkflowAction_SignalWithStart:
		if operation == nil || operation.SignalWithStart == nil {
			return invalid("spec.action.signal_with_start", "is required")
		}
		if err := start(operation.SignalWithStart.Start); err != nil {
			return err
		}
		return text("spec.action.signal_with_start.signal_name", operation.SignalWithStart.SignalName, 255)
	default:
		return invalid("spec.action.operation", "must select start, signal, or signal_with_start")
	}
}

func start(start *watchpb.WorkflowStart) error {
	if start == nil {
		return invalid("spec.action.start", "is required")
	}
	if err := text("spec.action.start.workflow_id", start.WorkflowId, 1000); err != nil {
		return err
	}
	remaining := strings.ReplaceAll(start.WorkflowId, "{action_id}", "")
	if strings.ContainsAny(remaining, "{}") {
		return invalid("spec.action.start.workflow_id", "contains an unsupported template token")
	}
	if err := text("spec.action.start.workflow_type", start.WorkflowType, 255); err != nil {
		return err
	}
	if err := text("spec.action.start.task_queue", start.TaskQueue, 255); err != nil {
		return err
	}
	for _, timeout := range []struct {
		name  string
		value *durationpb.Duration
	}{
		{"execution_timeout", start.ExecutionTimeout},
		{"run_timeout", start.RunTimeout},
		{"task_timeout", start.TaskTimeout},
	} {
		if timeout.value != nil && (timeout.value.CheckValid() != nil || timeout.value.Seconds < 0 ||
			(timeout.value.Seconds == 0 && timeout.value.Nanos <= 0)) {
			return invalid("spec.action.start."+timeout.name, "must be a positive duration")
		}
	}
	if _, ok := enumspb.WorkflowIdReusePolicy_name[int32(start.ReusePolicy)]; !ok {
		return invalid("spec.action.start.reuse_policy", "is not supported")
	}
	if _, ok := enumspb.WorkflowIdConflictPolicy_name[int32(start.ConflictPolicy)]; !ok {
		return invalid("spec.action.start.conflict_policy", "is not supported")
	}
	// Terminating a running workflow would make repeated action delivery destructive.
	if start.ReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING ||
		start.ConflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING {
		return invalid("spec.action.start", "must not terminate an existing workflow")
	}
	return nil
}

func signal(signal *watchpb.WorkflowSignal) error {
	if signal == nil {
		return invalid("spec.action.signal", "is required")
	}
	if err := text("spec.action.signal.signal_name", signal.SignalName, 255); err != nil {
		return err
	}
	switch target := signal.Target.(type) {
	case *watchpb.WorkflowSignal_Run:
		if target == nil {
			return invalid("spec.action.signal.run", "is required")
		}
		return execution("spec.action.signal.run", target.Run)
	case *watchpb.WorkflowSignal_CurrentWorkflowId:
		if target == nil {
			return invalid("spec.action.signal.current_workflow_id", "is required")
		}
		return text("spec.action.signal.current_workflow_id", target.CurrentWorkflowId, 1000)
	default:
		return invalid("spec.action.signal.target", "must explicitly select a run or current workflow ID")
	}
}

func execution(field string, value *commonpb.WorkflowExecution) error {
	if value == nil {
		return invalid(field, "is required")
	}
	if err := text(field+".workflow_id", value.WorkflowId, 1000); err != nil {
		return err
	}
	return text(field+".run_id", value.RunId, 64)
}

func reference(field string, value *watchpb.ResourceRef) error {
	if value == nil {
		return invalid(field, "is required")
	}
	if err := id(field+".id", value.Id); err != nil {
		return err
	}
	if value.Revision == 0 {
		return invalid(field+".revision", "must be nonzero")
	}
	return nil
}

func initialResults(value watchpb.InitialResults) error {
	switch value {
	case watchpb.InitialResults_INITIAL_RESULTS_IGNORE, watchpb.InitialResults_INITIAL_RESULTS_INCLUDE:
		return nil
	default:
		return invalid("initial_results", "must select IGNORE or INCLUDE")
	}
}

func plugin(catalog, plugin, version string) error {
	if err := id("catalog_id", catalog); err != nil {
		return err
	}
	if err := id("plugin_id", plugin); err != nil {
		return err
	}
	if err := text("version", version, 128); err != nil {
		return err
	}
	if strings.ContainsAny(version, `/\`) {
		return invalid("version", "must not contain a path separator")
	}
	return nil
}

func digest(field, value string) error {
	if !digestID.MatchString(value) {
		return invalid(field, "must be a lowercase sha256 digest")
	}
	return nil
}

func id(field, value string) error {
	if !resourceID.MatchString(value) {
		return invalid(field, "must be 1 to 128 ASCII letters, digits, dots, underscores, or hyphens and start with a letter or digit")
	}
	return nil
}

func text(field, value string, max int) error {
	if strings.TrimSpace(value) == "" || len(value) > max || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return invalid(field, fmt.Sprintf("must contain 1 to %d bytes without control characters", max))
	}
	return nil
}

func invalid(field, message string) error {
	return status.Error(codes.InvalidArgument, field+" "+message)
}
