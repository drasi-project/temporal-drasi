package validation_test

import (
	"context"
	"strings"
	"testing"
	"time"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidateRequests(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		request proto.Message
		field   string
	}{
		{"nil request", nil, "request"},
		{"typed nil request", (*watchpb.ListCatalogsRequest)(nil), "request"},
		{"unrelated message", &emptypb.Empty{}, "request"},
		{"capabilities", &watchpb.GetCapabilitiesRequest{Namespace: "test"}, ""},
		{"missing namespace", &watchpb.GetCapabilitiesRequest{}, "namespace"},
		{"invalid namespace", &watchpb.GetCapabilitiesRequest{Namespace: "other/test"}, "namespace"},
		{"source create", &watchpb.CreateSourceRequest{Namespace: "test", Id: "source", RequestId: "create", Spec: temporalSource()}, ""},
		{"source get", &watchpb.GetSourceRequest{Namespace: "test", Id: "source"}, ""},
		{"source list defaults", &watchpb.ListSourcesRequest{Namespace: "test"}, ""},
		{"source update", &watchpb.UpdateSourceRequest{Namespace: "test", Id: "source", ExpectedRevision: 1, RequestId: "update", Spec: temporalSource()}, ""},
		{"source lifecycle", &watchpb.SourceOperationRequest{Namespace: "test", Id: "source", ExpectedRevision: 1, RequestId: "start"}, ""},
		{"invalid resource id", &watchpb.GetSourceRequest{Namespace: "test", Id: "../source"}, "id"},
		{"missing idempotency key", &watchpb.SourceOperationRequest{Namespace: "test", Id: "source", ExpectedRevision: 1}, "request_id"},
		{"missing revision", &watchpb.SourceOperationRequest{Namespace: "test", Id: "source", RequestId: "start"}, "expected_revision"},
		{"query create", &watchpb.CreateQueryRequest{Namespace: "test", Id: "query", RequestId: "create", Spec: querySpec()}, ""},
		{"query get", &watchpb.GetQueryRequest{Namespace: "test", Id: "query"}, ""},
		{"query list", &watchpb.ListQueriesRequest{Namespace: "test", PageSize: 200}, ""},
		{"query update", &watchpb.UpdateQueryRequest{Namespace: "test", Id: "query", ExpectedRevision: 1, RequestId: "update", Spec: querySpec()}, ""},
		{"query lifecycle", &watchpb.QueryOperationRequest{Namespace: "test", Id: "query", ExpectedRevision: 1, RequestId: "start"}, ""},
		{"read only validation", &watchpb.ValidateQueryRequest{Namespace: "test", Spec: querySpec()}, ""},
		{"read only preview", &watchpb.PreviewQueryRequest{Namespace: "test", Spec: querySpec(), Limit: 200}, ""},
		{"unbounded preview", &watchpb.PreviewQueryRequest{Namespace: "test", Spec: querySpec()}, "limit"},
		{"oversized preview", &watchpb.PreviewQueryRequest{Namespace: "test", Spec: querySpec(), Limit: 201}, "limit"},
		{"negative page", &watchpb.ListSourcesRequest{Namespace: "test", PageSize: -1}, "page_size"},
		{"oversized page", &watchpb.ListSourcesRequest{Namespace: "test", PageSize: 201}, "page_size"},
		{"oversized token", &watchpb.ListSourcesRequest{Namespace: "test", PageToken: strings.Repeat("x", 4097)}, "page_token"},
		{"watch create", &watchpb.CreateWatchRequest{Namespace: "test", Id: "watch", RequestId: "create", Spec: watchSpec()}, ""},
		{"watch get", &watchpb.GetWatchRequest{Namespace: "test", Id: "watch"}, ""},
		{"watch list", &watchpb.ListWatchesRequest{Namespace: "test"}, ""},
		{"watch owner list", &watchpb.ListWatchesRequest{Namespace: "test", Owner: owner()}, ""},
		{"incomplete owner", &watchpb.ListWatchesRequest{Namespace: "test", Owner: &commonpb.WorkflowExecution{WorkflowId: "workflow"}}, "owner.run_id"},
		{"watch update", &watchpb.UpdateWatchRequest{Namespace: "test", Id: "watch", ExpectedRevision: 1, RequestId: "update", Spec: watchSpec()}, ""},
		{"watch lifecycle", &watchpb.WatchOperationRequest{Namespace: "test", Id: "watch", ExpectedRevision: 1, RequestId: "pause"}, ""},
		{"watch reset", &watchpb.ResetWatchRequest{Namespace: "test", Id: "watch", ExpectedRevision: 1, RequestId: "reset"}, ""},
		{"unknown reset policy", &watchpb.ResetWatchRequest{Namespace: "test", Id: "watch", ExpectedRevision: 1, RequestId: "reset", InitialResults: 99}, "initial_results"},
		{"deliveries", &watchpb.ListDeliveriesRequest{Namespace: "test", Id: "watch"}, ""},
		{"retry delivery", &watchpb.RetryDeliveryRequest{Namespace: "test", Id: "watch", ExpectedRevision: 1, RequestId: "retry", ActionId: "action"}, ""},
		{"missing action identity", &watchpb.RetryDeliveryRequest{Namespace: "test", Id: "watch", ExpectedRevision: 1, RequestId: "retry"}, "action_id"},
		{"catalogs", &watchpb.ListCatalogsRequest{}, ""},
		{"cluster capabilities without namespace", &watchpb.GetPluginCapabilitiesRequest{}, ""},
		{"typed nil cluster capabilities", (*watchpb.GetPluginCapabilitiesRequest)(nil), "request"},
		{"plugin discovery", &watchpb.ListPluginsRequest{CatalogId: "trusted"}, ""},
		{"plugin metadata", &watchpb.GetPluginRequest{CatalogId: "trusted", PluginId: "postgres", Version: "1.0.0"}, ""},
		{"plugin install", &watchpb.InstallPluginRequest{CatalogId: "trusted", PluginId: "postgres", Version: "1.0.0", Digest: artifactDigest(), RequestId: "install"}, ""},
		{"unconfigured path", &watchpb.InstallPluginRequest{CatalogId: "../untrusted", PluginId: "postgres", Version: "1.0.0", Digest: artifactDigest(), RequestId: "install"}, "catalog_id"},
		{"mutable tag", &watchpb.GetInstallationRequest{Digest: "latest"}, "digest"},
		{"uppercase digest", &watchpb.GetInstallationRequest{Digest: "sha256:" + strings.Repeat("A", 64)}, "digest"},
		{"plugin installations", &watchpb.ListInstallationsRequest{}, ""},
		{"plugin installation", &watchpb.GetInstallationRequest{Digest: artifactDigest()}, ""},
		{"plugin load", &watchpb.LoadPluginRequest{Digest: artifactDigest(), RequestId: "load"}, ""},
		{"oversized request", &watchpb.ListPluginsRequest{Search: strings.Repeat("x", validation.MaxRequestBytes+1)}, "request"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validation.ValidateRequest(test.request)
			if test.field == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.field)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			}
		})
	}
}

func TestWatchUnions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*watchpb.WatchSpec)
		field  string
	}{
		{"recurring ignore default", func(*watchpb.WatchSpec) {}, ""},
		{"include bootstrap", func(spec *watchpb.WatchSpec) { spec.InitialResults = watchpb.InitialResults_INITIAL_RESULTS_INCLUDE }, ""},
		{"update trigger", func(spec *watchpb.WatchSpec) { spec.Trigger = watchpb.Trigger_TRIGGER_UPDATE }, ""},
		{"leave trigger", func(spec *watchpb.WatchSpec) { spec.Trigger = watchpb.Trigger_TRIGGER_LEAVE }, ""},
		{"unknown trigger", func(spec *watchpb.WatchSpec) { spec.Trigger = 99 }, "spec.trigger"},
		{"unspecified trigger", func(spec *watchpb.WatchSpec) { spec.Trigger = 0 }, "spec.trigger"},
		{"unknown bootstrap policy", func(spec *watchpb.WatchSpec) { spec.InitialResults = 99 }, "initial_results"},
		{"unpinned query", func(spec *watchpb.WatchSpec) { spec.Query.Revision = 0 }, "spec.query.revision"},
		{"missing lifetime", func(spec *watchpb.WatchSpec) { spec.Lifetime = nil }, "spec.lifetime"},
		{"nil persistent", func(spec *watchpb.WatchSpec) { spec.Lifetime = &watchpb.WatchSpec_Persistent{} }, "spec.persistent"},
		{"typed nil persistent", func(spec *watchpb.WatchSpec) { spec.Lifetime = (*watchpb.WatchSpec_Persistent)(nil) }, "spec.persistent"},
		{"missing operation", func(spec *watchpb.WatchSpec) { spec.Action.Operation = nil }, "spec.action.operation"},
		{"typed nil start", func(spec *watchpb.WatchSpec) { spec.Action.Operation = (*watchpb.WorkflowAction_Start)(nil) }, "spec.action.start"},
		{"invalid result policy", func(spec *watchpb.WatchSpec) { spec.Action.ResultPayload = 99 }, "spec.action.result_payload"},
		{"start missing task queue", func(spec *watchpb.WatchSpec) { spec.Action.GetStart().TaskQueue = "" }, "task_queue"},
		{"unsupported template", func(spec *watchpb.WatchSpec) { spec.Action.GetStart().WorkflowId = "{row.id}" }, "workflow_id"},
		{"negative timeout", func(spec *watchpb.WatchSpec) { spec.Action.GetStart().RunTimeout = durationpb.New(-time.Second) }, "run_timeout"},
		{"unknown reuse policy", func(spec *watchpb.WatchSpec) { spec.Action.GetStart().ReusePolicy = 99 }, "reuse_policy"},
		{"destructive reuse", func(spec *watchpb.WatchSpec) {
			spec.Action.GetStart().ReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
		}, "must not terminate"},
		{"signal exact run", func(spec *watchpb.WatchSpec) { spec.Action = signalAction() }, ""},
		{"signal current run explicit", func(spec *watchpb.WatchSpec) {
			spec.Action = signalAction()
			spec.Action.GetSignal().Target = &watchpb.WorkflowSignal_CurrentWorkflowId{CurrentWorkflowId: "workflow"}
		}, ""},
		{"signal missing run", func(spec *watchpb.WatchSpec) {
			spec.Action = signalAction()
			spec.Action.GetSignal().GetRun().RunId = ""
		}, "run_id"},
		{"signal missing target", func(spec *watchpb.WatchSpec) {
			spec.Action = signalAction()
			spec.Action.GetSignal().Target = nil
		}, "target"},
		{"signal with start", func(spec *watchpb.WatchSpec) {
			start := spec.Action.GetStart()
			spec.Action.Operation = &watchpb.WorkflowAction_SignalWithStart{
				SignalWithStart: &watchpb.WorkflowSignalWithStart{Start: start, SignalName: "change"},
			}
		}, ""},
		{"one-shot existing match ignores recurring policy", func(spec *watchpb.WatchSpec) {
			oneShot(spec)
		}, ""},
		{"one-shot wrong run", func(spec *watchpb.WatchSpec) {
			oneShot(spec)
			spec.Action.GetSignal().GetRun().RunId = "different-run"
		}, "exact one-shot owner run"},
		{"one-shot requires enter", func(spec *watchpb.WatchSpec) {
			oneShot(spec)
			spec.Trigger = watchpb.Trigger_TRIGGER_LEAVE
		}, "must be ENTER"},
		{"one-shot requires deadline", func(spec *watchpb.WatchSpec) {
			oneShot(spec)
			spec.GetOnce().Deadline = nil
		}, "deadline"},
		{"one-shot requires owner run", func(spec *watchpb.WatchSpec) {
			oneShot(spec)
			spec.GetOnce().Owner.RunId = ""
		}, "owner.run_id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spec := watchSpec()
			test.change(spec)
			err := validation.ValidateRequest(&watchpb.CreateWatchRequest{Namespace: "test", Id: "watch", RequestId: "create", Spec: spec})
			if test.field == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.field)
			}
		})
	}
}

func TestSourceAndQueryConstraints(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		request proto.Message
		field   string
	}{
		{"missing source kind", &watchpb.CreateSourceRequest{Namespace: "test", Id: "source", RequestId: "create", Spec: &watchpb.SourceSpec{}}, "spec.kind"},
		{"missing temporal value", &watchpb.CreateSourceRequest{Namespace: "test", Id: "source", RequestId: "create", Spec: &watchpb.SourceSpec{Kind: &watchpb.SourceSpec_Temporal{}}}, "spec.temporal"},
		{"external descriptor", &watchpb.CreateSourceRequest{Namespace: "test", Id: "source", RequestId: "create", Spec: &watchpb.SourceSpec{
			Kind: &watchpb.SourceSpec_External{External: &watchpb.ComponentConfiguration{Plugin: &watchpb.PluginComponentRef{Digest: artifactDigest(), Component: "postgres"}}},
		}}, ""},
		{"unsigned artifact reference", &watchpb.CreateSourceRequest{Namespace: "test", Id: "source", RequestId: "create", Spec: &watchpb.SourceSpec{
			Kind: &watchpb.SourceSpec_External{External: &watchpb.ComponentConfiguration{Plugin: &watchpb.PluginComponentRef{Digest: "latest", Component: "postgres"}}},
		}}, "digest"},
		{"temporal bootstrap rejection", &watchpb.CreateSourceRequest{Namespace: "test", Id: "source", RequestId: "create", Spec: &watchpb.SourceSpec{
			Kind: &watchpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}, Bootstrap: &watchpb.ComponentConfiguration{},
		}}, "bootstrap"},
		{"empty query text", &watchpb.ValidateQueryRequest{Namespace: "test", Spec: &watchpb.QuerySpec{Language: "cypher"}}, "spec.text"},
		{"missing persistent backend", &watchpb.ValidateQueryRequest{Namespace: "test", Spec: &watchpb.QuerySpec{Language: "cypher", Text: "RETURN 1"}}, "state_store"},
		{"missing sources", &watchpb.ValidateQueryRequest{Namespace: "test", Spec: &watchpb.QuerySpec{Language: "cypher", Text: "RETURN 1", StateStore: "persistent"}}, "spec.sources"},
		{"duplicate sources", &watchpb.ValidateQueryRequest{Namespace: "test", Spec: &watchpb.QuerySpec{
			Language: "cypher", Text: "RETURN 1", StateStore: "persistent",
			Sources: []*watchpb.ResourceRef{{Id: "source", Revision: 1}, {Id: "source", Revision: 2}},
		}}, "duplicate source"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validation.ValidateRequest(test.request)
			if test.field == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.field)
			}
		})
	}
}

func TestInterceptorOnlyAppliesToPublicServices(t *testing.T) {
	t.Parallel()
	response, err := validation.UnaryServerInterceptor(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/other.Service/Method"},
		func(context.Context, any) (any, error) { return "untouched", nil })
	require.NoError(t, err)
	require.Equal(t, "untouched", response)
}

func temporalSource() *watchpb.SourceSpec {
	return &watchpb.SourceSpec{Kind: &watchpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}}
}

func querySpec() *watchpb.QuerySpec {
	return &watchpb.QuerySpec{
		Language: "cypher", Text: "MATCH (n) RETURN n", StateStore: "persistent",
		Sources: []*watchpb.ResourceRef{{Id: "source", Revision: 1}},
	}
}

func watchSpec() *watchpb.WatchSpec {
	return &watchpb.WatchSpec{
		Query: &watchpb.ResourceRef{Id: "query", Revision: 1}, Trigger: watchpb.Trigger_TRIGGER_ENTER,
		Lifetime: &watchpb.WatchSpec_Persistent{Persistent: &emptypb.Empty{}},
		Action: &watchpb.WorkflowAction{Operation: &watchpb.WorkflowAction_Start{Start: &watchpb.WorkflowStart{
			WorkflowId: "watch-{action_id}", WorkflowType: "workflow", TaskQueue: "queue",
		}}},
	}
}

func owner() *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "11111111-1111-4111-8111-111111111111"}
}

func signalAction() *watchpb.WorkflowAction {
	return &watchpb.WorkflowAction{Operation: &watchpb.WorkflowAction_Signal{Signal: &watchpb.WorkflowSignal{
		Target: &watchpb.WorkflowSignal_Run{Run: owner()}, SignalName: "change",
	}}}
}

func oneShot(spec *watchpb.WatchSpec) {
	spec.Lifetime = &watchpb.WatchSpec_Once{Once: &watchpb.Once{
		Owner: owner(), Deadline: timestamppb.New(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
	}}
	spec.Action = signalAction()
}

func artifactDigest() string {
	return "sha256:" + strings.Repeat("a", 64)
}
