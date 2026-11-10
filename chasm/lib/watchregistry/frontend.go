package watchregistry

import (
	"context"
	"slices"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"go.temporal.io/api/serviceerror"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	watchclient "go.temporal.io/server/client/watch"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/softassert"
	"google.golang.org/protobuf/proto"
)

type frontendHandler struct {
	contractpb.UnimplementedWatchServiceServer
	client     registrypb.RegistryServiceClient
	control    watchpb.ControlServiceClient
	namespaces namespace.Registry
	enabled    dynamicconfig.BoolPropertyFnWithNamespaceFilter
	authorizer authorization.Authorizer
	logger     log.Logger
	observer   *watchclient.Observer
}

func newFrontendHandler(
	client registrypb.RegistryServiceClient,
	control watchpb.ControlServiceClient,
	namespaces namespace.Registry,
	dc *dynamicconfig.Collection,
	authorizer authorization.Authorizer,
	logger log.Logger,
	observer *watchclient.Observer,
) *frontendHandler {
	return &frontendHandler{
		client: client, control: control, namespaces: namespaces, enabled: dynamicconfig.EnableDrasiWatches.Get(dc),
		authorizer: authorizer, logger: logger,
		observer: observer,
	}
}

var definitionMethods = []string{
	contractpb.WatchService_GetCapabilities_FullMethodName,
	contractpb.WatchService_CreateSource_FullMethodName,
	contractpb.WatchService_GetSource_FullMethodName,
	contractpb.WatchService_ListSources_FullMethodName,
	contractpb.WatchService_UpdateSource_FullMethodName,
	contractpb.WatchService_DeleteSource_FullMethodName,
	contractpb.WatchService_StartSource_FullMethodName,
	contractpb.WatchService_StopSource_FullMethodName,
	contractpb.WatchService_CreateQuery_FullMethodName,
	contractpb.WatchService_GetQuery_FullMethodName,
	contractpb.WatchService_ListQueries_FullMethodName,
	contractpb.WatchService_UpdateQuery_FullMethodName,
	contractpb.WatchService_DeleteQuery_FullMethodName,
	contractpb.WatchService_StartQuery_FullMethodName,
	contractpb.WatchService_StopQuery_FullMethodName,
	contractpb.WatchService_CreateWatch_FullMethodName,
	contractpb.WatchService_GetWatch_FullMethodName,
	contractpb.WatchService_ListWatches_FullMethodName,
	contractpb.WatchService_UpdateWatch_FullMethodName,
	contractpb.WatchService_DeleteWatch_FullMethodName,
	contractpb.WatchService_PauseWatch_FullMethodName,
	contractpb.WatchService_ResumeWatch_FullMethodName,
	contractpb.WatchService_CancelWatch_FullMethodName,
	contractpb.WatchService_ResetWatch_FullMethodName,
	contractpb.WatchService_ListDeliveries_FullMethodName,
	contractpb.WatchService_RetryDelivery_FullMethodName,
}

func (h *frontendHandler) GetCapabilities(ctx context.Context, request *contractpb.GetCapabilitiesRequest) (*contractpb.Capabilities, error) {
	ns, err := h.namespaces.GetNamespace(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}
	result := &contractpb.Capabilities{
		Enabled:    h.enabled(ns.Name().String()) && !ns.IsGlobalNamespace(),
		ApiVersion: "v1", MaxRequestBytes: DefaultLimits().RequestBytes,
		Message: "Durable definitions are supported; native runtime capabilities are not yet available",
	}
	if ns.IsGlobalNamespace() {
		result.Message = "Drasi watches do not support global namespaces"
	} else if !result.Enabled {
		result.Message = "Drasi watches are disabled for the namespace"
	}
	methods := slices.Clone(definitionMethods)
	if result.Enabled && h.observer != nil {
		capabilities, err := h.observer.QueryCapabilities(ctx, ns.ID().String())
		if err != nil {
			h.logger.Warn("Native query capabilities unavailable", tag.Error(err))
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			result.Message = "Durable definitions are supported; native query capabilities are unavailable"
		} else {
			result.QueryLanguages, result.StateStores = capabilities.QueryLanguages, capabilities.StateStores
			result.TemporalSource, result.Once = capabilities.TemporalSource, capabilities.Once
			result.ExternalSources = capabilities.ExternalSources
			result.Message = "Native compilation is available; query preview and external sources are not configured"
			if result.ExternalSources {
				result.Message = "Native compilation and external sources are available; query preview is not configured"
			}
			methods = append(methods, contractpb.WatchService_ValidateQuery_FullMethodName)
		}
	}
	result.AllowedMethods, err = capabilityMethods(ctx, h.authorizer, h.logger, ns.Name().String(), request, methods)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func capabilityMethods(
	ctx context.Context,
	authorizer authorization.Authorizer,
	logger log.Logger,
	namespaceName string,
	request proto.Message,
	methods []string,
) ([]string, error) {
	var claims *authorization.Claims
	if value := ctx.Value(authorization.MappedClaims); value != nil {
		var ok bool
		claims, ok = value.(*authorization.Claims)
		if !ok {
			softassert.Fail(logger, "Drasi capability authorization context is invalid")
			return nil, serviceerror.NewInternal("Drasi capability authorization context is invalid")
		}
	}
	if authorizer == nil {
		authorizer = authorization.NewNoopAuthorizer()
	}
	var allowed []string
	for _, method := range methods {
		decision, err := authorizer.Authorize(ctx, claims, &authorization.CallTarget{
			APIName: method, Namespace: namespaceName, Request: request,
		})
		if err != nil {
			logger.Error("Drasi capability authorization failed", tag.Error(err))
			return nil, serviceerror.NewInternal("Drasi capability authorization failed")
		}
		if decision.Decision == authorization.DecisionAllow {
			allowed = append(allowed, method)
		}
	}
	return allowed, nil
}

func (h *frontendHandler) mutate(ctx context.Context, name string, request *registrypb.MutateRequest) (*registrypb.Record, error) {
	id, err := h.namespaces.GetNamespaceID(namespace.Name(name))
	if err != nil {
		return nil, err
	}
	request.NamespaceId, request.ExecutionId = id.String(), BusinessID
	info, err := decodeMutation(request)
	if err != nil {
		return nil, err
	}
	result, err := h.client.Mutate(ctx, request)
	if err != nil {
		return nil, err
	}
	record, err := publicRecord(result.GetRecord())
	if err != nil {
		return nil, err
	}
	if info.kind == registrypb.RESOURCE_KIND_SOURCE && record.GetSource() == nil ||
		info.kind == registrypb.RESOURCE_KIND_QUERY && record.GetQuery() == nil ||
		info.kind == registrypb.RESOURCE_KIND_WATCH && record.GetWatch() == nil {
		return nil, serviceerror.NewInternal("registry mutation returned an invalid resource kind")
	}
	return record, nil
}

func (h *frontendHandler) read(ctx context.Context, name string, request *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
	id, err := h.namespaces.GetNamespaceID(namespace.Name(name))
	if err != nil {
		return nil, err
	}
	request.NamespaceId, request.ExecutionId = id.String(), BusinessID
	result, err := h.client.Read(ctx, request)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, serviceerror.NewInternal("registry returned no read response")
	}
	response := common.CloneProto(result)
	for i, record := range result.Records {
		response.Records[i], err = publicRecord(record)
		if err != nil {
			return nil, err
		}
	}
	if h.observer != nil {
		statuses, err := h.observer.Observe(ctx, id.String(), result)
		if err != nil {
			h.logger.Warn("Watch runtime observation unavailable", tag.Error(err))
		}
		for i, record := range response.Records {
			var status *contractpb.ResourceStatus
			if err != nil {
				status = &contractpb.ResourceStatus{Message: "Runtime observation unavailable"}
			} else if statuses != nil {
				status = statuses[i]
			}
			switch resource := record.Resource.(type) {
			case *registrypb.Record_Source:
				resource.Source.Status = status
			case *registrypb.Record_Query:
				resource.Query.Status = status
			case *registrypb.Record_Watch:
				resource.Watch.Status = status
			default:
				softassert.Fail(h.logger, "Watch observation projection has an invalid resource")
				return nil, serviceerror.NewInternal("Watch observation projection has an invalid resource")
			}
		}
	}
	return response, nil
}

func (h *frontendHandler) CreateSource(ctx context.Context, request *contractpb.CreateSourceRequest) (*contractpb.Source, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_CreateSource{CreateSource: request},
	})
	return result.GetSource(), err
}

func (h *frontendHandler) UpdateSource(ctx context.Context, request *contractpb.UpdateSourceRequest) (*contractpb.Source, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_UpdateSource{UpdateSource: request},
	})
	return result.GetSource(), err
}

func (h *frontendHandler) DeleteSource(ctx context.Context, request *contractpb.SourceOperationRequest) (*contractpb.Source, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_DeleteSource{DeleteSource: request},
	})
	return result.GetSource(), err
}

func (h *frontendHandler) StartSource(ctx context.Context, request *contractpb.SourceOperationRequest) (*contractpb.Source, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_StartSource{StartSource: request},
	})
	return result.GetSource(), err
}

func (h *frontendHandler) StopSource(ctx context.Context, request *contractpb.SourceOperationRequest) (*contractpb.Source, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_StopSource{StopSource: request},
	})
	return result.GetSource(), err
}

func (h *frontendHandler) CreateQuery(ctx context.Context, request *contractpb.CreateQueryRequest) (*contractpb.Query, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_CreateQuery{CreateQuery: request},
	})
	return result.GetQuery(), err
}

func (h *frontendHandler) UpdateQuery(ctx context.Context, request *contractpb.UpdateQueryRequest) (*contractpb.Query, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_UpdateQuery{UpdateQuery: request},
	})
	return result.GetQuery(), err
}

func (h *frontendHandler) DeleteQuery(ctx context.Context, request *contractpb.QueryOperationRequest) (*contractpb.Query, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_DeleteQuery{DeleteQuery: request},
	})
	return result.GetQuery(), err
}

func (h *frontendHandler) StartQuery(ctx context.Context, request *contractpb.QueryOperationRequest) (*contractpb.Query, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_StartQuery{StartQuery: request},
	})
	return result.GetQuery(), err
}

func (h *frontendHandler) StopQuery(ctx context.Context, request *contractpb.QueryOperationRequest) (*contractpb.Query, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_StopQuery{StopQuery: request},
	})
	return result.GetQuery(), err
}

func (h *frontendHandler) CreateWatch(ctx context.Context, request *contractpb.CreateWatchRequest) (*contractpb.Watch, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_CreateWatch{CreateWatch: request},
	})
	return result.GetWatch(), err
}

func (h *frontendHandler) UpdateWatch(ctx context.Context, request *contractpb.UpdateWatchRequest) (*contractpb.Watch, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_UpdateWatch{UpdateWatch: request},
	})
	return result.GetWatch(), err
}

func (h *frontendHandler) DeleteWatch(ctx context.Context, request *contractpb.WatchOperationRequest) (*contractpb.Watch, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_DeleteWatch{DeleteWatch: request},
	})
	return result.GetWatch(), err
}

func (h *frontendHandler) PauseWatch(ctx context.Context, request *contractpb.WatchOperationRequest) (*contractpb.Watch, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_PauseWatch{PauseWatch: request},
	})
	return result.GetWatch(), err
}

func (h *frontendHandler) ResumeWatch(ctx context.Context, request *contractpb.WatchOperationRequest) (*contractpb.Watch, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_ResumeWatch{ResumeWatch: request},
	})
	return result.GetWatch(), err
}

func (h *frontendHandler) CancelWatch(ctx context.Context, request *contractpb.WatchOperationRequest) (*contractpb.Watch, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_CancelWatch{CancelWatch: request},
	})
	return result.GetWatch(), err
}

func (h *frontendHandler) ResetWatch(ctx context.Context, request *contractpb.ResetWatchRequest) (*contractpb.Watch, error) {
	result, err := h.mutate(ctx, request.GetNamespace(), &registrypb.MutateRequest{
		Operation: &registrypb.MutateRequest_ResetWatch{ResetWatch: request},
	})
	return result.GetWatch(), err
}

func (h *frontendHandler) GetSource(ctx context.Context, request *contractpb.GetSourceRequest) (*contractpb.Source, error) {
	result, err := h.read(ctx, request.GetNamespace(), &registrypb.ReadRequest{
		Operation: &registrypb.ReadRequest_GetSource{GetSource: request},
	})
	if err != nil {
		return nil, err
	}
	if len(result.Records) != 1 || result.Records[0].GetSource() == nil {
		return nil, serviceerror.NewInternal("registry source lookup returned an invalid result")
	}
	return result.Records[0].GetSource(), nil
}

func (h *frontendHandler) GetQuery(ctx context.Context, request *contractpb.GetQueryRequest) (*contractpb.Query, error) {
	result, err := h.read(ctx, request.GetNamespace(), &registrypb.ReadRequest{
		Operation: &registrypb.ReadRequest_GetQuery{GetQuery: request},
	})
	if err != nil {
		return nil, err
	}
	if len(result.Records) != 1 || result.Records[0].GetQuery() == nil {
		return nil, serviceerror.NewInternal("registry query lookup returned an invalid result")
	}
	return result.Records[0].GetQuery(), nil
}

func (h *frontendHandler) GetWatch(ctx context.Context, request *contractpb.GetWatchRequest) (*contractpb.Watch, error) {
	result, err := h.read(ctx, request.GetNamespace(), &registrypb.ReadRequest{
		Operation: &registrypb.ReadRequest_GetWatch{GetWatch: request},
	})
	if err != nil {
		return nil, err
	}
	if len(result.Records) != 1 || result.Records[0].GetWatch() == nil {
		return nil, serviceerror.NewInternal("registry watch lookup returned an invalid result")
	}
	return result.Records[0].GetWatch(), nil
}

func (h *frontendHandler) ListSources(ctx context.Context, request *contractpb.ListSourcesRequest) (*contractpb.ListSourcesResponse, error) {
	result, err := h.read(ctx, request.GetNamespace(), &registrypb.ReadRequest{
		Operation: &registrypb.ReadRequest_ListSources{ListSources: request},
	})
	if err != nil {
		return nil, err
	}
	response := &contractpb.ListSourcesResponse{NextPageToken: result.NextPageToken}
	for _, record := range result.Records {
		if record.GetSource() == nil {
			return nil, serviceerror.NewInternal("registry source list returned an invalid result")
		}
		response.Sources = append(response.Sources, record.GetSource())
	}
	return response, nil
}

func (h *frontendHandler) ListQueries(ctx context.Context, request *contractpb.ListQueriesRequest) (*contractpb.ListQueriesResponse, error) {
	result, err := h.read(ctx, request.GetNamespace(), &registrypb.ReadRequest{
		Operation: &registrypb.ReadRequest_ListQueries{ListQueries: request},
	})
	if err != nil {
		return nil, err
	}
	response := &contractpb.ListQueriesResponse{NextPageToken: result.NextPageToken}
	for _, record := range result.Records {
		if record.GetQuery() == nil {
			return nil, serviceerror.NewInternal("registry query list returned an invalid result")
		}
		response.Queries = append(response.Queries, record.GetQuery())
	}
	return response, nil
}

func (h *frontendHandler) ListWatches(ctx context.Context, request *contractpb.ListWatchesRequest) (*contractpb.ListWatchesResponse, error) {
	result, err := h.read(ctx, request.GetNamespace(), &registrypb.ReadRequest{
		Operation: &registrypb.ReadRequest_ListWatches{ListWatches: request},
	})
	if err != nil {
		return nil, err
	}
	response := &contractpb.ListWatchesResponse{NextPageToken: result.NextPageToken}
	for _, record := range result.Records {
		if record.GetWatch() == nil {
			return nil, serviceerror.NewInternal("registry watch list returned an invalid result")
		}
		response.Watches = append(response.Watches, record.GetWatch())
	}
	return response, nil
}
