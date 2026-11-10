package watchregistry

import (
	"context"
	"errors"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func (h *frontendHandler) ValidateQuery(ctx context.Context, request *contractpb.ValidateQueryRequest) (*contractpb.ValidateQueryResponse, error) {
	if err := validation.ValidateRequest(request); err != nil {
		return nil, err
	}
	ns, err := h.namespaces.GetNamespace(namespace.Name(request.Namespace))
	if err != nil {
		return nil, err
	}
	if ns.IsGlobalNamespace() || !h.enabled(ns.Name().String()) {
		return nil, serviceerror.NewFailedPrecondition("query compilation is disabled for this namespace")
	}
	if request.Spec.Language != "cypher" && request.Spec.Language != "gql" {
		return invalidQuery("spec.language", "Query language is not supported"), nil
	}
	if request.Spec.StateStore != "persistent" {
		return invalidQuery("spec.state_store", "Query state store is not supported"), nil
	}
	snapshot, err := h.client.Read(ctx, &registrypb.ReadRequest{
		NamespaceId: ns.ID().String(), ExecutionId: BusinessID,
		Operation: &registrypb.ReadRequest_QuerySources{QuerySources: request},
	})
	if err != nil {
		return nil, err
	}
	if snapshot.GetGeneration() == 0 || len(snapshot.GetRecords()) != len(request.Spec.Sources) ||
		proto.Size(snapshot) > MaxPageBytes || snapshot.NextPageToken != "" {
		return nil, serviceerror.NewDataLoss("registry returned invalid query source bindings")
	}
	definitions := make([]*contractpb.Source, len(snapshot.Records))
	for i, record := range snapshot.Records {
		source, ref := record.GetSource(), request.Spec.Sources[i]
		if source.GetId() != ref.Id || source.GetRevision() != ref.Revision || source.GetNamespace() != request.Namespace ||
			record.Incarnation == "" || source.Spec == nil {
			return nil, serviceerror.NewDataLoss("registry query source binding does not match the requested pin")
		}
		if err := validation.ValidateRequest(&contractpb.CreateSourceRequest{
			Namespace: source.Namespace, Id: source.Id, Spec: source.Spec, RequestId: "validation",
		}); err != nil {
			return nil, serviceerror.NewDataLoss("registry query source schema is invalid")
		}
		definitions[i] = common.CloneProto(source)
	}
	if h.observer == nil {
		return nil, serviceerror.NewUnavailable("native query compilation is unavailable")
	}
	response, err := h.observer.ValidateQuery(ctx, ns.ID().String(), request.Spec, definitions)
	if err != nil {
		h.logger.Warn("Native query compilation unavailable", tag.Error(err))
		return nil, queryCompilationError(ctx, err)
	}
	return response, nil
}

func queryCompilationError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	switch serviceerror.ToStatus(err).Code() {
	case codes.ResourceExhausted:
		if exhausted, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok &&
			exhausted.Cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED {
			return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
				"query validation source bindings exceed the request byte limit")
		}
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "native compiler concurrency limit reached")
	case codes.DeadlineExceeded:
		return serviceerror.NewDeadlineExceeded("native query compilation deadline exceeded")
	case codes.Canceled:
		return serviceerror.NewCanceled("native query compilation canceled")
	case codes.DataLoss:
		return serviceerror.NewDataLoss("native query compilation returned an invalid response")
	case codes.Internal:
		return serviceerror.NewInternal("native query compilation failed")
	case codes.FailedPrecondition:
		return serviceerror.NewFailedPrecondition("external source schema validation is unavailable")
	default:
		return serviceerror.NewUnavailable("native query compilation is unavailable")
	}
}

func invalidQuery(field, message string) *contractpb.ValidateQueryResponse {
	return &contractpb.ValidateQueryResponse{Diagnostics: []*contractpb.Diagnostic{{Field: field, Message: message}}}
}
