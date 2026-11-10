package watch

import (
	"context"
	"fmt"
	"slices"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func (o *Observer) queryClient(namespaceID string) (watchservice.WatchServiceClient, error) {
	resolver, err := o.membership.GetResolver(primitives.WatchService)
	if err != nil {
		return nil, err
	}
	host, err := resolver.Lookup(namespaceID)
	if err != nil {
		return nil, err
	}
	return watchservice.NewWatchServiceClient(o.rpc.CreateWatchGRPCConnection(host.GetAddress())), nil
}

func (o *Observer) ValidateQuery(ctx context.Context, namespaceID string, spec *contractpb.QuerySpec, definitions []*contractpb.Source) (*contractpb.ValidateQueryResponse, error) {
	request := &watchservice.ValidateQueryRequest{NamespaceId: namespaceID, Spec: spec, SourceDefinitions: definitions}
	sourceAware := slices.ContainsFunc(definitions, func(source *contractpb.Source) bool {
		return source.GetSpec().GetTemporal() == nil || source.GetSpec().Bootstrap != nil
	})
	boundRequest := &watchservice.ValidateBoundQueryRequest{Validation: request}
	var wireRequest proto.Message = request
	if sourceAware {
		wireRequest = boundRequest
	}
	if proto.Size(wireRequest) > validation.MaxRequestBytes {
		return nil, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "query validation source bindings exceed the request byte limit")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	client, err := o.queryClient(namespaceID)
	if err != nil {
		return nil, err
	}
	options := []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(validation.MaxRequestBytes), grpc.MaxCallSendMsgSize(validation.MaxRequestBytes),
	}
	var response *contractpb.ValidateQueryResponse
	if sourceAware {
		var boundResponse *watchservice.ValidateBoundQueryResponse
		boundResponse, err = client.ValidateBoundQuery(ctx, boundRequest, options...)
		response = boundResponse.GetValidation()
	} else {
		response, err = client.ValidateQuery(ctx, request, options...)
	}
	if err != nil {
		if sourceAware && serviceerror.ToStatus(err).Code() == codes.Unimplemented {
			return nil, serviceerror.NewUnavailable("external source schema validation is unavailable on this Watch host")
		}
		return nil, err
	}
	if response == nil || proto.Size(response) > 1024 ||
		response.Valid && len(response.Diagnostics) != 0 ||
		!response.Valid && !queryDiagnostic(response.Diagnostics, definitions) {
		return nil, serviceerror.NewDataLoss("native compiler returned an invalid validation response")
	}
	return response, nil
}

func queryDiagnostic(diagnostics []*contractpb.Diagnostic, definitions []*contractpb.Source) bool {
	if len(diagnostics) != 1 {
		return false
	}
	diagnostic := diagnostics[0]
	if diagnostic.GetField() == "spec.text" {
		return diagnostic.GetMessage() == "Query syntax or structure is invalid"
	}
	if diagnostic.GetMessage() != "External source configuration does not match the pinned descriptors" {
		return false
	}
	for index, source := range definitions {
		if source.GetSpec().GetExternal() != nil && diagnostic.GetField() == fmt.Sprintf("spec.sources[%d]", index) {
			return true
		}
	}
	return false
}

func (o *Observer) QueryCapabilities(ctx context.Context, namespaceID string) (*contractpb.Capabilities, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	client, err := o.queryClient(namespaceID)
	if err != nil {
		return nil, err
	}
	response, err := client.GetCapabilities(ctx, &watchservice.GetCapabilitiesRequest{NamespaceId: namespaceID},
		grpc.MaxCallRecvMsgSize(validation.MaxRequestBytes), grpc.MaxCallSendMsgSize(validation.MaxRequestBytes))
	if err != nil {
		return nil, err
	}
	if response == nil || !response.Enabled || response.ApiVersion != "v1" ||
		proto.Size(response) > 1024 || len(response.QueryLanguages) != 2 ||
		response.QueryLanguages[0] != "cypher" || response.QueryLanguages[1] != "gql" ||
		len(response.StateStores) != 1 || response.StateStores[0] != "persistent" ||
		!response.TemporalSource || !response.Once || response.CrossHostRecovery ||
		response.GlobalNamespaces || response.MaxPreviewRows != 0 ||
		response.MaxRequestBytes != validation.MaxRequestBytes || len(response.AllowedMethods) != 0 || response.Message != "" {
		return nil, serviceerror.NewDataLoss("native compiler returned invalid capabilities")
	}
	return response, nil
}
