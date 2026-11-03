package watch

import (
	"context"
	"errors"
	"fmt"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func (s *Service) queryNamespace(ctx context.Context, id string) (*namespace.Namespace, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id {
		return nil, serviceerror.NewInvalidArgument("query compilation requires a canonical namespace UUID")
	}
	ns, err := s.params.Namespaces.GetNamespaceByID(namespace.ID(id))
	if err != nil {
		return nil, err
	}
	if ns.IsGlobalNamespace() || !s.enabled(ns.Name().String()) {
		return nil, serviceerror.NewFailedPrecondition("query compilation is disabled for this namespace")
	}
	if !native.Enabled() {
		return nil, serviceerror.NewUnavailable("native query compilation is not enabled")
	}
	return ns, nil
}

func (s *Service) GetCapabilities(ctx context.Context, request *watchservice.GetCapabilitiesRequest) (*contractpb.Capabilities, error) {
	if request == nil || proto.Size(request) > validation.MaxRequestBytes {
		return nil, serviceerror.NewInvalidArgument("query capability request is invalid")
	}
	if _, err := s.queryNamespace(ctx, request.NamespaceId); err != nil {
		return nil, err
	}
	external, err := s.externalSourcesAvailable(ctx)
	if err != nil {
		return nil, err
	}
	return &contractpb.Capabilities{
		Enabled: true, ApiVersion: "v1", QueryLanguages: []string{"cypher", "gql"},
		StateStores: []string{"persistent"}, TemporalSource: true, Once: true,
		ExternalSources: external, MaxRequestBytes: validation.MaxRequestBytes,
	}, nil
}

func (s *Service) ValidateBoundQuery(ctx context.Context, request *watchservice.ValidateBoundQueryRequest) (*watchservice.ValidateBoundQueryResponse, error) {
	validationRequest := request.GetValidation()
	if len(validationRequest.GetSourceDefinitions()) == 0 {
		return nil, serviceerror.NewInvalidArgument("source-aware query validation requires complete source definitions")
	}
	if proto.Size(request) > validation.MaxRequestBytes {
		return nil, serviceerror.NewInvalidArgument("query compilation request is invalid")
	}
	response, err := s.ValidateQuery(ctx, validationRequest)
	if err != nil {
		return nil, err
	}
	return &watchservice.ValidateBoundQueryResponse{Validation: response}, nil
}

func (s *Service) ValidateQuery(ctx context.Context, request *watchservice.ValidateQueryRequest) (*contractpb.ValidateQueryResponse, error) {
	if request == nil || proto.Size(request) > validation.MaxRequestBytes {
		return nil, serviceerror.NewInvalidArgument("query compilation request is invalid")
	}
	ns, err := s.queryNamespace(ctx, request.NamespaceId)
	if err != nil {
		return nil, err
	}
	if err := validation.ValidateRequest(&contractpb.ValidateQueryRequest{Namespace: ns.Name().String(), Spec: request.Spec}); err != nil {
		return nil, err
	}
	if request.Spec.StateStore != "persistent" || request.Spec.Language != string(native.Cypher) && request.Spec.Language != string(native.GQL) {
		return nil, serviceerror.NewInvalidArgument("query compilation requires a supported language and state store")
	}
	recipes, indices, err := querySourceRecipes(request, ns.Name().String())
	if err != nil {
		return nil, err
	}
	valid, err := native.ValidateQuery(ctx, native.Language(request.Spec.Language), request.Spec.Text)
	if err != nil {
		if nativeError, ok := errors.AsType[*native.Error](err); ok && nativeError.Code == native.CodeConflict {
			return nil, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "native compiler concurrency limit reached")
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, err
		}
		s.params.Logger.Error("Native query compilation failed", tag.Error(err))
		return nil, serviceerror.NewInternal("native query compilation failed")
	}
	response := &contractpb.ValidateQueryResponse{Valid: valid}
	if !valid {
		response.Diagnostics = []*contractpb.Diagnostic{{Field: "spec.text", Message: "Query syntax or structure is invalid"}}
		return response, nil
	}
	if len(recipes) != 0 {
		result, err := s.pluginOperation(ctx, native.ValidatePluginSourcesCommand{Recipes: recipes})
		if err != nil {
			return nil, s.querySchemaError(ctx, err)
		}
		return querySchemaResponse(result.SourceSchemas, indices)
	}
	return response, nil
}

func querySourceRecipes(request *watchservice.ValidateQueryRequest, namespaceName string) ([]native.PluginSourceRecipe, []int, error) {
	if len(request.SourceDefinitions) == 0 {
		return nil, nil, nil
	}
	if len(request.SourceDefinitions) != len(request.Spec.Sources) {
		return nil, nil, serviceerror.NewInvalidArgument("query validation requires complete source bindings")
	}
	var recipes []native.PluginSourceRecipe
	var indices []int
	for index, source := range request.SourceDefinitions {
		pin := request.Spec.Sources[index]
		if source.GetNamespace() != namespaceName || source.GetId() != pin.Id || source.GetRevision() != pin.Revision {
			return nil, nil, serviceerror.NewInvalidArgument("query validation source binding does not match its exact namespace and revision")
		}
		if err := validation.ValidateSourceDefinition(source.Spec); err != nil {
			return nil, nil, serviceerror.NewInvalidArgument("query validation source definition is invalid")
		}
		if source.Spec.GetExternal() == nil {
			continue
		}
		recipe, err := externalSourceRecipe(source.Spec)
		if err != nil {
			return nil, nil, err
		}
		recipes = append(recipes, recipe)
		indices = append(indices, index)
	}
	return recipes, indices, nil
}

func querySchemaResponse(verdicts []native.SourceSchemaVerdict, indices []int) (*contractpb.ValidateQueryResponse, error) {
	if len(verdicts) != len(indices) || len(indices) == 0 {
		return nil, serviceerror.NewDataLoss("native source schema validation returned incomplete results")
	}
	response := &contractpb.ValidateQueryResponse{Valid: true}
	for index, verdict := range verdicts {
		switch verdict {
		case native.SourceSchemaValid:
		case native.SourceSchemaInvalidConfiguration:
			if response.Valid {
				response.Valid = false
				response.Diagnostics = []*contractpb.Diagnostic{{
					Field:   fmt.Sprintf("spec.sources[%d]", indices[index]),
					Message: "External source configuration does not match the pinned descriptors",
				}}
			}
		default:
			return nil, serviceerror.NewDataLoss("native source schema validation returned an unknown verdict")
		}
	}
	return response, nil
}

func (s *Service) querySchemaError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if failure, ok := errors.AsType[*native.Error](err); ok {
		switch failure.Code {
		case native.CodeFailedPrecondition:
			return serviceerror.NewFailedPrecondition("external source schema validation is unavailable")
		case native.CodeConflict:
			return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "native source schema validation is busy")
		case native.CodeCancelled, native.CodeDeadline:
			return serviceerror.NewUnavailable("native source schema validation was interrupted")
		case native.CodeInvalidArgument, native.CodeNotFound, native.CodeGap, native.CodeInternal:
		default:
			return serviceerror.NewDataLoss("native source schema validation returned an unknown error code")
		}
	}
	if errors.Is(err, native.ErrClosed) || errors.Is(err, native.ErrDisabled) ||
		serviceerror.ToStatus(err).Code() == codes.Unavailable {
		return serviceerror.NewUnavailable("native source schema validation is unavailable")
	}
	s.params.Logger.Error("Native source schema validation failed", tag.Error(err))
	return serviceerror.NewInternal("native source schema validation failed")
}
