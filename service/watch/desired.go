package watch

import (
	"cmp"
	"context"
	"maps"
	"slices"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watchregistry"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

type resourceKey struct {
	kind registrypb.ResourceKind
	id   string
}

type definitionPin struct {
	resourceKey
	revision uint64
}

// DesiredView retains current lifecycle gates separately from exact dependency pins.
type DesiredView struct {
	namespaceID string
	namespace   string
	generation  uint64
	current     map[resourceKey]*registrypb.Record
	revisions   map[definitionPin]*registrypb.Record
}

type desiredReader struct {
	client        registrypb.RegistryServiceClient
	view          *DesiredView
	initialized   bool
	retainedBytes int
}

func (v *DesiredView) Generation() uint64 { return v.generation }

func (v *DesiredView) Source(ref *contractpb.ResourceRef) (*registrypb.Record, error) {
	return v.pinned(registrypb.RESOURCE_KIND_SOURCE, ref)
}

func (v *DesiredView) Query(ref *contractpb.ResourceRef) (*registrypb.Record, error) {
	return v.pinned(registrypb.RESOURCE_KIND_QUERY, ref)
}

func (v *DesiredView) Watch(id string) (*registrypb.Record, error) {
	record, ok := v.current[resourceKey{registrypb.RESOURCE_KIND_WATCH, id}]
	if !ok {
		return nil, serviceerror.NewNotFound("watch is not in the desired view")
	}
	return common.CloneProto(record), nil
}

func (v *DesiredView) pinned(kind registrypb.ResourceKind, ref *contractpb.ResourceRef) (*registrypb.Record, error) {
	if ref.GetId() == "" || ref.GetRevision() == 0 {
		return nil, serviceerror.NewInvalidArgument("desired view lookup requires an exact resource pin")
	}
	record, ok := v.revisions[definitionPin{resourceKey{kind, ref.Id}, ref.Revision}]
	if !ok {
		return nil, serviceerror.NewNotFound("resource pin is not in the desired view")
	}
	return common.CloneProto(record), nil
}

// ReadDesiredView returns no partial view if any page or retained lookup observes a mutation.
func ReadDesiredView(
	ctx context.Context,
	client registrypb.RegistryServiceClient,
	namespaceID string,
	namespace string,
) (*DesiredView, error) {
	id, err := uuid.Parse(namespaceID)
	if err != nil || id == uuid.Nil || id.String() != namespaceID {
		return nil, serviceerror.NewInvalidArgument("desired view requires a canonical nonzero namespace UUID")
	}
	if err := validation.ValidateRequest(&contractpb.ListSourcesRequest{Namespace: namespace}); err != nil {
		return nil, err
	}
	view := &DesiredView{
		namespaceID: namespaceID, namespace: namespace,
		current: make(map[resourceKey]*registrypb.Record), revisions: make(map[definitionPin]*registrypb.Record),
	}
	reader := desiredReader{client: client, view: view}
	for _, kind := range []registrypb.ResourceKind{
		registrypb.RESOURCE_KIND_SOURCE, registrypb.RESOURCE_KIND_QUERY,
		registrypb.RESOURCE_KIND_WATCH,
	} {
		if err := reader.list(ctx, kind); err != nil {
			return nil, err
		}
	}
	ordered := func() []definitionPin {
		return slices.SortedFunc(maps.Keys(view.revisions), func(a, b definitionPin) int {
			return cmp.Or(cmp.Compare(a.kind, b.kind), cmp.Compare(a.id, b.id), cmp.Compare(a.revision, b.revision))
		})
	}
	for _, pin := range ordered() {
		if pin.kind == registrypb.RESOURCE_KIND_WATCH {
			if err := reader.resolve(ctx, registrypb.RESOURCE_KIND_QUERY, view.revisions[pin].GetWatch().Spec.Query); err != nil {
				return nil, err
			}
		}
	}
	for _, pin := range ordered() {
		if pin.kind == registrypb.RESOURCE_KIND_QUERY {
			for _, ref := range view.revisions[pin].GetQuery().Spec.Sources {
				if err := reader.resolve(ctx, registrypb.RESOURCE_KIND_SOURCE, ref); err != nil {
					return nil, err
				}
			}
		}
	}
	return view, nil
}

func (r *desiredReader) read(ctx context.Context, request *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
	request.NamespaceId, request.ExecutionId = r.view.namespaceID, watchregistry.BusinessID
	response, err := r.client.Read(ctx, request)
	if err != nil {
		return nil, err
	}
	if response == nil || proto.Size(response) > watchregistry.MaxPageBytes ||
		(response.Generation == 0 && len(response.Records) != 0) {
		return nil, serviceerror.NewDataLoss("registry returned an invalid desired view response")
	}
	if r.initialized && response.Generation != r.view.generation {
		return nil, serviceerror.NewFailedPrecondition("registry changed while reading desired view; retry the complete read")
	}
	if !r.initialized {
		r.view.generation, r.initialized = response.Generation, true
	}
	return response, nil
}

func (r *desiredReader) retain(pin definitionPin, record *registrypb.Record) error {
	size := proto.Size(record)
	if len(r.view.revisions) >= watchregistry.MaxMutations || size > watchregistry.MaxStateBytes-r.retainedBytes {
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "desired view exceeds registry retention limits")
	}
	r.view.revisions[pin] = common.CloneProto(record)
	r.retainedBytes += size
	return nil
}

func (r *desiredReader) list(ctx context.Context, kind registrypb.ResourceKind) error {
	token, after := "", ""
	for {
		request := &registrypb.ReadRequest{}
		switch kind {
		case registrypb.RESOURCE_KIND_SOURCE:
			request.Operation = &registrypb.ReadRequest_ListSources{ListSources: &contractpb.ListSourcesRequest{
				Namespace: r.view.namespace, PageSize: validation.MaxPageSize, PageToken: token,
			}}
		case registrypb.RESOURCE_KIND_QUERY:
			request.Operation = &registrypb.ReadRequest_ListQueries{ListQueries: &contractpb.ListQueriesRequest{
				Namespace: r.view.namespace, PageSize: validation.MaxPageSize, PageToken: token,
			}}
		case registrypb.RESOURCE_KIND_WATCH:
			request.Operation = &registrypb.ReadRequest_ListWatches{ListWatches: &contractpb.ListWatchesRequest{
				Namespace: r.view.namespace, PageSize: validation.MaxPageSize, PageToken: token,
			}}
		default:
			return serviceerror.NewInternal("unsupported desired view resource kind")
		}
		response, err := r.read(ctx, request)
		if err != nil {
			return err
		}
		if len(response.Records) > validation.MaxPageSize || len(response.NextPageToken) > 4096 ||
			(len(response.Records) == 0 && response.NextPageToken != "") {
			return serviceerror.NewDataLoss("registry returned an invalid desired view page")
		}
		for _, record := range response.Records {
			pin, err := desiredRecordPin(record, kind, r.view.namespace)
			if err != nil {
				return err
			}
			if pin.id <= after {
				return serviceerror.NewDataLoss("registry desired view pages are not strictly ordered")
			}
			if len(r.view.current) >= watchregistry.MaxResourceIDs {
				return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "desired view exceeds registry resource limits")
			}
			if err := r.retain(pin, record); err != nil {
				return err
			}
			r.view.current[pin.resourceKey] = r.view.revisions[pin]
			after = pin.id
		}
		token = response.NextPageToken
		if token == "" {
			return nil
		}
	}
}

func (r *desiredReader) resolve(ctx context.Context, kind registrypb.ResourceKind, ref *contractpb.ResourceRef) error {
	if ref.GetId() == "" || ref.GetRevision() == 0 {
		return serviceerror.NewDataLoss("registry definition contains an incomplete dependency pin")
	}
	pin := definitionPin{resourceKey{kind, ref.Id}, ref.Revision}
	current, ok := r.view.current[pin.resourceKey]
	if !ok {
		return serviceerror.NewDataLoss("registry dependency has no current lifecycle record")
	}
	latest := cmp.Or(current.GetSource().GetRevision(), current.GetQuery().GetRevision())
	if ref.Revision > latest {
		return serviceerror.NewDataLoss("registry dependency is newer than its current lifecycle record")
	}
	if _, ok := r.view.revisions[pin]; ok {
		return nil
	}
	record, err := r.lookup(ctx, pin)
	if err != nil {
		return err
	}
	return r.retain(pin, record)
}

func (r *desiredReader) lookup(ctx context.Context, pin definitionPin) (*registrypb.Record, error) {
	response, err := r.read(ctx, &registrypb.ReadRequest{
		Operation: &registrypb.ReadRequest_Revision{Revision: &registrypb.RevisionLookup{
			Kind: pin.kind, Ref: &contractpb.ResourceRef{Id: pin.id, Revision: pin.revision},
		}},
	})
	if err != nil {
		return nil, err
	}
	if len(response.Records) != 1 || response.NextPageToken != "" {
		return nil, serviceerror.NewDataLoss("registry returned an invalid retained dependency")
	}
	actual, err := desiredRecordPin(response.Records[0], pin.kind, r.view.namespace)
	if err != nil {
		return nil, err
	}
	if actual != pin {
		return nil, serviceerror.NewDataLoss("registry returned a different retained dependency pin")
	}
	return common.CloneProto(response.Records[0]), nil
}

func desiredRecordPin(record *registrypb.Record, kind registrypb.ResourceKind, namespace string) (definitionPin, error) {
	var id, actualNamespace string
	var revision uint64
	var valid bool
	switch kind {
	case registrypb.RESOURCE_KIND_SOURCE:
		if source := record.GetSource(); source != nil {
			id, revision, actualNamespace = source.Id, source.Revision, source.Namespace
			valid = (source.Spec.GetTemporal() != nil || source.Spec.GetExternal() != nil) &&
				(record.Desired == registrypb.DESIRED_STATE_STOPPED ||
					record.Desired == registrypb.DESIRED_STATE_RUNNING ||
					record.Desired == registrypb.DESIRED_STATE_DELETED)
		}
	case registrypb.RESOURCE_KIND_QUERY:
		if query := record.GetQuery(); query != nil {
			id, revision, actualNamespace = query.Id, query.Revision, query.Namespace
			valid = query.Spec != nil && len(query.Spec.Sources) != 0 &&
				(record.Desired == registrypb.DESIRED_STATE_STOPPED ||
					record.Desired == registrypb.DESIRED_STATE_RUNNING ||
					record.Desired == registrypb.DESIRED_STATE_DELETED)
		}
	case registrypb.RESOURCE_KIND_WATCH:
		if watch := record.GetWatch(); watch != nil {
			id, revision, actualNamespace = watch.Id, watch.Revision, watch.Namespace
			valid = watch.Spec != nil &&
				(record.Desired == registrypb.DESIRED_STATE_RUNNING ||
					record.Desired == registrypb.DESIRED_STATE_PAUSED ||
					record.Desired == registrypb.DESIRED_STATE_DELETED ||
					record.Desired == registrypb.DESIRED_STATE_CANCELED)
		}
	default:
		return definitionPin{}, serviceerror.NewInternal("unsupported desired view resource kind")
	}
	if !valid || actualNamespace != namespace || revision == 0 ||
		validation.ValidateRequest(&contractpb.GetSourceRequest{Namespace: namespace, Id: id}) != nil {
		return definitionPin{}, serviceerror.NewDataLoss("registry returned an invalid desired resource definition")
	}
	return definitionPin{resourceKey{kind, id}, revision}, nil
}
