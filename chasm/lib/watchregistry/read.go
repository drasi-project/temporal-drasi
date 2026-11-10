package watchregistry

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"slices"
	"strings"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	watchclient "go.temporal.io/server/client/watch"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

type readInput struct {
	public       proto.Message
	kind         registrypb.ResourceKind
	id           string
	revision     uint64
	list         bool
	pageSize     int32
	token        string
	owner        *commonpb.WorkflowExecution
	history      *registrypb.WatchHistoryLookup
	retry        *contractpb.RetryDeliveryRequest
	namespaceID  string
	querySources *contractpb.QuerySpec
}

func decodeRead(request *registrypb.ReadRequest, namespace string) (readInput, error) {
	var input readInput
	switch op := request.Operation.(type) {
	case *registrypb.ReadRequest_GetSource:
		input = readInput{public: op.GetSource, kind: registrypb.RESOURCE_KIND_SOURCE}
	case *registrypb.ReadRequest_ListSources:
		input = readInput{public: op.ListSources, kind: registrypb.RESOURCE_KIND_SOURCE, list: true}
	case *registrypb.ReadRequest_GetQuery:
		input = readInput{public: op.GetQuery, kind: registrypb.RESOURCE_KIND_QUERY}
	case *registrypb.ReadRequest_ListQueries:
		input = readInput{public: op.ListQueries, kind: registrypb.RESOURCE_KIND_QUERY, list: true}
	case *registrypb.ReadRequest_GetWatch:
		input = readInput{public: op.GetWatch, kind: registrypb.RESOURCE_KIND_WATCH}
	case *registrypb.ReadRequest_ListWatches:
		input = readInput{public: op.ListWatches, kind: registrypb.RESOURCE_KIND_WATCH, list: true}
		input.owner = op.ListWatches.GetOwner()
	case *registrypb.ReadRequest_Revision:
		if op.Revision == nil || op.Revision.Ref == nil || op.Revision.Ref.Revision == 0 ||
			op.Revision.Kind < registrypb.RESOURCE_KIND_SOURCE ||
			op.Revision.Kind > registrypb.RESOURCE_KIND_WATCH {
			return readInput{}, serviceerror.NewInvalidArgument("revision lookup requires a kind, ID, and nonzero revision")
		}
		input = readInput{
			public: &contractpb.GetSourceRequest{Namespace: namespace, Id: op.Revision.Ref.Id},
			kind:   op.Revision.Kind, revision: op.Revision.Ref.Revision,
		}
	case *registrypb.ReadRequest_WatchHistory:
		if op.WatchHistory == nil || op.WatchHistory.Limit == 0 || op.WatchHistory.Limit > validation.MaxPageSize {
			return readInput{}, serviceerror.NewInvalidArgument("Watch history requires a bounded lifetime page")
		}
		input = readInput{
			public: &contractpb.GetWatchRequest{Namespace: namespace, Id: op.WatchHistory.Id},
			kind:   registrypb.RESOURCE_KIND_WATCH, history: op.WatchHistory,
		}
	case *registrypb.ReadRequest_DeliveryRetry:
		input = readInput{public: op.DeliveryRetry, retry: op.DeliveryRetry, kind: registrypb.RESOURCE_KIND_WATCH}
	case *registrypb.ReadRequest_QuerySources:
		input = readInput{public: op.QuerySources, querySources: op.QuerySources.GetSpec()}
	default:
		return readInput{}, serviceerror.NewInvalidArgument("registry read operation is required")
	}
	if err := validation.ValidateRequest(input.public); err != nil {
		return readInput{}, err
	}
	if validation.Namespace(input.public) != namespace {
		return readInput{}, serviceerror.NewInvalidArgument("request namespace does not match the authoritative namespace")
	}
	if identified, ok := input.public.(interface{ GetId() string }); ok {
		input.id = identified.GetId()
	}
	if paginated, ok := input.public.(interface {
		GetPageSize() int32
		GetPageToken() string
	}); ok {
		input.pageSize, input.token = paginated.GetPageSize(), paginated.GetPageToken()
		if input.pageSize == 0 {
			input.pageSize = 50
		}
	}
	input.namespaceID = request.NamespaceId
	return input, nil
}

func (c *Component) read(_ chasm.Context, input readInput) (*registrypb.ReadResponse, error) {
	if proto.Size(input.public) > int(c.Limits.RequestBytes) {
		return nil, exhausted("request exceeds the persisted namespace registry limit")
	}
	if input.list {
		return c.list(input)
	}
	if input.querySources != nil {
		response := &registrypb.ReadResponse{Generation: c.Generation}
		for _, ref := range input.querySources.Sources {
			record, err := c.pinned(registrypb.RESOURCE_KIND_SOURCE, ref)
			if err != nil {
				return nil, err
			}
			response.Records = append(response.Records, common.CloneProto(record))
			if proto.Size(response) > int(c.Limits.PageBytes) {
				return nil, exhausted("query source definitions exceed the registry page byte limit")
			}
		}
		return response, nil
	}
	if input.history != nil {
		return c.watchHistory(input)
	}
	if input.retry != nil {
		return c.readDeliveryRetry(input)
	}
	key := resourceKey(input.kind, input.id)
	record, ok := c.Current[key]
	if input.revision != 0 {
		record, ok = c.Revisions[revisionKey(key, input.revision)]
	}
	if !ok {
		return nil, serviceerror.NewNotFound("resource revision does not exist")
	}
	return &registrypb.ReadResponse{Records: []*registrypb.Record{common.CloneProto(record)}, Generation: c.Generation}, nil
}

func (c *Component) watchHistory(input readInput) (*registrypb.ReadResponse, error) {
	lookup := input.history
	if lookup.Generation != 0 && lookup.Generation != c.Generation {
		return nil, serviceerror.NewFailedPrecondition("registry changed during delivery pagination; restart the list")
	}
	if _, ok := c.Current[resourceKey(registrypb.RESOURCE_KIND_WATCH, input.id)]; !ok {
		return nil, serviceerror.NewNotFound("watch does not exist")
	}
	lifetimes := make(map[string]*registrypb.WatchLifetime)
	for _, record := range c.Revisions {
		watch := record.GetWatch()
		if watch == nil || watch.Id != input.id {
			continue
		}
		if record.Incarnation == "" || watch.Revision == 0 {
			return nil, serviceerror.NewDataLoss("retained Watch lifetime is incomplete")
		}
		lifetime, ok := lifetimes[record.Incarnation]
		if !ok {
			lifetime = &registrypb.WatchLifetime{
				Incarnation: record.Incarnation, FirstRevision: watch.Revision, LastRevision: watch.Revision,
			}
			lifetimes[record.Incarnation] = lifetime
		}
		lifetime.FirstRevision = min(lifetime.FirstRevision, watch.Revision)
		lifetime.LastRevision = max(lifetime.LastRevision, watch.Revision)
	}
	ordered := make([]*registrypb.WatchLifetime, 0, len(lifetimes))
	for _, lifetime := range lifetimes {
		if lifetime.FirstRevision > lookup.AfterRevision {
			ordered = append(ordered, lifetime)
		}
	}
	slices.SortFunc(ordered, func(a, b *registrypb.WatchLifetime) int {
		return cmp.Compare(a.FirstRevision, b.FirstRevision)
	})
	response := &registrypb.ReadResponse{Generation: c.Generation}
	for _, lifetime := range ordered {
		response.WatchLifetimes = append(response.WatchLifetimes, lifetime)
		if proto.Size(response)+32 > int(c.Limits.PageBytes) {
			response.WatchLifetimes = response.WatchLifetimes[:len(response.WatchLifetimes)-1]
			if len(response.WatchLifetimes) == 0 {
				return nil, exhausted("Watch lifetime exceeds the registry page byte limit")
			}
			break
		}
		if len(response.WatchLifetimes) == int(lookup.Limit) {
			break
		}
	}
	response.MoreLifetimes = len(response.WatchLifetimes) < len(ordered)
	return response, nil
}
func (c *Component) list(input readInput) (*registrypb.ReadResponse, error) {
	filter, err := proto.MarshalOptions{Deterministic: true}.Marshal(input.owner)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(filter)
	after, err := c.pageAfter(input, fingerprint[:])
	if err != nil {
		return nil, err
	}
	prefix := resourceKey(input.kind, "")
	ids := make([]string, 0, len(c.Current))
	for key, record := range c.Current {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		id := resource(record).GetId()
		if id <= after {
			continue
		}
		if input.owner != nil && !proto.Equal(record.GetWatch().GetSpec().GetOnce().GetOwner(), input.owner) {
			continue
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	response := &registrypb.ReadResponse{Generation: c.Generation}
	for i, id := range ids {
		record := c.Current[resourceKey(input.kind, id)]
		response.Records = append(response.Records, common.CloneProto(record))
		// Nested length prefixes also grow when the frontend attaches runtime status.
		if proto.Size(response)+1024+len(response.Records)*(watchclient.MaxStatusBytes+16) > int(c.Limits.PageBytes) {
			response.Records = response.Records[:len(response.Records)-1]
			if len(response.Records) == 0 {
				return nil, exhausted("resource exceeds the registry page byte limit")
			}
			break
		}
		if len(response.Records) == int(input.pageSize) || i == len(ids)-1 {
			break
		}
	}
	if len(response.Records) < len(ids) {
		last := resource(response.Records[len(response.Records)-1]).GetId()
		encoded, err := proto.Marshal(&registrypb.PageCursor{
			Namespace: c.Namespace, Generation: c.Generation, Kind: input.kind, After: last, Filter: fingerprint[:],
		})
		if err != nil {
			return nil, err
		}
		response.NextPageToken = base64.RawURLEncoding.EncodeToString(encoded)
	}
	return response, nil
}

func (c *Component) pageAfter(input readInput, fingerprint []byte) (string, error) {
	if input.token == "" {
		return "", nil
	}
	encoded, err := base64.RawURLEncoding.DecodeString(input.token)
	var cursor registrypb.PageCursor
	if err != nil || proto.Unmarshal(encoded, &cursor) != nil ||
		cursor.Namespace != c.Namespace || cursor.Kind != input.kind || !bytes.Equal(cursor.Filter, fingerprint) {
		return "", serviceerror.NewInvalidArgument("page token does not match this namespace resource list and filter")
	}
	if cursor.Generation != c.Generation {
		return "", serviceerror.NewFailedPrecondition("registry changed during pagination; restart the list")
	}
	return cursor.After, nil
}
