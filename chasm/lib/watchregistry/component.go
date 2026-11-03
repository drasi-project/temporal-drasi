package watchregistry

import (
	"bytes"
	"crypto/sha256"
	"math"
	"slices"
	"strconv"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// BusinessID is immutable across resource deletion, recreation, and host failover.
const BusinessID = "drasi-namespace-registry-v1"

const (
	MaxResourceIDs = 512
	MaxMutations   = 4096
	MaxStateBytes  = 2 << 20
	MaxPageBytes   = 512 << 10
)

type Component struct {
	chasm.UnimplementedComponent
	*registrypb.State
}

var _ chasm.RootComponent = (*Component)(nil)

func DefaultLimits() *registrypb.Limits {
	return &registrypb.Limits{
		ResourceIds: 128, Mutations: 1024, StateBytes: 1 << 20,
		RequestBytes: 64 << 10, PageBytes: 256 << 10,
	}
}

func validateLimits(limits *registrypb.Limits) error {
	if limits == nil || limits.ResourceIds == 0 || limits.ResourceIds > MaxResourceIDs ||
		limits.Mutations == 0 || limits.Mutations > MaxMutations ||
		limits.StateBytes < 4096 || limits.StateBytes > MaxStateBytes ||
		limits.RequestBytes == 0 || limits.RequestBytes > 128<<10 ||
		limits.PageBytes < limits.RequestBytes+4096 || limits.PageBytes > MaxPageBytes {
		return serviceerror.NewInvalidArgument("invalid watch registry limits")
	}
	return nil
}

func newComponent(name string, limits *registrypb.Limits) (*Component, error) {
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	return &Component{State: &registrypb.State{
		Namespace: name, Limits: common.CloneProto(limits),
		Current: make(map[string]*registrypb.Record), Revisions: make(map[string]*registrypb.Record),
		Receipts: make(map[string]*registrypb.Receipt),
	}}, nil
}

func (*Component) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (*Component) ContextMetadata(chasm.Context) map[string]string { return nil }

func (*Component) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPrecondition("namespace registry retains revision and request identities and cannot be terminated")
}

type mutationInput struct {
	request *registrypb.MutateRequest
	info    mutation
	// AcceptanceError is resolved outside the CHASM transaction.
	acceptanceError string
}

func (c *Component) apply(ctx chasm.MutableContext, input mutationInput) (*registrypb.MutationResult, error) {
	fingerprint, err := mutationFingerprint(input.request)
	if err != nil {
		return nil, err
	}
	requestID := input.info.public.GetRequestId()
	if receipt, ok := c.Receipts[requestID]; ok {
		if !bytes.Equal(receipt.Fingerprint, fingerprint[:]) {
			return nil, serviceerror.NewInvalidArgument("request_id was used with a different method or content")
		}
		return common.CloneProto(receipt.Result), nil
	}
	if len(c.Receipts) >= int(c.Limits.Mutations) {
		return nil, exhausted("namespace registry request identity capacity is exhausted")
	}
	if proto.Size(input.info.public) > int(c.Limits.RequestBytes) {
		return nil, exhausted("request exceeds the persisted namespace registry limit")
	}
	result := &registrypb.MutationResult{}
	var record *registrypb.Record
	if input.acceptanceError != "" && input.info.accepts() {
		err = serviceerror.NewFailedPrecondition(input.acceptanceError)
	} else {
		record, err = c.change(ctx, input.info)
	}
	if err != nil {
		code := serviceerror.ToStatus(err).Code()
		if code != codes.InvalidArgument && code != codes.NotFound && code != codes.AlreadyExists &&
			code != codes.FailedPrecondition && code != codes.ResourceExhausted {
			return nil, err
		}
		result.Rejection = &registrypb.Rejection{Code: registrypb.RejectionCode(code), Message: err.Error()}
	} else {
		result.Record = record
		if input.info.action == retryDelivery {
			result, err = c.retryAuthorization(input, record)
			if err != nil {
				return nil, err
			}
		}
	}

	// Stage the complete bounded state before publishing any change to the component.
	next := common.CloneProto(c.State)
	if next.Current == nil {
		next.Current = make(map[string]*registrypb.Record)
	}
	if next.Revisions == nil {
		next.Revisions = make(map[string]*registrypb.Record)
	}
	if next.Receipts == nil {
		next.Receipts = make(map[string]*registrypb.Receipt)
	}
	if record != nil && input.info.action != retryDelivery {
		if c.Generation == math.MaxUint64 {
			return nil, exhausted("namespace registry generation is exhausted")
		}
		next.Generation++
		key := resourceKey(input.info.kind, input.info.public.GetId())
		next.Current[key] = common.CloneProto(record)
		next.Revisions[revisionKey(key, resource(record).GetRevision())] = common.CloneProto(record)
	}
	next.Receipts[requestID] = &registrypb.Receipt{
		Fingerprint: bytes.Clone(fingerprint[:]), Result: common.CloneProto(result),
	}
	if proto.Size(next) > int(c.Limits.StateBytes) {
		return nil, exhausted("namespace registry retained state capacity is exhausted")
	}
	c.State = next
	return common.CloneProto(result), nil
}

func mutationFingerprint(request *registrypb.MutateRequest) ([sha256.Size]byte, error) {
	canonical := common.CloneProto(request)
	// Target resolution is not caller content and cannot change an accepted command.
	canonical.RetryIncarnation = ""
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func exhausted(message string) error {
	return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, message)
}

type publicResource interface {
	proto.Message
	GetId() string
	GetRevision() uint64
}

func resource(record *registrypb.Record) publicResource {
	switch resource := record.Resource.(type) {
	case *registrypb.Record_Source:
		return resource.Source
	case *registrypb.Record_Query:
		return resource.Query
	case *registrypb.Record_Watch:
		return resource.Watch
	default:
		return nil
	}
}

func publicRecord(record *registrypb.Record) (*registrypb.Record, error) {
	if record == nil {
		return nil, serviceerror.NewInternal("registry returned no resource")
	}
	value := resource(record)
	if value == nil || !value.ProtoReflect().IsValid() {
		return nil, serviceerror.NewInternal("registry returned an invalid resource")
	}
	var desired contractpb.DesiredState
	switch record.Desired {
	case registrypb.DESIRED_STATE_STOPPED:
		desired = contractpb.DesiredState_DESIRED_STATE_STOPPED
	case registrypb.DESIRED_STATE_RUNNING:
		desired = contractpb.DesiredState_DESIRED_STATE_RUNNING
	case registrypb.DESIRED_STATE_PAUSED:
		desired = contractpb.DesiredState_DESIRED_STATE_PAUSED
	case registrypb.DESIRED_STATE_DELETED:
		desired = contractpb.DesiredState_DESIRED_STATE_DELETED
	case registrypb.DESIRED_STATE_CANCELED:
		desired = contractpb.DesiredState_DESIRED_STATE_CANCELED
	default:
		return nil, serviceerror.NewInternal("registry returned invalid desired state")
	}
	result := common.CloneProto(record)
	switch resource := result.Resource.(type) {
	case *registrypb.Record_Source:
		resource.Source.DesiredState = desired
	case *registrypb.Record_Query:
		resource.Query.DesiredState = desired
	case *registrypb.Record_Watch:
		resource.Watch.DesiredState = desired
	default:
		return nil, serviceerror.NewInternal("registry returned an invalid resource")
	}
	return result, nil
}

func resourceKey(kind registrypb.ResourceKind, id string) string {
	return strconv.FormatInt(int64(kind), 10) + "/" + id
}

func revisionKey(key string, revision uint64) string {
	return key + "/" + strconv.FormatUint(revision, 10)
}

func (c *Component) change(ctx chasm.MutableContext, m mutation) (*registrypb.Record, error) {
	key := resourceKey(m.kind, m.public.GetId())
	previous, exists := c.Current[key]
	if m.action == retryDelivery {
		return c.retryRecord(m)
	}
	live := exists && previous.Desired != registrypb.DESIRED_STATE_DELETED
	if m.action == create {
		if live {
			return nil, serviceerror.NewAlreadyExists("resource already exists")
		}
		if !exists && len(c.Current) >= int(c.Limits.ResourceIds) {
			return nil, exhausted("namespace registry resource identity capacity is exhausted")
		}
	} else {
		if !live {
			return nil, serviceerror.NewNotFound("resource does not exist")
		}
		expected := m.public.(interface{ GetExpectedRevision() uint64 }).GetExpectedRevision()
		if expected != resource(previous).GetRevision() {
			return nil, serviceerror.NewFailedPrecondition("resource revision changed")
		}
		if previous.Desired == registrypb.DESIRED_STATE_CANCELED {
			if m.action != remove {
				return nil, serviceerror.NewFailedPrecondition("canceled watch can only be deleted")
			}
		}
	}
	var revision uint64 = 1
	if exists {
		if resource(previous).GetRevision() == math.MaxUint64 {
			return nil, exhausted("resource revision is exhausted")
		}
		revision = resource(previous).GetRevision() + 1
	}
	record, err := definition(m, previous, c.Namespace, revision)
	if err != nil {
		return nil, err
	}
	if err := c.validateChange(ctx, m, record, previous); err != nil {
		return nil, err
	}
	return record, nil
}

func definition(m mutation, previous *registrypb.Record, namespace string, revision uint64) (*registrypb.Record, error) {
	var record *registrypb.Record
	if m.action == create {
		record = &registrypb.Record{Desired: registrypb.DESIRED_STATE_STOPPED}
		switch m.kind {
		case registrypb.RESOURCE_KIND_SOURCE:
			record.Resource = &registrypb.Record_Source{Source: &contractpb.Source{Namespace: namespace, Id: m.public.GetId()}}
		case registrypb.RESOURCE_KIND_QUERY:
			record.Resource = &registrypb.Record_Query{Query: &contractpb.Query{Namespace: namespace, Id: m.public.GetId()}}
		case registrypb.RESOURCE_KIND_WATCH:
			record.Resource = &registrypb.Record_Watch{Watch: &contractpb.Watch{Namespace: namespace, Id: m.public.GetId()}}
			record.Desired = registrypb.DESIRED_STATE_RUNNING
		default:
			return nil, serviceerror.NewInternal("invalid registry resource kind")
		}
	} else {
		record = common.CloneProto(previous)
	}
	switch public := m.public.(type) {
	case interface{ GetSpec() *contractpb.SourceSpec }:
		record.GetSource().Spec = common.CloneProto(public.GetSpec())
	case interface{ GetSpec() *contractpb.QuerySpec }:
		record.GetQuery().Spec = common.CloneProto(public.GetSpec())
	case interface{ GetSpec() *contractpb.WatchSpec }:
		record.GetWatch().Spec = common.CloneProto(public.GetSpec())
	case *contractpb.ResetWatchRequest:
		record.GetWatch().Spec.InitialResults = public.InitialResults
	default:
	}
	switch m.action {
	case start, reset:
		record.Desired = registrypb.DESIRED_STATE_RUNNING
	case stop:
		record.Desired = registrypb.DESIRED_STATE_STOPPED
	case pause:
		record.Desired = registrypb.DESIRED_STATE_PAUSED
	case remove:
		record.Desired = registrypb.DESIRED_STATE_DELETED
	case cancel:
		record.Desired = registrypb.DESIRED_STATE_CANCELED
	default:
	}
	switch m.kind {
	case registrypb.RESOURCE_KIND_SOURCE:
		record.GetSource().Revision = revision
	case registrypb.RESOURCE_KIND_QUERY:
		record.GetQuery().Revision = revision
	case registrypb.RESOURCE_KIND_WATCH:
		record.GetWatch().Revision = revision
	default:
		return nil, serviceerror.NewInternal("invalid registry resource kind")
	}
	if m.action == create || m.action == update || m.action == reset {
		record.Incarnation = strconv.FormatUint(revision, 10)
	}
	return publicRecord(record)
}

func (c *Component) pinned(kind registrypb.ResourceKind, ref *contractpb.ResourceRef) (*registrypb.Record, error) {
	key := resourceKey(kind, ref.Id)
	current, ok := c.Current[key]
	if !ok || current.Desired == registrypb.DESIRED_STATE_DELETED {
		return nil, serviceerror.NewFailedPrecondition("pinned dependency does not exist or is deleted")
	}
	record, ok := c.Revisions[revisionKey(key, ref.Revision)]
	if !ok || record.Desired == registrypb.DESIRED_STATE_DELETED {
		return nil, serviceerror.NewFailedPrecondition("pinned dependency revision does not exist")
	}
	return record, nil
}

func (c *Component) currentPin(kind registrypb.ResourceKind, ref *contractpb.ResourceRef) error {
	record, err := c.pinned(kind, ref)
	if err != nil {
		return err
	}
	if record.Incarnation != c.Current[resourceKey(kind, ref.Id)].Incarnation {
		return serviceerror.NewFailedPrecondition("new dependency pins cannot reference a superseded resource definition")
	}
	return nil
}

func (c *Component) checkNewSourcePins(query, previous *contractpb.Query) error {
	for _, ref := range query.Spec.Sources {
		if previous != nil && slices.ContainsFunc(previous.Spec.Sources, func(old *contractpb.ResourceRef) bool {
			return proto.Equal(old, ref)
		}) {
			continue
		}
		if err := c.currentPin(registrypb.RESOURCE_KIND_SOURCE, ref); err != nil {
			return err
		}
	}
	return nil
}

func (c *Component) validateChange(ctx chasm.MutableContext, m mutation, record, previous *registrypb.Record) error {
	if m.action == remove {
		return c.checkDependents(m.kind, m.public.GetId())
	}
	if source := record.GetSource(); source != nil && (m.action == create || m.action == update) {
		if err := validation.ValidateSourceDefinition(source.Spec); err != nil {
			return serviceerror.FromStatus(status.Convert(err))
		}
	}
	if watch := record.GetWatch(); watch != nil {
		return c.validateWatchChange(ctx, m, watch, previous.GetWatch())
	}
	if query := record.GetQuery(); query != nil && m.action != stop {
		if m.action == create || m.action == update {
			old := previous.GetQuery()
			if m.action == create {
				old = nil
			}
			if err := c.checkNewSourcePins(query, old); err != nil {
				return err
			}
		}
		return c.checkSourcePins(query)
	}
	return nil
}

func (c *Component) validateWatchChange(ctx chasm.MutableContext, m mutation, watch, previous *contractpb.Watch) error {
	once := watch.Spec.GetOnce()
	if m.action == cancel && once == nil {
		return serviceerror.NewFailedPrecondition("only one-shot watches can be canceled")
	}
	if m.action == pause || m.action == cancel {
		return nil
	}
	if m.action != create && previous != nil && !proto.Equal(previous.Spec.GetOnce(), once) {
		return serviceerror.NewFailedPrecondition("watch lifetime, one-shot owner and deadline are immutable")
	}
	if once != nil && !once.Deadline.AsTime().After(ctx.Now(c)) {
		return serviceerror.NewInvalidArgument("one-shot deadline must be in the future")
	}
	query, err := c.pinned(registrypb.RESOURCE_KIND_QUERY, watch.Spec.Query)
	if err != nil {
		return err
	}
	if err := c.checkSourcePins(query.GetQuery()); err != nil {
		return err
	}
	if m.action == create || m.action == update && !proto.Equal(watch.Spec.Query, previous.Spec.Query) {
		return c.currentPin(registrypb.RESOURCE_KIND_QUERY, watch.Spec.Query)
	}
	return nil
}

func (c *Component) checkSourcePins(query *contractpb.Query) error {
	for _, ref := range query.Spec.Sources {
		if _, err := c.pinned(registrypb.RESOURCE_KIND_SOURCE, ref); err != nil {
			return err
		}
	}
	return nil
}

func (c *Component) checkDependents(kind registrypb.ResourceKind, id string) error {
	for _, record := range c.Current {
		if record.Desired == registrypb.DESIRED_STATE_DELETED ||
			record.Desired == registrypb.DESIRED_STATE_CANCELED {
			continue
		}
		if watch := record.GetWatch(); watch != nil {
			if kind == registrypb.RESOURCE_KIND_QUERY && watch.Spec.Query.Id == id {
				return serviceerror.NewFailedPrecondition("query is pinned by a watch")
			}
			if kind == registrypb.RESOURCE_KIND_SOURCE {
				query, ok := c.Revisions[revisionKey(resourceKey(registrypb.RESOURCE_KIND_QUERY, watch.Spec.Query.Id), watch.Spec.Query.Revision)]
				if !ok {
					return serviceerror.NewFailedPrecondition("watch pinned query revision is missing")
				}
				if referencesSource(query.GetQuery(), id) {
					return serviceerror.NewFailedPrecondition("source is pinned by a watch query revision")
				}
			}
		}
		if kind == registrypb.RESOURCE_KIND_SOURCE && referencesSource(record.GetQuery(), id) {
			return serviceerror.NewFailedPrecondition("source is pinned by a query")
		}
	}
	return nil
}

func referencesSource(query *contractpb.Query, id string) bool {
	for _, ref := range query.GetSpec().GetSources() {
		if ref.Id == id {
			return true
		}
	}
	return false
}
