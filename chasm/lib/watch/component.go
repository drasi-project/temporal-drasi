package watch

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"slices"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	Component struct {
		chasm.UnimplementedComponent
		*watchpb.State
		Intents chasm.Map[string, *watchpb.Intent]
	}

	Limits struct {
		Pending      uint32
		Batch        uint32
		Recent       uint32
		PendingBytes uint64
	}

	Create struct {
		Namespace   string
		ID          string
		Revision    uint64
		Definition  []byte
		Incarnation string
		Once        bool
		Deadline    time.Time
		Limits      Limits
	}

	Action struct {
		Ordinal uint64
		RowKey  string
		Request proto.Message
	}

	ActionIdentity struct {
		Namespace   string
		WatchID     string
		Revision    uint64
		Incarnation string
		Sequence    uint64
		Ordinal     uint64
		RowKey      string
	}

	Batch struct {
		Revision     uint64
		OwnerEpoch   uint64
		Incarnation  string
		Sequence     uint64
		FirstOrdinal uint64
		ItemCount    uint64
		Complete     bool
		Initial      bool
		Actions      []Action
	}

	Receipt struct {
		Cursor    *watchpb.Cursor
		ActionIDs []string
		Duplicate bool
	}
)

var _ chasm.RootComponent = (*Component)(nil)

func New(ctx chasm.MutableContext, input Create) (*Component, error) {
	if input.Namespace == "" || input.ID == "" || input.Revision == 0 || len(input.Definition) == 0 {
		return nil, serviceerror.NewInvalidArgument("watch namespace, ID, revision, and definition are required")
	}
	if input.Limits.Pending == 0 || input.Limits.Batch == 0 || input.Limits.PendingBytes == 0 ||
		input.Limits.Batch > input.Limits.Pending {
		return nil, serviceerror.NewInvalidArgument("watch action limits must be positive and batch must not exceed pending")
	}
	if !input.Deadline.IsZero() && !input.Once {
		return nil, serviceerror.NewInvalidArgument("watch deadline requires a one-shot condition")
	}
	c := &Component{
		State: &watchpb.State{
			Namespace:        input.Namespace,
			WatchId:          input.ID,
			Revision:         input.Revision,
			CreationRevision: input.Revision,
			Incarnation:      input.Incarnation,
			Definition:       bytes.Clone(input.Definition),
			Status:           watchpb.STATUS_RUNNING,
			Cursor:           &watchpb.Cursor{},
			Once:             input.Once,
			MaxPending:       input.Limits.Pending,
			MaxBatch:         input.Limits.Batch,
			MaxRecent:        input.Limits.Recent,
			MaxPendingBytes:  input.Limits.PendingBytes,
		},
		Intents: make(chasm.Map[string, *watchpb.Intent]),
	}
	if !input.Deadline.IsZero() {
		c.DeadlineUnixNano = input.Deadline.UnixNano()
		if !input.Deadline.After(ctx.Now(c)) {
			c.Status = watchpb.STATUS_TIMED_OUT
		} else {
			ctx.AddTask(c, chasm.TaskAttributes{ScheduledTime: input.Deadline}, &watchpb.DeadlineTask{
				DeadlineUnixNano: c.DeadlineUnixNano,
			})
		}
	}
	return c, nil
}

func (c *Component) LifecycleState(chasm.Context) chasm.LifecycleState {
	switch c.Status {
	case watchpb.STATUS_CANCELLED, watchpb.STATUS_SATISFIED, watchpb.STATUS_TIMED_OUT:
		if len(c.PendingIds) == 0 {
			return chasm.LifecycleStateCompleted
		}
		return chasm.LifecycleStateRunning
	default:
		return chasm.LifecycleStateRunning
	}
}

func (*Component) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

func (c *Component) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	// Force-closing with accepted actions would invalidate their dispatch tasks.
	if len(c.PendingIds) > 0 {
		return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPrecondition("watch has accepted actions awaiting delivery")
	}
	c.Status = watchpb.STATUS_CANCELLED
	return chasm.TerminateComponentResponse{}, nil
}

func (c *Component) Claim(expectedRevision, previousEpoch, newEpoch uint64) error {
	if expectedRevision != c.Revision || previousEpoch != c.OwnerEpoch || newEpoch <= previousEpoch {
		return serviceerror.NewFailedPrecondition("watch revision or runtime assignment changed")
	}
	c.OwnerEpoch = newEpoch
	return nil
}

func (c *Component) Pause(expectedRevision uint64, paused bool) error {
	if expectedRevision != c.Revision || c.Revision == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("watch revision changed")
	}
	if c.Status != watchpb.STATUS_RUNNING && c.Status != watchpb.STATUS_PAUSED {
		return serviceerror.NewFailedPrecondition("watch is no longer accepting changes")
	}
	c.Status = watchpb.STATUS_RUNNING
	if paused {
		c.Status = watchpb.STATUS_PAUSED
	}
	c.Revision++
	return nil
}

func (c *Component) Cancel(expectedRevision uint64) error {
	if expectedRevision != c.Revision || c.Revision == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("watch revision changed")
	}
	if c.Status != watchpb.STATUS_RUNNING && c.Status != watchpb.STATUS_PAUSED {
		return serviceerror.NewFailedPrecondition("watch is no longer accepting changes")
	}
	c.Status = watchpb.STATUS_CANCELLED
	c.Revision++
	return nil
}

func (c *Component) Accept(ctx chasm.MutableContext, batch Batch) (*Receipt, error) {
	duplicate, err := c.validateProgress(ctx, batch)
	if err != nil {
		return nil, err
	}
	if duplicate {
		return &Receipt{Cursor: common.CloneProto(c.Cursor), Duplicate: true}, nil
	}
	actions := batch.Actions
	if c.Once {
		actions = actions[:min(1, len(actions))]
	}
	if len(actions) > int(c.MaxBatch) || len(actions)+len(c.PendingIds) > int(c.MaxPending) {
		return nil, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "watch action buffer is full")
	}
	intents, addedBytes, err := c.prepareBatch(ctx, batch, actions)
	if err != nil {
		return nil, err
	}
	receipt := &Receipt{ActionIDs: make([]string, 0, len(intents))}
	startDispatch := len(c.PendingIds) == 0 && len(intents) > 0
	for _, intent := range intents {
		c.Intents[intent.ActionId] = chasm.NewDataField(ctx, intent)
		c.PendingIds = append(c.PendingIds, intent.ActionId)
		receipt.ActionIDs = append(receipt.ActionIDs, intent.ActionId)
	}
	if startDispatch {
		ctx.AddTask(c, chasm.TaskAttributes{}, &watchpb.DispatchTask{ActionId: c.PendingIds[0]})
	}
	c.PendingBytes += addedBytes
	c.Incarnation = batch.Incarnation
	c.Cursor = &watchpb.Cursor{
		Sequence:    batch.Sequence,
		NextOrdinal: batch.FirstOrdinal + batch.ItemCount,
		Complete:    batch.Complete,
	}
	if batch.Initial {
		c.ProgressPhase = watchpb.PROGRESS_PHASE_SNAPSHOTTING
		if batch.Complete {
			c.ProgressPhase = watchpb.PROGRESS_PHASE_LIVE
		}
	}
	if c.Once && len(intents) > 0 {
		c.Status = watchpb.STATUS_SATISFIED
	}
	receipt.Cursor = common.CloneProto(c.Cursor)
	return receipt, nil
}

func (c *Component) prepareBatch(ctx chasm.Context, batch Batch, actions []Action) ([]*watchpb.Intent, uint64, error) {
	var revision uint64
	if len(actions) > 0 {
		var err error
		revision, err = queryRevision(c.Definition)
		if err != nil {
			return nil, 0, err
		}
	}
	intents := make([]*watchpb.Intent, 0, len(actions))
	var addedBytes uint64
	ordinals := make(map[uint64]struct{}, len(actions))
	for _, action := range actions {
		if action.Ordinal < batch.FirstOrdinal || action.Ordinal >= batch.FirstOrdinal+batch.ItemCount ||
			action.Ordinal > math.MaxUint32 || action.RowKey == "" {
			return nil, 0, serviceerror.NewInvalidArgument("action must identify a row in the accepted query output chunk")
		}
		if _, ok := ordinals[action.Ordinal]; ok {
			return nil, 0, serviceerror.NewInvalidArgument("query output contains duplicate action ordinals")
		}
		ordinals[action.Ordinal] = struct{}{}
		id := ActionID(ActionIdentity{
			Namespace: c.Namespace, WatchID: c.WatchId, Revision: c.CreationRevision,
			Incarnation: batch.Incarnation, Sequence: batch.Sequence, Ordinal: action.Ordinal, RowKey: action.RowKey,
		})
		intent, err := prepareIntent(c.Namespace, id, action.Request)
		if err != nil {
			return nil, 0, err
		}
		intent.Output = &contractpb.OutputIdentity{
			QueryRevision: revision, Incarnation: batch.Incarnation,
			Sequence: batch.Sequence, RowIdentity: action.RowKey, DiffOrdinal: uint32(action.Ordinal),
		}
		intent.UpdatedAt = timestamppb.New(ctx.Now(c))
		requestBytes := intentBytes(intent)
		if requestBytes > c.MaxPendingBytes || addedBytes > c.MaxPendingBytes-requestBytes {
			return nil, 0, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "watch action byte buffer is full")
		}
		addedBytes += requestBytes
		intents = append(intents, intent)
	}
	if c.PendingBytes > c.MaxPendingBytes-addedBytes {
		return nil, 0, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "watch action byte buffer is full")
	}
	return intents, addedBytes, nil
}

func (c *Component) validateProgress(ctx chasm.Context, batch Batch) (bool, error) {
	if batch.Revision != c.Revision || batch.OwnerEpoch != c.OwnerEpoch || batch.Incarnation == "" ||
		(c.Incarnation != "" && batch.Incarnation != c.Incarnation) {
		return false, serviceerror.NewFailedPrecondition("watch definition, runtime assignment, or query incarnation changed")
	}
	if (!batch.Initial && batch.Sequence == 0) || batch.ItemCount > math.MaxUint64-batch.FirstOrdinal ||
		(batch.ItemCount == 0 && !batch.Complete) {
		return false, serviceerror.NewInvalidArgument("invalid query output chunk")
	}
	endOrdinal := batch.FirstOrdinal + batch.ItemCount
	cursor := c.Cursor
	if c.ProgressPhase != watchpb.PROGRESS_PHASE_WAITING_FOR_SNAPSHOT && (batch.Sequence < cursor.Sequence ||
		(batch.Sequence == cursor.Sequence && endOrdinal <= cursor.NextOrdinal &&
			(!batch.Complete || cursor.Complete))) {
		return true, nil
	}
	if c.Status != watchpb.STATUS_RUNNING {
		return false, serviceerror.NewFailedPrecondition("watch is not accepting changes")
	}
	if c.DeadlineUnixNano != 0 && !ctx.Now(c).Before(time.Unix(0, c.DeadlineUnixNano)) {
		return false, serviceerror.NewFailedPrecondition("watch deadline has elapsed")
	}
	return false, c.validateContinuity(batch)
}

func (c *Component) validateContinuity(batch Batch) error {
	cursor := c.Cursor
	switch c.ProgressPhase {
	case watchpb.PROGRESS_PHASE_WAITING_FOR_SNAPSHOT:
		if !batch.Initial || batch.FirstOrdinal != 0 {
			return serviceerror.NewFailedPrecondition("watch requires an initial query snapshot")
		}
		return nil
	case watchpb.PROGRESS_PHASE_SNAPSHOTTING:
		if !batch.Initial || batch.Sequence != cursor.Sequence || batch.FirstOrdinal != cursor.NextOrdinal {
			return serviceerror.NewFailedPrecondition("query snapshot chunk gap requires recovery")
		}
		return nil
	case watchpb.PROGRESS_PHASE_LIVE:
		if batch.Initial {
			return serviceerror.NewFailedPrecondition("unexpected query snapshot requires an explicit reset")
		}
	default:
		return serviceerror.NewInternal("watch progress phase is invalid")
	}
	if cursor.Complete {
		if cursor.Sequence == math.MaxUint64 || batch.Sequence != cursor.Sequence+1 || batch.FirstOrdinal != 0 {
			return serviceerror.NewFailedPrecondition("query output gap requires recovery")
		}
	} else if batch.Sequence != cursor.Sequence || batch.FirstOrdinal != cursor.NextOrdinal {
		return serviceerror.NewFailedPrecondition("query output chunk gap requires recovery")
	}
	return nil
}

func (c *Component) Complete(
	ctx chasm.MutableContext,
	id string,
	attempt uint32,
	runID, terminalError string,
) error {
	field, ok := c.Intents[id]
	if !ok {
		return nil
	}
	intent := field.Get(ctx)
	if (intent.Status != watchpb.DELIVERY_STATUS_PENDING &&
		intent.Status != watchpb.DELIVERY_STATUS_AMBIGUOUS) ||
		(attempt != 0 && intent.Attempts != attempt) {
		return nil
	}
	intent.UpdatedAt = timestamppb.New(ctx.Now(c))
	intent.InFlight = false
	if terminalError != "" {
		intent.Error = terminalError
		intent.Status = watchpb.DELIVERY_STATUS_FAILED
		c.Failed++
		return nil
	}
	c.PendingBytes -= intentBytes(intent)
	intent.Request = nil
	intent.RunId = runID
	intent.Error = ""
	intent.Status = watchpb.DELIVERY_STATUS_DELIVERED
	c.Delivered++
	c.PendingIds = slices.DeleteFunc(c.PendingIds, func(pending string) bool { return pending == id })
	c.RecentIds = append(c.RecentIds, id)
	c.RecentBytes += uint64(proto.Size(intent.Output))
	for len(c.RecentIds) > int(c.MaxRecent) || c.RecentBytes > c.MaxPendingBytes {
		c.RecentBytes -= uint64(proto.Size(c.Intents[c.RecentIds[0]].Get(ctx).Output))
		delete(c.Intents, c.RecentIds[0])
		c.RecentIds = c.RecentIds[1:]
	}
	if len(c.PendingIds) > 0 {
		ctx.AddTask(c, chasm.TaskAttributes{}, &watchpb.DispatchTask{ActionId: c.PendingIds[0]})
	}
	return nil
}

func (c *Component) BeginDispatch(ctx chasm.MutableContext, id string) (*watchpb.Intent, error) {
	field, ok := c.Intents[id]
	if !ok {
		return nil, nil
	}
	intent := field.Get(ctx)
	if intent.Status != watchpb.DELIVERY_STATUS_PENDING ||
		len(c.PendingIds) == 0 || c.PendingIds[0] != id {
		return nil, nil
	}
	uncertain := intent.InFlight || intent.Uncertain
	if intent.RetryAuthorized {
		uncertain = false
	}
	if uncertain && !intent.SafeRetry {
		c.Ambiguous(ctx, id, intent.Attempts)
		return nil, nil
	}
	if intent.Attempts == math.MaxUint32 {
		return nil, serviceerror.NewFailedPrecondition("watch delivery attempt limit reached")
	}
	intent.RetryAuthorized = false
	intent.Uncertain = uncertain
	intent.InFlight = true
	intent.Attempts++
	intent.UpdatedAt = timestamppb.New(ctx.Now(c))
	return common.CloneProto(intent), nil
}

func (c *Component) Ambiguous(ctx chasm.MutableContext, id string, attempt uint32) {
	field, ok := c.Intents[id]
	if !ok {
		return
	}
	intent := field.Get(ctx)
	if intent.Status != watchpb.DELIVERY_STATUS_PENDING || intent.Attempts != attempt {
		return
	}
	intent.Status = watchpb.DELIVERY_STATUS_AMBIGUOUS
	intent.Error = "workflow RPC outcome is uncertain; explicit acknowledgment is required before retry"
	intent.Uncertain = true
	intent.InFlight = false
	intent.UpdatedAt = timestamppb.New(ctx.Now(c))
}

func (c *Component) retryIntent(ctx chasm.Context, id string, acknowledgeAmbiguous bool) (*watchpb.Intent, error) {
	field, ok := c.Intents[id]
	if !ok {
		return nil, serviceerror.NewNotFound("watch delivery not found")
	}
	intent := field.Get(ctx)
	if intent.Status != watchpb.DELIVERY_STATUS_FAILED &&
		intent.Status != watchpb.DELIVERY_STATUS_AMBIGUOUS {
		return nil, serviceerror.NewFailedPrecondition("watch delivery is not blocked or ambiguous")
	}
	if intent.Status == watchpb.DELIVERY_STATUS_AMBIGUOUS && !acknowledgeAmbiguous {
		return nil, serviceerror.NewFailedPrecondition("retry requires explicit acknowledgment of the uncertain effect")
	}
	return intent, nil
}

func (c *Component) authorizeRetry(ctx chasm.MutableContext, intent *watchpb.Intent, acknowledgeAmbiguous bool) {
	intent.Status = watchpb.DELIVERY_STATUS_PENDING
	intent.RetryAuthorized = acknowledgeAmbiguous
	intent.Error = ""
	intent.UpdatedAt = timestamppb.New(ctx.Now(c))
	if len(c.PendingIds) > 0 && c.PendingIds[0] == intent.ActionId {
		ctx.AddTask(c, chasm.TaskAttributes{}, &watchpb.DispatchTask{ActionId: intent.ActionId})
	}
}

func intentBytes(intent *watchpb.Intent) uint64 {
	return uint64(len(intent.Request)) + uint64(proto.Size(intent.Output))
}

func ActionID(identity ActionIdentity) string {
	var data []byte
	for _, value := range []string{identity.Namespace, identity.WatchID, identity.Incarnation, identity.RowKey} {
		data = binary.BigEndian.AppendUint64(data, uint64(len(value)))
		data = append(data, value...)
	}
	data = binary.BigEndian.AppendUint64(data, identity.Revision)
	data = binary.BigEndian.AppendUint64(data, identity.Sequence)
	data = binary.BigEndian.AppendUint64(data, identity.Ordinal)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func prepareIntent(namespace, id string, request proto.Message) (*watchpb.Intent, error) {
	intent := &watchpb.Intent{
		ActionId: id,
		Status:   watchpb.DELIVERY_STATUS_PENDING,
	}
	switch request := request.(type) {
	case *workflowservice.StartWorkflowExecutionRequest:
		if request == nil || request.WorkflowId == "" || request.GetWorkflowType().GetName() == "" || request.GetTaskQueue().GetName() == "" {
			return nil, serviceerror.NewInvalidArgument("watch start requires workflow ID, type, and task queue")
		}
		cloned := common.CloneProto(request)
		if err := ValidateStartPolicies(cloned.WorkflowIdReusePolicy, cloned.WorkflowIdConflictPolicy); err != nil {
			return nil, err
		}
		intent.SafeRetry = cloned.WorkflowIdReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		cloned.Namespace, cloned.RequestId = namespace, id
		intent.Kind, intent.WorkflowId = watchpb.ACTION_KIND_START, cloned.WorkflowId
		encoded, err := proto.Marshal(cloned)
		if err != nil {
			return nil, err
		}
		intent.Request = encoded
	case *workflowservice.SignalWorkflowExecutionRequest:
		if request == nil || request.SignalName == "" ||
			request.GetWorkflowExecution().GetWorkflowId() == "" || request.GetWorkflowExecution().GetRunId() == "" {
			return nil, serviceerror.NewInvalidArgument("watch signal requires signal name and a resolved workflow run")
		}
		cloned := common.CloneProto(request)
		cloned.Namespace, cloned.RequestId = namespace, id
		intent.Kind = watchpb.ACTION_KIND_SIGNAL
		intent.SafeRetry = true
		intent.WorkflowId, intent.TargetRunId = cloned.WorkflowExecution.WorkflowId, cloned.WorkflowExecution.RunId
		encoded, err := proto.Marshal(cloned)
		if err != nil {
			return nil, err
		}
		intent.Request = encoded
	case *workflowservice.SignalWithStartWorkflowExecutionRequest:
		if request == nil || request.WorkflowId == "" || request.SignalName == "" ||
			request.GetWorkflowType().GetName() == "" || request.GetTaskQueue().GetName() == "" {
			return nil, serviceerror.NewInvalidArgument("watch signal-with-start requires workflow ID, type, task queue, and signal name")
		}
		cloned := common.CloneProto(request)
		if err := ValidateStartPolicies(cloned.WorkflowIdReusePolicy, cloned.WorkflowIdConflictPolicy); err != nil {
			return nil, err
		}
		cloned.Namespace, cloned.RequestId = namespace, id
		intent.Kind, intent.WorkflowId = watchpb.ACTION_KIND_SIGNAL_WITH_START, cloned.WorkflowId
		encoded, err := proto.Marshal(cloned)
		if err != nil {
			return nil, err
		}
		intent.Request = encoded
	default:
		return nil, serviceerror.NewInvalidArgument("unsupported watch action")
	}
	return intent, nil
}

func ValidateStartPolicies(reuse enumspb.WorkflowIdReusePolicy, conflict enumspb.WorkflowIdConflictPolicy) error {
	switch reuse {
	case enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE:
	default:
		return serviceerror.NewInvalidArgument("watch workflow reuse policy is unsupported or could terminate an existing workflow")
	}
	switch conflict {
	case enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING:
	default:
		return serviceerror.NewInvalidArgument("watch workflow conflict policy is unsupported or could terminate an existing workflow")
	}
	return nil
}
