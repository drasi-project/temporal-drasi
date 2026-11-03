package watch

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/executionfeed"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/watch/native"
)

func historyRecord(source string, scope native.HistoryScope, shard int32, epoch int64, payload []byte) (native.HistoryRecord, error) {
	record, err := decodeHistoryRecord(scope, shard, epoch, payload)
	if err != nil {
		return native.HistoryRecord{}, err
	}
	task := record.ExecutionFeedTask
	identity, err := json.Marshal([3]string{task.NamespaceID, task.WorkflowID, task.RunID})
	if err != nil {
		return native.HistoryRecord{}, err
	}
	digest := sha256.Sum256(identity)
	result := native.HistoryRecord{
		Position: task.TaskID, CaptureEpoch: epoch, NamespaceID: task.NamespaceID,
		ExecutionKey: hex.EncodeToString(digest[:]), EffectiveFrom: uint64(task.VisibilityTimestamp.UnixMilli()),
		State: make([]native.ElementImage, 0), Facts: make([]native.ElementImage, 0),
	}
	prefix := "x:" + result.ExecutionKey + ":state:"
	image := func(id, label string, data map[string]any) native.ElementImage {
		return native.ElementImage{
			ElementID: id, Labels: []string{label}, EffectiveFrom: result.EffectiveFrom,
			Properties: map[string]any{
				"namespace_id": task.NamespaceID, "workflow_id": task.WorkflowID, "run_id": task.RunID, "data": data,
			},
		}
	}
	relation := func(id, label, parent, child string) native.ElementImage {
		element := image(id, label, map[string]any{})
		element.Relation = &native.Relation{
			InSource: source, InID: parent, OutSource: source, OutID: child,
		}
		return element
	}
	if workflow := task.State.Workflow; workflow != nil {
		data, err := historyData(workflow)
		if err != nil {
			return result, err
		}
		result.State = append(result.State, image(prefix+"workflow", "Workflow", data))
	}
	for _, activity := range task.State.Activities {
		if activity.ID == "" || (!activity.Standalone && (activity.ScheduledEventID <= 0 || task.State.Workflow == nil)) {
			return result, serviceerror.NewDataLoss("History activity has an invalid retained identity")
		}
		data, err := historyData(activity)
		if err != nil {
			return result, err
		}
		id := prefix + "activity:" + strconv.FormatInt(activity.ScheduledEventID, 10)
		result.State = append(result.State, image(id, "Activity", data))
		if !activity.Standalone {
			result.State = append(result.State, relation(id+":owner", "HAS_ACTIVITY", prefix+"workflow", id))
		}
	}
	for _, timer := range task.State.Timers {
		if timer.ID == "" || timer.StartedEventID <= 0 || task.State.Workflow == nil {
			return result, serviceerror.NewDataLoss("History timer has an invalid retained identity")
		}
		data, err := historyData(timer)
		if err != nil {
			return result, err
		}
		id := prefix + "timer:" + strconv.FormatInt(timer.StartedEventID, 10)
		result.State = append(result.State, image(id, "Timer", data),
			relation(id+":owner", "HAS_TIMER", prefix+"workflow", id))
	}
	for ordinal, fact := range task.Facts {
		data, err := historyData(fact)
		if err != nil {
			return result, err
		}
		id := "x:" + result.ExecutionKey + ":fact:" + strconv.FormatInt(task.TaskID, 10) + ":" + strconv.Itoa(ordinal)
		result.Facts = append(result.Facts, image(id, "Fact", data))
	}
	return result, nil
}

func decodeHistoryRecord(scope native.HistoryScope, shard int32, epoch int64, payload []byte) (*executionfeed.FeedRecord, error) {
	var record executionfeed.FeedRecord
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return nil, fmt.Errorf("decode committed History projection: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, serviceerror.NewDataLoss("History projection contains trailing JSON")
	}
	task := record.ExecutionFeedTask
	if task == nil || !task.ValidSchema() || task.SchemaVersion != 2 || task.CaptureGap ||
		task.CaptureEpoch != epoch || task.NamespaceID != scope.NamespaceID || record.ShardID != shard ||
		task.TaskID <= 0 || task.WorkflowID == "" || task.RunID == "" ||
		task.VisibilityTimestamp.IsZero() || task.VisibilityTimestamp.UnixMilli() < 0 {
		return nil, serviceerror.NewDataLoss("History projection has an invalid identity, capture session, schema or commit time")
	}
	if task.Deleted && (task.State.Workflow != nil || len(task.State.Activities) != 0 || len(task.State.Timers) != 0) {
		return nil, serviceerror.NewDataLoss("deleted History projection contains retained execution state")
	}
	return &record, nil
}

func historyData(value any) (map[string]any, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode History entity: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var data map[string]any
	if err := decoder.Decode(&data); err != nil {
		return nil, fmt.Errorf("decode History entity properties: %w", err)
	}
	switch entity := value.(type) {
	case *tasks.FeedWorkflow:
		data["state"], data["status"] = entity.State.String(), entity.Status.String()
		if attributes, ok := data["search_attributes"].(map[string]any); ok {
			for name, attribute := range entity.SearchAttributes {
				value, ok := attributes[name].(map[string]any)
				if !ok {
					return nil, serviceerror.NewDataLoss("History search attribute is not a typed value")
				}
				value["type"] = attribute.Type.String()
			}
		}
	case tasks.FeedActivity:
		if entity.LastTimeoutType != 0 {
			data["last_timeout_type"] = entity.LastTimeoutType.String()
		}
	case tasks.FeedFact:
		if entity.TimeoutType != 0 {
			data["timeout_type"] = entity.TimeoutType.String()
		}
	default:
	}
	return data, nil
}
