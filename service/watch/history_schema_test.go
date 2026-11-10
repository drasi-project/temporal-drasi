package watch

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/executionfeed"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/watch/native"
)

type graphSchemaField struct {
	Path        string   `json:"path"`
	Type        string   `json:"type"`
	Optional    bool     `json:"optional"`
	Description string   `json:"description"`
	Format      string   `json:"format"`
	Enum        []string `json:"enum"`
	Definition  string   `json:"definition"`
}

func TestTemporalGraphSchemaMatchesProjection(t *testing.T) {
	t.Parallel()
	payload, err := os.ReadFile("../../contracts/watch/schema/temporal-execution-state.json")
	require.NoError(t, err)
	var schema struct {
		APIVersion     string             `json:"apiVersion"`
		SchemaVersion  int                `json:"schemaVersion"`
		IdentityFields []graphSchemaField `json:"identityFields"`
		Nodes          []struct {
			Label  string             `json:"label"`
			Fields []graphSchemaField `json:"fields"`
		} `json:"nodes"`
		Relationships []struct {
			Label string `json:"label"`
			From  string `json:"from"`
			To    string `json:"to"`
		} `json:"relationships"`
		Definitions map[string]struct {
			Fields []graphSchemaField `json:"fields"`
		} `json:"definitions"`
	}
	require.NoError(t, json.Unmarshal(payload, &schema))
	require.Equal(t, "v1", schema.APIVersion)
	require.Equal(t, 1, schema.SchemaVersion)

	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	record := historyTestRecord()
	record.State = tasks.FeedState{
		Workflow: &tasks.FeedWorkflow{
			Type: "workflow-type", TaskQueue: "queue",
			State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			StartTime: &now, ExecutionTime: &now, CloseTime: &now, FirstRunID: "first", PreviousRunID: "previous",
			NewRunID: "new", ResetRunID: "reset", BaseRunID: "base", ParentNamespaceID: "parent-namespace",
			ParentWorkflowID: "parent-workflow", ParentRunID: "parent-run", CreateRequestID: "request",
			SearchAttributes: map[string]tasks.FeedSearchAttribute{
				"attribute": {Type: enumspb.INDEXED_VALUE_TYPE_INT, Value: json.RawMessage("9007199254740993")},
			},
		},
		Activities: []tasks.FeedActivity{{
			ID: "activity", ScheduledEventID: 3, Type: "activity-type", TaskQueue: "queue", Attempt: 2, Status: "STARTED",
			ScheduledTime: &now, StartedTime: &now, CompleteTime: &now, RetryTime: &now, LastFailureTime: &now,
			LastFailureKind: "TIMEOUT", LastTimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT, Paused: true, CancelRequested: true,
		}},
		Timers: []tasks.FeedTimer{{ID: "timer", StartedEventID: 4, Deadline: &now, Status: "SCHEDULED"}},
	}
	record.Facts = []tasks.FeedFact{{
		Entity: "ACTIVITY", ID: "activity", ScheduledEventID: 3, Attempt: 2,
		Transition: "FAILED", EventID: 5, Time: &now, FailureKind: "TIMEOUT", TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
	}}
	project := func(record executionfeed.FeedRecord) native.HistoryRecord {
		t.Helper()
		data, err := json.Marshal(record)
		require.NoError(t, err)
		image, err := historyRecord("source", native.HistoryScope{NamespaceID: record.NamespaceID, ShardCount: 1}, 1, 7, data)
		require.NoError(t, err)
		return image
	}
	image := project(record)
	standalone := historyTestRecord()
	activity := record.State.Activities[0]
	activity.Standalone, activity.ScheduledEventID = true, 0
	standalone.State = tasks.FeedState{Activities: []tasks.FeedActivity{activity}}
	standaloneImage := project(standalone)
	require.Len(t, standaloneImage.State, 1)
	require.Nil(t, standaloneImage.State[0].Relation)
	require.NotContains(t, standaloneImage.State[0].Properties["data"], "scheduled_event_id")

	properties := make(map[string]map[string]any)
	labels := make(map[string]string)
	relationships := make(map[string][2]string)
	for _, element := range append(slices.Clone(image.State), image.Facts...) {
		if element.Relation == nil {
			labels[element.ElementID] = element.Labels[0]
			properties[element.Labels[0]] = element.Properties
		}
	}
	for _, element := range image.State {
		if element.Relation != nil {
			relationships[element.Labels[0]] = [2]string{labels[element.Relation.InID], labels[element.Relation.OutID]}
		}
	}
	properties["Activity"]["data"].(map[string]any)["standalone"] = standaloneImage.State[0].Properties["data"].(map[string]any)["standalone"]
	require.ElementsMatch(t, []string{"Workflow", "Activity", "Timer", "Fact"}, slices.Collect(maps.Keys(properties)))
	require.Len(t, schema.Nodes, len(properties))
	require.Len(t, schema.Relationships, len(relationships))
	for _, relationship := range schema.Relationships {
		require.Equal(t, [2]string{relationship.From, relationship.To}, relationships[relationship.Label])
	}

	source, err := parser.ParseFile(token.NewFileSet(), "../history/tasks/execution_feed.go", nil, 0)
	require.NoError(t, err)
	tagPattern := regexp.MustCompile(`json:"([^",]+)([^"]*)"`)
	declarations := make(map[string]map[string]bool)
	for _, declaration := range source.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, specification := range group.Specs {
			definition, ok := specification.(*ast.TypeSpec)
			if !ok {
				continue
			}
			fields, ok := definition.Type.(*ast.StructType)
			if !ok {
				continue
			}
			declarations[definition.Name.Name] = make(map[string]bool)
			for _, field := range fields.Fields.List {
				if field.Tag == nil {
					continue
				}
				tag, err := strconv.Unquote(field.Tag.Value)
				require.NoError(t, err)
				match := tagPattern.FindStringSubmatch(tag)
				require.Len(t, match, 3)
				declarations[definition.Name.Name][match[1]] = strings.Contains(match[2], ",omitempty")
			}
		}
	}

	check := func(t *testing.T, fields []graphSchemaField, values map[string]any, declared map[string]bool, prefix string) {
		t.Helper()
		documented := make(map[string]bool)
		for _, field := range fields {
			require.True(t, strings.HasPrefix(field.Path, prefix), field.Path)
			name := strings.TrimPrefix(field.Path, prefix)
			require.NotContains(t, documented, name)
			documented[name] = field.Optional
			value, ok := values[name]
			require.True(t, ok, field.Path)
			require.NotEmpty(t, field.Description, field.Path)
			switch field.Type {
			case "string":
				require.IsType(t, "", value, field.Path)
			case "integer":
				number, ok := value.(json.Number)
				require.True(t, ok, field.Path)
				_, err := number.Int64()
				require.NoError(t, err, field.Path)
			case "boolean":
				require.IsType(t, false, value, field.Path)
			case "object":
				require.IsType(t, map[string]any{}, value, field.Path)
			case "json":
				require.Equal(t, json.Number("9007199254740993"), value, field.Path)
			default:
				t.Fatalf("unsupported documented type %q for %s", field.Type, field.Path)
			}
			if field.Format != "" {
				require.Equal(t, "date-time", field.Format)
				_, err := time.Parse(time.RFC3339, value.(string))
				require.NoError(t, err, field.Path)
			}
			if len(field.Enum) != 0 {
				require.Contains(t, field.Enum, value, field.Path)
			}
			if field.Definition != "" {
				require.Contains(t, schema.Definitions, field.Definition)
			}
		}
		require.Equal(t, declared, documented)
		require.ElementsMatch(t, slices.Collect(maps.Keys(documented)), slices.Collect(maps.Keys(values)))
	}
	for _, node := range schema.Nodes {
		t.Run(node.Label, func(t *testing.T) {
			t.Parallel()
			nodeProperties, ok := properties[node.Label]
			require.True(t, ok)
			identity := maps.Clone(nodeProperties)
			delete(identity, "data")
			check(t, schema.IdentityFields, identity, map[string]bool{
				"namespace_id": false, "workflow_id": false, "run_id": false,
			}, "")
			check(t, node.Fields, nodeProperties["data"].(map[string]any), declarations["Feed"+node.Label], "data.")
		})
	}
	t.Run("omitted optional fields", func(t *testing.T) {
		t.Parallel()
		for _, test := range []struct {
			label string
			value any
		}{
			{"Workflow", &tasks.FeedWorkflow{}},
			{"Activity", tasks.FeedActivity{}},
			{"Timer", tasks.FeedTimer{}},
			{"Fact", tasks.FeedFact{}},
		} {
			t.Run(test.label, func(t *testing.T) {
				t.Parallel()
				actual, err := historyData(test.value)
				require.NoError(t, err)
				var required []string
				for name, optional := range declarations["Feed"+test.label] {
					if !optional {
						required = append(required, name)
					}
				}
				require.ElementsMatch(t, required, slices.Collect(maps.Keys(actual)))
			})
		}
	})
	t.Run("typed search attributes", func(t *testing.T) {
		t.Parallel()
		attributes := properties["Workflow"]["data"].(map[string]any)["search_attributes"].(map[string]any)
		check(t, schema.Definitions["SearchAttribute"].Fields, attributes["attribute"].(map[string]any), declarations["FeedSearchAttribute"], "")
		for _, test := range []struct {
			name     string
			kind     enumspb.IndexedValueType
			payload  json.RawMessage
			expected any
		}{
			{"null", enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, json.RawMessage("null"), nil},
			{"text", enumspb.INDEXED_VALUE_TYPE_TEXT, json.RawMessage(`"text"`), "text"},
			{"keyword", enumspb.INDEXED_VALUE_TYPE_KEYWORD, json.RawMessage(`"keyword"`), "keyword"},
			{"integer", enumspb.INDEXED_VALUE_TYPE_INT, json.RawMessage("9007199254740993"), json.Number("9007199254740993")},
			{"double", enumspb.INDEXED_VALUE_TYPE_DOUBLE, json.RawMessage("1.5"), json.Number("1.5")},
			{"boolean", enumspb.INDEXED_VALUE_TYPE_BOOL, json.RawMessage("true"), true},
			{"datetime", enumspb.INDEXED_VALUE_TYPE_DATETIME, json.RawMessage(`"2026-01-02T03:04:05Z"`), "2026-01-02T03:04:05Z"},
			{"keyword list", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, json.RawMessage(`["first","second"]`), []any{"first", "second"}},
		} {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				actual, err := historyData(&tasks.FeedWorkflow{SearchAttributes: map[string]tasks.FeedSearchAttribute{
					"attribute": {Type: test.kind, Value: test.payload},
				}})
				require.NoError(t, err)
				require.Equal(t, map[string]any{
					"attribute": map[string]any{"type": test.kind.String(), "value": test.expected},
				}, actual["search_attributes"])
			})
		}
	})
	t.Run("enum string encodings", func(t *testing.T) {
		t.Parallel()
		values := func(names map[int32]string, encode func(int32) string, omitZero bool) []string {
			result := make([]string, 0, len(names))
			for value := range names {
				if !omitZero || value != 0 {
					result = append(result, encode(value))
				}
			}
			return result
		}
		expected := map[string][]string{
			"Workflow.data.state":             values(enumsspb.WorkflowExecutionState_name, func(v int32) string { return enumsspb.WorkflowExecutionState(v).String() }, false),
			"Workflow.data.status":            values(enumspb.WorkflowExecutionStatus_name, func(v int32) string { return enumspb.WorkflowExecutionStatus(v).String() }, false),
			"Activity.data.last_timeout_type": values(enumspb.TimeoutType_name, func(v int32) string { return enumspb.TimeoutType(v).String() }, true),
			"Fact.data.timeout_type":          values(enumspb.TimeoutType_name, func(v int32) string { return enumspb.TimeoutType(v).String() }, true),
			"SearchAttribute.type":            values(enumspb.IndexedValueType_name, func(v int32) string { return enumspb.IndexedValueType(v).String() }, false),
		}
		for _, node := range schema.Nodes {
			for _, field := range node.Fields {
				if encoded, ok := expected[node.Label+"."+field.Path]; ok {
					require.ElementsMatch(t, encoded, field.Enum, node.Label+"."+field.Path)
				}
			}
		}
		for _, field := range schema.Definitions["SearchAttribute"].Fields {
			if field.Path == "type" {
				require.ElementsMatch(t, expected["SearchAttribute.type"], field.Enum)
			}
		}
	})
}
