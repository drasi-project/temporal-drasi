package native

import "encoding/json"

type HistoryScope struct {
	NamespaceID string `json:"namespace_id"`
	ShardCount  uint32 `json:"shard_count"`
}

type HistoryProgress struct {
	Revision uint64          `json:"revision"`
	Cursor   HistoryCursor   `json:"cursor"`
	Receipt  *HistoryReceipt `json:"receipt"`
}

type HistoryCursor struct {
	Phase               HistoryCursorPhase `json:"phase"`
	CaptureEpoch        int64              `json:"capture_epoch"`
	ReconstructionStart int64              `json:"reconstruction_start"`
	NextPosition        int64              `json:"next_position"`
	Scan                *HistoryScan       `json:"scan"`
}

type HistoryScan struct {
	Phase  HistoryScanPhase `json:"phase"`
	Token  string           `json:"token"`
	Cutoff int64            `json:"cutoff"`
}

type HistoryCursorPhase string

const (
	HistoryUnstarted HistoryCursorPhase = "unstarted"
	HistoryCapturing HistoryCursorPhase = "capturing"
)

type HistoryScanPhase string

const (
	HistoryScanning     HistoryScanPhase = "scanning"
	HistoryScanComplete HistoryScanPhase = "complete"
)

type HistoryReceipt struct {
	RequestID     string   `json:"request_id"`
	Revision      uint64   `json:"revision"`
	FirstPosition *uint64  `json:"first_position"`
	Head          uint64   `json:"head"`
	Digest        [32]byte `json:"digest"`
}

type HistoryStep struct {
	RequestID        string      `json:"request_id"`
	ShardID          uint32      `json:"shard_id"`
	ExpectedRevision uint64      `json:"expected_revision"`
	Page             HistoryPage `json:"page"`
}

type HistoryPage interface {
	json.Marshaler
	historyPage()
}

type HistoryBootstrapPage struct {
	CaptureEpoch        int64  `json:"capture_epoch"`
	ReconstructionStart int64  `json:"reconstruction_start"`
	NextToken           string `json:"next_token"`
	Cutoff              int64  `json:"cutoff"`
}

func (HistoryBootstrapPage) historyPage() {}

func (page HistoryBootstrapPage) MarshalJSON() ([]byte, error) {
	type fields HistoryBootstrapPage
	return json.Marshal(struct {
		Kind string `json:"kind"`
		fields
	}{Kind: "bootstrap", fields: fields(page)})
}

type HistoryRecordsPage struct {
	CaptureEpoch    int64           `json:"capture_epoch"`
	StartPosition   int64           `json:"start_position"`
	NextPosition    int64           `json:"next_position"`
	PublishedCutoff int64           `json:"published_cutoff"`
	Records         []HistoryRecord `json:"records"`
}

func (HistoryRecordsPage) historyPage() {}

func (page HistoryRecordsPage) MarshalJSON() ([]byte, error) {
	type fields HistoryRecordsPage
	value := fields(page)
	if value.Records == nil {
		value.Records = []HistoryRecord{}
	}
	return json.Marshal(struct {
		Kind string `json:"kind"`
		fields
	}{Kind: "records", fields: value})
}

type HistoryRecord struct {
	Position      int64          `json:"position"`
	CaptureEpoch  int64          `json:"capture_epoch"`
	NamespaceID   string         `json:"namespace_id"`
	ExecutionKey  string         `json:"execution_key"`
	EffectiveFrom uint64         `json:"effective_from"`
	State         []ElementImage `json:"state"`
	Facts         []ElementImage `json:"facts"`
}

type ElementImage struct {
	ElementID     string         `json:"element_id"`
	Labels        []string       `json:"labels"`
	EffectiveFrom uint64         `json:"effective_from"`
	Properties    map[string]any `json:"properties"`
	Relation      *Relation      `json:"relation,omitempty"`
}

type HistoryProgressCommand struct {
	SourceID string `json:"source_id"`
	ShardID  uint32 `json:"shard_id"`
}

type AcceptHistoryCommand struct {
	SourceID string      `json:"source_id"`
	Step     HistoryStep `json:"step"`
}

type BlockHistoryCommand struct {
	SourceID string `json:"source_id"`
	Problem  string `json:"problem"`
}

type SuspendHistoryCommand struct {
	SourceID string `json:"source_id"`
}

const MaxCommandBytes = 1 << 20

func CommandSize(command Command) (int, error) {
	payload, err := marshalCommand(command)
	return len(payload), err
}

func (SuspendHistoryCommand) operation() string { return "suspend_history" }

func (HistoryProgressCommand) operation() string { return "history_progress" }
func (AcceptHistoryCommand) operation() string   { return "accept_history" }
func (BlockHistoryCommand) operation() string    { return "block_history" }
