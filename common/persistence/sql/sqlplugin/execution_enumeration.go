package sqlplugin

import "context"

// ExecutionKeyPage walks the executions primary key in storage order.
// It is restartable enumeration, not an MVCC snapshot.
type ExecutionKeyPage struct {
	ShardID int32
	After   *ExecutionsFilter
	Limit   int
}

// ExecutionKeyReader is additive so older/custom plugins fail explicitly.
type ExecutionKeyReader interface {
	SelectExecutionKeys(context.Context, ExecutionKeyPage) ([]ExecutionsRow, error)
}
