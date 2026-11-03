package sql

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

func (c *DbConn) SelectExecutionKeys(ctx context.Context, page sqlplugin.ExecutionKeyPage) ([]sqlplugin.ExecutionsRow, error) {
	reader, ok := c.DB.(sqlplugin.ExecutionKeyReader)
	if !ok {
		return nil, serviceerror.NewUnimplemented("SQL plugin does not support primary execution enumeration")
	}
	return reader.SelectExecutionKeys(ctx, page)
}
