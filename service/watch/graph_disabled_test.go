//go:build !temporal_with_drasi || !cgo

package watch

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/watch/native"
)

func TestGraphDisabledCannotAcquireOwnership(t *testing.T) {
	t.Parallel()
	graph, err := OpenGraph(testcontext.For(t), GraphOptions{Assignments: &graphAssignmentClient{}})
	require.Nil(t, graph)
	require.ErrorIs(t, err, native.ErrDisabled)
}
