//go:build temporal_with_drasi && cgo

package native

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatelessQueryCompiler(t *testing.T) {
	t.Parallel()
	for _, language := range []Language{Cypher, GQL} {
		for _, tc := range []struct {
			name  string
			text  string
			valid bool
			code  Code
		}{
			{name: "unregistered projection", text: "MATCH (n:Record) RETURN n.id AS id", valid: true},
			{name: "aggregate", text: "MATCH (n:Record) RETURN count(n) AS total", valid: true},
			{name: "language functions", text: "MATCH (n:Record) RETURN toUpper(n.name) AS name", valid: true},
			{name: "invalid syntax", text: "MATCH ("},
			{name: "empty", code: CodeInvalidArgument},
			{name: "oversized", text: strings.Repeat("x", 65_537), code: CodeInvalidArgument},
		} {
			// Compilation shares a bounded process-wide pool, so cases run sequentially.
			t.Run(string(language)+"/"+tc.name, func(t *testing.T) {
				valid, err := ValidateQuery(t.Context(), language, tc.text)
				if tc.code != 0 {
					nativeError, ok := errors.AsType[*Error](err)
					require.True(t, ok)
					require.Equal(t, tc.code, nativeError.Code)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.valid, valid)
			})
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := ValidateQuery(ctx, Cypher, "MATCH (n) RETURN n")
	require.ErrorIs(t, err, context.Canceled)
	_, err = ValidateQuery(t.Context(), Language("unknown"), "MATCH (n) RETURN n")
	nativeError, ok := errors.AsType[*Error](err)
	require.True(t, ok)
	require.Equal(t, CodeInvalidArgument, nativeError.Code)
}

func TestCompilationDoesNotChangeRetainedQueryOutput(t *testing.T) {
	t.Parallel()
	runtime := openRuntime(t, configuration(workspace(t)))
	_, err := runtime.Apply(t.Context(), feedCommand(1, 15))
	require.NoError(t, err)
	before := requireSnapshot(t, runtime, `[{"name":"alpha","doubled":30}]`)
	valid, err := ValidateQuery(t.Context(), Cypher, "MATCH (n:Item) RETURN n.amount AS value")
	require.NoError(t, err)
	require.True(t, valid)
	valid, err = ValidateQuery(t.Context(), GQL, "MATCH (")
	require.NoError(t, err)
	require.False(t, valid)
	after := requireSnapshot(t, runtime, `[{"name":"alpha","doubled":30}]`)
	require.Equal(t, before, after)
}
