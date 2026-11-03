package watchregistry

import (
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestRegistryLimitValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*registrypb.Limits)
	}{
		{"zero resource capacity", func(l *registrypb.Limits) { l.ResourceIds = 0 }},
		{"unbounded resources", func(l *registrypb.Limits) { l.ResourceIds = 513 }},
		{"zero receipts", func(l *registrypb.Limits) { l.Mutations = 0 }},
		{"unbounded receipts", func(l *registrypb.Limits) { l.Mutations = 4097 }},
		{"small state", func(l *registrypb.Limits) { l.StateBytes = 1 }},
		{"large state", func(l *registrypb.Limits) { l.StateBytes = 2<<20 + 1 }},
		{"zero request bytes", func(l *registrypb.Limits) { l.RequestBytes = 0 }},
		{"large request bytes", func(l *registrypb.Limits) { l.RequestBytes = 128<<10 + 1 }},
		{"page cannot fit request", func(l *registrypb.Limits) { l.PageBytes = l.RequestBytes }},
		{"large pages", func(l *registrypb.Limits) { l.PageBytes = 512<<10 + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			limits := DefaultLimits()
			tc.change(limits)
			_, err := newComponent("namespace", limits)
			require.ErrorContains(t, err, "invalid watch registry limits")
		})
	}
	_, err := newComponent("namespace", nil)
	require.ErrorContains(t, err, "invalid watch registry limits")
}

func TestRegistryRequestLimitIsPersistedAndAtomic(t *testing.T) {
	t.Parallel()
	limits := DefaultLimits()
	limits.RequestBytes = 4096
	env := newEnvironment(t, limits)
	_, err := env.mutate(env.createSource("source", "original"))
	require.NoError(t, err)
	before := env.state(t)
	env.handler.limits = DefaultLimits()
	request := env.createSource("large", "large")
	request.GetCreateSource().Spec = &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{
		Plugin: &contractpb.PluginComponentRef{Digest: "sha256:" + strings.Repeat("a", 64), Component: "postgres"},
		Configuration: &structpb.Struct{Fields: map[string]*structpb.Value{
			"configuration": structpb.NewStringValue(strings.Repeat("x", 5000)),
		}},
	}}}
	_, err = env.mutate(request)
	require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
	protorequire.ProtoEqual(t, before, env.state(t))
}
