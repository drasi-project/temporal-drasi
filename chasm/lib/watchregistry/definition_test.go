package watchregistry

import (
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestDefinitionIncarnationSeparatesLifecycleFromReplacement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		create    *registrypb.MutateRequest
		lifecycle *registrypb.MutateRequest
		update    *registrypb.MutateRequest
	}{
		{
			"source",
			&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CreateSource{CreateSource: &contractpb.CreateSourceRequest{
				Namespace: "test", Id: "source", RequestId: "create", Spec: sourceSpec(),
			}}},
			&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StartSource{StartSource: &contractpb.SourceOperationRequest{
				Namespace: "test", Id: "source", ExpectedRevision: 1, RequestId: "start",
			}}},
			&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateSource{UpdateSource: &contractpb.UpdateSourceRequest{
				Namespace: "test", Id: "source", ExpectedRevision: 2, RequestId: "update", Spec: sourceSpec(),
			}}},
		},
		{
			"query",
			&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CreateQuery{CreateQuery: &contractpb.CreateQueryRequest{
				Namespace: "test", Id: "query", RequestId: "create", Spec: querySpec("source", 1),
			}}},
			&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StartQuery{StartQuery: &contractpb.QueryOperationRequest{
				Namespace: "test", Id: "query", ExpectedRevision: 1, RequestId: "start",
			}}},
			&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateQuery{UpdateQuery: &contractpb.UpdateQueryRequest{
				Namespace: "test", Id: "query", ExpectedRevision: 2, RequestId: "update", Spec: querySpec("source", 1),
			}}},
		},
		{
			"watch",
			&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CreateWatch{CreateWatch: &contractpb.CreateWatchRequest{
				Namespace: "test", Id: "watch", RequestId: "create", Spec: watchSpec("query", 1),
			}}},
			&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_PauseWatch{PauseWatch: &contractpb.WatchOperationRequest{
				Namespace: "test", Id: "watch", ExpectedRevision: 1, RequestId: "pause",
			}}},
			&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateWatch{UpdateWatch: &contractpb.UpdateWatchRequest{
				Namespace: "test", Id: "watch", ExpectedRevision: 2, RequestId: "update", Spec: watchSpec("query", 1),
			}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			create, err := decodeMutation(tc.create)
			require.NoError(t, err)
			created, err := definition(create, nil, "test", 1)
			require.NoError(t, err)
			require.Equal(t, "1", created.Incarnation)
			before := common.CloneProto(created)
			lifecycle, err := decodeMutation(tc.lifecycle)
			require.NoError(t, err)
			changed, err := definition(lifecycle, created, "test", 2)
			require.NoError(t, err)
			require.Equal(t, "1", changed.Incarnation)
			require.Equal(t, uint64(2), resource(changed).GetRevision())
			protorequire.ProtoEqual(t, before, created)
			legacy := common.CloneProto(created)
			legacy.Incarnation = ""
			legacyChanged, err := definition(lifecycle, legacy, "test", 2)
			require.NoError(t, err)
			require.Empty(t, legacyChanged.Incarnation)
			update, err := decodeMutation(tc.update)
			require.NoError(t, err)
			replaced, err := definition(update, changed, "test", 3)
			require.NoError(t, err)
			require.Equal(t, "3", replaced.Incarnation)
			recreated, err := definition(create, nil, "test", 7)
			require.NoError(t, err)
			require.Equal(t, "7", recreated.Incarnation)
			require.Equal(t, uint64(7), resource(recreated).GetRevision())
		})
	}
}
