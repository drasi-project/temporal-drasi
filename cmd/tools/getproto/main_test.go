package main

import (
	"testing"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestDependencySetIncludesCompleteContract(t *testing.T) {
	t.Parallel()
	contract := watchpb.File_drasi_watch_v1_service_proto
	set := dependencySet(contract)
	files, err := protodesc.NewFiles(set)
	require.NoError(t, err)
	resolved, err := files.FindFileByPath(contract.Path())
	require.NoError(t, err)
	require.Equal(t, contract.Messages().Len(), resolved.Messages().Len())
	previous := ""
	for _, file := range set.File {
		require.Greater(t, file.GetName(), previous)
		previous = file.GetName()
	}
	first, err := proto.Marshal(set)
	require.NoError(t, err)
	second, err := proto.Marshal(dependencySet(emptypb.File_google_protobuf_empty_proto, contract, contract))
	require.NoError(t, err)
	require.Equal(t, first, second)
}
