package main

import (
	"go/format"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestRewriteChasmEnumDefaults(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	state := "package state\n\ntype Kind int32\n\nconst KIND_UNSPECIFIED Kind = 0\n"
	service := "package state\n\nfunc KindDefault() Kind { return Kind_KIND_UNSPECIFIED }\n\nfunc ExternalDefault() external.Kind { return external.Kind_KIND_UNSPECIFIED }\n"
	external := "package external\n\nfunc KindDefault() Kind { return Kind_KIND_UNSPECIFIED }\n"
	require.NoError(t, os.WriteFile(filepath.Join(root, "state.pb.go"), []byte(state), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "service.pb.go"), []byte(service), 0600))
	other := filepath.Join(root, "other")
	require.NoError(t, os.Mkdir(other, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(other, "service.pb.go"), []byte(external), 0600))
	require.NoError(t, rewriteChasmEnumDefaults(root))
	actual, err := os.ReadFile(filepath.Join(root, "service.pb.go"))
	require.NoError(t, err)
	expected, err := format.Source([]byte("package state\n\nfunc KindDefault() Kind { return KIND_UNSPECIFIED }\n\nfunc ExternalDefault() external.Kind { return external.Kind_KIND_UNSPECIFIED }\n"))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	actual, err = os.ReadFile(filepath.Join(other, "service.pb.go"))
	require.NoError(t, err)
	require.Equal(t, external, string(actual))
	require.NoError(t, rewriteChasmEnumDefaults(root))
	actual, err = os.ReadFile(filepath.Join(root, "service.pb.go"))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestChasmOutputPackages(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		packages []string
		want     string
	}{
		{
			name:     "unversioned declaration retains source version",
			packages: []string{"go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb;assignmentpb"},
			want:     "chasm/lib/watchassignment/gen/assignmentpb/v1",
		},
		{
			name: "declared package is not inferred from library name",
			packages: []string{
				"go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1;assignmentpb",
				"go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1;assignmentpb",
			},
			want: "chasm/lib/watchassignment/gen/assignmentpb/v1",
		},
		{name: "missing declaration", packages: []string{""}},
		{name: "external declaration", packages: []string{"example.com/other"}},
		{name: "unclean declaration", packages: []string{"go.temporal.io/server/chasm/lib/../outside"}},
		{name: "conflicting declarations", packages: []string{
			"go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1",
			"go.temporal.io/server/chasm/lib/watchassignment/gen/otherpb/v1",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			set := &descriptorpb.FileDescriptorSet{}
			for i, value := range test.packages {
				set.File = append(set.File, &descriptorpb.FileDescriptorProto{
					Name:    proto.String("chasm/lib/watchassignment/proto/v1/" + []string{"state.proto", "service.proto"}[i]),
					Options: &descriptorpb.FileOptions{GoPackage: proto.String(value)},
				})
			}
			encoded, err := proto.Marshal(set)
			require.NoError(t, err)
			output, err := chasmOutputPackages(encoded)
			if test.want == "" {
				require.Error(t, err)
				require.Nil(t, output)
				return
			}
			require.NoError(t, err)
			require.Equal(t, map[string]string{"chasm/lib/watchassignment/proto/v1": test.want}, output)
		})
	}
}
