package api

import (
	"testing"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestDrasiMethodMetadata(t *testing.T) {
	t.Parallel()
	for _, service := range []string{"WatchService", "PluginService"} {
		methods := watchpb.File_drasi_watch_v1_service_proto.Services().ByName(protoreflect.Name(service)).Methods()
		for i := 0; i < methods.Len(); i++ {
			method := methods.Get(i)
			t.Run(string(method.FullName()), func(t *testing.T) {
				t.Parallel()
				fullMethod := "/drasi.watch.v1." + service + "/" + string(method.Name())
				metadata := GetMethodMetadata(fullMethod)
				require.NotEqual(t, ScopeUnknown, metadata.Scope)
				require.NotEqual(t, AccessUnknown, metadata.Access)
				require.Equal(t, PollingNone, metadata.Polling)
				hasNamespace := method.Input().Fields().ByName("namespace") != nil
				require.Equal(t, hasNamespace, metadata.Scope == ScopeNamespace)
			})
		}
	}
}
