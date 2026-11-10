package authorization

import (
	"testing"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
)

func TestDrasiAuthorization(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		method    string
		namespace string
		claims    *Claims
		decision  Decision
	}{
		{"reader can preview", watchpb.WatchService_PreviewQuery_FullMethodName, "allowed",
			&Claims{Namespaces: map[string]Role{"allowed": RoleReader}}, DecisionAllow},
		{"reader can validate", watchpb.WatchService_ValidateQuery_FullMethodName, "allowed",
			&Claims{Namespaces: map[string]Role{"allowed": RoleReader}}, DecisionAllow},
		{"reader can list deliveries", watchpb.WatchService_ListDeliveries_FullMethodName, "allowed",
			&Claims{Namespaces: map[string]Role{"allowed": RoleReader}}, DecisionAllow},
		{"reader cannot retry deliveries", watchpb.WatchService_RetryDelivery_FullMethodName, "allowed",
			&Claims{Namespaces: map[string]Role{"allowed": RoleReader}}, DecisionDeny},
		{"writer can retry deliveries", watchpb.WatchService_RetryDelivery_FullMethodName, "allowed",
			&Claims{Namespaces: map[string]Role{"allowed": RoleWriter}}, DecisionAllow},
		{"reader cannot create", watchpb.WatchService_CreateWatch_FullMethodName, "allowed",
			&Claims{Namespaces: map[string]Role{"allowed": RoleReader}}, DecisionDeny},
		{"writer can create", watchpb.WatchService_CreateWatch_FullMethodName, "allowed",
			&Claims{Namespaces: map[string]Role{"allowed": RoleWriter}}, DecisionAllow},
		{"writer cannot cross namespace", watchpb.WatchService_CreateSource_FullMethodName, "different",
			&Claims{Namespaces: map[string]Role{"allowed": RoleWriter}}, DecisionDeny},
		{"namespace admin cannot install", watchpb.PluginService_InstallPlugin_FullMethodName, "",
			&Claims{Namespaces: map[string]Role{"allowed": RoleAdmin}}, DecisionDeny},
		{"system writer cannot load", watchpb.PluginService_LoadPlugin_FullMethodName, "",
			&Claims{System: RoleWriter}, DecisionDeny},
		{"system admin can install", watchpb.PluginService_InstallPlugin_FullMethodName, "",
			&Claims{System: RoleAdmin}, DecisionAllow},
		{"system reader can discover", watchpb.PluginService_ListPlugins_FullMethodName, "",
			&Claims{System: RoleReader}, DecisionAllow},
		{"anonymous cannot read capabilities", watchpb.WatchService_GetCapabilities_FullMethodName, "allowed",
			nil, DecisionDeny},
		{"unknown method denied", "/drasi.watch.v1.PluginService/Unknown", "",
			&Claims{System: RoleAdmin}, DecisionDeny},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result, err := NewDefaultAuthorizer().Authorize(t.Context(), tc.claims, &CallTarget{
				APIName: tc.method, Namespace: tc.namespace,
			})
			require.NoError(t, err)
			require.Equal(t, tc.decision, result.Decision)
		})
	}
}
