package api

import (
	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
)

func drasiMethodMetadata(fullMethod string) MethodMetadata {
	policy, ok := validation.Policy(fullMethod)
	if !ok {
		return MethodMetadata{Scope: ScopeUnknown, Access: AccessUnknown}
	}
	switch policy.Access {
	case watchpb.Access_ACCESS_NAMESPACE_READ:
		return MethodMetadata{Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone}
	case watchpb.Access_ACCESS_NAMESPACE_WRITE:
		return MethodMetadata{Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone}
	case watchpb.Access_ACCESS_CLUSTER_READ:
		return MethodMetadata{Scope: ScopeCluster, Access: AccessReadOnly, Polling: PollingNone}
	case watchpb.Access_ACCESS_CLUSTER_ADMIN:
		return MethodMetadata{Scope: ScopeCluster, Access: AccessAdmin, Polling: PollingNone}
	default:
		return MethodMetadata{Scope: ScopeUnknown, Access: AccessUnknown}
	}
}
