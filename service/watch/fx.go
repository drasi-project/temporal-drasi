package watch

import (
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

var Module = fx.Options(
	resource.Module,
	fx.Provide(
		NewService,
		controlpb.NewControlServiceLayeredClient,
		assignmentpb.NewAssignmentServiceLayeredClient,
		registrypb.NewRegistryServiceLayeredClient,
		registrypb.NewClusterPluginServiceLayeredClient,
		func(monitor membership.Monitor) (membership.ServiceResolver, error) {
			return monitor.GetResolver(primitives.WatchService)
		},
		func(dc *dynamicconfig.Collection) resource.ThrottledLoggerRpsFn {
			rate := dynamicconfig.WatchThrottledLogRPS.Get(dc)
			return func() float64 { return float64(rate()) }
		},
		func(dc *dynamicconfig.Collection) service.PersistenceRateLimitingParams {
			return service.PersistenceRateLimitingParams{
				PersistenceMaxQps:                  persistenceclient.PersistenceMaxQps(dynamicconfig.WatchPersistenceMaxQPS.Get(dc)),
				PersistenceNamespaceMaxQps:         func(string) int { return 0 },
				PersistencePerShardNamespaceMaxQPS: func(string) int { return 0 },
				OperatorRPSRatio:                   persistenceclient.OperatorRPSRatio(dynamicconfig.OperatorRPSRatio.Get(dc)),
				PersistenceBurstRatio:              persistenceclient.PersistenceBurstRatio(dynamicconfig.PersistenceQPSBurstRatio.Get(dc)),
				DynamicRateLimitingParams: func() dynamicconfig.DynamicRateLimitingParams {
					return dynamicconfig.DefaultDynamicRateLimitingParams
				},
			}
		},
		func(factory common.RPCFactory, dc *dynamicconfig.Collection, metrics metrics.Handler, logger log.Logger) (*grpc.Server, error) {
			options, err := factory.GetInternodeGRPCServerOptions()
			if err != nil {
				return nil, err
			}
			errors := interceptor.NewServiceErrorInterceptor(dynamicconfig.MaxServiceErrorMessageLength.Get(dc), metrics, logger)
			options = append(options,
				grpc.ChainUnaryInterceptor(errors.Intercept),
				grpc.MaxRecvMsgSize(validation.MaxRequestBytes),
				grpc.MaxSendMsgSize(validation.MaxRequestBytes),
			)
			return grpc.NewServer(options...), nil
		},
	),
	fx.Invoke(func(lifecycle fx.Lifecycle, service *Service) {
		lifecycle.Append(fx.StartStopHook(service.Start, service.Stop))
	}),
)
