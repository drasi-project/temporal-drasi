package watch

import (
	"context"

	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/watch/native"
)

func (v *DesiredView) NativeWatchBinding(id string) (native.WatchBinding, error) {
	watch, err := v.Watch(id)
	if err != nil {
		return native.WatchBinding{}, err
	}
	query, err := v.Query(watch.GetWatch().Spec.Query)
	if err != nil {
		return native.WatchBinding{}, err
	}
	if watch.Incarnation == "" || query.Incarnation == "" {
		return native.WatchBinding{}, serviceerror.NewFailedPrecondition("native watch binding requires retained definition lifetimes")
	}
	return native.WatchBinding{
		Watch: native.ResourceLifetime{
			NamespaceID: v.namespaceID, ResourceID: id, Incarnation: watch.Incarnation,
		},
		WatchRevision: watch.GetWatch().Revision,
		Query: native.ResourceLifetime{
			NamespaceID: v.namespaceID, ResourceID: query.GetQuery().Id, Incarnation: query.Incarnation,
		},
		QueryRevision: query.GetQuery().Revision,
	}, nil
}

// RestoreNativeBinding does not grant ownership or authorize lifecycle transitions.
// It recovers the original control definition, not the current lifecycle revision.
func (v *DesiredView) RestoreNativeBinding(
	ctx context.Context,
	client registrypb.RegistryServiceClient,
	observation native.SubscriptionObservation,
	ownerEpoch uint64,
) (NativeBinding, error) {
	if err := v.validateNativeBinding(observation, ownerEpoch); err != nil {
		return NativeBinding{}, err
	}
	binding := observation.Binding
	if observation.Deleted {
		return NativeBinding{}, serviceerror.NewFailedPrecondition("native watch binding is a tombstone")
	}
	current, err := v.Watch(binding.Watch.ResourceID)
	if err != nil {
		return NativeBinding{}, err
	}
	if current.Incarnation != binding.Watch.Incarnation || binding.WatchRevision > current.GetWatch().Revision {
		return NativeBinding{}, serviceerror.NewFailedPrecondition("native watch binding has a different definition lifetime")
	}
	if ref := current.GetWatch().Spec.Query; ref.GetId() != binding.Query.ResourceID || ref.GetRevision() != binding.QueryRevision {
		return NativeBinding{}, serviceerror.NewDataLoss("native watch binding does not match the current watch query pin")
	}
	return v.retainedNativeBinding(ctx, client, observation, ownerEpoch)
}

// Retired bindings still need their original control to close acceptance safely.
func (v *DesiredView) retainedNativeBinding(
	ctx context.Context,
	client registrypb.RegistryServiceClient,
	observation native.SubscriptionObservation,
	ownerEpoch uint64,
) (NativeBinding, error) {
	if err := v.validateNativeBinding(observation, ownerEpoch); err != nil {
		return NativeBinding{}, err
	}
	binding := observation.Binding
	reader := desiredReader{client: client, view: v, initialized: true}
	pin := definitionPin{resourceKey{registrypb.RESOURCE_KIND_WATCH, binding.Watch.ResourceID}, binding.WatchRevision}
	watch, ok := v.revisions[pin]
	if ok {
		watch = common.CloneProto(watch)
	} else {
		var err error
		watch, err = reader.lookup(ctx, pin)
		if err != nil {
			return NativeBinding{}, err
		}
	}
	queryRef := watch.GetWatch().Spec.Query
	if watch.Incarnation != binding.Watch.Incarnation || queryRef.GetId() != binding.Query.ResourceID ||
		queryRef.GetRevision() != binding.QueryRevision {
		return NativeBinding{}, serviceerror.NewDataLoss("native watch binding does not match the retained public definition")
	}
	query, err := v.Query(queryRef)
	if err != nil {
		query, err = reader.lookup(ctx, definitionPin{
			resourceKey: resourceKey{registrypb.RESOURCE_KIND_QUERY, queryRef.Id}, revision: queryRef.Revision,
		})
		if err != nil {
			return NativeBinding{}, err
		}
	}
	if query.Incarnation != binding.Query.Incarnation {
		return NativeBinding{}, serviceerror.NewDataLoss("native watch binding does not match the retained query lifetime")
	}
	return NativeBinding{
		Target: IntakeTarget{
			NamespaceID: v.namespaceID, ExecutionID: observation.Definition.ID,
			OwnerEpoch: ownerEpoch, Watch: watch.GetWatch(),
		},
		Subscription: observation.Definition,
	}, nil
}

func (v *DesiredView) validateNativeBinding(observation native.SubscriptionObservation, ownerEpoch uint64) error {
	if ownerEpoch == 0 {
		return serviceerror.NewInvalidArgument("native watch binding requires a current owner epoch")
	}
	binding := observation.Binding
	if binding == nil || binding.Watch.NamespaceID != v.namespaceID || binding.Query.NamespaceID != v.namespaceID ||
		binding.Watch.Incarnation == "" || binding.Query.Incarnation == "" ||
		binding.WatchRevision == 0 || binding.QueryRevision == 0 ||
		observation.Definition.ID == "" || observation.Definition.QueryID == "" || observation.Definition.QueryRevision != 1 {
		return serviceerror.NewDataLoss("native public watch binding is incomplete or has a different scope")
	}
	return nil
}
