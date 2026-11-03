package watch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/proto"
)

func recoverGraphOwnership(ctx context.Context, options GraphOptions, previous *assignmentpb.State) (_ *assignmentpb.State, failure error) {
	owner := previous.GetActive()
	grant := owner.GetGrant()
	if grant.GetStorageHostId() != options.StorageHostID {
		return nil, serviceerror.NewFailedPrecondition("Watch crash recovery requires the original storage host")
	}
	if err := checkGraphOwnership(previous, &assignmentpb.BeginRequest{
		NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID,
		StorageHostId: options.StorageHostID, ProcessBootId: grant.GetProcessBootId(),
		RequestId: owner.GetRequestId(), ExpectedVersion: previous.GetVersion() - 1,
	}); err != nil {
		return nil, err
	}
	configuration := graphConfiguration(options, grant.Epoch)
	configuration.StartMode = native.StartModeCheckpointRecovery
	retained, err := native.Open(ctx, configuration)
	if err != nil {
		if nativeError, ok := errors.AsType[*native.Error](err); ok && nativeError.Code == native.CodeConflict {
			return nil, serviceerror.NewFailedPrecondition("retained Watch owner cannot be fenced: " + nativeError.Message)
		}
		return nil, fmt.Errorf("fence retained Watch owner for recovery: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		failure = errors.Join(failure, retained.Close(cleanup))
	}()
	current, err := options.Assignments.Describe(ctx, &assignmentpb.DescribeRequest{
		NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID,
	})
	if err != nil {
		return nil, err
	}
	if !proto.Equal(current, previous) {
		return nil, serviceerror.NewFailedPrecondition("Watch assignment advanced while fencing its retained owner")
	}
	destination := graphDirectory(options.StateRoot, options.NamespaceID, "recoveries", grant.Epoch)
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return nil, fmt.Errorf("prepare Watch recovery checkpoint directory: %w", err)
	}
	result, err := retained.Apply(ctx, native.CheckpointCommand{Destination: destination})
	if err != nil {
		return nil, err
	}
	cut, err := graphCheckpointCut(destination, result.Checkpoint)
	if err != nil {
		return nil, err
	}
	if cut.InstanceId != grant.InstanceId || cut.OwnerEpoch != grant.Epoch ||
		result.Checkpoint.RetirementID == "" {
		return nil, serviceerror.NewDataLoss("native recovery checkpoint does not match the retained owner")
	}
	request := &assignmentpb.SealRequest{
		NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID,
		Grant: common.CloneProto(grant), ExpectedVersion: previous.Version,
		RequestId: "recover/" + result.Checkpoint.RetirementID, Checkpoint: cut,
	}
	current, sealErr := options.Assignments.Seal(ctx, request)
	if sealErr != nil {
		current, err = options.Assignments.Describe(ctx, &assignmentpb.DescribeRequest{
			NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID,
		})
		if err != nil {
			return nil, sealErr
		}
	}
	if graphSealMatches(current, request) {
		return common.CloneProto(current), nil
	}
	if sealed := current.GetSealed(); sealed != nil &&
		proto.Equal(sealed.GetPredecessor(), owner) && current.Version == previous.Version+1 {
		return common.CloneProto(current), nil
	}
	if sealErr != nil {
		return nil, sealErr
	}
	return nil, serviceerror.NewDataLoss("assignment did not confirm the recovered Watch checkpoint seal")
}
