package replication

import (
	"context"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// CID is a type alias of
	// CID from refs package of neofs-api-go.
	CID = refs.CID

	// Object is a type alias of
	// Object from object package of neofs-api-go.
	Object = object.Object

	// OwnerID is a type alias of
	// OwnerID from object package of neofs-api-go.
	OwnerID = object.OwnerID

	// Address is a type alias of
	// Address from refs package of neofs-api-go.
	Address = refs.Address

	// ObjectVerificationParams groups the parameters of stored object verification.
	ObjectVerificationParams struct {
		Address
		Node         multiaddr.Multiaddr
		Handler      func(valid bool, obj *Object)
		LocalInvalid bool
	}

	// ObjectVerifier is an interface of stored object verifier.
	ObjectVerifier interface {
		Verify(ctx context.Context, params *ObjectVerificationParams) bool
	}

	// ObjectSource is an interface of the object storage with read access.
	ObjectSource interface {
		Get(ctx context.Context, addr Address) (*Object, error)
	}

	// ObjectStoreParams groups the parameters for object storing.
	ObjectStoreParams struct {
		*Object
		Nodes   []ObjectLocation
		Handler func(ObjectLocation, bool)
	}

	// ObjectReceptacle is an interface of object storage with write access.
	ObjectReceptacle interface {
		Put(ctx context.Context, params ObjectStoreParams) error
	}

	// ObjectCleaner Entity for removing object by address from somewhere
	ObjectCleaner interface {
		Del(Address) error
	}

	// ContainerActualityChecker is an interface of entity
	// for checking local node presence in container
	// Return true if no errors && local node is in container
	ContainerActualityChecker interface {
		Actual(ctx context.Context, cid CID) bool
	}

	// ObjectPool is a queue of objects selected for data audit.
	// It is updated once in epoch.
	ObjectPool interface {
		Update([]Address)
		Pop() (Address, error)
		Undone() int
	}

	// Scheduler returns slice of addresses for data audit.
	// These addresses put into ObjectPool.
	Scheduler interface {
		SelectForReplication(limit int) ([]Address, error)
	}

	// ReservationRatioReceiver is an interface of entity
	// for getting reservation ratio value of object by address.
	ReservationRatioReceiver interface {
		ReservationRatio(ctx context.Context, objAddr Address) (int, error)
	}

	// RemoteStorageSelector is an interface of entity
	// for getting remote nodes from placement for object by address
	// Result doesn't contain nodes from exclude list
	RemoteStorageSelector interface {
		SelectRemoteStorages(ctx context.Context, addr Address, excl ...multiaddr.Multiaddr) ([]ObjectLocation, error)
	}

	// MultiSolver is an interface that encapsulates other different utilities.
	MultiSolver interface {
		AddressStore
		RemoteStorageSelector
		ReservationRatioReceiver
		ContainerActualityChecker
		EpochReceiver
		WeightComparator
	}

	// ObjectLocator is an itnerface of entity
	// for building list current object remote nodes by address
	ObjectLocator interface {
		LocateObject(ctx context.Context, objAddr Address) ([]multiaddr.Multiaddr, error)
	}

	// WeightComparator is an itnerface of entity
	// for comparing weight by address of local node with passed node
	// returns -1 if local node is weightier or on error
	// returns 0 if weights are equal
	// returns 1 if passed node is weightier
	WeightComparator interface {
		CompareWeight(ctx context.Context, addr Address, node multiaddr.Multiaddr) int
	}

	// EpochReceiver is an interface of entity for getting current epoch number.
	EpochReceiver interface {
		Epoch() uint64
	}

	// ObjectLocation groups the information about object current remote location.
	ObjectLocation struct {
		Node          multiaddr.Multiaddr
		WeightGreater bool // true if Node field value has less index in placement vector than localhost
	}

	// ObjectLocationRecord groups the information about all current locations.
	ObjectLocationRecord struct {
		Address
		ReservationRatio int
		Locations        []ObjectLocation
	}

	// ReplicateTask groups the information about object replication task.
	// Task solver should not process nodes from exclude list,
	// Task solver should perform up to Shortage replications.
	ReplicateTask struct {
		Address
		Shortage     int
		ExcludeNodes []multiaddr.Multiaddr
	}

	// ReplicateResult groups the information about object replication task result.
	ReplicateResult struct {
		*ReplicateTask
		NewStorages []multiaddr.Multiaddr
	}

	// PresenceChecker is an interface of object storage with presence check access.
	PresenceChecker interface {
		Has(address Address) (bool, error)
	}

	// AddressStore is an interface of local peer's network address storage.
	AddressStore interface {
		SelfAddr() (multiaddr.Multiaddr, error)
	}
)

const (
	writeResultTimeout = "write result timeout"

	taskChanClosed = " process finish finish: task channel closed"
	ctxDoneMsg     = " process finish: context done"

	objectPoolPart               = "object pool"
	loggerPart                   = "logger"
	objectVerifierPart           = "object verifier"
	objectReceptaclePart         = "object receptacle"
	remoteStorageSelectorPart    = "remote storage elector"
	objectSourcePart             = "object source"
	reservationRatioReceiverPart = "reservation ratio receiver"
	objectLocatorPart            = "object locator"
	epochReceiverPart            = "epoch receiver"
	presenceCheckerPart          = "object presence checker"
	weightComparatorPart         = "weight comparator"
	addrStorePart                = "address store"
)

func instanceError(entity, part string) error {
	return errors.Errorf("could not instantiate %s: empty %s", entity, part)
}

func addressFields(addr Address) []zap.Field {
	return []zap.Field{
		zap.Stringer("oid", addr.ObjectID),
		zap.Stringer("cid", addr.CID),
	}
}
