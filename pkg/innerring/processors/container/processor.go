package container

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// AlphabetState is a callback interface for inner ring global state.
	AlphabetState interface {
		IsAlphabet() bool
	}

	// Processor of events produced by container contract in FS chain.
	Processor struct {
		log           *zap.Logger
		pool          *ants.Pool
		objectPool    *ants.Pool
		alphabetState AlphabetState
		cnrClient     *container.Client // notary must be enabled
		netState      NetworkState
		metaEnabled   bool
		allowEC       bool
	}

	// Params of the processor constructor.
	Params struct {
		Log             *zap.Logger
		PoolSize        int
		AlphabetState   AlphabetState
		ContainerClient *container.Client
		NetworkState    NetworkState
		MetaEnabled     bool
		AllowEC         bool
	}
)

// NetworkState is an interface of a component
// that provides access to network state.
type NetworkState interface {
	// Epoch must return the number of the current epoch.
	//
	// Must return any error encountered
	// which did not allow reading the value.
	Epoch() (uint64, error)

	// HomomorphicHashDisabled must return boolean that
	// represents homomorphic network state:
	// 	* true if hashing is disabled;
	// 	* false if hashing is enabled.
	//
	// which did not allow reading the value.
	HomomorphicHashDisabled() (bool, error)

	// NetMap must return actual network map.
	NetMap() (*netmap.NetMap, error)

	// GetEpochBlock returns FS chain height when given NeoFS epoch was ticked.
	GetEpochBlock(epoch uint64) (uint32, error)
}

// New creates a container contract processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/container: logger is not set")
	case p.AlphabetState == nil:
		return nil, errors.New("ir/container: global state is not set")
	case p.ContainerClient == nil:
		return nil, errors.New("ir/container: Container client is not set")
	case p.NetworkState == nil:
		return nil, errors.New("ir/container: network state is not set")
	}

	p.Log.Debug("container worker pool", zap.Int("size", p.PoolSize))

	pool, err := ants.NewPool(p.PoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/container: can't create worker pool: %w", err)
	}

	const objectPoolSize = 1024
	objectPool, _ := ants.NewPool(objectPoolSize)

	return &Processor{
		log:           p.Log,
		pool:          pool,
		objectPool:    objectPool,
		alphabetState: p.AlphabetState,
		cnrClient:     p.ContainerClient,
		netState:      p.NetworkState,
		metaEnabled:   p.MetaEnabled,
		allowEC:       p.AllowEC,
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (cp *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	return nil
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (cp *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	return nil
}

// ListenerNotaryParsers for the 'event.Listener' notary event producer.
func (cp *Processor) ListenerNotaryParsers() []event.NotaryParserInfo {
	var (
		p event.NotaryParserInfo

		pp = make([]event.NotaryParserInfo, 0, 9)
	)

	p.SetScriptHash(cp.cnrClient.ContractAddress())

	// container put
	p.SetRequestType(containerEvent.PutNotaryEvent)
	p.SetParser(containerEvent.ParsePutNotary)
	pp = append(pp, p)

	// container named put
	p.SetRequestType(containerEvent.PutNamedNotaryEvent)
	p.SetParser(containerEvent.ParsePutNamedNotary)
	pp = append(pp, p)

	// create container
	p.SetRequestType(fschaincontracts.CreateContainerMethod)
	p.SetParser(containerEvent.RestoreCreateContainerRequest)
	pp = append(pp, p)

	p.SetRequestType(fschaincontracts.CreateContainerV2Method)
	p.SetParser(containerEvent.RestoreCreateContainerV2Request)
	pp = append(pp, p)

	// container delete
	p.SetRequestType(containerEvent.DeleteNotaryEvent)
	p.SetParser(containerEvent.ParseDeleteNotary)
	pp = append(pp, p)

	// remove container
	p.SetRequestType(fschaincontracts.RemoveContainerMethod)
	p.SetParser(containerEvent.RestoreRemoveContainerRequest)
	pp = append(pp, p)

	// set EACL
	p.SetRequestType(containerEvent.SetEACLNotaryEvent)
	p.SetParser(containerEvent.ParseSetEACLNotary)
	pp = append(pp, p)

	// put eACL
	p.SetRequestType(fschaincontracts.PutContainerEACLMethod)
	p.SetParser(containerEvent.RestorePutContainerEACLRequest)
	pp = append(pp, p)

	// announce load
	p.SetRequestType(fschaincontracts.PutContainerReportMethod)
	p.SetParser(containerEvent.ParsePutReport)
	pp = append(pp, p)

	// object put meta data
	p.SetRequestType(containerEvent.ObjectPutNotaryEvent)
	p.SetParser(containerEvent.ParseObjectPut)
	pp = append(pp, p)

	// migrate protobuf->struct
	p.SetRequestType(fschaincontracts.AddContainerStructsMethod)
	p.SetParser(containerEvent.RestoreAddStructsRequest)
	pp = append(pp, p)

	return pp
}

// ListenerNotaryHandlers for the 'event.Listener' notary event producer.
func (cp *Processor) ListenerNotaryHandlers() []event.NotaryHandlerInfo {
	var (
		h event.NotaryHandlerInfo

		hh = make([]event.NotaryHandlerInfo, 0, 9)
	)

	h.SetScriptHash(cp.cnrClient.ContractAddress())

	// container put
	h.SetRequestType(containerEvent.PutNotaryEvent)
	h.SetHandler(cp.handlePut)
	hh = append(hh, h)

	// container named put (same handler)
	h.SetRequestType(containerEvent.PutNamedNotaryEvent)
	hh = append(hh, h)

	// create container
	h.SetRequestType(fschaincontracts.CreateContainerMethod)
	h.SetHandler(cp.handlePut)
	hh = append(hh, h)

	h.SetRequestType(fschaincontracts.CreateContainerV2Method)
	h.SetHandler(cp.handleCreationRequest)
	hh = append(hh, h)

	// container delete
	h.SetRequestType(containerEvent.DeleteNotaryEvent)
	h.SetHandler(cp.handleDelete)
	hh = append(hh, h)

	// remove container
	h.SetRequestType(fschaincontracts.RemoveContainerMethod)
	h.SetHandler(cp.handleDelete)
	hh = append(hh, h)

	// set eACL
	h.SetRequestType(containerEvent.SetEACLNotaryEvent)
	h.SetHandler(cp.handleSetEACL)
	hh = append(hh, h)

	// put eACL
	h.SetRequestType(fschaincontracts.PutContainerEACLMethod)
	h.SetHandler(cp.handleSetEACL)
	hh = append(hh, h)

	// announce load
	h.SetRequestType(fschaincontracts.PutContainerReportMethod)
	h.SetHandler(cp.handleAnnounceLoad)
	hh = append(hh, h)

	// object put meta data
	h.SetRequestType(containerEvent.ObjectPutNotaryEvent)
	h.SetHandler(cp.handleObjectPut)
	hh = append(hh, h)

	// migrate protobuf->struct
	h.SetRequestType(fschaincontracts.AddContainerStructsMethod)
	h.SetHandler(func(ev event.Event) {
		cp.log.Info("received notary tx migrating containers' protobuf->struct, signing...")

		req := ev.(containerEvent.AddStructsRequest)
		err := cp.cnrClient.Morph().NotarySignAndInvokeTX(&req.MainTransaction, false)
		if err != nil {
			cp.log.Error("failed to sign notary tx migrating containers' protobuf->struct", zap.Error(err))
			return
		}

		cp.log.Info("notary tx migrating containers' protobuf->struct signed successfully")
	})
	hh = append(hh, h)

	return hh
}

// TimersHandlers for the 'Timers' event producer.
func (cp *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}

// AddContainerStructs iteratively calls the contract to add structured storage
// items for containers.
func (cp *Processor) AddContainerStructs(ctx context.Context) error {
	cp.log.Info("structuring containers in the contract...")

	cnrContract := cp.cnrClient.ContractAddress()
	fsChain := cp.cnrClient.Morph()
	for ; ; time.Sleep(5 * time.Second) {
		txRes, err := fsChain.CallNotary(ctx, cnrContract, fschaincontracts.AddContainerStructsMethod)
		if err != nil {
			if !errors.Is(err, neorpc.ErrInsufficientFunds) {
				return fmt.Errorf("notary call %s contract method: %w", fschaincontracts.AddContainerStructsMethod, err)
			}

			cp.log.Warn("not enough GAS for notary call, will try again later",
				zap.String("method", fschaincontracts.AddContainerStructsMethod), zap.Error(err))
			continue
		}

		txs := txRes.Container.StringLE()

		if !txRes.VMState.HasFlag(vmstate.Halt) {
			cp.log.Warn("non-HALT VM state, will try again later",
				zap.String("method", fschaincontracts.AddContainerStructsMethod), zap.Stringer("state", txRes.VMState),
				zap.String("exception", txRes.FaultException), zap.String("tx", txs))
			continue
		}

		if len(txRes.Stack) == 0 {
			return fmt.Errorf("empty stack in %s call result, tx %s", fschaincontracts.AddContainerStructsMethod, txs)
		}

		b, err := txRes.Stack[0].TryBool()
		if err != nil {
			return fmt.Errorf("convert stack item in %s call result to bool (tx %s); %w", fschaincontracts.AddContainerStructsMethod, txs, err)
		}

		if !b {
			cp.log.Warn("all containers have been successfully structured in the contract, interrupt", zap.String("tx", txs))
			return nil
		}

		cp.log.Info("more containers have been successfully structured in the contract, continue", zap.String("tx", txs))
	}
}
