package innerring

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	irsubnet "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/subnet"
	morphsubnet "github.com/nspcc-dev/neofs-node/pkg/morph/client/subnet"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	subnetevents "github.com/nspcc-dev/neofs-node/pkg/morph/event/subnet"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/subnet"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// IR server's component to handle Subnet contract notifications.
type subnetHandler struct {
	workerPool util.WorkerPool

	morphClient morphsubnet.Client

	putValidator irsubnet.PutValidator

	delValidator irsubnet.DeleteValidator
}

// configuration of subnet component.
type subnetConfig struct {
	queueSize uint32
}

// makes IR server to catch Subnet notifications from sidechain listener,
// and to release corresponding processing queue on stop.
func (s *Server) initSubnet(cfg subnetConfig) {
	s.registerStarter(func() error {
		var err error

		// initialize queue for processing of the events from Subnet contract
		s.subnetHandler.workerPool, err = ants.NewPool(int(cfg.queueSize), ants.WithNonblocking(true))
		if err != nil {
			return fmt.Errorf("subnet queue initialization: %w", err)
		}

		// initialize morph client of Subnet contract
		clientMode := morphsubnet.NotaryAlphabet

		if s.sideNotaryConfig.disabled {
			clientMode = morphsubnet.NonNotary
		}

		var initPrm morphsubnet.InitPrm

		initPrm.SetBaseClient(s.morphClient)
		initPrm.SetContractAddress(s.contracts.subnet)
		initPrm.SetMode(clientMode)

		err = s.subnetHandler.morphClient.Init(initPrm)
		if err != nil {
			return fmt.Errorf("init morph subnet client: %w", err)
		}

		s.listenSubnet()

		return nil
	})

	s.registerCloser(func() error {
		s.stopSubnet()
		return nil
	})
}

// releases the Subnet contract notification processing queue.
func (s *Server) stopSubnet() {
	s.workerPool.Release()
}

// names of listened notification events from Subnet contract.
const (
	// subnet creation
	subnetCreateEvName = "put"
	// subnet removal
	subnetRemoveEvName = "delete"
	// subnet creation (notary)
	notarySubnetCreateEvName = "Put"
)

// makes IR server to listen notifications of Subnet contract.
// All required resources must be initialized before (initSubnet).
// Works in one of two modes (configured): notary and non-notary.
//
// All handlers are executed only if local node is an alphabet one.
//
// Events (notary):
//   * put (parser: subnetevents.ParseNotaryPut, handler: catchSubnetCreation);
//   * delete (parser: subnetevents.ParseNotaryDelete, handler: catchSubnetRemoval).
//
// Events (non-notary):
//   * put (parser: subnetevents.ParsePut, handler: catchSubnetCreation);
//   * delete (parser: subnetevents.ParseDelete, handler: catchSubnetCreation).
func (s *Server) listenSubnet() {
	if s.sideNotaryConfig.disabled {
		s.listenSubnetWithoutNotary()
		return
	}

	var (
		parserInfo  event.NotaryParserInfo
		handlerInfo event.NotaryHandlerInfo
	)

	parserInfo.SetScriptHash(s.contracts.subnet)
	handlerInfo.SetScriptHash(s.contracts.subnet)

	listenEvent := func(notifyName string, parser event.NotaryParser, handler event.Handler) {
		notifyTyp := event.NotaryTypeFromString(notifyName)

		parserInfo.SetMempoolType(mempoolevent.TransactionAdded)
		handlerInfo.SetMempoolType(mempoolevent.TransactionAdded)

		parserInfo.SetParser(parser)
		handlerInfo.SetHandler(handler)

		parserInfo.SetRequestType(notifyTyp)
		handlerInfo.SetRequestType(notifyTyp)

		s.morphListener.SetNotaryParser(parserInfo)
		s.morphListener.RegisterNotaryHandler(handlerInfo)
	}

	// subnet creation
	listenEvent(notarySubnetCreateEvName, subnetevents.ParseNotaryPut, s.onlyAlphabetEventHandler(s.catchSubnetCreation))
}

func (s *Server) listenSubnetWithoutNotary() {
	var (
		parserInfo  event.NotificationParserInfo
		handlerInfo event.NotificationHandlerInfo
	)

	parserInfo.SetScriptHash(s.contracts.subnet)
	handlerInfo.SetScriptHash(s.contracts.subnet)

	listenEvent := func(notifyName string, parser event.NotificationParser, handler event.Handler) {
		notifyTyp := event.TypeFromString(notifyName)

		parserInfo.SetType(notifyTyp)
		handlerInfo.SetType(notifyTyp)

		parserInfo.SetParser(parser)
		handlerInfo.SetHandler(handler)

		s.morphListener.SetNotificationParser(parserInfo)
		s.morphListener.RegisterNotificationHandler(handlerInfo)
	}

	// subnet creation
	listenEvent(subnetCreateEvName, subnetevents.ParsePut, s.onlyAlphabetEventHandler(s.catchSubnetCreation))
	// subnet removal
	listenEvent(subnetRemoveEvName, subnetevents.ParseDelete, s.onlyAlphabetEventHandler(s.catchSubnetRemoval))
}

// catchSubnetCreation catches event of subnet creation from listener and queues the processing.
func (s *Server) catchSubnetCreation(e event.Event) {
	err := s.subnetHandler.workerPool.Submit(func() {
		s.handleSubnetCreation(e)
	})
	if err != nil {
		s.log.Error("subnet creation queue failure",
			zap.String("error", err.Error()),
		)
	}
}

// implements irsubnet.Put event interface required by irsubnet.PutValidator.
type putSubnetEvent struct {
	ev subnetevents.Put
}

// ReadID unmarshals subnet ID from a binary NeoFS API protocol's format.
func (x putSubnetEvent) ReadID(id *subnetid.ID) error {
	return id.Unmarshal(x.ev.ID())
}

var errMissingSubnetOwner = errors.New("missing subnet owner")

// ReadCreator unmarshals subnet creator from a binary NeoFS API protocol's format.
// Returns an error if byte array is empty.
func (x putSubnetEvent) ReadCreator(id *owner.ID) error {
	data := x.ev.Owner()

	if len(data) == 0 {
		return errMissingSubnetOwner
	}

	key, err := keys.NewPublicKeyFromBytes(data, elliptic.P256())
	if err != nil {
		return err
	}

	wal, err := owner.NEO3WalletFromPublicKey((*ecdsa.PublicKey)(key))
	if err != nil {
		return err
	}

	// it would be better if we could do it not like this
	*id = *owner.NewIDFromNeo3Wallet(wal)

	return nil
}

// ReadInfo unmarshal subnet info from a binary NeoFS API protocol's format.
func (x putSubnetEvent) ReadInfo(info *subnet.Info) error {
	return info.Unmarshal(x.ev.Info())
}

// handleSubnetCreation handles event of subnet creation parsed via subnetevents.ParsePut.
//
// Validates the event using irsubnet.PutValidator. Logs message about (dis)agreement.
func (s *Server) handleSubnetCreation(e event.Event) {
	putEv := e.(subnetevents.Put) // panic occurs only if we registered handler incorrectly

	err := s.subnetHandler.putValidator.Assert(putSubnetEvent{
		ev: putEv,
	})
	if err != nil {
		s.log.Info("discard subnet creation",
			zap.String("reason", err.Error()),
		)

		return
	}

	notaryMainTx := putEv.NotaryMainTx()

	isNotary := notaryMainTx != nil
	if isNotary {
		// re-sign notary request
		err = s.morphClient.NotarySignAndInvokeTX(notaryMainTx)
	} else {
		// send new transaction
		var prm morphsubnet.PutPrm

		prm.SetID(putEv.ID())
		prm.SetOwner(putEv.Owner())
		prm.SetInfo(putEv.Info())
		prm.SetTxHash(putEv.TxHash())

		_, err = s.subnetHandler.morphClient.Put(prm)
	}

	if err != nil {
		s.log.Error("approve subnet creation",
			zap.Bool("notary", isNotary),
			zap.String("error", err.Error()),
		)

		return
	}
}

// catchSubnetRemoval catches event of subnet removal from listener and queues the processing.
func (s *Server) catchSubnetRemoval(e event.Event) {
	err := s.subnetHandler.workerPool.Submit(func() {
		s.handleSubnetCreation(e)
	})
	if err != nil {
		s.log.Error("subnet removal queue failure",
			zap.String("error", err.Error()),
		)
	}
}

// implements irsubnet.Delete event interface required by irsubnet.DeleteValidator.
type deleteSubnetEvent struct {
	ev subnetevents.Delete
}

// ReadID unmarshals subnet ID from a binary NeoFS API protocol's format.
func (x deleteSubnetEvent) ReadID(id *subnetid.ID) error {
	return id.Unmarshal(x.ev.ID())
}

// handleSubnetRemoval handles event of subnet removal parsed via subnetevents.ParseDelete.
func (s *Server) handleSubnetRemoval(e event.Event) {
	delEv := e.(subnetevents.Delete) // panic occurs only if we registered handler incorrectly

	err := s.subnetHandler.delValidator.Assert(deleteSubnetEvent{
		ev: delEv,
	})
	if err != nil {
		s.log.Info("discard subnet removal",
			zap.String("reason", err.Error()),
		)

		return
	}

	// send new transaction
	var prm morphsubnet.DeletePrm

	prm.SetID(delEv.ID())
	prm.SetTxHash(delEv.TxHash())

	_, err = s.subnetHandler.morphClient.Delete(prm)
	if err != nil {
		s.log.Error("approve subnet removal",
			zap.String("error", err.Error()),
		)

		return
	}

	// TODO: handle removal of the subnet in netmap candidates
}
