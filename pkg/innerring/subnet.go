package innerring

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	neogoutil "github.com/nspcc-dev/neo-go/pkg/util"
	irsubnet "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/subnet"
	netmapclient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	morphsubnet "github.com/nspcc-dev/neofs-node/pkg/morph/client/subnet"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	subnetevents "github.com/nspcc-dev/neofs-node/pkg/morph/event/subnet"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/subnet"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// IR server's component to handle Subnet contract notifications.
type subnetHandler struct {
	workerPool util.WorkerPool

	morphClient morphsubnet.Client

	putValidator irsubnet.PutValidator
}

// configuration of subnet component.
type subnetConfig struct {
	queueSize uint32
}

// makes IR server to catch Subnet notifications from the sidechain listener,
// and to release the corresponding processing queue on stop.
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
	subnetRemoveEvName       = "Delete"
	notarySubnetCreateEvName = "put"
)

// makes the IR server to listen to notifications of Subnet contract.
// All required resources must be initialized before (initSubnet).
// It works in one of two modes (configured): notary and non-notary.
//
// All handlers are executed only if the local node is an alphabet one.
//
// Events (notary):
//   - put (parser: subnetevents.ParseNotaryPut, handler: catchSubnetCreation);
//   - Delete (parser: subnetevents.ParseDelete, handler: catchSubnetCreation).
//
// Events (non-notary):
//   - Put (parser: subnetevents.ParsePut, handler: catchSubnetCreation);
//   - Delete (parser: subnetevents.ParseDelete, handler: catchSubnetCreation).
func (s *Server) listenSubnet() {
	var (
		parserInfo  event.NotaryParserInfo
		handlerInfo event.NotaryHandlerInfo
	)

	parserInfo.SetScriptHash(s.contracts.subnet)
	handlerInfo.SetScriptHash(s.contracts.subnet)

	listenNotaryEvent := func(notifyName string, parser event.NotaryParser, handler event.Handler) {
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
	listenNotaryEvent(notarySubnetCreateEvName, subnetevents.ParseNotaryPut, s.onlyAlphabetEventHandler(s.catchSubnetCreation))
	// subnet removal
	listenNotifySubnetEvent(s, subnetRemoveEvName, subnetevents.ParseDelete, s.onlyAlphabetEventHandler(s.catchSubnetRemoval))
}

func listenNotifySubnetEvent(s *Server, notifyName string, parser event.NotificationParser, handler event.Handler) {
	var (
		parserInfo  event.NotificationParserInfo
		handlerInfo event.NotificationHandlerInfo
	)

	parserInfo.SetScriptHash(s.contracts.subnet)
	handlerInfo.SetScriptHash(s.contracts.subnet)

	notifyTyp := event.TypeFromString(notifyName)

	parserInfo.SetType(notifyTyp)
	handlerInfo.SetType(notifyTyp)

	parserInfo.SetParser(parser)
	handlerInfo.SetHandler(handler)

	s.morphListener.SetNotificationParser(parserInfo)
	s.morphListener.RegisterNotificationHandler(handlerInfo)
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

// ReadID unmarshals the subnet ID from a binary NeoFS API protocol's format.
func (x putSubnetEvent) ReadID(id *subnetid.ID) error {
	return id.Unmarshal(x.ev.ID())
}

var errMissingSubnetOwner = errors.New("missing subnet owner")

// ReadCreator unmarshals the subnet creator from a binary NeoFS API protocol's format.
// Returns an error if the byte array is empty.
func (x putSubnetEvent) ReadCreator(id *user.ID) error {
	data := x.ev.Owner()

	if len(data) == 0 {
		return errMissingSubnetOwner
	}

	return user.IDFromKey(id, data)
}

// ReadInfo unmarshal the subnet info from a binary NeoFS API protocol's format.
func (x putSubnetEvent) ReadInfo(info *subnet.Info) error {
	return info.Unmarshal(x.ev.Info())
}

// handleSubnetCreation handles an event of subnet creation parsed via subnetevents.ParsePut.
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

	// re-sign notary request
	err = s.morphClient.NotarySignAndInvokeTX(notaryMainTx)

	if err != nil {
		s.log.Error("approve subnet creation",
			zap.String("error", err.Error()),
		)

		return
	}
}

// catchSubnetRemoval catches an event of subnet removal from listener and queues the processing.
func (s *Server) catchSubnetRemoval(e event.Event) {
	err := s.subnetHandler.workerPool.Submit(func() {
		s.handleSubnetRemoval(e)
	})
	if err != nil {
		s.log.Error("subnet removal handling failure",
			zap.String("error", err.Error()),
		)
	}
}

// handleSubnetRemoval handles event of subnet removal parsed via subnetevents.ParseDelete.
func (s *Server) handleSubnetRemoval(e event.Event) {
	delEv := e.(subnetevents.Delete) // panic occurs only if we registered handler incorrectly

	// handle subnet changes in netmap

	candidates, err := s.netmapClient.GetCandidates()
	if err != nil {
		s.log.Error("getting netmap candidates",
			zap.Error(err),
		)

		return
	}

	var removedID subnetid.ID
	err = removedID.Unmarshal(delEv.ID())
	if err != nil {
		s.log.Error("unmarshalling removed subnet ID",
			zap.String("error", err.Error()),
		)

		return
	}

	for i := range candidates {
		s.processCandidate(delEv.TxHash(), removedID, candidates[i])
	}
}

func (s *Server) processCandidate(txHash neogoutil.Uint256, removedID subnetid.ID, c netmap.NodeInfo) {
	removeSubnet := false
	log := s.log.With(
		zap.String("public_key", netmap.StringifyPublicKey(c)),
		zap.String("removed_subnet", removedID.String()),
	)

	err := c.IterateSubnets(func(id subnetid.ID) error {
		if removedID.Equals(id) {
			removeSubnet = true
			return netmap.ErrRemoveSubnet
		}

		return nil
	})
	if err != nil {
		log.Error("iterating node's subnets", zap.Error(err))
		log.Debug("removing node from netmap candidates")

		var updateStatePrm netmapclient.UpdatePeerPrm
		updateStatePrm.SetKey(c.PublicKey())
		updateStatePrm.SetHash(txHash)

		err = s.netmapClient.UpdatePeerState(updateStatePrm)
		if err != nil {
			log.Error("removing node from candidates",
				zap.Error(err),
			)
		}

		return
	}

	// remove subnet from node's information
	// if it contains removed subnet
	if removeSubnet {
		log.Debug("removing subnet from the node")

		var addPeerPrm netmapclient.AddPeerPrm
		addPeerPrm.SetNodeInfo(c)
		addPeerPrm.SetHash(txHash)

		err = s.netmapClient.AddPeer(addPeerPrm)
		if err != nil {
			log.Error("updating subnet info",
				zap.Error(err),
			)
		}
	}
}
