package audit

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"time"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	wrapContainer "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	wrapNetmap "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// Indexer is a callback interface for inner ring global state.
	Indexer interface {
		InnerRingIndex() int
		InnerRingSize() int
	}

	TaskManager interface {
		PushTask(*audit.Task) error

		// Must skip all tasks planned for execution and
		// return their number.
		Reset() int
	}

	// Processor of events related with data audit.
	Processor struct {
		log           *zap.Logger
		pool          *ants.Pool
		irList        Indexer
		sgSrc         SGSource
		searchTimeout time.Duration

		containerClient *wrapContainer.Wrapper
		netmapClient    *wrapNetmap.Wrapper

		taskManager       TaskManager
		reporter          audit.Reporter
		prevAuditCanceler context.CancelFunc
	}

	// Params of the processor constructor.
	Params struct {
		Log              *zap.Logger
		NetmapClient     *wrapNetmap.Wrapper
		ContainerClient  *wrapContainer.Wrapper
		IRList           Indexer
		SGSource         SGSource
		RPCSearchTimeout time.Duration
		TaskManager      TaskManager
		Reporter         audit.Reporter
		Key              *ecdsa.PrivateKey
	}
)

// SearchSGPrm groups the parameters which are formed by Processor to search the storage group objects.
type SearchSGPrm struct {
	ctx context.Context

	id *cid.ID

	info client.NodeInfo
}

// Context returns context to use for network communication.
func (x SearchSGPrm) Context() context.Context {
	return x.ctx
}

// CID returns identifier of the container to search SG in.
func (x SearchSGPrm) CID() *cid.ID {
	return x.id
}

// NodeInfo returns information about storage node to communicate with.
func (x SearchSGPrm) NodeInfo() client.NodeInfo {
	return x.info
}

// SearchSGDst groups target values which Processor expects from SG searching to process.
type SearchSGDst struct {
	ids []*object.ID
}

// WriteIDList writes list of identifiers of storage group objects stored in the container.
func (x *SearchSGDst) WriteIDList(ids []*object.ID) {
	x.ids = ids
}

// SGSource is a storage group information source interface.
type SGSource interface {
	// Lists storage group objects in the container. Formed list must be written to destination.
	//
	// Must return any error encountered which did not allow to form the list.
	ListSG(*SearchSGDst, SearchSGPrm) error
}

type epochAuditReporter struct {
	epoch uint64

	rep audit.Reporter
}

// ProcessorPoolSize limits pool size for audit Processor. Processor manages
// audit tasks and fills queue for next epoch. This process must not be interrupted
// by new audit epoch, so we limit pool size for processor to one.
const ProcessorPoolSize = 1

// New creates audit processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/audit: logger is not set")
	case p.IRList == nil:
		return nil, errors.New("ir/audit: global state is not set")
	case p.SGSource == nil:
		return nil, errors.New("ir/audit: SG source is not set")
	case p.TaskManager == nil:
		return nil, errors.New("ir/audit: audit task manager is not set")
	case p.Reporter == nil:
		return nil, errors.New("ir/audit: audit result reporter is not set")
	case p.Key == nil:
		return nil, errors.New("ir/audit: signing key is not set")
	}

	pool, err := ants.NewPool(ProcessorPoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/audit: can't create worker pool: %w", err)
	}

	return &Processor{
		log:               p.Log,
		pool:              pool,
		containerClient:   p.ContainerClient,
		irList:            p.IRList,
		sgSrc:             p.SGSource,
		searchTimeout:     p.RPCSearchTimeout,
		netmapClient:      p.NetmapClient,
		taskManager:       p.TaskManager,
		reporter:          p.Reporter,
		prevAuditCanceler: func() {},
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (ap *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	return nil
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (ap *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (ap *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}

// StartAuditHandler for the internal event producer.
func (ap *Processor) StartAuditHandler() event.Handler {
	return ap.handleNewAuditRound
}

func (r *epochAuditReporter) WriteReport(rep *audit.Report) error {
	res := rep.Result()
	res.SetAuditEpoch(r.epoch)

	return r.rep.WriteReport(rep)
}
