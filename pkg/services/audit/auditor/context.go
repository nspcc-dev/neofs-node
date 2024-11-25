package auditor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Context represents container data audit execution context.
type Context struct {
	ContextPrm

	task *audit.Task

	report *audit.Report

	sgMembersMtx   sync.RWMutex
	sgMembersCache map[oid.ID][]oid.ID

	placementMtx   sync.Mutex
	placementCache map[string][][]netmap.NodeInfo

	porRequests, porRetries atomic.Uint32

	pairs []gamePair

	pairedMtx   sync.Mutex
	pairedNodes map[uint64]*pairMemberInfo

	counters struct {
		hit, miss, fail uint32
	}

	cnrNodesNum int

	headMtx       sync.RWMutex
	headResponses map[string]shortHeader
}

type pairMemberInfo struct {
	failedPDP, passedPDP bool // at least one

	node netmap.NodeInfo
}

type gamePair struct {
	n1, n2 netmap.NodeInfo

	id oid.ID

	rn1, rn2 []*object.Range

	hh1, hh2 [][]byte
}

type shortHeader struct {
	tzhash []byte

	objectSize uint64
}

// ContextPrm groups components required to conduct data audit checks.
type ContextPrm struct {
	maxPDPSleep uint64

	log *zap.Logger

	cnrCom ContainerCommunicator

	pdpWorkerPool, porWorkerPool util.WorkerPool
}

type commonCommunicatorPrm struct {
	Context context.Context

	Node netmap.NodeInfo

	OID oid.ID
	CID cid.ID
}

// GetHeaderPrm groups parameter of GetHeader operation.
type GetHeaderPrm struct {
	commonCommunicatorPrm

	NodeIsRelay bool
}

// GetRangeHashPrm groups parameter of GetRangeHash operation.
type GetRangeHashPrm struct {
	commonCommunicatorPrm

	Range *object.Range
}

// ContainerCommunicator is an interface of
// component of communication with container nodes.
type ContainerCommunicator interface {
	// GetHeader must return object header from the container node.
	GetHeader(GetHeaderPrm) (*object.Object, error)

	// GetRangeHash must return homomorphic Tillich-Zemor hash of payload range of the
	// object stored in container node.
	GetRangeHash(GetRangeHashPrm) ([]byte, error)
}

// NewContext creates, initializes and returns Context.
func NewContext(prm ContextPrm) *Context {
	return &Context{
		ContextPrm: prm,
	}
}

// SetLogger sets logging component.
func (p *ContextPrm) SetLogger(l *zap.Logger) {
	if p != nil {
		p.log = l
	}
}

// SetContainerCommunicator sets component of communication with container nodes.
func (p *ContextPrm) SetContainerCommunicator(cnrCom ContainerCommunicator) {
	if p != nil {
		p.cnrCom = cnrCom
	}
}

// SetMaxPDPSleep sets maximum sleep interval between range hash requests.
// as part of PDP check.
func (p *ContextPrm) SetMaxPDPSleep(dur time.Duration) {
	if p != nil {
		p.maxPDPSleep = uint64(dur)
	}
}

// WithTask sets container audit parameters.
func (c *Context) WithTask(t *audit.Task) *Context {
	if c != nil {
		c.task = t
	}

	return c
}

// WithPDPWorkerPool sets worker pool for PDP pairs processing.
func (c *Context) WithPDPWorkerPool(pool util.WorkerPool) *Context {
	if c != nil {
		c.pdpWorkerPool = pool
	}

	return c
}

// WithPoRWorkerPool sets worker pool for PoR SG processing.
func (c *Context) WithPoRWorkerPool(pool util.WorkerPool) *Context {
	if c != nil {
		c.porWorkerPool = pool
	}

	return c
}

func (c *Context) containerID() cid.ID {
	return c.task.ContainerID()
}

func (c *Context) init() {
	c.report = audit.NewReport(c.containerID())

	c.sgMembersCache = make(map[oid.ID][]oid.ID)

	c.placementCache = make(map[string][][]netmap.NodeInfo)

	cnrVectors := c.task.ContainerNodes()
	for i := range cnrVectors {
		c.cnrNodesNum += len(cnrVectors[i])
	}

	c.pairedNodes = make(map[uint64]*pairMemberInfo)

	c.headResponses = make(map[string]shortHeader)

	c.log = c.log.With(
		zap.Stringer("container ID", c.task.ContainerID()),
	)
}

func (c *Context) expired() bool {
	ctx := c.task.AuditContext()

	select {
	case <-ctx.Done():
		c.log.Debug("audit context is done",
			zap.Error(ctx.Err()),
		)

		return true
	default:
		return false
	}
}

func (c *Context) complete() {
	c.report.Complete()
}

func (c *Context) writeReport() {
	c.log.Debug("writing audit report...")

	if err := c.task.Reporter().WriteReport(c.report); err != nil {
		c.log.Error("could not write audit report")
	}
}

func (c *Context) buildPlacement(id oid.ID) ([][]netmap.NodeInfo, error) {
	c.placementMtx.Lock()
	defer c.placementMtx.Unlock()

	strID := id.EncodeToString()

	if nn, ok := c.placementCache[strID]; ok {
		return nn, nil
	}

	nn, err := placement.BuildObjectPlacement(
		c.task.NetworkMap(),
		c.task.ContainerNodes(),
		&id,
	)
	if err != nil {
		return nil, err
	}

	c.placementCache[strID] = nn

	return nn, nil
}

func (c *Context) objectSize(id oid.ID) uint64 {
	c.headMtx.RLock()
	defer c.headMtx.RUnlock()

	strID := id.EncodeToString()

	if hdr, ok := c.headResponses[strID]; ok {
		return hdr.objectSize
	}

	return 0
}

func (c *Context) objectHomoHash(id oid.ID) []byte {
	c.headMtx.RLock()
	defer c.headMtx.RUnlock()

	strID := id.EncodeToString()

	if hdr, ok := c.headResponses[strID]; ok {
		return hdr.tzhash
	}

	return nil
}

func (c *Context) updateHeadResponses(hdr *object.Object) {
	id := hdr.GetID()
	if id.IsZero() {
		return
	}

	strID := id.EncodeToString()
	cs, _ := hdr.PayloadHomomorphicHash()

	c.headMtx.Lock()
	defer c.headMtx.Unlock()

	if _, ok := c.headResponses[strID]; !ok {
		c.headResponses[strID] = shortHeader{
			tzhash:     cs.Value(),
			objectSize: hdr.PayloadSize(),
		}
	}
}

func (c *Context) updateSGInfo(id oid.ID, members []oid.ID) {
	c.sgMembersMtx.Lock()
	defer c.sgMembersMtx.Unlock()

	c.sgMembersCache[id] = members
}
