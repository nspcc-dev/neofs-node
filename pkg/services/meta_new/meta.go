package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	metabase "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const (
	// notificationBuffSize is a nesessary buffer for neo-go's client proper
	// notification work; it is required to always read notifications without
	// any blocking or making additional RPC.
	notificationBuffSize = 10000
)

type (
	// NeoFSNetwork describes current NeoFS storage network state.
	NeoFSNetwork interface {
		// Head returns actual object header from the NeoFS network (non-local
		// objects should also be returned). Missing, removed object statuses
		// must be reported according to API statuses from SDK.
		Head(context.Context, cid.ID, oid.ID) (object.Object, error)

		// IsMineWithMeta checks if the given container has meta enabled and current
		// node belongs to it.
		IsMineWithMeta(cid.ID, []byte) (bool, error)
	}

	// MetaChain describes metadata chain.
	MetaChain interface {
		// Magic must return metadata chain magic number.
		Magic() uint32
		// Height must return actual chain height.
		Height() uint32
		// AddTx must add transaction to the chain without blocking.
		AddTx(tx *transaction.Transaction) error
		// SubscribeForBlocks must subscribe for new block headers.
		// Block shouldbe sent to provided channel.
		SubscribeForBlocks(ch chan *block.Header)
		// SubscribeForNotifications must subscribe for new chain notifications.
		// Notifications should be sent to provided channel.
		SubscribeForNotifications(ch chan *state.ContainedNotificationEvent)
		// TransactionTestInvocation must validate transaction execution
		// without persisting it.
		TransactionTestInvocation(tx *transaction.Transaction) error
	}
)

func newNotifier(metaSvc *Meta) *objectNotifier {
	return &objectNotifier{
		metaSvc: metaSvc,
		subs:    make(map[oid.Address]objSubInfo),
	}
}

type objSubInfo struct {
	txH                      util.Uint256
	ch                       chan<- struct{}
	timeSubscriptionStarted  time.Time
	blockSubscriptionStarted uint32
	objHeader                object.Object
}

type objectNotifier struct {
	metaSvc *Meta

	m    sync.Mutex
	subs map[oid.Address]objSubInfo
}

func (on *objectNotifier) subscribe(o object.Object, ch chan<- struct{}, h util.Uint256) {
	var (
		subTime  = time.Now()
		subBlock = on.metaSvc.chainHeight.Load()
		addr     = o.Address()
	)

	on.m.Lock()
	defer on.m.Unlock()

	on.subs[addr] = objSubInfo{
		txH:                      h,
		ch:                       ch,
		timeSubscriptionStarted:  subTime,
		blockSubscriptionStarted: subBlock,
		objHeader:                o,
	}
}

func (on *objectNotifier) unsubscribe(addr oid.Address) {
	on.m.Lock()
	defer on.m.Unlock()

	delete(on.subs, addr)
}

func (on *objectNotifier) notifyReceived(addr oid.Address) {
	var (
		timeTook   time.Duration
		blocksTook uint32
		currHeight = on.metaSvc.chainHeight.Load()
	)

	on.m.Lock()

	sub, ok := on.subs[addr]
	if ok {
		close(sub.ch)
		delete(on.subs, addr)

		timeTook = time.Since(sub.timeSubscriptionStarted)
		blocksTook = currHeight - sub.blockSubscriptionStarted
	}

	on.m.Unlock()

	if ok {
		on.metaSvc.taskQueue <- storageTask{addr: addr, o: &sub.objHeader}
	} else {
		on.metaSvc.taskQueue <- storageTask{addr: addr}
	}

	on.metaSvc.l.Debug("object notification received",
		zap.Stringer("addr", addr),
		zap.Duration("timeTook", timeTook),
		zap.Uint32("blocksTook", blocksTook),
		zap.String("txHash", sub.txH.StringLE()))
}

// Meta handles object meta information received from metadata chain and object
// storages. It must be created using [New].
type Meta struct {
	l       *zap.Logger
	metrics metrics

	net NeoFSNetwork

	chainHeight atomic.Uint32
	chain       MetaChain
	magicNumber uint32
	bCh         chan *block.Header
	evsCh       chan *state.ContainedNotificationEvent

	taskQueue chan storageTask

	metabase *metabase.DB
	notifier *objectNotifier
}

const blockBuffSize = 10000

// Parameters groups arguments for [New] call. Logger, Chain and Network
// must not be nil, path must not be empty.
type Parameters struct {
	Logger  *zap.Logger
	Chain   MetaChain
	Path    string
	Network NeoFSNetwork
}

func validatePrm(p Parameters) error {
	if p.Logger == nil {
		return errors.New("missing logger")
	}
	if p.Chain == nil {
		return errors.New("missing metadata chain")
	}
	if p.Path == "" {
		return errors.New("empty metadata path")
	}
	if p.Network == nil {
		return errors.New("missing NeoFS network")
	}

	return nil
}

// this is only neeeded to treat every object as a non-expired one in metabase
// GC routines do not relate to meta service directly.
type epochStateStub struct{}

func (e epochStateStub) CurrentEpoch() uint64 {
	return math.MaxUint64
}

// New makes [Meta] using [Parameters]. [metabase.DB] is created, opened and
// inited when called.
func New(p Parameters) (*Meta, error) {
	err := validatePrm(p)
	if err != nil {
		return nil, err
	}
	metaDB := metabase.New(
		metabase.WithPath(path.Join(p.Path, "metadataDB.bolt")),
		metabase.WithLogger(
			p.Logger.With(zap.String("component", "metadata storage"))),
		metabase.WithEpochState(epochStateStub{}),
	)
	err = metaDB.Open(false)
	if err != nil {
		_ = metaDB.Close()
		return nil, fmt.Errorf("failed to open metabase: %w", err)
	}
	var dbIDRaw [common.IDSize]byte
	copy(dbIDRaw[:], "metadataobjectDB")
	dbID, err := common.NewIDFromBytes(dbIDRaw[:])
	if err != nil {
		_ = metaDB.Close()
		panic(fmt.Errorf("failed to create metabase ID: %w", err))
	}
	err = metaDB.Init(dbID)
	if err != nil {
		_ = metaDB.Close()
		return nil, fmt.Errorf("failed to init metabase: %w", err)
	}

	m := &Meta{
		l:         p.Logger,
		chain:     p.Chain,
		net:       p.Network,
		bCh:       make(chan *block.Header, blockBuffSize),
		evsCh:     make(chan *state.ContainedNotificationEvent, notificationBuffSize),
		taskQueue: make(chan storageTask, notificationBuffSize),
		metabase:  metaDB,
	}
	notifier := newNotifier(m)
	m.notifier = notifier

	m.addMetrics()

	return m, nil
}

// MagicNumber returns metadata chain's magic number.
func (m *Meta) MagicNumber() uint32 {
	return m.chain.Magic()
}

// Height returns current metadata chain block height.
func (m *Meta) Height() uint32 {
	return m.chain.Height()
}

// TransactionTestInvocation validates transaction.
func (m *Meta) TransactionTestInvocation(tx *transaction.Transaction) error {
	return m.chain.TransactionTestInvocation(tx)
}

// IndexedSignature is indexed signature, pointing at place of signature's
// public keys in a sorted (by these public keys) placement vector. It will be
// used as an optimized signatures check.
type IndexedSignature struct {
	Index     uint8
	Signature neofscrypto.Signature
}

// SubmitObjectPut sends transaction to metadata chain. Transaction must be
// completed excepting [transaction.Witness.InvocationScript] that will be
// filled using provided signatures. signatures must be a two two-dimensional
// array that corresponds to placement vectors for a container that tx was
// made for. Signature optimized sorting is not this func's responsibility.
func (m *Meta) SubmitObjectPut(tx *transaction.Transaction, signatures [][]IndexedSignature) error {
	var (
		invokBuff = io.NewBufBinWriter()
		writer    = invokBuff.BinWriter
	)
	for _, signature := range slices.Backward(signatures) {
		vectorLen := len(signature)
		for j := vectorLen - 1; j >= 0; j-- {
			emit.Array(writer,
				stackitem.Make(signature[j].Index),             // singature's index
				stackitem.Make(signature[j].Signature.Value()), // signature
			)
		}
		emit.Int(writer, int64(vectorLen))
		emit.Opcodes(writer, opcode.PACK)
	}

	if invokBuff.Err != nil {
		panic(invokBuff.Err)
	}

	tx.Scripts[0].InvocationScript = invokBuff.Bytes()

	m.l.Debug("sending transaction to chain...", zap.String("txHash", tx.Hash().StringLE()))
	now := time.Now()
	err := m.chain.AddTx(tx)
	took := time.Since(now)
	m.l.Debug("sent transaction to chain", zap.String("txHash", tx.Hash().StringLE()), zap.Duration("took", took), zap.Error(err))

	return err
}

// Run starts notification handling. Must be called only on instances created
// with [New]. Blocked until context is done. Cancelling the context stops
// the service.
func (m *Meta) Run(ctx context.Context) error {
	m.magicNumber = m.chain.Magic()

	var wg sync.WaitGroup

	wg.Go(func() { m.blockHandler(ctx, m.bCh) })
	wg.Go(func() { m.notificationHandler(ctx, m.evsCh) })
	wg.Go(func() { m.storager(ctx, m.taskQueue) })

	m.chain.SubscribeForBlocks(m.bCh)
	m.chain.SubscribeForNotifications(m.evsCh)

	wg.Wait()

	close(m.bCh)
	close(m.evsCh)
	close(m.taskQueue)

	return m.metabase.Close()
}
