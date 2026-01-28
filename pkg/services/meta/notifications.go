package meta

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-contract/rpc/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const (
	objPutEvName  = "ObjectPut"
	cnrDeleteName = "DeleteSuccess"
	cnrRmName     = "Removed"
	cnrPutName    = "PutSuccess"
	cnrCrtName    = "Created"
	newEpochName  = "NewEpoch"
)

// subscribeForBlocks reqauires [Meta.cliM] to be taken.
func (m *Meta) subscribeForBlocks(ch chan<- *block.Header) (string, error) {
	m.l.Debug("subscribe for blocks")
	return m.ws.ReceiveHeadersOfAddedBlocks(nil, ch)
}

// unsubscribeFromBlocks requires [Meta.cliM] to be taken.
func (m *Meta) unsubscribeFromBlocks() {
	var err error
	m.l.Debug("unsubscribing from blocks")

	err = m.ws.Unsubscribe(m.blockSubID)
	if err != nil {
		m.l.Warn("could not unsubscribe from blocks", zap.String("ID", m.blockSubID), zap.Error(err))
		return
	}

	m.blockSubID = ""

	m.l.Debug("successfully unsubscribed from blocks")
}

// subscribeForNewContainers requires [Meta.cliM] to be taken.
func (m *Meta) subscribeForNewContainers() error {
	m.l.Debug("subscribing for containers")

	cnrPutEv := cnrPutName
	var err1 error
	m.cnrSubID, err1 = m.ws.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &m.cnrH, Name: &cnrPutEv}, m.cnrPutEv)

	cnrCrtEv := cnrCrtName
	var err2 error
	m.cnrCrtSubID, err2 = m.ws.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &m.cnrH, Name: &cnrCrtEv}, m.cnrPutEv)

	return errors.Join(err1, err2)
}

// unsubscribeFromNewContainers requires [Meta.cliM] to be taken.
func (m *Meta) unsubscribeFromNewContainers() {
	m.l.Debug("unsubscribing from containers")

	if err := m.ws.Unsubscribe(m.cnrCrtSubID); err != nil {
		m.l.Error("could not unsubscribe from containers, ignore", zap.String("event", cnrCrtName), zap.String("sub", m.cnrCrtSubID), zap.Error(err))
	}

	err := m.ws.Unsubscribe(m.cnrSubID)
	if err != nil {
		m.l.Error("could not unsubscribe from containers", zap.String("event", cnrPutName), zap.String("sub", m.cnrSubID), zap.Error(err))
		return
	}

	m.cnrSubID = ""

	m.l.Debug("successfully unsubscribed from containers")
}

func (m *Meta) subscribeEvents() error {
	epochEv := newEpochName
	_, err := m.ws.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &m.netmapH, Name: &epochEv}, m.epochEv)
	if err != nil {
		return fmt.Errorf("subscribe for epoch notifications: %w", err)
	}

	return nil
}

func (m *Meta) listenNotifications(ctx context.Context) error {
	for {
		select {
		case h, ok := <-m.bCh:
			if !ok {
				err := m.reconnect(ctx)
				if err != nil {
					return err
				}

				continue
			}

			m.blockHeadersBuff <- h
		case aer, ok := <-m.cnrPutEv:
			if !ok {
				err := m.reconnect(ctx)
				if err != nil {
					return err
				}

				continue
			}

			m.cliM.RLock()
			alreadyListenToContainers := m.blockSubID != ""
			m.cliM.RUnlock()
			if alreadyListenToContainers {
				// container will be handled
				continue
			}

			l := m.l.With(zap.Stringer("notification container", aer.Container))

			ev, err := parseCnrNotification(*aer)
			if err != nil {
				l.Error("invalid container notification received", zap.Error(err))
				continue
			}

			go m.addContainerIfMine(l, ev.cID)
		case aer, ok := <-m.epochEv:
			if !ok {
				err := m.reconnect(ctx)
				if err != nil {
					return err
				}

				continue
			}

			l := m.l.With(zap.Stringer("notification container", aer.Container))

			epoch, err := parseEpochNotification(aer)
			if err != nil {
				l.Error("invalid new epoch notification received", zap.Error(err))
				continue
			}

			go func() {
				err = m.handleEpochNotification(epoch)
				if err != nil {
					l.Error("handling new epoch notification", zap.Uint64("epoch", epoch), zap.Error(err))
					return
				}
			}()
		case <-ctx.Done():
			m.l.Info("stop listening meta notifications")
			return nil
		}
	}
}

func (m *Meta) addContainerIfMine(l *zap.Logger, cID cid.ID) {
	m.cliM.RLock()
	reader := container.NewReader(invoker.New(m.ws, nil), m.cnrH)
	cData, err := reader.GetContainerData(cID[:])
	m.cliM.RUnlock()
	if err != nil {
		l.Error("can't get container data", zap.Stringer("cid", cID), zap.Error(err))
		return
	}

	ok, err := m.net.IsMineWithMeta(cID, cData)
	if err != nil {
		l.Error("failed to check container relation to node", zap.Stringer("cid", cID), zap.Error(err))
		return
	}
	if !ok {
		return
	}

	err = m.addContainer(cID)
	if err != nil {
		l.Error("can't add new container storage", zap.Stringer("cid", cID), zap.Error(err))
		return
	}

	l.Debug("added container storage", zap.Stringer("cid", cID))
}

func (m *Meta) reconnect(ctx context.Context) error {
	m.l.Warn("reconnecting to web socket client due to connection lost")

	m.cliM.Lock()
	defer m.cliM.Unlock()

	var err error
	m.ws, err = m.connect(ctx)
	if err != nil {
		return fmt.Errorf("reconnecting to web socket: %w", err)
	}

	m.stM.RLock()
	if len(m.storages) > 0 {
		m.bCh = make(chan *block.Header, notificationBuffSize)
		m.blockSubID, err = m.subscribeForBlocks(m.bCh)
		if err != nil {
			m.stM.RUnlock()
			return fmt.Errorf("subscription for blocks: %w", err)
		}
	} else {
		m.cnrPutEv = make(chan *state.ContainedNotificationEvent, notificationBuffSize)
		err = m.subscribeForNewContainers()
		if err != nil {
			m.stM.RUnlock()
			return fmt.Errorf("subscription for containers: %w", err)
		}
	}
	m.stM.RUnlock()

	m.epochEv = make(chan *state.ContainedNotificationEvent, notificationBuffSize)

	err = m.subscribeEvents()
	if err != nil {
		return fmt.Errorf("subscribe for meta notifications: %w", err)
	}

	return nil
}

func (m *Meta) connect(ctx context.Context) (*rpcclient.WSClient, error) {
	m.cfgM.RLock()
	endpoints := slices.Clone(m.endpoints)
	m.cfgM.RUnlock()

	var cli *rpcclient.WSClient
	var err error
outer:
	for {
		for _, e := range endpoints {
			cli, err = rpcclient.NewWS(ctx, e, rpcclient.WSOptions{
				Options: rpcclient.Options{
					DialTimeout: m.timeout,
				},
			})
			if err == nil {
				break outer
			}

			m.l.Warn("creating rpc client", zap.String("endpoint", e), zap.Error(err))
		}

		const reconnectionCooldown = time.Second * 5
		m.l.Error("FS chain reconnection failed", zap.Duration("cooldown time", reconnectionCooldown))

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(reconnectionCooldown):
		}
	}

	err = cli.Init()
	if err != nil {
		return nil, fmt.Errorf("web socket client initializing: %w", err)
	}

	return cli, nil
}

const (
	// MPT-only key prefixes.
	oidIndex = iota
	attrIntToOIDIndex
	attrPlainToOIDIndex
	oidToAttrIndex
	sizeIndex
	firstPartIndex
	previousPartIndex
	deletedIndex
	lockedIndex
	typeIndex

	// storage-only key prefixes.
	lockedByIndex

	lastEnumIndex
)

const (
	// meta map keys from FS chain.
	sizeKey         = "size"
	networkMagicKey = "network"
	firstPartKey    = "firstPart"
	previousPartKey = "previousPart"
	deletedKey      = "deleted"
	lockedKey       = "locked"
	typeKey         = "type"
)

type objEvent struct {
	cID            cid.ID
	oID            oid.ID
	size           *big.Int
	network        *big.Int
	firstObject    []byte
	prevObject     []byte
	deletedObjects []byte
	lockedObjects  []byte
	typ            object.Type
}

func parseObjNotification(ev state.ContainedNotificationEvent) (objEvent, error) {
	const expectedNotificationArgs = 3
	var res objEvent

	arr, ok := ev.Item.Value().([]stackitem.Item)
	if !ok {
		return res, fmt.Errorf("unexpected notification stack item: %T", ev.Item.Value())
	}
	if len(arr) != expectedNotificationArgs {
		return res, fmt.Errorf("unexpected number of items on stack: %d, expected: %d", len(arr), expectedNotificationArgs)
	}

	cID, ok := arr[0].Value().([]byte)
	if !ok {
		return res, fmt.Errorf("unexpected container ID stack item: %T", arr[0].Value())
	}
	oID, ok := arr[1].Value().([]byte)
	if !ok {
		return res, fmt.Errorf("unexpected object ID stack item: %T", arr[1].Value())
	}
	meta, ok := arr[2].(*stackitem.Map)
	if !ok {
		return res, fmt.Errorf("unexpected meta stack item: %T", arr[2])
	}

	if len(cID) != cid.Size {
		return res, fmt.Errorf("unexpected container ID len: %d", len(cID))
	}
	if len(oID) != oid.Size {
		return res, fmt.Errorf("unexpected object ID len: %d", len(oID))
	}

	res.cID = cid.ID(cID)
	res.oID = oid.ID(oID)

	v := getFromMap(meta, sizeKey)
	if v == nil {
		return res, fmt.Errorf("missing '%s' key", sizeKey)
	}
	res.size, ok = v.Value().(*big.Int)
	if !ok {
		return res, fmt.Errorf("unexpected object size type: %T", v.Value())
	}

	v = getFromMap(meta, networkMagicKey)
	if v == nil {
		return res, fmt.Errorf("missing '%s' key", networkMagicKey)
	}
	res.network, ok = v.Value().(*big.Int)
	if !ok {
		return res, fmt.Errorf("unexpected network type: %T", v.Value())
	}

	v = getFromMap(meta, firstPartKey)
	if v != nil {
		res.firstObject, ok = v.Value().([]byte)
		if !ok {
			return res, fmt.Errorf("unexpected first part type: %T", v.Value())
		}
	}

	v = getFromMap(meta, previousPartKey)
	if v != nil {
		res.prevObject, ok = v.Value().([]byte)
		if !ok {
			return res, fmt.Errorf("unexpected previous part type: %T", v.Value())
		}
	}

	v = getFromMap(meta, typeKey)
	if v != nil {
		typ, ok := v.Value().(*big.Int)
		if !ok {
			return res, fmt.Errorf("unexpected object type field: %T", v.Value())
		}
		res.typ = object.Type(typ.Uint64())

		switch res.typ {
		case object.TypeTombstone:
			v = getFromMap(meta, deletedKey)
			if v == nil {
				return res, fmt.Errorf("missing '%s' key for %s object type", deletedKey, res.typ)
			}
			stackDeleted := v.Value().([]stackitem.Item)
			for i, d := range stackDeleted {
				rawDeleted, ok := d.Value().([]byte)
				if !ok {
					return res, fmt.Errorf("unexpected %d deleted object type: %T", i, d.Value())
				}
				res.deletedObjects = append(res.deletedObjects, rawDeleted...)
			}
		case object.TypeLock:
			v = getFromMap(meta, lockedKey)
			if v == nil {
				return res, fmt.Errorf("missing '%s' key for %s object type", lockedKey, res.typ)
			}
			stackLocked := v.Value().([]stackitem.Item)
			for i, d := range stackLocked {
				rawLocked, ok := d.Value().([]byte)
				if !ok {
					return res, fmt.Errorf("unexpected %d locked object type: %T", i, d.Value())
				}
				res.lockedObjects = append(res.deletedObjects, rawLocked...)
			}
		case object.TypeLink, object.TypeRegular:
		default:
			return res, fmt.Errorf("unknown '%s' object type", res.typ)
		}
	}

	return res, nil
}

func getFromMap(m *stackitem.Map, key string) stackitem.Item {
	i := m.Index(stackitem.Make(key))
	if i < 0 {
		return nil
	}

	return m.Value().([]stackitem.MapElement)[i].Value
}

type cnrEvent struct {
	cID cid.ID
}

func parseCnrNotification(ev state.ContainedNotificationEvent) (cnrEvent, error) {
	var res cnrEvent

	arr, ok := ev.Item.Value().([]stackitem.Item)
	if !ok {
		return res, fmt.Errorf("unexpected notification stack item: %T", ev.Item.Value())
	}

	switch ev.Name {
	case cnrDeleteName:
		const expectedNotificationArgs = 1
		if len(arr) != expectedNotificationArgs {
			return res, fmt.Errorf("unexpected number of items on stack: %d, expected: %d", len(arr), expectedNotificationArgs)
		}
	case cnrPutName, cnrCrtName, cnrRmName:
		const expectedNotificationArgs = 2
		if len(arr) != expectedNotificationArgs {
			return res, fmt.Errorf("unexpected number of items on stack: %d, expected: %d", len(arr), expectedNotificationArgs)
		}
	}

	cID, ok := arr[0].Value().([]byte)
	if !ok {
		return res, fmt.Errorf("unexpected container ID stack item: %T", arr[0].Value())
	}
	if len(cID) != cid.Size {
		return res, fmt.Errorf("unexpected container ID len: %d", len(cID))
	}

	return cnrEvent{cID: cid.ID(cID)}, nil
}

func (m *Meta) dropContainer(cID cid.ID) error {
	m.stM.Lock()
	defer m.stM.Unlock()

	st, ok := m.storages[cID]
	if !ok {
		return nil
	}

	err := st.drop()
	if err != nil {
		m.l.Warn("drop container %s: %w", zap.Stringer("cID", cID), zap.Error(err))
	}

	delete(m.storages, cID)

	if len(m.storages) == 0 {
		m.cliM.Lock()
		m.unsubscribeFromBlocks()
		err = m.subscribeForNewContainers()
		m.cliM.Unlock()
		if err != nil {
			return fmt.Errorf("subscribing for new containers: %w", err)
		}
	}

	return nil
}

func (m *Meta) addContainer(cID cid.ID) error {
	var err error
	m.stM.Lock()
	defer m.stM.Unlock()

	if len(m.storages) == 0 {
		m.cliM.Lock()

		m.blockSubID, err = m.subscribeForBlocks(m.bCh)
		if err != nil {
			m.cliM.Unlock()
			return fmt.Errorf("blocks subscription: %w", err)
		}
		m.unsubscribeFromNewContainers()

		m.cliM.Unlock()
	}

	st, err := storageForContainer(m.l, m.rootPath, cID)
	if err != nil {
		return fmt.Errorf("open new storage for %s container: %w", cID, err)
	}
	m.storages[cID] = st

	return nil
}

func parseEpochNotification(ev *state.ContainedNotificationEvent) (uint64, error) {
	const expectedNotificationArgs = 1

	arr, ok := ev.Item.Value().([]stackitem.Item)
	if !ok {
		return 0, fmt.Errorf("unexpected notification stack item: %T", ev.Item.Value())
	}
	if len(arr) != expectedNotificationArgs {
		return 0, fmt.Errorf("unexpected number of items on stack: %d, expected: %d", len(arr), expectedNotificationArgs)
	}

	epoch, ok := arr[0].Value().(*big.Int)
	if !ok {
		return 0, fmt.Errorf("unexpected epoch stack item: %T", arr[0].Value())
	}

	return epoch.Uint64(), nil
}

func (m *Meta) handleEpochNotification(e uint64) error {
	l := m.l.With(zap.Uint64("epoch", e))
	l.Debug("handling new epoch notification")

	cnrsNetwork, err := m.net.List(e)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}

	m.stM.Lock()

	for cID, st := range m.storages {
		_, ok := cnrsNetwork[cID]
		if !ok {
			l.Debug("drop container node does not belong to", zap.Stringer("cid", cID))

			err = st.drop()
			if err != nil {
				l.Warn("drop inactual container", zap.Stringer("cID", cID), zap.Error(err))
			}

			delete(m.storages, cID)
		}
	}
	for cID := range cnrsNetwork {
		if _, ok := m.storages[cID]; ok {
			continue
		}

		st, err := storageForContainer(m.l, m.rootPath, cID)
		if err != nil {
			m.stM.Unlock()
			return fmt.Errorf("create storage for container %s: %w", cID, err)
		}

		m.storages[cID] = st
	}

	m.stM.Unlock()

	m.cliM.Lock()
	if len(m.storages) > 0 {
		if m.blockSubID == "" {
			m.blockSubID, err = m.subscribeForBlocks(m.bCh)
			if err != nil {
				m.cliM.Unlock()
				return fmt.Errorf("blocks subscription: %w", err)
			}
		}
		if m.cnrSubID != "" {
			m.unsubscribeFromBlocks()
		}
	} else {
		if m.blockSubID != "" {
			m.unsubscribeFromBlocks()
		}

		if m.cnrSubID == "" {
			err = m.subscribeForNewContainers()
			if err != nil {
				m.cliM.Unlock()
				return fmt.Errorf("containers subscription: %w", err)
			}
		}
	}
	m.cliM.Unlock()

	m.stM.RLock()
	defer m.stM.RUnlock()
	var gcWG sync.WaitGroup
	for cID, st := range m.storages {
		gcWG.Add(1)
		go func() {
			defer gcWG.Done()
			err := st.handleNewEpoch(e)
			if err != nil {
				l.Error("handling new epoch", zap.Stringer("cID", cID), zap.Error(err))
			}
		}()
	}
	gcWG.Wait()

	l.Debug("handled new epoch successfully")

	return nil
}

// NotifyObjectSuccess subscribes channel for object notification chain inclusion.
// Channel must be read before subscription is made and writing to it must be
// non-blocking.
func (m *Meta) NotifyObjectSuccess(ch chan<- struct{}, addr oid.Address) {
	m.notifier.subscribe(addr, ch)
}

// UnsubscribeFromObject unsibscribes from object notification. Should be called
// if notification is not required as a memory clean up.
func (m *Meta) UnsubscribeFromObject(addr oid.Address) {
	m.notifier.unsubscribe(addr)
}
