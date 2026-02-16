package meta

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
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

func (m *Meta) listenNotifications(ctx context.Context) error {
	for {
		select {
		case h, ok := <-m.bCh:
			if !ok {
				return errors.New("notification channel closed")
			}

			m.blockHeadersBuff <- h
		case <-ctx.Done():
			m.l.Info("stop listening meta notifications")
			return nil
		}
	}
}

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
