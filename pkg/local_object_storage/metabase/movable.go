package meta

import (
	"fmt"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// ToMoveItPrm groups the parameters of ToMoveIt operation.
type ToMoveItPrm struct {
	addr oid.Address
}

// ToMoveItRes groups the resulting values of ToMoveIt operation.
type ToMoveItRes struct{}

// SetAddress sets address of the object to move into another shard.
func (p *ToMoveItPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// DoNotMovePrm groups the parameters of DoNotMove operation.
type DoNotMovePrm struct {
	addr oid.Address
}

// DoNotMoveRes groups the resulting values of DoNotMove operation.
type DoNotMoveRes struct{}

// SetAddress sets address of the object to prevent moving into another shard.
func (p *DoNotMovePrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// MovablePrm groups the parameters of Movable operation.
type MovablePrm struct{}

// MovableRes groups the resulting values of Movable operation.
type MovableRes struct {
	addrList []oid.Address
}

// AddressList returns resulting addresses of Movable operation.
func (p MovableRes) AddressList() []oid.Address {
	return p.addrList
}

// ToMoveIt marks objects to move it into another shard. This useful for
// faster HRW fetching.
func (db *DB) ToMoveIt(prm ToMoveItPrm) (res ToMoveItRes, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return res, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return res, ErrReadOnlyMode
	}

	key := make([]byte, addressKeySize)
	key = addressKey(prm.addr, key)

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		toMoveIt := tx.Bucket(toMoveItBucketName)
		return toMoveIt.Put(key, zeroValue)
	})

	return
}

// DoNotMove removes `MoveIt` mark from the object.
func (db *DB) DoNotMove(prm DoNotMovePrm) (res DoNotMoveRes, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return res, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return res, ErrReadOnlyMode
	}

	key := make([]byte, addressKeySize)
	key = addressKey(prm.addr, key)

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		toMoveIt := tx.Bucket(toMoveItBucketName)
		return toMoveIt.Delete(key)
	})

	return
}

// Movable returns list of marked objects to move into other shard.
func (db *DB) Movable(_ MovablePrm) (MovableRes, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return MovableRes{}, ErrDegradedMode
	}

	var strAddrs []string

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		toMoveIt := tx.Bucket(toMoveItBucketName)
		return toMoveIt.ForEach(func(k, v []byte) error {
			strAddrs = append(strAddrs, string(k))

			return nil
		})
	})
	if err != nil {
		return MovableRes{}, err
	}

	// we can parse strings to structures in-place, but probably it seems
	// more efficient to keep bolt db TX code smaller because it might be
	// bottleneck.
	addrs := make([]oid.Address, len(strAddrs))

	for i := range strAddrs {
		err = decodeAddressFromKey(&addrs[i], []byte(strAddrs[i]))
		if err != nil {
			return MovableRes{}, fmt.Errorf("can't parse object address %v: %w",
				strAddrs[i], err)
		}
	}

	return MovableRes{
		addrList: addrs,
	}, nil
}
