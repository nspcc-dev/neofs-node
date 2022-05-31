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

// WithAddress sets address of the object to move into another shard.
func (p *ToMoveItPrm) WithAddress(addr oid.Address) *ToMoveItPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// DoNotMovePrm groups the parameters of DoNotMove operation.
type DoNotMovePrm struct {
	addr oid.Address
}

// DoNotMoveRes groups the resulting values of DoNotMove operation.
type DoNotMoveRes struct{}

// WithAddress sets address of the object to prevent moving into another shard.
func (p *DoNotMovePrm) WithAddress(addr oid.Address) *DoNotMovePrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// MovablePrm groups the parameters of Movable operation.
type MovablePrm struct{}

// MovableRes groups the resulting values of Movable operation.
type MovableRes struct {
	addrList []oid.Address
}

// AddressList returns resulting addresses of Movable operation.
func (p *MovableRes) AddressList() []oid.Address {
	return p.addrList
}

// ToMoveIt marks object to move it into another shard.
func ToMoveIt(db *DB, addr oid.Address) error {
	_, err := db.ToMoveIt(new(ToMoveItPrm).WithAddress(addr))
	return err
}

// ToMoveIt marks objects to move it into another shard. This useful for
// faster HRW fetching.
func (db *DB) ToMoveIt(prm *ToMoveItPrm) (res *ToMoveItRes, err error) {
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		toMoveIt, err := tx.CreateBucketIfNotExists(toMoveItBucketName)
		if err != nil {
			return err
		}

		return toMoveIt.Put(addressKey(prm.addr), zeroValue)
	})

	return
}

// DoNotMove prevents the object to be moved into another shard.
func DoNotMove(db *DB, addr oid.Address) error {
	_, err := db.DoNotMove(new(DoNotMovePrm).WithAddress(addr))
	return err
}

// DoNotMove removes `MoveIt` mark from the object.
func (db *DB) DoNotMove(prm *DoNotMovePrm) (res *DoNotMoveRes, err error) {
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		toMoveIt := tx.Bucket(toMoveItBucketName)
		if toMoveIt == nil {
			return nil
		}

		return toMoveIt.Delete(addressKey(prm.addr))
	})

	return
}

// Movable returns all movable objects of DB.
func Movable(db *DB) ([]oid.Address, error) {
	r, err := db.Movable(new(MovablePrm))
	if err != nil {
		return nil, err
	}

	return r.AddressList(), nil
}

// Movable returns list of marked objects to move into other shard.
func (db *DB) Movable(prm *MovablePrm) (*MovableRes, error) {
	var strAddrs []string

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		toMoveIt := tx.Bucket(toMoveItBucketName)
		if toMoveIt == nil {
			return nil
		}

		return toMoveIt.ForEach(func(k, v []byte) error {
			strAddrs = append(strAddrs, string(k))

			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// we can parse strings to structures in-place, but probably it seems
	// more efficient to keep bolt db TX code smaller because it might be
	// bottleneck.
	addrs := make([]oid.Address, len(strAddrs))

	for i := range strAddrs {
		err = decodeAddressFromKey(&addrs[i], []byte(strAddrs[i]))
		if err != nil {
			return nil, fmt.Errorf("can't parse object address %v: %w",
				strAddrs[i], err)
		}
	}

	return &MovableRes{
		addrList: addrs,
	}, nil
}
