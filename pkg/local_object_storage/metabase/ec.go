package meta

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// TODO: docs.
func (db *DB) ResolveECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (oid.ID, error) {
	// FIXME: check degraded
	// TODO: check already removed case;. https://github.com/nspcc-dev/neofs-node/issues/3502
	var res oid.ID
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		var err error
		res, err = db.resolveECPartTx(tx, cnr, parent, pi)
		return err
	})
	return res, err
}

func (db *DB) resolveECPartTx(tx *bbolt.Tx, cnr cid.ID, parent oid.ID, pi iec.PartInfo) (oid.ID, error) {
	metaBkt := tx.Bucket(metaBucketKey(cnr))
	if metaBkt == nil {
		return oid.ID{}, fmt.Errorf("%w: missing meta bucket", apistatus.ErrObjectNotFound)
	}
	metaBktCursor := metaBkt.Cursor()

	pref := slices.Concat([]byte{metaPrefixAttrIDPlain}, []byte(object.FilterParentID), objectcore.MetaAttributeDelimiter,
		parent[:], objectcore.MetaAttributeDelimiter,
	)
	k, _ := metaBktCursor.Seek(pref)
	partID, ok := bytes.CutPrefix(k, pref)
	if !ok {
		return oid.ID{}, fmt.Errorf("%w: not found by index %s", apistatus.ErrObjectNotFound, object.FilterParentID)
	}
	if len(partID) != oid.Size {
		return oid.ID{}, invalidMetaBucketKeyErr(k, fmt.Errorf("wrong OID len %d", len(partID)))
	}

	// TODO: make one buffer for all keys
	k = slices.Concat([]byte{metaPrefixIDAttr}, partID, []byte(iec.AttributeRuleIdx), objectcore.MetaAttributeDelimiter, []byte(strconv.Itoa(pi.RuleIndex)))
	if metaBkt.Get(k) == nil {
		return oid.ID{}, fmt.Errorf("%w: not found by index %s", apistatus.ErrObjectNotFound, iec.AttributeRuleIdx)
	}

	k = slices.Concat([]byte{metaPrefixIDAttr}, partID, []byte(iec.AttributePartIdx), objectcore.MetaAttributeDelimiter, []byte(strconv.Itoa(pi.Index)))
	if metaBkt.Get(k) == nil {
		return oid.ID{}, fmt.Errorf("%w: not found by index %s", apistatus.ErrObjectNotFound, iec.AttributePartIdx)
	}

	return oid.ID(partID), nil
}
