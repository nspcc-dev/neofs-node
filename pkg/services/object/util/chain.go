package util

import (
	"errors"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ObjectSource is an interface of entity that can receive
// object header, the whole object or the information about
// the object relations.
type ObjectSource interface {
	// Head must return one of:
	// * object header (*object.Object);
	// * structured information about split-chain (*object.SplitInfo).
	Head(id oid.Address) (any, error)

	// Get must return object by its address.
	Get(address oid.Address) (object.Object, error)
}

// SplitMemberHandler is a handler of next split-chain element.
//
// If reverseDirection arg is true, then the traversal is done in reverse order.
// Stop boolean result provides the ability to interrupt the traversal.
type SplitMemberHandler func(member *object.Object, reverseDirection bool) (stop bool)

// IterateSplitLeaves is an iterator over object split-tree leaves in direct order.
//
// If member handler returns true, then the iterator aborts without error.
func IterateSplitLeaves(r ObjectSource, addr oid.Address, h func(*object.Object) bool) error {
	info, err := r.Head(addr)
	if err != nil {
		return fmt.Errorf("receiving information about the object: %w", err)
	}

	switch res := info.(type) {
	default:
		panic(fmt.Sprintf("unexpected result of %T: %T", r, info))
	case *object.Object:
		h(res)
	case *object.SplitInfo:
		if res.SplitID() == nil {
			return iterateV2Split(r, res, addr.Container(), h)
		} else {
			return iterateV1Split(r, res, addr.Container(), h)
		}
	}

	return nil
}

func iterateV1Split(r ObjectSource, info *object.SplitInfo, cID cid.ID, handler func(*object.Object) bool) error {
	var addr oid.Address
	addr.SetContainer(cID)

	linkID := info.GetLink()
	if !linkID.IsZero() {
		addr.SetObject(linkID)

		linkObj, err := headFromReceiver(r, addr)
		if err != nil {
			return err
		}

		for _, child := range linkObj.Children() {
			addr.SetObject(child)

			childHeader, err := headFromReceiver(r, addr)
			if err != nil {
				return err
			}

			if stop := handler(childHeader); stop {
				return nil
			}
		}

		handler(linkObj)

		return nil
	}

	lastID := info.GetLastPart()
	if !lastID.IsZero() {
		addr.SetObject(lastID)
		return iterateFromLastObject(r, addr, handler)
	}

	return errors.New("neither link, nor last object ID is found")
}

func iterateV2Split(r ObjectSource, info *object.SplitInfo, cID cid.ID, handler func(*object.Object) bool) error {
	var addr oid.Address
	addr.SetContainer(cID)

	linkID := info.GetLink()
	if !linkID.IsZero() {
		addr.SetObject(linkID)

		linkObjRaw, err := r.Get(addr)
		if err != nil {
			return fmt.Errorf("receiving link object %s: %w", addr, err)
		}

		if stop := handler(&linkObjRaw); stop {
			return nil
		}

		var linkObj object.Link
		err = linkObjRaw.ReadLink(&linkObj)
		if err != nil {
			return fmt.Errorf("decoding link object (%d): %w", addr, err)
		}

		for _, child := range linkObj.Objects() {
			addr.SetObject(child.ObjectID())

			childObj, err := headFromReceiver(r, addr)
			if err != nil {
				return fmt.Errorf("fetching child object (%s): %w", addr, err)
			}

			if stop := handler(childObj); stop {
				return nil
			}
		}

		return nil
	}

	lastID := info.GetLastPart()
	if !lastID.IsZero() {
		addr.SetObject(lastID)
		return iterateFromLastObject(r, addr, handler)
	}

	return errors.New("neither link, nor last object ID is found")
}

func iterateFromLastObject(r ObjectSource, lastAddr oid.Address, handler func(*object.Object) bool) error {
	var idBuff []oid.ID
	addr := lastAddr

	for {
		obj, err := headFromReceiver(r, addr)
		if err != nil {
			return err
		}

		oID := obj.GetID()
		idBuff = append(idBuff, oID)

		prevOID := obj.GetPreviousID()
		if prevOID.IsZero() {
			break
		}

		addr.SetObject(prevOID)
	}

	for i := len(idBuff) - 1; i >= 0; i-- {
		addr.SetObject(idBuff[i])

		childObj, err := headFromReceiver(r, addr)
		if err != nil {
			return err
		}

		if stop := handler(childObj); stop {
			return nil
		}
	}

	return nil
}

func headFromReceiver(r ObjectSource, addr oid.Address) (*object.Object, error) {
	res, err := r.Head(addr)
	if err != nil {
		return nil, fmt.Errorf("fetching information about %s: %w", addr, err)
	}

	switch v := res.(type) {
	case *object.Object:
		return v, nil
	default:
		return nil, fmt.Errorf("unexpected information: %T", res)
	}
}
