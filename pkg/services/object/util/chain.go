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

// IterateAllSplitLeaves is an iterator over all object split-tree leaves in direct order.
func IterateAllSplitLeaves(r ObjectSource, addr oid.Address, h func(*object.Object)) error {
	return IterateSplitLeaves(r, addr, func(leaf *object.Object) bool {
		h(leaf)
		return false
	})
}

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

	linkID, ok := info.Link()
	if ok {
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

	lastID, ok := info.LastPart()
	if ok {
		addr.SetObject(lastID)
		return iterateFromLastObject(r, addr, handler)
	}

	return errors.New("neither link, nor last object ID is found")
}

func iterateV2Split(r ObjectSource, info *object.SplitInfo, cID cid.ID, handler func(*object.Object) bool) error {
	var addr oid.Address
	addr.SetContainer(cID)

	linkID, ok := info.Link()
	if ok {
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

	lastID, ok := info.LastPart()
	if ok {
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

		oID, _ := obj.ID()
		idBuff = append(idBuff, oID)

		prevOID, set := obj.PreviousID()
		if !set {
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

// TraverseSplitChain is an iterator over object split-tree leaves.
//
// Traversal occurs in one of two directions, which depends on what pslit info was received:
// * in direct order for link part;
// * in reverse order for last part.
func TraverseSplitChain(r ObjectSource, addr oid.Address, h SplitMemberHandler) error {
	_, err := traverseSplitChain(r, addr, h)
	return err
}

func traverseSplitChain(r ObjectSource, addr oid.Address, h SplitMemberHandler) (bool, error) {
	v, err := r.Head(addr)
	if err != nil {
		return false, err
	}

	cnr := addr.Container()

	switch res := v.(type) {
	default:
		panic(fmt.Sprintf("unexpected result of %T: %T", r, v))
	case *object.Object:
		return h(res, false), nil
	case *object.SplitInfo:
		link, withLink := res.Link()
		last, withLast := res.LastPart()

		switch {
		default:
			return false, errors.New("lack of split information")
		case withLink:
			var addr oid.Address
			addr.SetContainer(cnr)
			addr.SetObject(link)

			chain := make([]oid.ID, 0)

			if _, err := traverseSplitChain(r, addr, func(member *object.Object, reverseDirection bool) (stop bool) {
				children := member.Children()

				if reverseDirection {
					chain = append(children, chain...)
				} else {
					chain = append(chain, children...)
				}

				return false
			}); err != nil {
				return false, err
			}

			var reverseChain []*object.Object

			for i := range chain {
				addr.SetObject(chain[i])

				if stop, err := traverseSplitChain(r, addr, func(member *object.Object, reverseDirection bool) (stop bool) {
					if !reverseDirection {
						return h(member, false)
					}

					reverseChain = append(reverseChain, member)
					return false
				}); err != nil || stop {
					return stop, err
				}
			}

			for i := len(reverseChain) - 1; i >= 0; i-- {
				if h(reverseChain[i], false) {
					return true, nil
				}
			}
		case withLast:
			var addr oid.Address
			addr.SetContainer(cnr)

			for last, withLast = res.LastPart(); withLast; {
				addr.SetObject(last)

				var directChain []*object.Object

				if _, err := traverseSplitChain(r, addr, func(member *object.Object, reverseDirection bool) (stop bool) {
					if reverseDirection {
						last, withLast = member.PreviousID()
						return h(member, true)
					}

					directChain = append(directChain, member)

					return false
				}); err != nil {
					return false, err
				}

				for i := len(directChain) - 1; i >= 0; i-- {
					if h(directChain[i], true) {
						return true, nil
					}
				}

				if len(directChain) > 0 {
					last, withLast = directChain[len(directChain)-1].PreviousID()
				}
			}
		}
	}

	return false, nil
}
