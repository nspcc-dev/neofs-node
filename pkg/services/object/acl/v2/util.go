package v2

import (
	"errors"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
)

// assertVerb checks that token is applicable to the particular request.
func assertVerb(tok session.Object, reqVerb session.ObjectVerb) bool {
	switch reqVerb {
	default:
		return tok.AssertVerb(reqVerb)
	case session.VerbObjectHead:
		return tok.AssertVerb(
			session.VerbObjectHead,
			session.VerbObjectGet,
			session.VerbObjectDelete,
			session.VerbObjectRange)
	case session.VerbObjectSearch:
		return tok.AssertVerb(session.VerbObjectSearch, session.VerbObjectDelete)
	case session.VerbObjectRange:
		return tok.AssertVerb(session.VerbObjectRange)
	}
}

// assertSessionRelation checks if given token describing the NeoFS session
// relates to the given container and optional object. Missing object
// means that the context isn't bound to any NeoFS object in the container.
// Returns no error iff relation is correct. Criteria:
//
//	session is bound to the given container
//	object is not specified or session is bound to this object
//
// Session MUST be bound to the particular container, otherwise behavior is undefined.
func assertSessionRelation(tok session.Object, cnr cid.ID, obj oid.ID) error {
	if !tok.AssertContainer(cnr) {
		return errors.New("requested container is not related to the session")
	}

	// if session relates to object's removal, we don't check
	// relation of the tombstone to the session here since user
	// can't predict tomb's ID.
	if !tok.AssertVerb(session.VerbObjectDelete) && !obj.IsZero() && !tok.AssertObject(obj) {
		return errors.New("requested object is not related to the session")
	}

	return nil
}
