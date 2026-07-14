package v2

import (
	"slices"
	"testing"

	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/stretchr/testify/require"
)

func TestIsVerbCompatible(t *testing.T) {
	// Source: https://nspcc.ru/upload/neofs-spec-latest.pdf#page=28
	table := map[session.ObjectVerb][]session.ObjectVerb{
		session.VerbObjectPut:    {session.VerbObjectPut},
		session.VerbObjectDelete: {session.VerbObjectDelete},
		session.VerbObjectGet:    {session.VerbObjectGet},
		session.VerbObjectHead: {
			session.VerbObjectHead,
			session.VerbObjectGet,
			session.VerbObjectDelete,
			session.VerbObjectRange,
		},
		session.VerbObjectRange:  {session.VerbObjectRange},
		session.VerbObjectSearch: {session.VerbObjectSearch, session.VerbObjectDelete},
	}

	verbs := []session.ObjectVerb{
		session.VerbObjectPut,
		session.VerbObjectDelete,
		session.VerbObjectHead,
		session.VerbObjectRange,
		session.VerbObjectGet,
		session.VerbObjectSearch,
	}

	var tok session.Object

	for op, list := range table {
		for _, verb := range verbs {
			var contains = slices.Contains(list, verb)
			tok.ForVerb(verb)

			require.Equal(t, contains, assertVerb(tok, op),
				"%v in token, %s executing", verb, op)
		}
	}
}

func TestAssertSessionRelation(t *testing.T) {
	var tok session.Object
	cnr := cidtest.ID()
	cnrOther := cidtest.ID()
	obj := oidtest.ID()
	objOther := oidtest.ID()

	// make sure ids differ, otherwise test won't work correctly
	require.False(t, cnrOther == cnr)
	require.False(t, objOther == obj)

	// bind session to the container (required)
	tok.BindContainer(cnr)

	// test container-global session
	require.NoError(t, assertSessionRelation(tok, cnr, oid.ID{}))
	require.NoError(t, assertSessionRelation(tok, cnr, obj))
	require.Error(t, assertSessionRelation(tok, cnrOther, oid.ID{}))
	require.Error(t, assertSessionRelation(tok, cnrOther, obj))

	// limit the session to the particular object
	tok.LimitByObjects(obj)

	// test fixed object session (here obj arg must be non-nil everywhere)
	require.NoError(t, assertSessionRelation(tok, cnr, obj))
	require.Error(t, assertSessionRelation(tok, cnr, objOther))
}
