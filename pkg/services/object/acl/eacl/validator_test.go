package eacl

import (
	"math/rand"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/stretchr/testify/require"
)

func TestTargetMatches(t *testing.T) {
	pubs := make([][]byte, 3)
	for i := range pubs {
		pubs[i] = make([]byte, 33)
		pubs[i][0] = 0x02

		_, err := rand.Read(pubs[i][1:])
		require.NoError(t, err)
	}

	tgt1 := eacl.NewTarget()
	tgt1.SetBinaryKeys(pubs[0:2])
	tgt1.SetRole(eacl.RoleUser)

	tgt2 := eacl.NewTarget()
	tgt2.SetRole(eacl.RoleOthers)

	r := eacl.NewRecord()
	r.SetTargets(tgt1, tgt2)

	u := newValidationUnit(eacl.RoleUser, pubs[0])
	require.True(t, targetMatches(u, r))

	u = newValidationUnit(eacl.RoleUser, pubs[2])
	require.False(t, targetMatches(u, r))

	u = newValidationUnit(eacl.RoleUnknown, pubs[1])
	require.True(t, targetMatches(u, r))

	u = newValidationUnit(eacl.RoleOthers, pubs[2])
	require.True(t, targetMatches(u, r))

	u = newValidationUnit(eacl.RoleSystem, pubs[2])
	require.False(t, targetMatches(u, r))
}

func newValidationUnit(role eacl.Role, key []byte) *ValidationUnit {
	return &ValidationUnit{
		role: role,
		key:  key,
	}
}
