package acl

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type emptyEACLSource struct{}

func (e emptyEACLSource) GetEACL(_ cid.ID) (eacl.Table, error) {
	return eacl.Table{}, nil
}

type testEACLSource struct {
	table eacl.Table
}

func (s testEACLSource) GetEACL(cid.ID) (eacl.Table, error) {
	return s.table, nil
}

func TestCheckEACLUnavailableObjectAttributes(t *testing.T) {
	cnr, obj := cidtest.ID(), oidtest.ID()
	targets := []eacl.Target{eacl.NewTargetByRole(eacl.RoleOthers)}

	for _, op := range []acl.Op{acl.OpObjectSearch, acl.OpObjectDelete} {
		t.Run(op.String(), func(t *testing.T) {
			var req any = &protoobject.SearchV2Request{}
			var reqObj oid.ID
			if op == acl.OpObjectDelete {
				req = &protoobject.DeleteRequest{}
				reqObj = obj
			}
			for _, useBearer := range []bool{false, true} {
				name := "container table"
				if useBearer {
					name = "bearer table"
				}
				t.Run(name, func(t *testing.T) {
					for _, tc := range []struct {
						name    string
						action  eacl.Action
						filters []eacl.Filter
						deny    bool
					}{
						{
							name:    "unavailable attribute does not deny",
							action:  eacl.ActionDeny,
							filters: []eacl.Filter{eacl.NewObjectPropertyFilter("classified", eacl.MatchNotPresent, "")},
						},
						{
							name:    "unavailable attribute does not bypass later denial",
							action:  eacl.ActionAllow,
							filters: []eacl.Filter{eacl.NewObjectPropertyFilter("classified", eacl.MatchNotPresent, "")},
							deny:    true,
						},
						{
							name:   "whole rule is skipped",
							action: eacl.ActionDeny,
							filters: []eacl.Filter{
								eacl.NewFilterObjectsFromContainer(cnr),
								eacl.NewObjectPropertyFilter("classified", eacl.MatchNotPresent, ""),
							},
						},
						{
							name:    "container ID remains available",
							action:  eacl.ActionDeny,
							filters: []eacl.Filter{eacl.NewFilterObjectsFromContainer(cnr)},
							deny:    true,
						},
						{
							name:    "object ID is available only for DELETE",
							action:  eacl.ActionDeny,
							filters: []eacl.Filter{eacl.NewFilterObjectWithID(obj)},
							deny:    op == acl.OpObjectDelete,
						},
						{
							name:    "unavailable system field does not deny",
							action:  eacl.ActionDeny,
							filters: []eacl.Filter{eacl.NewFilterObjectOwnerEquals(usertest.ID())},
						},
						{
							name:    "absent request header still matches",
							action:  eacl.ActionDeny,
							filters: []eacl.Filter{eacl.NewRequestHeaderFilter("missing", eacl.MatchNotPresent, "")},
							deny:    true,
						},
					} {
						t.Run(tc.name, func(t *testing.T) {
							records := []eacl.Record{eacl.ConstructRecord(tc.action, eacl.Operation(op), targets, tc.filters...)}
							if tc.action == eacl.ActionAllow {
								records = append(records, eacl.ConstructRecord(eacl.ActionDeny, eacl.Operation(op), targets))
							}
							table := eacl.ConstructTable(records)
							var original eacl.Table
							table.CopyTo(&original)
							checker := NewChecker(new(CheckerPrm).
								SetLocalStorage(&engine.StorageEngine{}).
								SetValidator(eacl.NewValidator()).
								SetEACLSource(testEACLSource{table}).
								SetHeaderSource(emptyHeaderSource{}))
							info := v2.RequestInfo{Operation: op, RequestRole: acl.RoleOthers}
							basic := acl.PublicRWExtended
							basic.AllowBearerRules(op)
							info.Container.SetBasicACL(basic)
							if useBearer {
								info.Bearer = new(bearer.Token)
								info.Bearer.SetEACLTable(table)
							}
							err := checker.CheckEACL(t.Context(), req, cnr, reqObj, info)
							if tc.deny {
								require.ErrorIs(t, err, errEACLDeniedByRule)
							} else {
								require.NoError(t, err)
							}
							require.Equal(t, original, table, "shared table must not be mutated")
							if useBearer {
								require.Equal(t, original, info.Bearer.EACLTable())
							}
						})
					}
				})
			}
		})
	}
}

func TestCheckEACLNotPresentObjectAttribute(t *testing.T) {
	cnr, obj := cidtest.ID(), oidtest.ID()
	for _, op := range []acl.Op{acl.OpObjectGet, acl.OpObjectHead} {
		t.Run(op.String(), func(t *testing.T) {
			table := eacl.ConstructTable([]eacl.Record{eacl.ConstructRecord(
				eacl.ActionDeny, eacl.Operation(op),
				[]eacl.Target{eacl.NewTargetByRole(eacl.RoleOthers)},
				eacl.NewObjectPropertyFilter("classified", eacl.MatchNotPresent, ""),
			)})
			checker := NewChecker(new(CheckerPrm).
				SetLocalStorage(&engine.StorageEngine{}).
				SetValidator(eacl.NewValidator()).
				SetEACLSource(testEACLSource{table}).
				SetHeaderSource(emptyHeaderSource{}))
			info := v2.RequestInfo{Operation: op, RequestRole: acl.RoleOthers}
			info.Container.SetBasicACL(acl.PublicRWExtended)

			for _, present := range []bool{false, true} {
				var header object.Object
				header.SetContainerID(cnr)
				if present {
					header.SetAttributes(object.NewAttribute("classified", "secret"))
				}
				msg := header.ProtoMessage().Header
				buf := make([]byte, msg.MarshaledSize())
				msg.MarshalStable(buf)
				err := checker.CheckEACL(t.Context(), buf, cnr, obj, info)
				if present {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, errEACLDeniedByRule)
				}
			}
		})
	}
}

type emptyHeaderSource struct{}

func (e emptyHeaderSource) Head(_ context.Context, _ oid.Address) (*object.Object, error) {
	return nil, nil
}

func TestStickyCheck(t *testing.T) {
	checker := NewChecker(new(CheckerPrm).
		SetLocalStorage(&engine.StorageEngine{}).
		SetValidator(eacl.NewValidator()).
		SetEACLSource(emptyEACLSource{}).SetHeaderSource(emptyHeaderSource{}))

	t.Run("system role", func(t *testing.T) {
		var info = v2.RequestInfo{
			RequestRole: acl.RoleContainer,
			SenderKey:   make([]byte, 33), // any non-empty key
		}

		require.True(t, checker.StickyBitCheck(info, usertest.ID()))

		var basicACL acl.Basic
		basicACL.MakeSticky()

		info.Container.SetBasicACL(basicACL)

		require.True(t, checker.StickyBitCheck(info, usertest.ID()))
	})

	t.Run("owner ID and/or public key emptiness", func(t *testing.T) {
		var info = v2.RequestInfo{
			RequestRole: acl.RoleOthers, // should be non-system role
		}

		assertFn := func(isSticky, withKey, withOwner, expected bool) {
			info := info
			if isSticky {
				var basicACL acl.Basic
				basicACL.MakeSticky()

				info.Container.SetBasicACL(basicACL)
			}

			if withKey {
				info.SenderKey = make([]byte, 33)
			} else {
				info.SenderKey = nil
			}

			var ownerID user.ID

			if withOwner {
				ownerID = usertest.ID()
			}

			require.Equal(t, expected, checker.StickyBitCheck(info, ownerID))
		}

		assertFn(true, false, false, false)
		assertFn(true, true, false, false)
		assertFn(true, false, true, false)
		assertFn(false, false, false, true)
		assertFn(false, true, false, true)
		assertFn(false, false, true, true)
		assertFn(false, true, true, true)
	})
}
