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
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type emptyEACLSource struct{}

func (e emptyEACLSource) GetEACL(_ cid.ID) (eacl.Table, error) {
	return eacl.Table{}, nil
}

type emptyHeaderSource struct{}

func (e emptyHeaderSource) Head(_ context.Context, _ oid.Address) (*object.Object, error) {
	return nil, nil
}

func TestCheckEACLBinaryRequestHeaders(t *testing.T) {
	cnr, id := cidtest.ID(), oidtest.ID()
	targets := []eacl.Target{eacl.NewTargetByRole(eacl.RoleOthers)}
	for _, op := range []eacl.Operation{eacl.OperationGet, eacl.OperationHead} {
		t.Run(op.String(), func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				classified   string
				probe        string
				firstAction  eacl.Action
				requestMatch eacl.Match
				sameRule     bool
				deny         bool
			}{
				{name: "request deny", classified: "public", probe: "yes", deny: true},
				{name: "different request header", classified: "public", probe: "no"},
				{name: "missing request header", classified: "public"},
				{name: "object deny", classified: "secret", probe: "no", deny: true},
				{name: "earlier allow", classified: "secret", probe: "yes", firstAction: eacl.ActionAllow},
				{name: "request not present matches", classified: "public", requestMatch: eacl.MatchNotPresent, deny: true},
				{name: "request not present does not match", classified: "public", probe: "yes", requestMatch: eacl.MatchNotPresent},
				{name: "combined rule matches", classified: "secret", probe: "yes", sameRule: true, deny: true},
				{name: "combined rule request mismatch", classified: "secret", probe: "no", sameRule: true},
				{name: "combined rule object mismatch", classified: "public", probe: "yes", sameRule: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if tc.firstAction == eacl.ActionUnspecified {
						tc.firstAction = eacl.ActionDeny
					}
					if tc.requestMatch == eacl.MatchUnspecified {
						tc.requestMatch = eacl.MatchStringEqual
					}
					objectFilter := eacl.NewObjectPropertyFilter("classified", eacl.MatchStringEqual, "secret")
					requestFilter := eacl.NewRequestHeaderFilter("X-Probe", tc.requestMatch, "yes")
					records := []eacl.Record{
						eacl.ConstructRecord(tc.firstAction, op, targets, objectFilter),
						eacl.ConstructRecord(eacl.ActionDeny, op, targets, requestFilter),
					}
					if tc.sameRule {
						records = []eacl.Record{eacl.ConstructRecord(eacl.ActionDeny, op, targets, objectFilter, requestFilter)}
					}
					table := eacl.ConstructTable(records)
					var info v2.RequestInfo
					info.Container.SetBasicACL(acl.PublicRWExtended)
					info.RequestRole = acl.RoleOthers
					info.Operation = acl.Op(op)
					info.Bearer = new(bearer.Token)
					info.Bearer.SetEACLTable(table)
					meta := new(protosession.RequestMetaHeader)
					if tc.probe != "" {
						meta.XHeaders = []*protosession.XHeader{{Key: "X-Probe", Value: tc.probe}}
					}
					if op == eacl.OperationGet {
						info.SrcRequest = &protoobject.GetRequest{MetaHeader: meta}
					} else {
						info.SrcRequest = &protoobject.HeadRequest{MetaHeader: meta}
					}
					obj := object.New(cnr, usertest.ID())
					obj.SetAttributes(object.NewAttribute("classified", tc.classified))
					checker := NewChecker(new(CheckerPrm).
						SetLocalStorage(&engine.StorageEngine{}).
						SetValidator(eacl.NewValidator()).
						SetEACLSource(emptyEACLSource{}).SetHeaderSource(emptyHeaderSource{}))
					buf, err := proto.Marshal(obj.ProtoMessage().Header)
					require.NoError(t, err)
					err = checker.CheckEACL(t.Context(), buf, cnr, id, info)
					if tc.deny {
						require.ErrorIs(t, err, errEACLDeniedByRule)
					} else {
						require.NoError(t, err)
					}
				})
			}
		})
	}
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
