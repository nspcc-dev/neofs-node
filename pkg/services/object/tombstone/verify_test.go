package tombstone

import (
	"context"
	"fmt"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

type headRes struct {
	h   *object.Object
	err error
}

type testObjectSource struct {
	searchV1 map[object.SplitID][]oid.ID
	searchV2 map[oid.ID][]oid.ID
	head     map[oid.Address]headRes
}

func (t *testObjectSource) Head(_ context.Context, addr oid.Address) (*object.Object, error) {
	res := t.head[addr]
	return res.h, res.err
}

func (t *testObjectSource) SearchOne(_ context.Context, _ cid.ID, ff object.SearchFilters) (oid.ID, error) {
	var id oid.ID
	f := ff[0]

	switch f.Header() {
	case object.FilterSplitID:
		if t.searchV1 == nil {
			return id, nil
		}

		var splitID object.SplitID
		err := splitID.Parse(f.Value())
		if err != nil {
			panic(err)
		}

		if len(t.searchV1[splitID]) == 1 {
			return t.searchV1[splitID][0], nil
		}
		return id, nil
	case object.FilterFirstSplitObject:
		if t.searchV2 == nil {
			return id, nil
		}

		var firstObject oid.ID
		err := firstObject.DecodeString(f.Value())
		if err != nil {
			panic(err)
		}

		if len(t.searchV2[firstObject]) == 1 {
			return t.searchV2[firstObject][0], nil
		}
		return id, nil
	default:
		panic("unexpected search call")
	}
}

func TestVerifier_VerifyTomb(t *testing.T) {
	os := &testObjectSource{}
	ctx := context.Background()

	v := NewVerifier(os)

	t.Run("tombstone with children", func(t *testing.T) {
		ts := objecttest.Object()
		cnr := cidtest.ID()
		child := objectWithCnr(cnr, true)
		childID := child.GetID()
		splitID := child.SplitID()

		var addr oid.Address
		addr.SetContainer(cnr)
		addr.SetObject(childID)

		ts.SetContainerID(cnr)
		ts.AssociateDeleted(childID)

		t.Run("V1", func(t *testing.T) {
			t.Run("LINKs can not be found", func(t *testing.T) {
				*os = testObjectSource{
					head: map[oid.Address]headRes{
						addr: {
							h: &child,
						},
					},
				}

				require.NoError(t, v.VerifyTombStoneWithoutPayload(ctx, ts))
			})

			t.Run("LINKs can be found", func(t *testing.T) {
				link := objectWithCnr(cnr, false)
				link.SetChildren(childID)
				linkID := link.GetID()

				objectcore.AddressOf(&link)

				*os = testObjectSource{
					head: map[oid.Address]headRes{
						addr: {
							h: &child,
						},
						objectcore.AddressOf(&link): {
							h: &link,
						},
					},
					searchV1: map[object.SplitID][]oid.ID{
						*splitID: {linkID},
					},
				}

				err := v.VerifyTombStoneWithoutPayload(ctx, ts)
				require.ErrorContains(t, err, "V1")
				require.ErrorContains(t, err, "found link object")
			})
		})

		t.Run("V2", func(t *testing.T) {
			child.SetSplitID(nil)

			t.Run("removing first object", func(t *testing.T) {
				*os = testObjectSource{
					head: map[oid.Address]headRes{
						addr: {
							h: &child,
						},
					},
					searchV2: map[oid.ID][]oid.ID{
						childID: {oidtest.ID()}, // the first object is a chain ID in itself
					},
				}

				err := v.VerifyTombStoneWithoutPayload(ctx, ts)
				require.ErrorContains(t, err, "V2")
				require.ErrorContains(t, err, "found link object")
			})

			firstObject := oidtest.ID()
			child.SetFirstID(firstObject)

			t.Run("LINKs can not be found", func(t *testing.T) {
				*os = testObjectSource{
					head: map[oid.Address]headRes{
						addr: {
							h: &child,
						},
					},
				}

				require.NoError(t, v.VerifyTombStoneWithoutPayload(ctx, ts))
			})

			t.Run("LINKs can be found", func(t *testing.T) {
				*os = testObjectSource{
					head: map[oid.Address]headRes{
						addr: {
							h: &child,
						},
					},
					searchV2: map[oid.ID][]oid.ID{
						firstObject: {oidtest.ID()},
					},
				}

				err := v.VerifyTombStoneWithoutPayload(ctx, ts)
				require.ErrorContains(t, err, "V2")
				require.ErrorContains(t, err, "found link object")
			})
		})

		t.Run("EC", func(t *testing.T) {
			var partHdr object.Object
			partHdr.SetParent(new(object.Object)) // any
			partHdr.SetAttributes(
				object.NewAttribute("__NEOFS__EC_ANY", "any"),
			)

			os.head = map[oid.Address]headRes{
				oid.NewAddress(cnr, childID): {h: &partHdr},
			}

			err := v.VerifyTombStoneWithoutPayload(ctx, ts)
			require.EqualError(t, err, "object has EC attributes")
		})
	})

	t.Run("tomb with parent", func(t *testing.T) {
		addr := oidtest.Address()
		si := object.NewSplitInfo()
		siErr := object.NewSplitInfoError(si)

		ts := objecttest.Object()
		ts.SetContainerID(addr.Container())
		ts.AssociateDeleted(addr.Object())

		*os = testObjectSource{
			head: map[oid.Address]headRes{
				addr: {
					err: siErr,
				},
			},
		}

		require.NoError(t, v.VerifyTombStoneWithoutPayload(ctx, ts))
	})

	t.Run("already removed", func(t *testing.T) {
		cnr := cidtest.ID()
		id := oidtest.ID()
		ts := objecttest.Object()
		ts.SetContainerID(cnr)
		ts.AssociateDeleted(id)

		os.head = make(map[oid.Address]headRes)
		os.head[oid.NewAddress(cnr, id)] = headRes{err: apistatus.ErrObjectAlreadyRemoved}

		require.NoError(t, v.VerifyTombStoneWithoutPayload(ctx, ts))

		t.Run("V1", func(t *testing.T) {
			rootV1ID := oidtest.ID()
			splitID := objecttest.SplitID()
			var rootV1Hdr object.Object
			rootV1Hdr.SetSplitID(&splitID)

			ts.AssociateDeleted(rootV1ID)

			os.head[oid.NewAddress(cnr, rootV1ID)] = headRes{h: &rootV1Hdr}

			v1Children := oidtest.IDs(3)
			for i := range v1Children {
				os.head[oid.NewAddress(cnr, v1Children[i])] = headRes{err: apistatus.ErrObjectAlreadyRemoved}
			}

			os.searchV1 = make(map[object.SplitID][]oid.ID)
			os.searchV1[splitID] = v1Children

			require.NoError(t, v.VerifyTombStoneWithoutPayload(ctx, ts))
		})
	})

	t.Run("not a TS", func(t *testing.T) {
		o := objecttest.Object()
		o.SetType(object.TypeRegular)

		require.Error(t, v.VerifyTombStoneWithoutPayload(ctx, o))
	})

	t.Run("incorrect version", func(t *testing.T) {
		o := objecttest.Object()
		ver := versionSDK.New(1, 17)
		o.SetVersion(&ver)

		require.Error(t, v.VerifyTombStoneWithoutPayload(ctx, o))
	})

	t.Run("empty target", func(t *testing.T) {
		o := objecttest.Object()
		o.AssociateDeleted(oid.ID{})

		require.Error(t, v.VerifyTombStoneWithoutPayload(ctx, o))
	})

	t.Run("EC", func(t *testing.T) {
		cnr := cidtest.ID()
		ver := versionSDK.New(2, 18)
		partID := oidtest.ID()

		var partHdr object.Object
		partHdr.SetParent(new(object.Object)) // any
		partHdr.SetAttributes(
			object.NewAttribute("__NEOFS__EC_ANY", "any"),
		)

		var tomb object.Object
		tomb.SetVersion(&ver)
		tomb.SetContainerID(cnr)
		tomb.AssociateDeleted(partID)

		os.head = map[oid.Address]headRes{
			oid.NewAddress(cnr, partID): {h: &partHdr},
		}

		err := v.VerifyTombStoneWithoutPayload(ctx, tomb)
		require.EqualError(t, err, "object has EC attributes")
	})

	for _, typ := range []object.Type{object.TypeLink, object.TypeTombstone, object.TypeLock} {
		t.Run(fmt.Sprintf("target %s", typ), func(t *testing.T) {
			ts := objecttest.Object()
			deleted := objectWithCnr(ts.GetContainerID(), false)

			deleted.SetType(typ)
			ts.AssociateDeleted(objectcore.AddressOf(&deleted).Object())

			*os = testObjectSource{
				head: map[oid.Address]headRes{
					objectcore.AddressOf(&deleted): {h: &deleted},
				},
			}

			err := v.VerifyTombStoneWithoutPayload(ctx, ts)
			require.EqualError(t, err, fmt.Sprintf("tombstone target is %s", typ))
		})
	}

	t.Run("ok", func(t *testing.T) {
		cnr := cidtest.ID()

		ts := objecttest.Object()
		deleted := objectWithCnr(cnr, false)

		ts.SetContainerID(cnr)
		ts.AssociateDeleted(objectcore.AddressOf(&deleted).Object())

		*os = testObjectSource{
			head: map[oid.Address]headRes{
				objectcore.AddressOf(&deleted): {h: &deleted},
			},
		}

		require.NoError(t, v.VerifyTombStoneWithoutPayload(ctx, ts))
	})
}

func objectWithCnr(cnr cid.ID, hasParent bool) object.Object {
	obj := objecttest.Object()
	obj.SetType(object.TypeRegular)
	obj.SetContainerID(cnr)

	if !hasParent {
		obj.ResetRelations()
	}

	return obj
}
