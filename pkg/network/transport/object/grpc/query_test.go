package object

import (
	"context"
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/query"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/storagegroup"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testQueryEntity struct {
		// Set of interfaces which testQueryEntity must implement, but some methods from those does not call.
		Filter

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ filterCreator     = (*testQueryEntity)(nil)
	_ localQueryImposer = (*testQueryEntity)(nil)
)

func (s *testQueryEntity) imposeQuery(_ context.Context, c CID, q []byte, v int) ([]Address, error) {
	if s.f != nil {
		s.f(c, q, v)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]Address), nil
}

func (s *testQueryEntity) createFilter(p query.Query) Filter {
	if s.f != nil {
		s.f(p)
	}
	return s
}

func (s *testQueryEntity) Iterate(p Filter, h localstore.MetaHandler) error {
	if s.f != nil {
		s.f(p)
	}
	if s.err != nil {
		return s.err
	}
	for _, item := range s.res.([]localstore.ListItem) {
		h(&item.ObjectMeta)
	}
	return nil
}

func Test_queryVersionController_imposeQuery(t *testing.T) {
	ctx := context.TODO()
	cid := testObjectAddress(t).CID

	t.Run("unsupported version", func(t *testing.T) {
		qImp := &queryVersionController{
			m: make(map[int]localQueryImposer),
		}

		res, err := qImp.imposeQuery(ctx, cid, nil, 1)
		require.EqualError(t, err, errUnsupportedQueryVersion.Error())
		require.Empty(t, res)
	})

	t.Run("correct imposer choose", func(t *testing.T) {
		m := make(map[int]localQueryImposer)
		qData := testData(t, 10)

		qImp := &queryVersionController{m: m}

		m[0] = &testQueryEntity{
			f: func(items ...interface{}) {
				t.Run("correct imposer params", func(t *testing.T) {
					require.Equal(t, cid, items[0].(CID))
					require.Equal(t, qData, items[1].([]byte))
					require.Equal(t, 0, items[2].(int))
				})
			},
			err: errors.New(""), // just to prevent panic
		}

		_, _ = qImp.imposeQuery(ctx, cid, qData, 0)
	})

	t.Run("correct imposer result", func(t *testing.T) {
		t.Run("error", func(t *testing.T) {
			m := make(map[int]localQueryImposer)
			qImp := &queryVersionController{m: m}

			impErr := errors.New("test error for query imposer")

			m[0] = &testQueryEntity{
				err: impErr, // force localQueryImposer to return impErr
			}

			res, err := qImp.imposeQuery(ctx, cid, nil, 0)

			// ascertain that error returns as expected
			require.EqualError(t, err, impErr.Error())
			// ascertain that result is empty
			require.Empty(t, res)

			// create test address list
			addrList := testAddrList(t, 5)

			m[1] = &testQueryEntity{
				res: addrList, // force localQueryImposer to return addrList
			}

			res, err = qImp.imposeQuery(ctx, cid, nil, 1)
			require.NoError(t, err)

			// ascertain that result returns as expected
			require.Equal(t, addrList, res)
		})
	})
}

func Test_coreQueryImposer_imposeQuery(t *testing.T) {
	v := 1
	ctx := context.TODO()
	cid := testObjectAddress(t).CID
	log := zap.L()

	t.Run("query unmarshal failure", func(t *testing.T) {
		var (
			qErr error
			data []byte
		)

		// create invalid query binary representation
		for {
			data = testData(t, 1024)
			if qErr = new(query.Query).Unmarshal(data); qErr != nil {
				break
			}
		}

		s := &coreQueryImposer{
			log: zap.L(),
		}

		// trying to impose invalid query data
		res, err := s.imposeQuery(ctx, cid, data, v)

		// ascertain that reached error exactly like in unmarshal
		require.EqualError(t, err, errSearchQueryUnmarshal.Error())

		// ascertain that empty result returned
		require.Nil(t, res)
	})

	t.Run("mould query failure", func(t *testing.T) {
		// create testQuery with CID filter with value other than cid
		testQuery := &query.Query{Filters: []QueryFilter{{Type: query.Filter_Exact, Name: KeyCID, Value: cid.String() + "1"}}}

		// try to mould this testQuery
		mErr := mouldQuery(cid, testQuery)

		// ascertain that testQuery mould failed
		require.Error(t, mErr)

		// ascertain that testQuery marshals normally
		d, err := testQuery.Marshal()
		require.NoError(t, err)

		s := &coreQueryImposer{
			log: log,
		}

		// try to impose testQuery
		res, err := s.imposeQuery(ctx, cid, d, v)

		// ascertain that impose fails with same error as mould
		require.EqualError(t, err, errLocalQueryImpose.Error())

		// ascertain that result is empty
		require.Nil(t, res)
	})

	t.Run("local store listing", func(t *testing.T) {
		// create testQuery and object which matches to it
		testQuery, obj := testFullObjectWithQuery(t)

		// ascertain testQuery marshals normally
		qBytes, err := testQuery.Marshal()
		require.NoError(t, err)

		t.Run("listing error", func(t *testing.T) {
			// create new error for test
			lsErr := errors.New("test error of local store listing")

			// create test query imposer with mocked always failing lister
			qImposer := &coreQueryImposer{
				fCreator: new(coreFilterCreator),
				lsLister: &testQueryEntity{err: lsErr},
				log:      log,
			}

			// try to impose testQuery
			list, err := qImposer.imposeQuery(ctx, obj.SystemHeader.CID, qBytes, v)

			// ascertain that impose fails same error as lister
			require.EqualError(t, err, errLocalQueryImpose.Error())

			// ascertain that result is empty
			require.Empty(t, list)
		})

		t.Run("correct parameter", func(t *testing.T) {
			// create new mocked filter creator
			fc := new(testQueryEntity)
			fc.res = fc

			// create testQuery imposer
			qImposer := &coreQueryImposer{
				fCreator: fc,
				lsLister: &testQueryEntity{
					f: func(p ...interface{}) {
						// intercept lister arguments
						// ascertain that argument is as expected
						require.Equal(t, fc, p[0].(Filter))
					},
					err: errors.New(""),
				},
				log: log,
			}

			_, _ = qImposer.imposeQuery(ctx, obj.SystemHeader.CID, qBytes, v)
		})

		t.Run("correct result", func(t *testing.T) {
			// create list of random address items
			addrList := testAddrList(t, 10)
			items := make([]localstore.ListItem, 0, len(addrList))
			for i := range addrList {
				items = append(items, localstore.ListItem{
					ObjectMeta: Meta{
						Object: &Object{
							SystemHeader: SystemHeader{
								ID:  addrList[i].ObjectID,
								CID: addrList[i].CID,
							},
						},
					},
				})
			}

			// create imposer with mocked lister
			qImposer := &coreQueryImposer{
				fCreator: new(coreFilterCreator),
				lsLister: &testQueryEntity{res: items},
			}

			// try to impose testQuery
			list, err := qImposer.imposeQuery(ctx, obj.SystemHeader.CID, qBytes, v)

			// ascertain that imposing finished normally
			require.NoError(t, err)

			// ascertain that resulting list size as expected
			require.Len(t, list, len(addrList))

			// ascertain that all source items are presented in result
			for i := range addrList {
				require.Contains(t, list, addrList[i])
			}
		})
	})
}

func Test_coreFilterCreator_createFilter(t *testing.T) {
	ctx := context.TODO()
	fCreator := new(coreFilterCreator)

	t.Run("composing correct filter", func(t *testing.T) {
		var f Filter

		// ascertain filter creation does not panic
		require.NotPanics(t, func() { f = fCreator.createFilter(query.Query{}) })

		// ascertain that created filter is not empty
		require.NotNil(t, f)

		// ascertain that created filter has expected name
		require.Equal(t, queryFilterName, f.GetName())
	})

	t.Run("passage on matching query", func(t *testing.T) {
		// create testQuery and object which matches to it
		testQuery, obj := testFullObjectWithQuery(t)

		// create filter for testQuery and pass object to it
		res := fCreator.createFilter(testQuery).Pass(ctx, &Meta{Object: obj})

		// ascertain that filter is passed
		require.Equal(t, localstore.CodePass, res.Code())
	})

	t.Run("failure on mismatching query", func(t *testing.T) {
		testQuery, obj := testFullObjectWithQuery(t)
		obj.SystemHeader.ID[0]++
		require.False(t, imposeQuery(testQuery, obj))

		res := fCreator.createFilter(testQuery).Pass(ctx, &Meta{Object: obj})

		require.Equal(t, localstore.CodeFail, res.Code())
	})
}

func Test_mouldQuery(t *testing.T) {
	cid := testObjectAddress(t).CID

	t.Run("invalid CID filter", func(t *testing.T) {
		// create query with CID filter with other than cid value
		query := &query.Query{Filters: []QueryFilter{{Type: query.Filter_Exact, Name: KeyCID, Value: cid.String() + "1"}}}

		// try to mould this query for cid
		err := mouldQuery(cid, query)

		// ascertain wrong CID value is not allowed
		require.EqualError(t, err, errInvalidCIDFilter.Error())
	})

	t.Run("correct CID filter", func(t *testing.T) {
		// create testQuery with CID filter with cid value
		cidF := QueryFilter{Type: query.Filter_Exact, Name: KeyCID, Value: cid.String()}
		testQuery := &query.Query{Filters: []QueryFilter{cidF}}

		// ascertain mould is processed
		require.NoError(t, mouldQuery(cid, testQuery))

		// ascertain filter is still in testQuery
		require.Contains(t, testQuery.Filters, cidF)
	})

	t.Run("missing CID filter", func(t *testing.T) {
		// create CID filter with cid value
		expF := QueryFilter{Type: query.Filter_Exact, Name: KeyCID, Value: cid.String()}

		// create empty testQuery
		testQuery := new(query.Query)

		// ascertain mould is processed
		require.NoError(t, mouldQuery(cid, testQuery))

		// ascertain exact CID filter added to testQuery
		require.Contains(t, testQuery.Filters, expF)
	})
}

func Test_applyFilter(t *testing.T) {
	k, v := "key", "value"

	t.Run("empty map", func(t *testing.T) {
		// ascertain than applyFilter always return true on empty filter map
		require.True(t, applyFilter(nil, k, v))
	})

	t.Run("passage on missing key", func(t *testing.T) {
		t.Run("exact", func(t *testing.T) {
			require.True(t, applyFilter(map[string]*QueryFilter{k: {Type: query.Filter_Exact, Value: v + "1"}}, k+"1", v))
		})

		t.Run("regex", func(t *testing.T) {
			require.True(t, applyFilter(map[string]*QueryFilter{k: {Type: query.Filter_Regex, Value: v + "1"}}, k+"1", v))
		})
	})

	t.Run("passage on key presence and matching value", func(t *testing.T) {
		t.Run("exact", func(t *testing.T) {
			require.True(t, applyFilter(map[string]*QueryFilter{k: {Type: query.Filter_Exact, Value: v}}, k, v))
		})

		t.Run("regex", func(t *testing.T) {
			require.True(t, applyFilter(map[string]*QueryFilter{k: {Type: query.Filter_Regex, Value: v + "|" + v + "1"}}, k, v))
		})
	})

	t.Run("failure on key presence and mismatching value", func(t *testing.T) {
		t.Run("exact", func(t *testing.T) {
			require.False(t, applyFilter(map[string]*QueryFilter{k: {Type: query.Filter_Exact, Value: v + "1"}}, k, v))
		})

		t.Run("regex", func(t *testing.T) {
			require.False(t, applyFilter(map[string]*QueryFilter{k: {Type: query.Filter_Regex, Value: v + "&" + v + "1"}}, k, v))
		})
	})

	t.Run("key removes from filter map", func(t *testing.T) {
		// create filter map with several elements
		m := map[string]*QueryFilter{
			k:       {Type: query.Filter_Exact, Value: v},
			k + "1": {Type: query.Filter_Exact, Value: v},
		}

		// save initial len
		initLen := len(m)

		// apply filter with key from filter map
		applyFilter(m, k, v)

		// ascertain exactly key was removed from filter map
		require.Len(t, m, initLen-1)

		// ascertain this is exactly applyFilter argument
		_, ok := m[k]
		require.False(t, ok)
	})

	t.Run("panic on unknown filter type", func(t *testing.T) {
		// create filter type other than FilterExact and FilterRegex
		fType := query.Filter_Exact + query.Filter_Regex + 1
		require.NotEqual(t, query.Filter_Exact, fType)
		require.NotEqual(t, query.Filter_Regex, fType)

		// ascertain applyFilter does not process this type but panic
		require.PanicsWithValue(t,
			fmt.Sprintf(pmUndefinedFilterType, fType),
			func() { applyFilter(map[string]*QueryFilter{k: {Type: fType}}, k, v) },
		)
	})
}

func Test_imposeQuery(t *testing.T) {
	t.Run("tombstone filter", func(t *testing.T) {
		// create testQuery with only tombstone filter
		testQuery := query.Query{Filters: []QueryFilter{{Name: transport.KeyTombstone}}}

		// create object which is not a tombstone
		obj := new(Object)

		testQueryMatch(t, testQuery, obj, func(t *testing.T, obj *Object) {
			// adding tombstone header makes object to satisfy tombstone testQuery
			obj.Headers = append(obj.Headers, Header{Value: new(object.Header_Tombstone)})
		})
	})

	t.Run("system header", func(t *testing.T) {
		addr := testObjectAddress(t)
		cid, oid, ownerID := addr.CID, addr.ObjectID, OwnerID{3}

		// create testQuery with system header filters
		testQuery := query.Query{Filters: []QueryFilter{
			{Type: query.Filter_Exact, Name: KeyCID, Value: cid.String()},
			{Type: query.Filter_Exact, Name: KeyID, Value: oid.String()},
			{Type: query.Filter_Exact, Name: KeyOwnerID, Value: ownerID.String()},
		}}

		// fn sets system header fields values to ones from filters
		fn := func(t *testing.T, obj *Object) { obj.SystemHeader = SystemHeader{CID: cid, ID: oid, OwnerID: ownerID} }

		// create object with empty system header fields
		obj := new(Object)
		testQueryMatch(t, testQuery, obj, fn)

		// create object with CID from filters
		sysHdr := SystemHeader{CID: cid}
		obj = &Object{SystemHeader: sysHdr}
		testQueryMatch(t, testQuery, obj, fn)

		// create object with OID from filters
		sysHdr.CID = CID{}
		sysHdr.ID = oid
		obj = &Object{SystemHeader: sysHdr}
		testQueryMatch(t, testQuery, obj, fn)

		// create object with OwnerID from filters
		sysHdr.ID = ID{}
		sysHdr.OwnerID = ownerID
		obj = &Object{SystemHeader: sysHdr}
		testQueryMatch(t, testQuery, obj, fn)

		// create object with CID and OwnerID from filters
		sysHdr.CID = cid
		obj = &Object{SystemHeader: sysHdr}
		testQueryMatch(t, testQuery, obj, fn)

		// create object with OID and OwnerID from filters
		sysHdr.CID = CID{}
		sysHdr.ID = oid
		obj = &Object{SystemHeader: sysHdr}
		testQueryMatch(t, testQuery, obj, fn)

		// create object with OID and OwnerID from filters
		sysHdr.ID = oid
		obj = &Object{SystemHeader: sysHdr}
		testQueryMatch(t, testQuery, obj, fn)

		// create object with CID and OID from filters
		sysHdr.CID = cid
		sysHdr.OwnerID = OwnerID{}
		obj = &Object{SystemHeader: sysHdr}
		testQueryMatch(t, testQuery, obj, fn)
	})

	t.Run("no children filter", func(t *testing.T) {
		// create testQuery with only orphan filter
		testQuery := query.Query{Filters: []QueryFilter{{Type: query.Filter_Exact, Name: transport.KeyNoChildren}}}

		// create object with child relation
		obj := &Object{Headers: []Header{{Value: &object.Header_Link{Link: &object.Link{Type: object.Link_Child}}}}}

		testQueryMatch(t, testQuery, obj, func(t *testing.T, obj *Object) {
			// child relation removal makes object to satisfy orphan testQuery
			obj.Headers = nil
		})
	})

	t.Run("has parent filter", func(t *testing.T) {
		// create testQuery with parent relation filter
		testQuery := query.Query{Filters: []QueryFilter{{Type: query.Filter_Exact, Name: transport.KeyHasParent}}}

		// create object w/o parent
		obj := new(Object)

		testQueryMatch(t, testQuery, obj, func(t *testing.T, obj *Object) {
			// adding parent relation makes object to satisfy parent testQuery
			obj.Headers = append(obj.Headers, Header{Value: &object.Header_Link{Link: &object.Link{Type: object.Link_Parent}}})
		})
	})

	t.Run("root object filter", func(t *testing.T) {
		// create testQuery with only root filter
		testQuery := query.Query{Filters: []QueryFilter{{Type: query.Filter_Exact, Name: KeyRootObject}}}

		// create object with parent relation
		obj := &Object{Headers: []Header{{Value: &object.Header_Link{Link: &object.Link{Type: object.Link_Parent}}}}}

		testQueryMatch(t, testQuery, obj, func(t *testing.T, obj *Object) {
			// parent removal makes object to satisfy root testQuery
			obj.Headers = nil
		})
	})

	t.Run("link value filters", func(t *testing.T) {
		t.Run("parent", func(t *testing.T) {
			testLinkQuery(t, transport.KeyParent, object.Link_Parent)
		})

		t.Run("child", func(t *testing.T) {
			testLinkQuery(t, KeyChild, object.Link_Child)
		})

		t.Run("previous", func(t *testing.T) {
			testLinkQuery(t, KeyPrev, object.Link_Previous)
		})

		t.Run("next", func(t *testing.T) {
			testLinkQuery(t, KeyNext, object.Link_Next)
		})

		t.Run("other", func(t *testing.T) {
			// create not usable link type
			linkKey := object.Link_Parent + object.Link_Child + object.Link_Next + object.Link_Previous

			// add some usable link to testQuery
			par := ID{1, 2, 3}
			testQuery := query.Query{Filters: []QueryFilter{{Type: query.Filter_Exact, Name: transport.KeyParent, Value: par.String()}}}

			// ascertain that undefined link type has no affect on testQuery imposing
			require.True(t, imposeQuery(testQuery, &Object{
				Headers: []Header{
					{Value: &object.Header_Link{Link: &object.Link{Type: linkKey}}},
					{Value: &object.Header_Link{Link: &object.Link{Type: object.Link_Parent, ID: par}}},
				},
			}))
		})
	})

	t.Run("user header filter", func(t *testing.T) {
		// user header key-value pair
		k, v := "header", "value"

		// query with user header filter
		query := query.Query{Filters: []QueryFilter{{
			Type:  query.Filter_Exact,
			Name:  k,
			Value: v,
		}}}

		// create user header with same key and different value
		hdr := &UserHeader{Key: k, Value: v + "1"}

		// create object with this user header
		obj := &Object{Headers: []Header{{Value: &object.Header_UserHeader{UserHeader: hdr}}}}

		testQueryMatch(t, query, obj, func(t *testing.T, obj *Object) {
			// correcting value to one from filter makes object to satisfy query
			hdr.Value = v
		})
	})

	t.Run("storage group filter", func(t *testing.T) {
		// create testQuery with only storage group filter
		testQuery := query.Query{Filters: []QueryFilter{{Type: query.Filter_Exact, Name: transport.KeyStorageGroup}}}

		// create object w/o storage group header
		obj := new(Object)

		testQueryMatch(t, testQuery, obj, func(t *testing.T, obj *Object) {
			// adding storage group headers make object to satisfy testQuery
			obj.Headers = append(obj.Headers, Header{Value: &object.Header_StorageGroup{StorageGroup: new(storagegroup.StorageGroup)}})
		})
	})
}

func Test_filterSystemHeader(t *testing.T) {
	var (
		ownerID1, ownerID2 = OwnerID{1}, OwnerID{2}
		addr1, addr2       = testObjectAddress(t), testObjectAddress(t)
		cid1, cid2         = addr1.CID, addr2.CID
		oid1, oid2         = addr1.ObjectID, addr2.ObjectID
		sysHdr             = SystemHeader{ID: oid1, OwnerID: ownerID1, CID: cid1}
	)
	require.NotEqual(t, ownerID1, ownerID2)
	require.NotEqual(t, cid1, cid2)
	require.NotEqual(t, oid1, oid2)

	t.Run("empty filter map", func(t *testing.T) {
		// ascertain that any system header satisfies to empty (nil) filter map
		require.True(t, filterSystemHeader(nil, &sysHdr))
	})

	t.Run("missing of some of the fields", func(t *testing.T) {
		// create filter map for system header
		m := sysHeaderFilterMap(sysHdr)

		// copy system header for initial values saving
		h := sysHdr

		// change CID
		h.CID = cid2

		// ascertain filter failure
		require.False(t, filterSystemHeader(m, &h))

		// remove CID from filter map
		delete(m, KeyCID)

		// ascertain filter passage
		require.True(t, filterSystemHeader(m, &h))

		m = sysHeaderFilterMap(sysHdr)
		h = sysHdr

		// change OwnerID
		h.OwnerID = ownerID2

		// ascertain filter failure
		require.False(t, filterSystemHeader(m, &h))

		// remove OwnerID from filter map
		delete(m, KeyOwnerID)

		// ascertain filter passage
		require.True(t, filterSystemHeader(m, &h))

		m = sysHeaderFilterMap(sysHdr)
		h = sysHdr

		// change ObjectID
		h.ID = oid2

		// ascertain filter failure
		require.False(t, filterSystemHeader(m, &h))

		// remove ObjectID from filter map
		delete(m, KeyID)

		// ascertain filter passage
		require.True(t, filterSystemHeader(m, &h))
	})

	t.Run("valid fields passage", func(t *testing.T) {
		require.True(t, filterSystemHeader(sysHeaderFilterMap(sysHdr), &sysHdr))
	})

	t.Run("mismatching values failure", func(t *testing.T) {
		h := sysHdr

		// make CID value not matching
		h.CID = cid2

		require.False(t, filterSystemHeader(sysHeaderFilterMap(sysHdr), &h))

		h = sysHdr

		// make ObjectID value not matching
		h.ID = oid2

		require.False(t, filterSystemHeader(sysHeaderFilterMap(sysHdr), &h))

		h = sysHdr

		// make OwnerID value not matching
		h.OwnerID = ownerID2

		require.False(t, filterSystemHeader(sysHeaderFilterMap(sysHdr), &h))
	})
}

// testQueryMatch imposes passed query to passed object for tests.
// Passed object should not match to passed query.
// Passed function must mutate object so that becomes query matching.
func testQueryMatch(t *testing.T, q query.Query, obj *Object, fn func(*testing.T, *Object)) {
	require.False(t, imposeQuery(q, obj))
	fn(t, obj)
	require.True(t, imposeQuery(q, obj))
}

// testLinkQuery tests correctness of imposing query with link filters.
// Inits object with value different from one from filter. Then uses testQueryMatch with correcting value func.
func testLinkQuery(t *testing.T, key string, lt object.Link_Type) {
	// create new relation link
	relative, err := refs.NewObjectID()
	require.NoError(t, err)

	// create another relation link
	wrongRelative := relative
	for wrongRelative.Equal(relative) {
		wrongRelative, err = refs.NewObjectID()
		require.NoError(t, err)
	}

	// create query with relation filter
	query := query.Query{Filters: []QueryFilter{{
		Type:  query.Filter_Exact,
		Name:  key,
		Value: relative.String(),
	}}}

	// create link with relation different from one from filter
	link := &object.Link{Type: lt, ID: wrongRelative}
	// create object with this link
	obj := &Object{Headers: []Header{{Value: &object.Header_Link{Link: link}}}}
	testQueryMatch(t, query, obj, func(t *testing.T, object *Object) {
		// changing link value to one from filter make object to satisfy relation query
		link.ID = relative
	})
}

// sysHeaderFilterMap creates filter map for passed system header.
func sysHeaderFilterMap(hdr SystemHeader) map[string]*QueryFilter {
	return map[string]*QueryFilter{
		KeyCID: {
			Type:  query.Filter_Exact,
			Name:  KeyCID,
			Value: hdr.CID.String(),
		},
		KeyOwnerID: {
			Type:  query.Filter_Exact,
			Name:  KeyOwnerID,
			Value: hdr.OwnerID.String(),
		},
		KeyID: {
			Type:  query.Filter_Exact,
			Name:  KeyID,
			Value: hdr.ID.String(),
		},
	}
}

// testFullObjectWithQuery creates query with set of permissible filters and object matching to this query.
func testFullObjectWithQuery(t *testing.T) (query.Query, *Object) {
	addr := testObjectAddress(t)
	selfID, cid := addr.ObjectID, addr.CID

	ownerID := OwnerID{}
	copy(ownerID[:], testData(t, refs.OwnerIDSize))

	addrList := testAddrList(t, 4)

	parID, childID, nextID, prevID := addrList[0].ObjectID, addrList[1].ObjectID, addrList[2].ObjectID, addrList[3].ObjectID

	query := query.Query{Filters: []QueryFilter{
		{Type: query.Filter_Exact, Name: transport.KeyParent, Value: parID.String()},
		{Type: query.Filter_Exact, Name: KeyPrev, Value: prevID.String()},
		{Type: query.Filter_Exact, Name: KeyNext, Value: nextID.String()},
		{Type: query.Filter_Exact, Name: KeyChild, Value: childID.String()},
		{Type: query.Filter_Exact, Name: KeyOwnerID, Value: ownerID.String()},
		{Type: query.Filter_Exact, Name: KeyID, Value: selfID.String()},
		{Type: query.Filter_Exact, Name: KeyCID, Value: cid.String()},
		{Type: query.Filter_Exact, Name: transport.KeyStorageGroup},
		{Type: query.Filter_Exact, Name: transport.KeyTombstone},
		{Type: query.Filter_Exact, Name: transport.KeyHasParent},
	}}

	obj := &Object{
		SystemHeader: SystemHeader{
			ID:      selfID,
			OwnerID: ownerID,
			CID:     cid,
		},
		Headers: []Header{
			{Value: &object.Header_Link{Link: &object.Link{Type: object.Link_Parent, ID: parID}}},
			{Value: &object.Header_Link{Link: &object.Link{Type: object.Link_Previous, ID: prevID}}},
			{Value: &object.Header_Link{Link: &object.Link{Type: object.Link_Next, ID: nextID}}},
			{Value: &object.Header_Link{Link: &object.Link{Type: object.Link_Child, ID: childID}}},
			{Value: &object.Header_StorageGroup{StorageGroup: new(storagegroup.StorageGroup)}},
			{Value: &object.Header_Tombstone{Tombstone: new(object.Tombstone)}},
		},
	}

	require.True(t, imposeQuery(query, obj))

	return query, obj
}
