package object

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/storagegroup"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/verifier"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testFilterEntity struct {
		// Set of interfaces which entity must implement, but some methods from those does not call.
		localstore.Localstore

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}

	testFilterUnit struct {
		obj *Object
		exp localstore.FilterCode
	}
)

var (
	_ storagegroup.InfoReceiver = (*testFilterEntity)(nil)
	_ verifier.Verifier         = (*testFilterEntity)(nil)
	_ EpochReceiver             = (*testFilterEntity)(nil)
	_ localstore.Localstore     = (*testFilterEntity)(nil)
	_ tombstonePresenceChecker  = (*testFilterEntity)(nil)
)

func (s *testFilterEntity) Meta(addr Address) (*Meta, error) {
	if s.f != nil {
		s.f(addr)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*Meta), nil
}

func (s *testFilterEntity) GetSGInfo(ctx context.Context, cid CID, group []ID) (*storagegroup.StorageGroup, error) {
	if s.f != nil {
		s.f(cid, group)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*storagegroup.StorageGroup), nil
}

func (s *testFilterEntity) hasLocalTombstone(addr Address) (bool, error) {
	if s.f != nil {
		s.f(addr)
	}
	if s.err != nil {
		return false, s.err
	}
	return s.res.(bool), nil
}

func (s *testFilterEntity) Size() int64 { return s.res.(int64) }

func (s *testFilterEntity) Epoch() uint64 { return s.res.(uint64) }

func (s *testFilterEntity) Verify(_ context.Context, obj *Object) error {
	if s.f != nil {
		s.f(obj)
	}
	return s.err
}

func Test_creationEpochFC(t *testing.T) {
	ctx := context.TODO()
	localEpoch := uint64(100)

	ff := creationEpochFC(&filterParams{epochRecv: &testFilterEntity{res: localEpoch}})

	valid := []Object{
		{SystemHeader: SystemHeader{CreatedAt: CreationPoint{Epoch: localEpoch - 1}}},
		{SystemHeader: SystemHeader{CreatedAt: CreationPoint{Epoch: localEpoch}}},
	}

	invalid := []Object{
		{SystemHeader: SystemHeader{CreatedAt: CreationPoint{Epoch: localEpoch + 1}}},
		{SystemHeader: SystemHeader{CreatedAt: CreationPoint{Epoch: localEpoch + 2}}},
	}

	testFilteringObjects(t, ctx, ff, valid, invalid, nil)
}

func Test_objectSizeFC(t *testing.T) {
	maxProcSize := uint64(100)

	t.Run("forwarding TTL", func(t *testing.T) {
		var (
			ctx = context.WithValue(context.TODO(), ttlValue, uint32(service.SingleForwardingTTL))
			ff  = objectSizeFC(&filterParams{maxProcSize: maxProcSize})
		)

		valid := []Object{
			{SystemHeader: SystemHeader{PayloadLength: maxProcSize - 1}},
			{SystemHeader: SystemHeader{PayloadLength: maxProcSize}},
		}

		invalid := []Object{
			{SystemHeader: SystemHeader{PayloadLength: maxProcSize + 1}},
			{SystemHeader: SystemHeader{PayloadLength: maxProcSize + 2}},
		}

		testFilteringObjects(t, ctx, ff, valid, invalid, nil)
	})

	t.Run("non-forwarding TTL", func(t *testing.T) {
		var (
			ctx     = context.WithValue(context.TODO(), ttlValue, uint32(service.NonForwardingTTL-1))
			objSize = maxProcSize / 2
			ls      = &testFilterEntity{res: int64(maxProcSize - objSize)}
		)

		ff := objectSizeFC(&filterParams{
			maxProcSize: maxProcSize,
			storageCap:  maxProcSize,
			localStore:  ls,
		})

		valid := []Object{{SystemHeader: SystemHeader{PayloadLength: objSize}}}
		invalid := []Object{{SystemHeader: SystemHeader{PayloadLength: objSize + 1}}}

		testFilteringObjects(t, ctx, ff, valid, invalid, nil)
	})
}

func Test_objectIntegrityFC(t *testing.T) {
	var (
		ctx     = context.TODO()
		valid   = &Object{SystemHeader: SystemHeader{ID: testObjectAddress(t).ObjectID}}
		invalid = &Object{SystemHeader: SystemHeader{ID: testObjectAddress(t).ObjectID}}
	)
	valid.Headers = append(valid.Headers, Header{Value: new(object.Header_PayloadChecksum)})

	ver := new(testFilterEntity)
	ver.f = func(items ...interface{}) {
		if items[0].(*Object).SystemHeader.ID.Equal(valid.SystemHeader.ID) {
			ver.err = nil
		} else {
			ver.err = errors.New("")
		}
	}

	ff := objectIntegrityFC(&filterParams{verifier: ver})

	testFilterFunc(t, ctx, ff, testFilterUnit{obj: valid, exp: localstore.CodePass})
	testFilterFunc(t, ctx, ff, testFilterUnit{obj: invalid, exp: localstore.CodeFail})
}

func Test_tombstoneOverwriteFC(t *testing.T) {
	var (
		obj1 = Object{
			SystemHeader: SystemHeader{ID: testObjectAddress(t).ObjectID},
			Headers:      []Header{{Value: new(object.Header_Tombstone)}},
		}
		obj2 = Object{
			SystemHeader: SystemHeader{ID: testObjectAddress(t).ObjectID},
		}
		obj3 = Object{
			SystemHeader: SystemHeader{ID: testObjectAddress(t).ObjectID},
		}
		obj4 = Object{
			SystemHeader: SystemHeader{ID: testObjectAddress(t).ObjectID},
		}
	)

	ts := new(testFilterEntity)
	ts.f = func(items ...interface{}) {
		addr := items[0].(Address)
		if addr.ObjectID.Equal(obj2.SystemHeader.ID) {
			ts.res, ts.err = nil, errors.New("")
		} else if addr.ObjectID.Equal(obj3.SystemHeader.ID) {
			ts.res, ts.err = true, nil
		} else {
			ts.res, ts.err = false, nil
		}
	}

	valid := []Object{obj1, obj4}
	invalid := []Object{obj2, obj3}

	ff := tombstoneOverwriteFC(&filterParams{tsPresChecker: ts})

	testFilteringObjects(t, context.TODO(), ff, valid, invalid, nil)
}

func Test_storageGroupFC(t *testing.T) {
	var (
		valid, invalid []Object
		cid            = testObjectAddress(t).CID
		sgSize, sgHash = uint64(10), hash.Sum(testData(t, 10))

		sg = &storagegroup.StorageGroup{
			ValidationDataSize: sgSize,
			ValidationHash:     sgHash,
		}

		sgHeaders = []Header{
			{Value: &object.Header_StorageGroup{StorageGroup: sg}},
			{Value: &object.Header_Link{Link: &object.Link{Type: object.Link_StorageGroup}}},
		}
	)

	valid = append(valid, Object{
		SystemHeader: SystemHeader{
			CID: cid,
		},
	})

	valid = append(valid, Object{
		SystemHeader: SystemHeader{
			CID: cid,
		},
		Headers: sgHeaders,
	})

	invalid = append(invalid, Object{
		SystemHeader: SystemHeader{
			CID: cid,
		},
		Headers: sgHeaders[:1],
	})

	invalid = append(invalid, Object{
		SystemHeader: SystemHeader{
			CID: cid,
		},
		Headers: []Header{
			{
				Value: &object.Header_StorageGroup{
					StorageGroup: &storagegroup.StorageGroup{
						ValidationDataSize: sg.ValidationDataSize + 1,
					},
				},
			},
			{
				Value: &object.Header_Link{
					Link: &object.Link{
						Type: object.Link_StorageGroup,
					},
				},
			},
		},
	})

	invalid = append(invalid, Object{
		SystemHeader: SystemHeader{
			CID: cid,
		},
		Headers: []Header{
			{
				Value: &object.Header_StorageGroup{
					StorageGroup: &storagegroup.StorageGroup{
						ValidationDataSize: sg.ValidationDataSize,
						ValidationHash:     Hash{1, 2, 3},
					},
				},
			},
			{
				Value: &object.Header_Link{
					Link: &object.Link{
						Type: object.Link_StorageGroup,
					},
				},
			},
		},
	})

	sr := &testFilterEntity{
		f: func(items ...interface{}) {
			require.Equal(t, cid, items[0])
		},
		res: sg,
	}

	ff := storageGroupFC(&filterParams{sgInfoRecv: sr})

	testFilteringObjects(t, context.TODO(), ff, valid, invalid, nil)
}

func Test_coreTSPresChecker(t *testing.T) {
	addr := testObjectAddress(t)

	t.Run("local storage failure", func(t *testing.T) {
		ls := &testFilterEntity{
			f: func(items ...interface{}) {
				require.Equal(t, addr, items[0])
			},
			err: errors.Wrap(bucket.ErrNotFound, "some message"),
		}

		s := &coreTSPresChecker{localStore: ls}

		res, err := s.hasLocalTombstone(addr)
		require.NoError(t, err)
		require.False(t, res)

		lsErr := errors.New("test error for local storage")
		ls.err = lsErr

		res, err = s.hasLocalTombstone(addr)
		require.EqualError(t, err, lsErr.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		m := &Meta{Object: new(Object)}

		ls := &testFilterEntity{res: m}

		s := &coreTSPresChecker{localStore: ls}

		res, err := s.hasLocalTombstone(addr)
		require.NoError(t, err)
		require.False(t, res)

		m.Object.AddHeader(&object.Header{Value: new(object.Header_Tombstone)})

		res, err = s.hasLocalTombstone(addr)
		require.NoError(t, err)
		require.True(t, res)
	})
}

func testFilteringObjects(t *testing.T, ctx context.Context, f localstore.FilterFunc, valid, invalid, ignored []Object) {
	units := make([]testFilterUnit, 0, len(valid)+len(invalid)+len(ignored))

	for i := range valid {
		units = append(units, testFilterUnit{
			obj: &valid[i],
			exp: localstore.CodePass,
		})
	}

	for i := range invalid {
		units = append(units, testFilterUnit{
			obj: &invalid[i],
			exp: localstore.CodeFail,
		})
	}

	for i := range ignored {
		units = append(units, testFilterUnit{
			obj: &ignored[i],
			exp: localstore.CodeIgnore,
		})
	}

	testFilterFunc(t, ctx, f, units...)
}

func testFilterFunc(t *testing.T, ctx context.Context, f localstore.FilterFunc, units ...testFilterUnit) {
	for i := range units {
		res := f(ctx, &Meta{Object: units[i].obj})
		require.Equal(t, units[i].exp, res.Code())
	}
}

func Test_payloadSizeFC(t *testing.T) {
	maxPayloadSize := uint64(100)

	valid := []Object{
		{SystemHeader: SystemHeader{PayloadLength: maxPayloadSize - 1}},
		{SystemHeader: SystemHeader{PayloadLength: maxPayloadSize}},
	}

	invalid := []Object{
		{SystemHeader: SystemHeader{PayloadLength: maxPayloadSize + 1}},
		{SystemHeader: SystemHeader{PayloadLength: maxPayloadSize + 2}},
	}

	ff := payloadSizeFC(&filterParams{
		maxPayloadSize: maxPayloadSize,
	})

	testFilteringObjects(t, context.TODO(), ff, valid, invalid, nil)
}
