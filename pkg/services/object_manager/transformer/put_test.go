package transformer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"math/rand"
	"sort"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-api-go/storagegroup"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/replication/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/verifier"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testPutEntity struct {
		// Set of interfaces which entity must implement, but some methods from those does not call.

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ io.Writer                 = (*testPutEntity)(nil)
	_ EpochReceiver             = (*testPutEntity)(nil)
	_ Transformer               = (*testPutEntity)(nil)
	_ storagegroup.InfoReceiver = (*testPutEntity)(nil)
	_ verifier.Verifier         = (*testPutEntity)(nil)
)

func (s *testPutEntity) Verify(_ context.Context, obj *Object) error {
	if s.f != nil {
		s.f(obj)
	}
	return s.err
}

func (s *testPutEntity) Write(p []byte) (int, error) {
	if s.f != nil {
		s.f(p)
	}
	return 0, s.err
}

func (s *testPutEntity) Transform(_ context.Context, u ProcUnit, h ...ProcUnitHandler) error {
	if s.f != nil {
		s.f(u, h)
	}
	return s.err
}

func (s *testPutEntity) GetSGInfo(_ context.Context, cid CID, group []ObjectID) (*StorageGroup, error) {
	if s.f != nil {
		s.f(cid, group)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*StorageGroup), nil
}

func (s *testPutEntity) Epoch() uint64 { return s.res.(uint64) }

func TestNewTransformer(t *testing.T) {
	validParams := Params{
		SGInfoReceiver: new(testPutEntity),
		EpochReceiver:  new(testPutEntity),
		SizeLimit:      1,
		Verifier:       new(testPutEntity),
	}

	t.Run("valid params", func(t *testing.T) {
		res, err := NewTransformer(validParams)
		require.NoError(t, err)
		require.NotNil(t, res)
	})
	t.Run("non-positive size", func(t *testing.T) {
		p := validParams
		p.SizeLimit = 0
		_, err := NewTransformer(p)
		require.EqualError(t, err, errors.Wrap(errInvalidSizeLimit, transformerInstanceFailMsg).Error())
	})
	t.Run("empty SG info receiver", func(t *testing.T) {
		p := validParams
		p.SGInfoReceiver = nil
		_, err := NewTransformer(p)
		require.EqualError(t, err, errors.Wrap(errEmptySGInfoRecv, transformerInstanceFailMsg).Error())
	})
	t.Run("empty epoch receiver", func(t *testing.T) {
		p := validParams
		p.EpochReceiver = nil
		_, err := NewTransformer(p)
		require.EqualError(t, err, errors.Wrap(errEmptyEpochReceiver, transformerInstanceFailMsg).Error())
	})
	t.Run("empty object verifier", func(t *testing.T) {
		p := validParams
		p.Verifier = nil
		_, err := NewTransformer(p)
		require.EqualError(t, err, errors.Wrap(errEmptyVerifier, transformerInstanceFailMsg).Error())
	})
}

func Test_transformer(t *testing.T) {
	ctx := context.TODO()

	u := ProcUnit{
		Head: &Object{
			Payload: testData(t, 10),
		},
		Payload: new(emptyReader),
	}

	handlers := []ProcUnitHandler{func(context.Context, ProcUnit) error { return nil }}

	t.Run("preliminary transformation failure", func(t *testing.T) {
		// create custom error for test
		pErr := errors.New("test error for prelim transformer")

		s := &transformer{
			tPrelim: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct prelim transformer params", func(t *testing.T) {
						require.Equal(t, u, items[0])
						require.Empty(t, items[1])
					})
				},
				err: pErr, // force Transformer to return pErr
			},
		}

		// ascertain that error returns as expected
		require.EqualError(t, s.Transform(ctx, u, handlers...), pErr.Error())
	})

	t.Run("size limiter error/correct sign processing", func(t *testing.T) {
		// create custom error for test
		sErr := errors.New("test error for signer")
		lErr := errors.New("test error for size limiter")

		s := &transformer{
			tPrelim: new(testPutEntity),
			tSizeLim: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct size limiter params", func(t *testing.T) {
						require.Equal(t, u, items[0])
						hs := items[1].([]ProcUnitHandler)
						require.Len(t, hs, 1)
						require.EqualError(t, hs[0](ctx, u), sErr.Error())
					})
				},
				err: lErr, // force Transformer to return lErr
			},
			tSign: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct signer params", func(t *testing.T) {
						require.Equal(t, u, items[0])
						require.Equal(t, handlers, items[1])
					})
				},
				err: sErr, // force Transformer to return sErr
			},
		}

		// ascertain that error returns as expected
		require.EqualError(t, s.Transform(ctx, u, handlers...), lErr.Error())
	})
}

func Test_preliminaryTransformer(t *testing.T) {
	ctx := context.TODO()

	u := ProcUnit{
		Head: &Object{
			Payload: testData(t, 10),
		},
		Payload: new(emptyReader),
	}

	t.Run("field moulder failure", func(t *testing.T) {
		// create custom error for test
		mErr := errors.New("test error for field moulder")

		s := &preliminaryTransformer{
			fMoulder: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct field moulder params", func(t *testing.T) {
						require.Equal(t, u, items[0])
						require.Empty(t, items[1])
					})
				},
				err: mErr, // force Transformer to return mErr
			},
		}

		// ascertain that error returns as expected
		require.EqualError(t, s.Transform(ctx, u), mErr.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		// create custom error for test
		sgErr := errors.New("test error for SG moulder")

		s := &preliminaryTransformer{
			fMoulder: new(testPutEntity),
			sgMoulder: &testPutEntity{
				f: func(items ...interface{}) {
					t.Run("correct field moulder params", func(t *testing.T) {
						require.Equal(t, u, items[0])
						require.Empty(t, items[1])
					})
				},
				err: sgErr, // force Transformer to return sgErr
			},
		}

		// ascertain that error returns as expected
		require.EqualError(t, s.Transform(ctx, u), sgErr.Error())
	})
}

func Test_readChunk(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		t.Run("missing checksum header", func(t *testing.T) {
			obj := new(Object)

			_, h := obj.LastHeader(object.HeaderType(object.PayloadChecksumHdr))
			require.Nil(t, h)

			require.NoError(t, readChunk(ProcUnit{
				Head:    obj,
				Payload: bytes.NewBuffer(testData(t, 10)),
			}, nil, nil, nil))

			_, h = obj.LastHeader(object.HeaderType(object.PayloadChecksumHdr))

			require.NotNil(t, h)
			require.Equal(t, sha256.New().Sum(nil), h.Value.(*object.Header_PayloadChecksum).PayloadChecksum)
		})

		t.Run("existing checksum header", func(t *testing.T) {
			h := &object.Header_PayloadChecksum{PayloadChecksum: testData(t, 10)}

			obj := &Object{Headers: []object.Header{{Value: h}}}

			require.NoError(t, readChunk(ProcUnit{
				Head:    obj,
				Payload: bytes.NewBuffer(testData(t, 10)),
			}, nil, nil, nil))

			require.NotNil(t, h)
			require.Equal(t, sha256.New().Sum(nil), h.PayloadChecksum)
		})
	})

	t.Run("non-empty slice", func(t *testing.T) {
		t.Run("non-full data", func(t *testing.T) {
			var (
				size = 10
				buf  = testData(t, size)
				r    = bytes.NewBuffer(buf[:size-1])
			)

			require.EqualError(t,
				readChunk(ProcUnit{Head: new(Object), Payload: r}, buf, nil, nil),
				ErrPayloadEOF.Error(),
			)
		})

		t.Run("hash accumulator write", func(t *testing.T) {
			var (
				d       = testData(t, 10)
				srcHash = sha256.Sum256(d)
				hAcc    = sha256.New()
				buf     = bytes.NewBuffer(d)
				b       = make([]byte, len(d))
				obj     = new(Object)

				srcHomoHash = hash.Sum(d)
				homoHashHdr = &object.Header_HomoHash{HomoHash: hash.Sum(make([]byte, 0))}
			)

			t.Run("failure", func(t *testing.T) {
				hErr := errors.New("test error for hash writer")
				b := testData(t, len(d))

				require.EqualError(t, readChunk(EmptyPayloadUnit(new(Object)), b, &testPutEntity{
					f: func(items ...interface{}) {
						t.Run("correct accumulator params", func(t *testing.T) {
							require.Equal(t, b, items[0])
						})
					},
					err: hErr,
				}, nil), hErr.Error())
			})

			require.NoError(t, readChunk(ProcUnit{Head: obj, Payload: buf}, b, hAcc, homoHashHdr))

			_, h := obj.LastHeader(object.HeaderType(object.PayloadChecksumHdr))
			require.NotNil(t, h)
			require.Equal(t, srcHash[:], h.Value.(*object.Header_PayloadChecksum).PayloadChecksum)

			require.Equal(t, srcHash[:], hAcc.Sum(nil))
			require.Equal(t, srcHomoHash, homoHashHdr.HomoHash)
		})
	})
}

func Test_headSigner(t *testing.T) {
	ctx := context.TODO()

	t.Run("invalid input", func(t *testing.T) {
		t.Run("missing token", func(t *testing.T) {
			u := ProcUnit{Head: new(Object)}
			require.Error(t, u.Head.Verify())
			s := &headSigner{verifier: &testPutEntity{err: errors.New("")}}
			require.EqualError(t, s.Transform(ctx, u), errNoToken.Error())
		})

		t.Run("with token", func(t *testing.T) {
			u := ProcUnit{Head: new(Object)}

			v, err := storage.NewLocalHeadIntegrityVerifier()
			require.NoError(t, err)

			require.Error(t, u.Head.Verify())

			privateToken, err := session.NewPrivateToken(0)
			require.NoError(t, err)
			ctx := context.WithValue(ctx, PrivateSessionToken, privateToken)

			s := &headSigner{
				verifier: &testPutEntity{
					err: errors.New(""),
				},
			}

			key := &privateToken.PrivateKey().PublicKey

			u.Head.SystemHeader.OwnerID, err = refs.NewOwnerID(key)
			require.NoError(t, err)
			u.Head.AddHeader(&object.Header{
				Value: &object.Header_PublicKey{
					PublicKey: &object.PublicKey{
						Value: crypto.MarshalPublicKey(key),
					},
				},
			})

			require.NoError(t, s.Transform(ctx, u, func(_ context.Context, unit ProcUnit) error {
				require.NoError(t, v.Verify(ctx, unit.Head))
				_, h := unit.Head.LastHeader(object.HeaderType(object.IntegrityHdr))
				require.NotNil(t, h)
				d, err := verifier.MarshalHeaders(unit.Head, len(unit.Head.Headers)-1)
				require.NoError(t, err)
				cs := sha256.Sum256(d)
				require.Equal(t, cs[:], h.Value.(*object.Header_Integrity).Integrity.GetHeadersChecksum())
				return nil
			}))

			t.Run("valid input", func(t *testing.T) {
				s := &headSigner{verifier: new(testPutEntity)}
				require.NoError(t, s.Transform(ctx, u, func(_ context.Context, unit ProcUnit) error {
					require.Equal(t, u, unit)
					return nil
				}))
			})
		})
	})
}

func Test_fieldMoulder(t *testing.T) {
	ctx := context.TODO()
	epoch := uint64(100)

	fMoulder := &fieldMoulder{epochRecv: &testPutEntity{res: epoch}}

	t.Run("no token", func(t *testing.T) {
		require.EqualError(t, new(fieldMoulder).Transform(ctx, ProcUnit{}), errNoToken.Error())
	})

	t.Run("with token", func(t *testing.T) {
		token := new(service.Token)
		token.SetID(service.TokenID{1, 2, 3})

		ctx := context.WithValue(ctx, PublicSessionToken, token)

		u := ProcUnit{Head: new(Object)}

		_, h := u.Head.LastHeader(object.HeaderType(object.TokenHdr))
		require.Nil(t, h)

		require.NoError(t, fMoulder.Transform(ctx, u))

		_, h = u.Head.LastHeader(object.HeaderType(object.TokenHdr))
		require.Equal(t, token, h.Value.(*object.Header_Token).Token)

		require.False(t, u.Head.SystemHeader.ID.Empty())
		require.NotZero(t, u.Head.SystemHeader.CreatedAt.UnixTime)
		require.Equal(t, epoch, u.Head.SystemHeader.CreatedAt.Epoch)
		require.Equal(t, uint64(1), u.Head.SystemHeader.Version)
	})
}

func Test_sgMoulder(t *testing.T) {
	ctx := context.TODO()

	t.Run("invalid SG linking", func(t *testing.T) {
		t.Run("w/ header and w/o links", func(t *testing.T) {
			obj := new(Object)
			obj.SetStorageGroup(new(storagegroup.StorageGroup))
			require.EqualError(t, new(sgMoulder).Transform(ctx, ProcUnit{Head: obj}), ErrInvalidSGLinking.Error())
		})

		t.Run("w/o header and w/ links", func(t *testing.T) {
			obj := new(Object)
			addLink(obj, object.Link_StorageGroup, ObjectID{})
			require.EqualError(t, new(sgMoulder).Transform(ctx, ProcUnit{Head: obj}), ErrInvalidSGLinking.Error())
		})
	})

	t.Run("non-SG", func(t *testing.T) {
		obj := new(Object)
		require.NoError(t, new(sgMoulder).Transform(ctx, ProcUnit{Head: obj}))
	})

	t.Run("receive SG info", func(t *testing.T) {
		cid := testObjectAddress(t).CID
		group := make([]ObjectID, 5)
		for i := range group {
			group[i] = testObjectAddress(t).ObjectID
		}

		t.Run("failure", func(t *testing.T) {
			obj := &Object{SystemHeader: object.SystemHeader{CID: cid}}

			obj.SetStorageGroup(new(storagegroup.StorageGroup))
			for i := range group {
				addLink(obj, object.Link_StorageGroup, group[i])
			}

			sgErr := errors.New("test error for SG info receiver")

			mSG := &sgMoulder{
				sgInfoRecv: &testPutEntity{
					f: func(items ...interface{}) {
						t.Run("correct SG info receiver params", func(t *testing.T) {
							cp := make([]ObjectID, len(group))
							copy(cp, group)
							sort.Sort(storagegroup.IDList(cp))
							require.Equal(t, cid, items[0])
							require.Equal(t, cp, items[1])
						})
					},
					err: sgErr,
				},
			}

			require.EqualError(t, mSG.Transform(ctx, ProcUnit{Head: obj}), sgErr.Error())
		})
	})

	t.Run("correct result", func(t *testing.T) {
		obj := new(Object)
		obj.SetStorageGroup(new(storagegroup.StorageGroup))
		addLink(obj, object.Link_StorageGroup, ObjectID{})

		sgInfo := &storagegroup.StorageGroup{
			ValidationDataSize: 19,
			ValidationHash:     hash.Sum(testData(t, 10)),
		}

		mSG := &sgMoulder{
			sgInfoRecv: &testPutEntity{
				res: sgInfo,
			},
		}

		require.NoError(t, mSG.Transform(ctx, ProcUnit{Head: obj}))

		_, h := obj.LastHeader(object.HeaderType(object.StorageGroupHdr))
		require.NotNil(t, h)
		require.Equal(t, sgInfo, h.Value.(*object.Header_StorageGroup).StorageGroup)
	})
}

func Test_sizeLimiter(t *testing.T) {
	ctx := context.TODO()

	t.Run("limit entry", func(t *testing.T) {
		payload := testData(t, 10)
		payloadSize := uint64(len(payload) - 1)

		u := ProcUnit{
			Head: &Object{SystemHeader: object.SystemHeader{
				PayloadLength: payloadSize,
			}},
			Payload: bytes.NewBuffer(payload[:payloadSize]),
		}

		sl := &sizeLimiter{limit: payloadSize}

		t.Run("cut payload", func(t *testing.T) {
			require.Error(t, sl.Transform(ctx, ProcUnit{
				Head:    &Object{SystemHeader: object.SystemHeader{PayloadLength: payloadSize}},
				Payload: bytes.NewBuffer(payload[:payloadSize-1]),
			}))
		})

		require.NoError(t, sl.Transform(ctx, u, func(_ context.Context, unit ProcUnit) error {
			_, err := unit.Payload.Read(make([]byte, 1))
			require.EqualError(t, err, io.EOF.Error())
			require.Equal(t, payload[:payloadSize], unit.Head.Payload)
			_, h := unit.Head.LastHeader(object.HeaderType(object.HomoHashHdr))
			require.NotNil(t, h)
			require.Equal(t, hash.Sum(payload[:payloadSize]), h.Value.(*object.Header_HomoHash).HomoHash)
			return nil
		}))
	})

	t.Run("limit exceed", func(t *testing.T) {
		payload := testData(t, 100)
		sizeLimit := uint64(len(payload)) / 13

		pToken, err := session.NewPrivateToken(0)
		require.NoError(t, err)

		srcObj := &object.Object{
			SystemHeader: object.SystemHeader{
				Version:       12,
				PayloadLength: uint64(len(payload)),
				ID:            testObjectAddress(t).ObjectID,
				OwnerID:       object.OwnerID{1, 2, 3},
				CID:           testObjectAddress(t).CID,
			},
			Headers: []object.Header{
				{Value: &object.Header_UserHeader{UserHeader: &object.UserHeader{Key: "key", Value: "value"}}},
			},
		}

		u := ProcUnit{
			Head:    srcObj,
			Payload: bytes.NewBuffer(payload),
		}

		epoch := uint64(77)

		sl := &sizeLimiter{
			limit:     sizeLimit,
			epochRecv: &testPutEntity{res: epoch},
		}

		t.Run("no token", func(t *testing.T) {
			require.EqualError(t, sl.Transform(ctx, ProcUnit{
				Head: &Object{
					SystemHeader: object.SystemHeader{
						PayloadLength: uint64(len(payload)),
					},
				},
				Payload: bytes.NewBuffer(payload),
			}), errNoToken.Error())
		})

		ctx := context.WithValue(ctx, PrivateSessionToken, pToken)

		t.Run("cut payload", func(t *testing.T) {
			require.Error(t, sl.Transform(ctx, ProcUnit{
				Head: &Object{
					SystemHeader: object.SystemHeader{
						PayloadLength: uint64(len(payload)) + 1,
					},
				},
				Payload: bytes.NewBuffer(payload),
			}))
		})

		objs := make([]Object, 0)

		t.Run("handler error", func(t *testing.T) {
			hErr := errors.New("test error for handler")

			require.EqualError(t, sl.Transform(ctx, ProcUnit{
				Head: &Object{
					SystemHeader: object.SystemHeader{PayloadLength: uint64(len(payload))},
					Headers:      make([]object.Header, 0),
				},
				Payload: bytes.NewBuffer(payload),
			}, func(context.Context, ProcUnit) error { return hErr }), hErr.Error())
		})

		require.NoError(t, sl.Transform(ctx, u, func(_ context.Context, unit ProcUnit) error {
			_, err := unit.Payload.Read(make([]byte, 1))
			require.EqualError(t, err, io.EOF.Error())
			objs = append(objs, *unit.Head.Copy())
			return nil
		}))

		ln := len(objs)

		res := make([]byte, 0, len(payload))

		zObj := objs[ln-1]
		require.Zero(t, zObj.SystemHeader.PayloadLength)
		require.Empty(t, zObj.Payload)
		require.Empty(t, zObj.Links(object.Link_Next))
		require.Empty(t, zObj.Links(object.Link_Previous))
		require.Empty(t, zObj.Links(object.Link_Parent))
		children := zObj.Links(object.Link_Child)
		require.Len(t, children, ln-1)
		for i := range objs[:ln-1] {
			require.Equal(t, objs[i].SystemHeader.ID, children[i])
		}

		for i := range objs[:ln-1] {
			res = append(res, objs[i].Payload...)
			if i == 0 {
				require.Equal(t, objs[i].Links(object.Link_Next)[0], objs[i+1].SystemHeader.ID)
				require.True(t, objs[i].Links(object.Link_Previous)[0].Empty())
			} else if i < ln-2 {
				require.Equal(t, objs[i].Links(object.Link_Previous)[0], objs[i-1].SystemHeader.ID)
				require.Equal(t, objs[i].Links(object.Link_Next)[0], objs[i+1].SystemHeader.ID)
			} else {
				_, h := objs[i].LastHeader(object.HeaderType(object.HomoHashHdr))
				require.NotNil(t, h)
				require.Equal(t, hash.Sum(payload), h.Value.(*object.Header_HomoHash).HomoHash)
				require.Equal(t, objs[i].Links(object.Link_Previous)[0], objs[i-1].SystemHeader.ID)
				require.True(t, objs[i].Links(object.Link_Next)[0].Empty())
			}
		}

		require.Equal(t, payload, res)
	})
}

// testData returns size bytes of random data.
func testData(t *testing.T, size int) []byte {
	res := make([]byte, size)
	_, err := rand.Read(res)
	require.NoError(t, err)
	return res
}

// testObjectAddress returns new random object address.
func testObjectAddress(t *testing.T) refs.Address {
	oid, err := refs.NewObjectID()
	require.NoError(t, err)
	return refs.Address{CID: refs.CIDForBytes(testData(t, refs.CIDSize)), ObjectID: oid}
}

func TestIntegration(t *testing.T) {
	ownerKey := test.DecodeKey(1)

	ownerID, err := refs.NewOwnerID(&ownerKey.PublicKey)
	require.NoError(t, err)

	privToken, err := session.NewPrivateToken(0)
	require.NoError(t, err)

	pkBytes, err := session.PublicSessionToken(privToken)
	require.NoError(t, err)

	ctx := context.WithValue(context.TODO(), PrivateSessionToken, privToken)

	pubToken := new(service.Token)
	pubToken.SetID(service.TokenID{1, 2, 3})
	pubToken.SetSessionKey(pkBytes)
	pubToken.SetOwnerID(ownerID)
	pubToken.SetOwnerKey(crypto.MarshalPublicKey(&ownerKey.PublicKey))
	require.NoError(t, service.AddSignatureWithKey(ownerKey, service.NewSignedSessionToken(pubToken)))

	ctx = context.WithValue(ctx, PublicSessionToken, pubToken)

	t.Run("non-SG object", func(t *testing.T) {
		t.Run("with split", func(t *testing.T) {
			tr, err := NewTransformer(Params{
				SGInfoReceiver: new(testPutEntity),
				EpochReceiver:  &testPutEntity{res: uint64(1)},
				SizeLimit:      13,
				Verifier: &testPutEntity{
					err: errors.New(""), // force verifier to return non-nil error
				},
			})
			require.NoError(t, err)

			payload := make([]byte, 20)
			_, err = rand.Read(payload)
			require.NoError(t, err)

			obj := &Object{
				SystemHeader: object.SystemHeader{
					PayloadLength: uint64(len(payload)),
					CID:           CID{3},
				},
				Headers: []object.Header{
					{Value: &object.Header_UserHeader{UserHeader: &object.UserHeader{Key: "key", Value: "value"}}},
				},
			}

			obj.SystemHeader.OwnerID = ownerID

			obj.SetHeader(&object.Header{
				Value: &object.Header_Token{
					Token: pubToken,
				},
			})

			testTransformer(t, ctx, ProcUnit{
				Head:    obj,
				Payload: bytes.NewBuffer(payload),
			}, tr, payload)
		})
	})
}

func testTransformer(t *testing.T, ctx context.Context, u ProcUnit, tr Transformer, src []byte) {
	objList := make([]Object, 0)
	verifier, err := storage.NewLocalHeadIntegrityVerifier()
	require.NoError(t, err)

	require.NoError(t, tr.Transform(ctx, u, func(_ context.Context, unit ProcUnit) error {
		require.NoError(t, verifier.Verify(ctx, unit.Head))
		objList = append(objList, *unit.Head.Copy())
		return nil
	}))

	reverse := NewRestorePipeline(SplitRestorer())

	res, err := reverse.Restore(ctx, objList...)
	require.NoError(t, err)

	integrityVerifier, err := storage.NewLocalIntegrityVerifier()
	require.NoError(t, err)
	require.NoError(t, integrityVerifier.Verify(ctx, &res[0]))

	require.Equal(t, src, res[0].Payload)
	_, h := res[0].LastHeader(object.HeaderType(object.HomoHashHdr))
	require.True(t, hash.Sum(src).Equal(h.Value.(*object.Header_HomoHash).HomoHash))
}

func addLink(o *Object, t object.Link_Type, id ObjectID) {
	o.AddHeader(&object.Header{Value: &object.Header_Link{
		Link: &object.Link{Type: t, ID: id},
	}})
}
