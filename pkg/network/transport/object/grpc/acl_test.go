package object

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"testing"

	eacl "github.com/nspcc-dev/neofs-api-go/acl/extended"
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	crypto "github.com/nspcc-dev/neofs-crypto"
	libcnr "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/basic"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/storage"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	testlogger "github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/stretchr/testify/require"
)

type (
	testACLEntity struct {
		// Set of interfaces which testCommonEntity must implement, but some methods from those does not call.
		serviceRequest
		RequestTargeter
		eacl.Table
		storage.Storage
		containerNodesLister

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

type testBasicChecker struct {
	actionErr error
	action    bool

	sticky bool

	extended bool

	bearer bool
}

func (t *testACLEntity) Get(cid storage.CID) (*storage.Container, error) {
	if t.err != nil {
		return nil, t.err
	}

	return t.res.(*storage.Container), nil
}

func (t *testACLEntity) calculateRequestAction(context.Context, requestActionParams) eacl.Action {
	return t.res.(eacl.Action)
}

func (t *testACLEntity) GetBasicACL(context.Context, CID) (libcnr.BasicACL, error) {
	if t.err != nil {
		return 0, t.err
	}

	return t.res.(libcnr.BasicACL), nil
}

func (t *testACLEntity) Target(context.Context, serviceRequest) requestTarget {
	return t.res.(requestTarget)
}

func (t *testACLEntity) CID() CID { return CID{} }

func (t *testACLEntity) Type() object.RequestType { return t.res.(object.RequestType) }

func (t *testACLEntity) GetBearerToken() service.BearerToken { return nil }

func (t *testACLEntity) GetOwner() (*ecdsa.PublicKey, error) {
	if t.err != nil {
		return nil, t.err
	}

	return t.res.(*ecdsa.PublicKey), nil
}

func (t testACLEntity) InnerRingKeys() ([][]byte, error) {
	if t.err != nil {
		return nil, t.err
	}

	return t.res.([][]byte), nil
}

func (t *testACLEntity) ContainerNodesInfo(ctx context.Context, cid CID, prev int) ([]netmap.Info, error) {
	if t.err != nil {
		return nil, t.err
	}

	return t.res.([][]netmap.Info)[prev], nil
}

func (t testACLEntity) GetSignKeyPairs() []service.SignKeyPair {
	if t.res == nil {
		return nil
	}
	return t.res.([]service.SignKeyPair)
}

func TestPreprocessor(t *testing.T) {
	ctx := context.TODO()

	t.Run("empty request", func(t *testing.T) {
		require.PanicsWithValue(t, pmEmptyServiceRequest, func() {
			_ = new(aclPreProcessor).preProcess(ctx, nil)
		})
	})

	t.Run("everything is okay", func(t *testing.T) {
		var rule basic.ACL
		rule.SetFinal()
		rule.AllowOthers(requestACLSection(object.RequestGet))

		cnr := new(storage.Container)
		cnr.SetBasicACL(rule)

		reqTarget := requestTarget{
			group: eacl.GroupOthers,
		}

		preprocessor := aclPreProcessor{
			log: testlogger.NewLogger(false),
			aclInfoReceiver: aclInfoReceiver{
				cnrStorage: &testACLEntity{
					res: cnr,
				},
				targetFinder: &testACLEntity{res: reqTarget},
			},
		}
		require.NoError(t, preprocessor.preProcess(ctx, &testACLEntity{res: object.RequestGet}))

		reqTarget.group = eacl.GroupSystem
		preprocessor.aclInfoReceiver.targetFinder = &testACLEntity{res: reqTarget}
		require.NoError(t, preprocessor.preProcess(ctx, &testACLEntity{res: object.RequestGet}))

		reqTarget.group = eacl.GroupUser
		preprocessor.aclInfoReceiver.targetFinder = &testACLEntity{res: reqTarget}
		require.Error(t, preprocessor.preProcess(ctx, &testACLEntity{res: object.RequestGet}))
	})

	t.Run("can't fetch container", func(t *testing.T) {
		preprocessor := aclPreProcessor{
			log: testlogger.NewLogger(false),
			aclInfoReceiver: aclInfoReceiver{
				cnrStorage: &testACLEntity{err: container.ErrNotFound},
				targetFinder: &testACLEntity{res: requestTarget{
					group: eacl.GroupOthers,
				}},
			},
		}
		require.Error(t, preprocessor.preProcess(ctx, &testACLEntity{res: object.RequestGet}))

	})

	t.Run("sticky bit", func(t *testing.T) {
		var rule basic.ACL
		rule.SetSticky()
		rule.SetFinal()
		for i := uint8(0); i < 7; i++ {
			rule.AllowUser(i)
		}

		cnr := new(storage.Container)
		cnr.SetBasicACL(rule)

		s := &aclPreProcessor{
			log: testlogger.NewLogger(false),
			aclInfoReceiver: aclInfoReceiver{
				cnrStorage: &testACLEntity{
					res: cnr,
				},
				targetFinder: &testACLEntity{
					res: requestTarget{
						group: eacl.GroupUser,
					},
				},
			},
		}

		ownerKey := &test.DecodeKey(0).PublicKey

		ownerID, err := refs.NewOwnerID(ownerKey)
		require.NoError(t, err)

		okItems := []func() []serviceRequest{
			// Read requests
			func() []serviceRequest {
				return []serviceRequest{
					new(object.GetRequest),
					new(object.HeadRequest),
					new(object.SearchRequest),
					new(GetRangeRequest),
					new(object.GetRangeHashRequest),
				}
			},
			// PutRequest / DeleteRequest (w/o token)
			func() []serviceRequest {
				req := object.MakePutRequestHeader(&Object{
					SystemHeader: SystemHeader{
						OwnerID: ownerID,
					},
				})
				req.AddSignKey(nil, ownerKey)
				putReq := &putRequest{
					PutRequest: req,
				}

				delReq := new(object.DeleteRequest)
				delReq.OwnerID = ownerID
				delReq.AddSignKey(nil, ownerKey)

				return []serviceRequest{putReq, delReq}
			},
			// PutRequest / DeleteRequest (w/ token)
			func() []serviceRequest {
				token := new(service.Token)
				token.SetOwnerID(ownerID)
				token.SetOwnerKey(crypto.MarshalPublicKey(ownerKey))

				req := object.MakePutRequestHeader(&Object{
					SystemHeader: SystemHeader{
						OwnerID: ownerID,
					},
				})
				req.SetToken(token)
				putReq := &putRequest{
					PutRequest: req,
				}

				delReq := new(object.DeleteRequest)
				delReq.OwnerID = ownerID
				delReq.SetToken(token)

				return []serviceRequest{putReq, delReq}
			},
		}

		failItems := []func() []serviceRequest{
			// PutRequest / DeleteRequest (w/o token and wrong owner)
			func() []serviceRequest {
				otherOwner := ownerID
				otherOwner[0]++

				req := object.MakePutRequestHeader(&Object{
					SystemHeader: SystemHeader{
						OwnerID: otherOwner,
					},
				})
				req.AddSignKey(nil, ownerKey)
				putReq := &putRequest{
					PutRequest: req,
				}

				delReq := new(object.DeleteRequest)
				delReq.OwnerID = otherOwner
				delReq.AddSignKey(nil, ownerKey)

				return []serviceRequest{putReq, delReq}
			},
			// PutRequest / DeleteRequest (w/ token w/ wrong owner)
			func() []serviceRequest {
				otherOwner := ownerID
				otherOwner[0]++

				token := new(service.Token)
				token.SetOwnerID(ownerID)
				token.SetOwnerKey(crypto.MarshalPublicKey(ownerKey))

				req := object.MakePutRequestHeader(&Object{
					SystemHeader: SystemHeader{
						OwnerID: otherOwner,
					},
				})
				req.SetToken(token)
				putReq := &putRequest{
					PutRequest: req,
				}

				delReq := new(object.DeleteRequest)
				delReq.OwnerID = otherOwner
				delReq.SetToken(token)

				return []serviceRequest{putReq, delReq}
			},
		}

		for _, ok := range okItems {
			for _, req := range ok() {
				require.NoError(t, s.preProcess(ctx, req))
			}
		}

		for _, fail := range failItems {
			for _, req := range fail() {
				require.Error(t, s.preProcess(ctx, req))
			}
		}
	})

	t.Run("eacl ACL", func(t *testing.T) {
		target := requestTarget{
			group: eacl.GroupOthers,
		}

		req := &testACLEntity{
			res: object.RequestGet,
		}

		actCalc := new(testACLEntity)

		var rule basic.ACL
		rule.AllowOthers(requestACLSection(object.RequestGet))

		cnr := new(storage.Container)
		cnr.SetBasicACL(rule)

		s := &aclPreProcessor{
			log: testlogger.NewLogger(false),
			aclInfoReceiver: aclInfoReceiver{
				cnrStorage: &testACLEntity{
					res: cnr,
				},
				targetFinder: &testACLEntity{
					res: target,
				},
			},

			reqActionCalc: actCalc,
		}

		// force to return non-ActionAllow
		actCalc.res = eacl.ActionAllow + 1
		require.EqualError(t, s.preProcess(ctx, req), errAccessDenied.Error())

		// force to return ActionAllow
		actCalc.res = eacl.ActionAllow
		require.NoError(t, s.preProcess(ctx, req))
	})

	t.Run("inner ring group", func(t *testing.T) {
		reqTarget := requestTarget{
			group: eacl.GroupSystem,
			ir:    true,
		}

		cnr := new(storage.Container)
		cnr.SetBasicACL(basic.FromUint32(^uint32(0)))

		preprocessor := aclPreProcessor{
			log: testlogger.NewLogger(false),
			aclInfoReceiver: aclInfoReceiver{
				cnrStorage:   &testACLEntity{res: cnr},
				targetFinder: &testACLEntity{res: reqTarget},
			},
		}

		for _, rt := range []object.RequestType{
			object.RequestSearch,
			object.RequestHead,
			object.RequestRangeHash,
		} {
			require.NoError(t,
				preprocessor.preProcess(ctx, &testACLEntity{
					res: rt,
				}),
			)
		}

		for _, rt := range []object.RequestType{
			object.RequestRange,
			object.RequestPut,
			object.RequestDelete,
			object.RequestGet,
		} {
			require.EqualError(t,
				preprocessor.preProcess(ctx, &testACLEntity{
					res: rt,
				}),
				errAccessDenied.Error(),
			)
		}
	})
}

func TestTargetFinder(t *testing.T) {
	ctx := context.TODO()
	irKey := test.DecodeKey(2)
	containerKey := test.DecodeKey(3)
	prevContainerKey := test.DecodeKey(4)

	var infoList1 []netmap.Info
	info := netmap.Info{}
	info.SetPublicKey(crypto.MarshalPublicKey(&containerKey.PublicKey))
	infoList1 = append(infoList1, info)

	var infoList2 []netmap.Info
	info.SetPublicKey(crypto.MarshalPublicKey(&prevContainerKey.PublicKey))
	infoList2 = append(infoList2, info)

	finder := &targetFinder{
		log: testlogger.NewLogger(false),
		irKeysRecv: &testACLEntity{
			res: [][]byte{crypto.MarshalPublicKey(&irKey.PublicKey)},
		},
		cnrLister: &testACLEntity{res: [][]netmap.Info{
			infoList1,
			infoList2,
		}},
	}

	t.Run("trusted node", func(t *testing.T) {

		pk := &test.DecodeKey(0).PublicKey

		ownerKey := &test.DecodeKey(1).PublicKey
		owner, err := refs.NewOwnerID(ownerKey)
		require.NoError(t, err)

		token := new(service.Token)
		token.SetSessionKey(crypto.MarshalPublicKey(pk))
		token.SetOwnerKey(crypto.MarshalPublicKey(ownerKey))
		token.SetOwnerID(owner)

		req := new(object.SearchRequest)
		req.ContainerID = CID{1, 2, 3}
		req.SetToken(token)
		req.AddSignKey(nil, pk)

		cnr := new(storage.Container)
		cnr.SetOwnerID(owner)

		finder.cnrStorage = &testACLEntity{
			res: cnr,
		}

		require.Equal(t,
			requestTarget{
				group: eacl.GroupUser,
			},
			finder.Target(ctx, req),
		)
	})

	t.Run("container owner", func(t *testing.T) {
		key := &test.DecodeKey(0).PublicKey
		owner, err := refs.NewOwnerID(key)
		require.NoError(t, err)

		cnr := new(storage.Container)
		cnr.SetOwnerID(owner)

		finder.cnrStorage = &testACLEntity{res: cnr}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, key)

		require.Equal(t,
			requestTarget{
				group: eacl.GroupUser,
			},
			finder.Target(ctx, req),
		)
	})

	t.Run("system owner", func(t *testing.T) {
		finder.cnrStorage = &testACLEntity{res: new(storage.Container)}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &irKey.PublicKey)
		require.Equal(t,
			requestTarget{
				group: eacl.GroupSystem,
				ir:    true,
			},
			finder.Target(ctx, req),
		)

		req = new(object.SearchRequest)
		req.AddSignKey(nil, &containerKey.PublicKey)
		require.Equal(t,
			requestTarget{
				group: eacl.GroupSystem,
			},
			finder.Target(ctx, req),
		)

		req = new(object.SearchRequest)
		req.AddSignKey(nil, &prevContainerKey.PublicKey)
		require.Equal(t,
			requestTarget{
				group: eacl.GroupSystem,
			},
			finder.Target(ctx, req),
		)
	})

	t.Run("other owner", func(t *testing.T) {
		finder.cnrStorage = &testACLEntity{res: new(storage.Container)}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &test.DecodeKey(0).PublicKey)
		require.Equal(t,
			requestTarget{
				group: eacl.GroupOthers,
			},
			finder.Target(ctx, req),
		)
	})

	t.Run("can't fetch request owner", func(t *testing.T) {
		req := new(object.SearchRequest)

		require.Equal(t,
			requestTarget{
				group: eacl.GroupUnknown,
			},
			finder.Target(ctx, req),
		)
	})

	t.Run("can't fetch container", func(t *testing.T) {
		finder.cnrStorage = &testACLEntity{err: container.ErrNotFound}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &test.DecodeKey(0).PublicKey)
		require.Equal(t,
			requestTarget{
				group: eacl.GroupUnknown,
			},
			finder.Target(ctx, req),
		)
	})

	t.Run("can't fetch ir list", func(t *testing.T) {
		finder.cnrStorage = &testACLEntity{res: new(storage.Container)}
		finder.irKeysRecv = &testACLEntity{err: errors.New("blockchain is busy")}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &test.DecodeKey(0).PublicKey)
		require.Equal(t,
			requestTarget{
				group: eacl.GroupUnknown,
			},
			finder.Target(ctx, req),
		)
	})

	t.Run("can't fetch container list", func(t *testing.T) {
		finder.cnrStorage = &testACLEntity{res: new(storage.Container)}
		finder.cnrLister = &testACLEntity{err: container.ErrNotFound}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &test.DecodeKey(0).PublicKey)
		require.Equal(t,
			requestTarget{
				group: eacl.GroupUnknown,
			},
			finder.Target(ctx, req),
		)
	})
}
