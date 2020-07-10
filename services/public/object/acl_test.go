package object

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/acl"
	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	crypto "github.com/nspcc-dev/neofs-crypto"
	libacl "github.com/nspcc-dev/neofs-node/lib/acl"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/ir"
	"github.com/nspcc-dev/neofs-node/lib/test"
	"github.com/stretchr/testify/require"
)

type (
	testACLEntity struct {
		// Set of interfaces which testCommonEntity must implement, but some methods from those does not call.
		serviceRequest
		RequestTargeter
		implementations.ACLHelper
		implementations.ContainerNodesLister
		implementations.ContainerOwnerChecker
		acl.ExtendedACLTable
		libacl.RequestInfo

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

type testBasicChecker struct {
	libacl.BasicChecker

	actionErr error
	action    bool

	sticky bool

	extended bool

	bearer bool
}

func (t *testACLEntity) calculateRequestAction(context.Context, requestActionParams) acl.ExtendedACLAction {
	return t.res.(acl.ExtendedACLAction)
}

func (t *testACLEntity) buildRequestInfo(req serviceRequest, target acl.Target) (libacl.RequestInfo, error) {
	if t.f != nil {
		t.f(req, target)
	}

	if t.err != nil {
		return nil, t.err
	}

	return t.res.(libacl.RequestInfo), nil
}

func (t *testACLEntity) Action(table acl.ExtendedACLTable, req libacl.RequestInfo) acl.ExtendedACLAction {
	if t.f != nil {
		t.f(table, req)
	}

	return t.res.(acl.ExtendedACLAction)
}

func (t *testACLEntity) GetExtendedACLTable(_ context.Context, cid CID) (acl.ExtendedACLTable, error) {
	if t.f != nil {
		t.f(cid)
	}

	if t.err != nil {
		return nil, t.err
	}

	return t.res.(acl.ExtendedACLTable), nil
}

func (s *testBasicChecker) Extended(uint32) bool {
	return s.extended
}

func (s *testBasicChecker) Sticky(uint32) bool {
	return s.sticky
}

func (s *testBasicChecker) Bearer(uint32, object.RequestType) (bool, error) {
	return s.bearer, nil
}

func (s *testBasicChecker) Action(uint32, object.RequestType, acl.Target) (bool, error) {
	return s.action, s.actionErr
}

func (t *testACLEntity) GetBasicACL(context.Context, CID) (uint32, error) {
	if t.err != nil {
		return 0, t.err
	}

	return t.res.(uint32), nil
}

func (t *testACLEntity) Target(context.Context, serviceRequest) acl.Target {
	return t.res.(acl.Target)
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

func (t testACLEntity) GetIRInfo(ir.GetInfoParams) (*ir.GetInfoResult, error) {
	if t.err != nil {
		return nil, t.err
	}

	res := new(ir.GetInfoResult)
	res.SetInfo(*t.res.(*ir.Info))

	return res, nil
}

func (t *testACLEntity) ContainerNodesInfo(ctx context.Context, cid CID, prev int) ([]bootstrap.NodeInfo, error) {
	if t.err != nil {
		return nil, t.err
	}

	return t.res.([][]bootstrap.NodeInfo)[prev], nil
}

func (t *testACLEntity) IsContainerOwner(_ context.Context, cid CID, owner OwnerID) (bool, error) {
	if t.f != nil {
		t.f(cid, owner)
	}
	if t.err != nil {
		return false, t.err
	}

	return t.res.(bool), nil
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
		rule := uint32(0x00000003)
		// set F-bit
		rule |= 1 << 28

		checker := new(libacl.BasicACLChecker)

		preprocessor := aclPreProcessor{
			log: test.NewTestLogger(false),
			aclInfoReceiver: aclInfoReceiver{
				basicACLGetter: &testACLEntity{res: rule},
				basicChecker:   checker,
				targetFinder:   &testACLEntity{res: acl.Target_Others},
			},
			basicChecker: checker,
		}
		require.NoError(t, preprocessor.preProcess(ctx, &testACLEntity{res: object.RequestGet}))

		preprocessor.aclInfoReceiver.targetFinder = &testACLEntity{res: acl.Target_System}
		require.Error(t, preprocessor.preProcess(ctx, &testACLEntity{res: object.RequestGet}))
		preprocessor.aclInfoReceiver.targetFinder = &testACLEntity{res: acl.Target_User}
		require.Error(t, preprocessor.preProcess(ctx, &testACLEntity{res: object.RequestGet}))
	})

	t.Run("can't fetch container", func(t *testing.T) {
		preprocessor := aclPreProcessor{
			log: test.NewTestLogger(false),
			aclInfoReceiver: aclInfoReceiver{
				basicACLGetter: &testACLEntity{err: container.ErrNotFound},
				targetFinder:   &testACLEntity{res: acl.Target_Others},
			},
		}
		require.Error(t, preprocessor.preProcess(ctx, &testACLEntity{res: object.RequestGet}))

	})

	t.Run("sticky bit", func(t *testing.T) {
		checker := &testBasicChecker{
			actionErr: nil,
			action:    true,
			sticky:    true,
		}

		s := &aclPreProcessor{
			log: test.NewTestLogger(false),
			aclInfoReceiver: aclInfoReceiver{
				basicACLGetter: &testACLEntity{
					res: uint32(0),
				},
				basicChecker: checker,
				targetFinder: &testACLEntity{
					res: acl.Target_User,
				},
			},
			basicChecker: checker,
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

	t.Run("extended ACL", func(t *testing.T) {
		target := acl.Target_Others

		req := &testACLEntity{
			res: object.RequestGet,
		}

		actCalc := new(testACLEntity)

		checker := &testBasicChecker{
			action:   true,
			extended: true,
		}

		s := &aclPreProcessor{
			log: test.NewTestLogger(false),
			aclInfoReceiver: aclInfoReceiver{
				basicACLGetter: &testACLEntity{
					res: uint32(1),
				},
				basicChecker: checker,
				targetFinder: &testACLEntity{
					res: target,
				},
			},
			basicChecker: checker,

			reqActionCalc: actCalc,
		}

		// force to return non-ActionAllow
		actCalc.res = acl.ActionAllow + 1
		require.EqualError(t, s.preProcess(ctx, req), errAccessDenied.Error())

		// force to return ActionAllow
		actCalc.res = acl.ActionAllow
		require.NoError(t, s.preProcess(ctx, req))
	})
}

func TestTargetFinder(t *testing.T) {
	ctx := context.TODO()
	irKey := test.DecodeKey(2)
	containerKey := test.DecodeKey(3)
	prevContainerKey := test.DecodeKey(4)

	irInfo := new(ir.Info)
	irNode := ir.Node{}
	irNode.SetKey(crypto.MarshalPublicKey(&irKey.PublicKey))
	irInfo.SetNodes([]ir.Node{irNode})

	finder := &targetFinder{
		log: test.NewTestLogger(false),
		irStorage: &testACLEntity{
			res: irInfo,
		},
		cnrLister: &testACLEntity{res: [][]bootstrap.NodeInfo{
			{{PubKey: crypto.MarshalPublicKey(&containerKey.PublicKey)}},
			{{PubKey: crypto.MarshalPublicKey(&prevContainerKey.PublicKey)}},
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

		finder.cnrOwnerChecker = &testACLEntity{
			f: func(items ...interface{}) {
				require.Equal(t, req.CID(), items[0])
				require.Equal(t, owner, items[1])
			},
			res: true,
		}

		require.Equal(t, acl.Target_User, finder.Target(ctx, req))
	})

	t.Run("container owner", func(t *testing.T) {
		finder.cnrOwnerChecker = &testACLEntity{res: true}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &test.DecodeKey(0).PublicKey)

		require.Equal(t, acl.Target_User, finder.Target(ctx, req))
	})

	t.Run("system owner", func(t *testing.T) {
		finder.cnrOwnerChecker = &testACLEntity{res: false}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &irKey.PublicKey)
		require.Equal(t, acl.Target_System, finder.Target(ctx, req))

		req = new(object.SearchRequest)
		req.AddSignKey(nil, &containerKey.PublicKey)
		require.Equal(t, acl.Target_System, finder.Target(ctx, req))

		req = new(object.SearchRequest)
		req.AddSignKey(nil, &prevContainerKey.PublicKey)
		require.Equal(t, acl.Target_System, finder.Target(ctx, req))
	})

	t.Run("other owner", func(t *testing.T) {
		finder.cnrOwnerChecker = &testACLEntity{res: false}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &test.DecodeKey(0).PublicKey)
		require.Equal(t, acl.Target_Others, finder.Target(ctx, req))
	})

	t.Run("can't fetch request owner", func(t *testing.T) {
		req := new(object.SearchRequest)

		require.Equal(t, acl.Target_Unknown, finder.Target(ctx, req))
	})

	t.Run("can't fetch container", func(t *testing.T) {
		finder.cnrOwnerChecker = &testACLEntity{err: container.ErrNotFound}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &test.DecodeKey(0).PublicKey)
		require.Equal(t, acl.Target_Unknown, finder.Target(ctx, req))
	})

	t.Run("can't fetch ir list", func(t *testing.T) {
		finder.cnrOwnerChecker = &testACLEntity{res: false}
		finder.irStorage = &testACLEntity{err: errors.New("blockchain is busy")}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &test.DecodeKey(0).PublicKey)
		require.Equal(t, acl.Target_Unknown, finder.Target(ctx, req))
	})

	t.Run("can't fetch container list", func(t *testing.T) {
		finder.cnrOwnerChecker = &testACLEntity{res: false}
		finder.cnrLister = &testACLEntity{err: container.ErrNotFound}

		req := new(object.SearchRequest)
		req.AddSignKey(nil, &test.DecodeKey(0).PublicKey)
		require.Equal(t, acl.Target_Unknown, finder.Target(ctx, req))
	})
}
