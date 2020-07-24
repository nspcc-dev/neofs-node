package object

import (
	"context"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testDeleteEntity struct {
		// Set of interfaces which testDeleteEntity must implement, but some methods from those does not call.
		session.PrivateTokenStore

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ EpochReceiver        = (*testDeleteEntity)(nil)
	_ objectStorer         = (*testDeleteEntity)(nil)
	_ tombstoneCreator     = (*testDeleteEntity)(nil)
	_ objectChildrenLister = (*testDeleteEntity)(nil)
	_ objectRemover        = (*testDeleteEntity)(nil)
	_ requestHandler       = (*testDeleteEntity)(nil)
	_ deletePreparer       = (*testDeleteEntity)(nil)
	_ responsePreparer     = (*testDeleteEntity)(nil)
)

func (s *testDeleteEntity) verify(context.Context, *session.Token, *Object) error {
	return nil
}

func (s *testDeleteEntity) Fetch(id session.PrivateTokenKey) (session.PrivateToken, error) {
	if s.f != nil {
		s.f(id)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(session.PrivateToken), nil
}

func (s *testDeleteEntity) prepareResponse(_ context.Context, req serviceRequest, resp serviceResponse) error {
	if s.f != nil {
		s.f(req, resp)
	}
	return s.err
}

func (s *testDeleteEntity) Epoch() uint64 { return s.res.(uint64) }

func (s *testDeleteEntity) putObject(_ context.Context, p transport.PutInfo) (*Address, error) {
	if s.f != nil {
		s.f(p)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.(*Address), nil
}

func (s *testDeleteEntity) createTombstone(_ context.Context, p deleteInfo) *Object {
	if s.f != nil {
		s.f(p)
	}
	return s.res.(*Object)
}

func (s *testDeleteEntity) children(ctx context.Context, addr Address) []ID {
	if s.f != nil {
		s.f(addr, ctx)
	}
	return s.res.([]ID)
}

func (s *testDeleteEntity) delete(ctx context.Context, p deleteInfo) error {
	if s.f != nil {
		s.f(p, ctx)
	}
	return s.err
}

func (s *testDeleteEntity) prepare(_ context.Context, p deleteInfo) ([]deleteInfo, error) {
	if s.f != nil {
		s.f(p)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]deleteInfo), nil
}

func (s *testDeleteEntity) handleRequest(_ context.Context, p handleRequestParams) (interface{}, error) {
	if s.f != nil {
		s.f(p)
	}
	return s.res, s.err
}

func Test_objectService_Delete(t *testing.T) {
	ctx := context.TODO()
	req := &object.DeleteRequest{Address: testObjectAddress(t)}

	t.Run("handler error", func(t *testing.T) {
		rhErr := errors.New("test error for request handler")

		s := &objectService{
			statusCalculator: newStatusCalculator(),
		}

		s.requestHandler = &testDeleteEntity{
			f: func(items ...interface{}) {
				t.Run("correct request handler params", func(t *testing.T) {
					p := items[0].(handleRequestParams)
					require.Equal(t, req, p.request)
					require.Equal(t, s, p.executor)
				})
			},
			err: rhErr, // force requestHandler to return rhErr
		}

		res, err := s.Delete(ctx, req)
		// ascertain that error returns as expected
		require.EqualError(t, err, rhErr.Error())
		require.Nil(t, res)
	})

	t.Run("correct result", func(t *testing.T) {
		s := objectService{
			requestHandler: new(testDeleteEntity),
			respPreparer:   &testDeleteEntity{res: new(object.DeleteResponse)},

			statusCalculator: newStatusCalculator(),
		}

		res, err := s.Delete(ctx, req)
		require.NoError(t, err)
		require.Equal(t, new(object.DeleteResponse), res)
	})
}

func Test_coreObjRemover_delete(t *testing.T) {
	ctx := context.TODO()
	pToken, err := session.NewPrivateToken(0)
	require.NoError(t, err)

	addr := testObjectAddress(t)

	token := new(service.Token)
	token.SetAddress(addr)

	req := newRawDeleteInfo()
	req.setAddress(addr)
	req.setSessionToken(token)

	t.Run("nil token", func(t *testing.T) {
		s := new(coreObjRemover)

		req := newRawDeleteInfo()
		require.Nil(t, req.GetSessionToken())

		require.EqualError(t, s.delete(ctx, req), errNilToken.Error())
	})

	t.Run("prepare error", func(t *testing.T) {
		dpErr := errors.New("test error for delete preparer")

		dp := &testDeleteEntity{
			f: func(items ...interface{}) {
				t.Run("correct delete preparer params", func(t *testing.T) {
					require.Equal(t, req, items[0])
				})
			},
			err: dpErr, // force deletePreparer to return dpErr
		}

		s := &coreObjRemover{
			delPrep:    dp,
			tokenStore: &testDeleteEntity{res: pToken},
			mErr: map[error]struct{}{
				dpErr: {},
			},
			log: zap.L(),
		}

		// ascertain that error returns as expected
		require.EqualError(t, s.delete(ctx, req), dpErr.Error())

		dp.err = errors.New("some other error")

		// ascertain that error returns as expected
		require.EqualError(t, s.delete(ctx, req), errDeletePrepare.Error())
	})

	t.Run("straight remover error", func(t *testing.T) {
		dInfo := newRawDeleteInfo()
		dInfo.setAddress(addr)
		dInfo.setSessionToken(token)

		list := []deleteInfo{
			dInfo,
		}

		srErr := errors.New("test error for straight remover")

		s := &coreObjRemover{
			delPrep: &testDeleteEntity{
				res: list, // force deletePreparer to return list
			},
			straightRem: &testDeleteEntity{
				f: func(items ...interface{}) {
					t.Run("correct straight remover params", func(t *testing.T) {
						require.Equal(t, list[0], items[0])

						ctx := items[1].(context.Context)

						require.Equal(t,
							dInfo.GetSessionToken(),
							ctx.Value(transformer.PublicSessionToken),
						)

						require.Equal(t,
							pToken,
							ctx.Value(transformer.PrivateSessionToken),
						)
					})
				},
				err: srErr, // force objectRemover to return srErr
			},
			tokenStore: &testDeleteEntity{res: pToken},
		}

		// ascertain that error returns as expected
		require.EqualError(t, s.delete(ctx, req), errors.Wrapf(srErr, emRemovePart, 1, 1).Error())
	})

	t.Run("success", func(t *testing.T) {
		dInfo := newRawDeleteInfo()
		dInfo.setAddress(addr)
		dInfo.setSessionToken(token)

		list := []deleteInfo{
			dInfo,
		}

		s := &coreObjRemover{
			delPrep: &testDeleteEntity{
				res: list, // force deletePreparer to return list
			},
			straightRem: &testDeleteEntity{
				err: nil, // force objectRemover to return empty error
			},
			tokenStore: &testDeleteEntity{res: pToken},
		}

		// ascertain that nil error returns
		require.NoError(t, s.delete(ctx, req))
	})
}

func Test_coreDelPreparer_prepare(t *testing.T) {
	var (
		ctx        = context.TODO()
		ownerID    = OwnerID{1, 2, 3}
		addr       = testObjectAddress(t)
		timeout    = 5 * time.Second
		token      = new(service.Token)
		childCount = 10
		children   = make([]ID, 0, childCount)
	)

	req := newRawDeleteInfo()
	req.setAddress(addr)
	req.setSessionToken(token)
	req.setOwnerID(ownerID)

	token.SetID(session.TokenID{1, 2, 3})

	for i := 0; i < childCount; i++ {
		children = append(children, testObjectAddress(t).ObjectID)
	}

	s := &coreDelPreparer{
		timeout: timeout,
		childLister: &testDeleteEntity{
			f: func(items ...interface{}) {
				t.Run("correct children lister params", func(t *testing.T) {
					require.Equal(t, addr, items[0])
					require.Equal(t,
						token,
						items[1].(context.Context).Value(transformer.PublicSessionToken),
					)
				})
			},
			res: children,
		},
	}

	res, err := s.prepare(ctx, req)
	require.NoError(t, err)

	require.Len(t, res, childCount+1)

	for i := range res {
		require.Equal(t, timeout, res[i].GetTimeout())
		require.Equal(t, token, res[i].GetSessionToken())
		require.Equal(t, uint32(service.NonForwardingTTL), res[i].GetTTL())

		a := res[i].GetAddress()
		require.Equal(t, addr.CID, a.CID)
		if i > 0 {
			require.Equal(t, children[i-1], a.ObjectID)
		} else {
			require.Equal(t, addr.ObjectID, a.ObjectID)
		}
	}
}

func Test_straightObjRemover_delete(t *testing.T) {
	var (
		ctx     = context.TODO()
		addr    = testObjectAddress(t)
		ttl     = uint32(10)
		timeout = 5 * time.Second
		token   = new(service.Token)
		obj     = &Object{SystemHeader: SystemHeader{ID: addr.ObjectID, CID: addr.CID}}
	)

	token.SetID(session.TokenID{1, 2, 3})

	req := newRawDeleteInfo()
	req.setTTL(ttl)
	req.setTimeout(timeout)
	req.setAddress(testObjectAddress(t))
	req.setSessionToken(token)

	t.Run("correct result", func(t *testing.T) {
		osErr := errors.New("test error for object storer")

		s := &straightObjRemover{
			tombCreator: &testDeleteEntity{
				f: func(items ...interface{}) {
					t.Run("correct tombstone creator params", func(t *testing.T) {
						require.Equal(t, req, items[0])
					})
				},
				res: obj,
			},
			objStorer: &testDeleteEntity{
				f: func(items ...interface{}) {
					t.Run("correct object storer params", func(t *testing.T) {
						p := items[0].(transport.PutInfo)
						require.Equal(t, timeout, p.GetTimeout())
						require.Equal(t, ttl, p.GetTTL())
						require.Equal(t, obj, p.GetHead())
						require.Equal(t, token, p.GetSessionToken())
					})
				},
				err: osErr, // force objectStorer to return osErr
			},
		}

		// ascertain that error returns as expected
		require.EqualError(t, s.delete(ctx, req), osErr.Error())
	})
}

func Test_coreTombCreator_createTombstone(t *testing.T) {
	var (
		ctx     = context.TODO()
		addr    = testObjectAddress(t)
		ownerID = OwnerID{1, 2, 3}
	)

	req := newRawDeleteInfo()
	req.setAddress(addr)
	req.setOwnerID(ownerID)

	t.Run("correct result", func(t *testing.T) {
		s := new(coreTombCreator)

		res := s.createTombstone(ctx, req)
		require.Equal(t, addr.CID, res.SystemHeader.CID)
		require.Equal(t, addr.ObjectID, res.SystemHeader.ID)
		require.Equal(t, ownerID, res.SystemHeader.OwnerID)

		_, tsHdr := res.LastHeader(object.HeaderType(object.TombstoneHdr))
		require.NotNil(t, tsHdr)
		require.Equal(t, new(object.Tombstone), tsHdr.Value.(*object.Header_Tombstone).Tombstone)
	})
}

func Test_deleteInfo(t *testing.T) {
	t.Run("address", func(t *testing.T) {
		addr := testObjectAddress(t)

		req := newRawDeleteInfo()
		req.setAddress(addr)

		require.Equal(t, addr, req.GetAddress())
	})

	t.Run("owner ID", func(t *testing.T) {
		ownerID := OwnerID{}
		_, err := rand.Read(ownerID[:])
		require.NoError(t, err)

		req := newRawDeleteInfo()
		req.setOwnerID(ownerID)
		require.Equal(t, ownerID, req.GetOwnerID())

		tReq := &transportRequest{serviceRequest: &object.DeleteRequest{OwnerID: ownerID}}
		require.Equal(t, ownerID, tReq.GetOwnerID())
	})

	t.Run("token", func(t *testing.T) {
		token := new(session.Token)
		_, err := rand.Read(token.ID[:])
		require.NoError(t, err)

		req := newRawDeleteInfo()
		req.setSessionToken(token)
		require.Equal(t, token, req.GetSessionToken())

		dReq := new(object.DeleteRequest)
		dReq.SetToken(token)
		tReq := &transportRequest{serviceRequest: dReq}
		require.Equal(t, token, tReq.GetSessionToken())
	})
}
