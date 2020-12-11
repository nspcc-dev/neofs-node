package deletesvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

type tombstoneBodyWriter struct {
	body *objectV2.DeleteResponseBody
}

func (s *Service) toPrm(req *objectV2.DeleteRequest, respBody *objectV2.DeleteResponseBody) (*deletesvc.Prm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(token.NewSessionTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	p := new(deletesvc.Prm)
	p.SetPrivateKey(key)
	p.SetCommonParameters(commonParameters(meta))
	p.SetRemoteCallOptions(remoteCallOptionsFromMeta(meta)...)

	body := req.GetBody()
	p.WithAddress(object.NewAddressFromV2(body.GetAddress()))
	p.WithTombstoneAddressTarget(&tombstoneBodyWriter{
		body: respBody,
	})

	return p, nil
}

func (w *tombstoneBodyWriter) SetAddress(addr *object.Address) {
	w.body.SetTombstone(addr.ToV2())
}

func commonParameters(meta *session.RequestMetaHeader) *util.CommonPrm {
	prm := new(util.CommonPrm)

	if tok := meta.GetBearerToken(); tok != nil {
		prm.WithBearerToken(token.NewBearerTokenFromV2(tok))
	}

	if tok := meta.GetSessionToken(); tok != nil {
		prm.WithSessionToken(token.NewSessionTokenFromV2(tok))
	}

	return prm
}

// can be shared accross all services
func remoteCallOptionsFromMeta(meta *session.RequestMetaHeader) []client.CallOption {
	opts := make([]client.CallOption, 0, 3)
	opts = append(opts, client.WithTTL(meta.GetTTL()-1))

	if tok := meta.GetBearerToken(); tok != nil {
		opts = append(opts, client.WithBearer(token.NewBearerTokenFromV2(tok)))
	}

	if tok := meta.GetSessionToken(); tok != nil {
		opts = append(opts, client.WithSession(token.NewSessionTokenFromV2(tok)))
	}

	return opts
}
