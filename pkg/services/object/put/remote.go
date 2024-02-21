package putsvc

import (
	"context"
	"fmt"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type remoteTarget struct {
	ctx context.Context

	keyStorage *util.KeyStorage

	commonPrm *util.CommonPrm

	nodeInfo clientcore.NodeInfo

	obj *object.Object
	enc encodedObject

	clientConstructor ClientConstructor
	transport         Transport
}

// RemotePutPrm groups remote put operation parameters.
type RemotePutPrm struct {
	node netmap.NodeInfo

	obj *object.Object
}

func (t *remoteTarget) WriteObject(obj *object.Object, _ objectcore.ContentMeta, enc encodedObject) error {
	t.obj = obj
	t.enc = enc
	return nil
}

func (t *remoteTarget) Close() (oid.ID, error) {
	if t.enc.hdrOff > 0 {
		err := t.transport.ReplicateToNode(t.ctx, t.enc.b, t.nodeInfo)
		if err != nil {
			return oid.ID{}, fmt.Errorf("replicate object: %w", err)
		}
		id, _ := t.obj.ID()
		return id, nil
	}

	var sessionInfo *util.SessionInfo

	if tok := t.commonPrm.SessionToken(); tok != nil {
		sessionInfo = &util.SessionInfo{
			ID:    tok.ID(),
			Owner: tok.Issuer(),
		}
	}

	key, err := t.keyStorage.GetKey(sessionInfo)
	if err != nil {
		return oid.ID{}, fmt.Errorf("(%T) could not receive private key: %w", t, err)
	}

	c, err := t.clientConstructor.Get(t.nodeInfo)
	if err != nil {
		return oid.ID{}, fmt.Errorf("(%T) could not create SDK client %s: %w", t, t.nodeInfo, err)
	}

	var prm internalclient.PutObjectPrm

	prm.SetContext(t.ctx)
	prm.SetClient(c)
	prm.SetPrivateKey(key)
	prm.SetSessionToken(t.commonPrm.SessionToken())
	prm.SetBearerToken(t.commonPrm.BearerToken())
	prm.SetXHeaders(t.commonPrm.XHeaders())
	prm.SetObject(t.obj)

	res, err := internalclient.PutObject(prm)
	if err != nil {
		return oid.ID{}, fmt.Errorf("(%T) could not put object to %s: %w", t, t.nodeInfo.AddressGroup(), err)
	}

	return res.ID(), nil
}
