package putsvc

import (
	"context"
	"fmt"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

type remoteTarget struct {
	transformer.ObjectTarget

	ctx context.Context

	keyStorage *util.KeyStorage

	commonPrm *util.CommonPrm

	nodeInfo clientcore.NodeInfo

	obj *object.Object

	clientConstructor ClientConstructor
}

// RemoteSender represents utility for
// sending an object to a remote host.
type RemoteSender struct {
	keyStorage *util.KeyStorage

	clientConstructor ClientConstructor
}

// RemotePutPrm groups remote put operation parameters.
type RemotePutPrm struct {
	node *netmap.NodeInfo

	obj *object.Object
}

func (t *remoteTarget) WriteHeader(obj *object.RawObject) error {
	t.obj = obj.Object()

	return nil
}

func (t *remoteTarget) Close() (*transformer.AccessIdentifiers, error) {
	key, err := t.keyStorage.GetKey(t.commonPrm.SessionToken())
	if err != nil {
		return nil, fmt.Errorf("(%T) could not receive private key: %w", t, err)
	}

	c, err := t.clientConstructor.Get(t.nodeInfo)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not create SDK client %s: %w", t, t.nodeInfo, err)
	}

	var prm internalclient.PutObjectPrm

	prm.SetContext(t.ctx)
	prm.SetClient(c)
	prm.SetPrivateKey(key)
	prm.SetSessionToken(t.commonPrm.SessionToken())
	prm.SetBearerToken(t.commonPrm.BearerToken())
	prm.SetXHeaders(t.commonPrm.XHeaders())
	prm.SetObject(t.obj.SDK())

	res, err := internalclient.PutObject(prm)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not put object to %s: %w", t, t.nodeInfo.AddressGroup(), err)
	}

	return new(transformer.AccessIdentifiers).
		WithSelfID(res.ID()), nil
}

// NewRemoteSender creates, initializes and returns new RemoteSender instance.
func NewRemoteSender(keyStorage *util.KeyStorage, cons ClientConstructor) *RemoteSender {
	return &RemoteSender{
		keyStorage:        keyStorage,
		clientConstructor: cons,
	}
}

// WithNodeInfo sets information about the remote node.
func (p *RemotePutPrm) WithNodeInfo(v *netmap.NodeInfo) *RemotePutPrm {
	if p != nil {
		p.node = v
	}

	return p
}

// WithObject sets transferred object.
func (p *RemotePutPrm) WithObject(v *object.Object) *RemotePutPrm {
	if p != nil {
		p.obj = v
	}

	return p
}

// PutObject sends object to remote node.
func (s *RemoteSender) PutObject(ctx context.Context, p *RemotePutPrm) error {
	t := &remoteTarget{
		ctx:               ctx,
		keyStorage:        s.keyStorage,
		clientConstructor: s.clientConstructor,
	}

	err := clientcore.NodeInfoFromRawNetmapElement(&t.nodeInfo, p.node)
	if err != nil {
		return fmt.Errorf("parse client node info: %w", err)
	}

	if err := t.WriteHeader(object.NewRawFromObject(p.obj)); err != nil {
		return fmt.Errorf("(%T) could not send object header: %w", s, err)
	} else if _, err := t.Close(); err != nil {
		return fmt.Errorf("(%T) could not send object: %w", s, err)
	}

	return nil
}
