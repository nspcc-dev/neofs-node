package putsvc

import (
	"context"
	"fmt"
	"io"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
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

// RemoteSender represents utility for
// sending an object to a remote host.
type RemoteSender struct {
	keyStorage *util.KeyStorage

	clientConstructor ClientConstructor
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

func (t *remoteTarget) Close() (oid.ID, []byte, error) {
	if t.enc.hdrOff > 0 {
		sigs, err := t.transport.SendReplicationRequestToNode(t.ctx, t.enc.b, t.nodeInfo)
		if err != nil {
			return oid.ID{}, nil, fmt.Errorf("replicate object to remote node (key=%x): %w", t.nodeInfo.PublicKey(), err)
		}
		return t.obj.GetID(), sigs, nil
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
		return oid.ID{}, nil, fmt.Errorf("(%T) could not receive private key: %w", t, err)
	}

	c, err := t.clientConstructor.Get(t.nodeInfo)
	if err != nil {
		return oid.ID{}, nil, fmt.Errorf("(%T) could not create SDK client %s: %w", t, t.nodeInfo, err)
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
		return oid.ID{}, nil, fmt.Errorf("(%T) could not put object to %s: %w", t, t.nodeInfo.AddressGroup(), err)
	}

	return res.ID(), nil, nil
}

// NewRemoteSender creates, initializes and returns new RemoteSender instance.
func NewRemoteSender(keyStorage *util.KeyStorage, cons ClientConstructor) *RemoteSender {
	return &RemoteSender{
		keyStorage:        keyStorage,
		clientConstructor: cons,
	}
}

// WithNodeInfo sets information about the remote node.
func (p *RemotePutPrm) WithNodeInfo(v netmap.NodeInfo) *RemotePutPrm {
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

	err := clientcore.NodeInfoFromRawNetmapElement(&t.nodeInfo, netmapCore.Node(p.node))
	if err != nil {
		return fmt.Errorf("parse client node info: %w", err)
	}

	err = t.WriteObject(p.obj, objectcore.ContentMeta{}, encodedObject{})
	if err != nil {
		return fmt.Errorf("(%T) could not send object header: %w", s, err)
	}

	_, _, err = t.Close()
	if err != nil {
		return fmt.Errorf("(%T) could not send object: %w", s, err)
	}

	return nil
}

// ReplicateObjectToNode copies binary-encoded NeoFS object from the given
// [io.ReadSeeker] into local storage of the node described by specified
// [netmap.NodeInfo].
func (s *RemoteSender) ReplicateObjectToNode(ctx context.Context, id oid.ID, src io.ReadSeeker, nodeInfo netmap.NodeInfo) error {
	var nodeInfoForCons clientcore.NodeInfo

	err := clientcore.NodeInfoFromRawNetmapElement(&nodeInfoForCons, netmapCore.Node(nodeInfo))
	if err != nil {
		return fmt.Errorf("parse remote node info: %w", err)
	}

	key, err := s.keyStorage.GetKey(nil)
	if err != nil {
		return fmt.Errorf("fetch local node's private key: %w", err)
	}

	c, err := s.clientConstructor.Get(nodeInfoForCons)
	if err != nil {
		return fmt.Errorf("init NeoFS API client of the remote node: %w", err)
	}

	_, err = c.ReplicateObject(ctx, id, src, (*neofsecdsa.Signer)(key), false)
	if err != nil {
		return fmt.Errorf("copy object using NeoFS API client of the remote node: %w", err)
	}

	return nil
}
