package headsvc

import (
	"context"
	"errors"
	"fmt"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

type ClientConstructor interface {
	Get(clientcore.NodeInfo) (client.Client, error)
}

// RemoteHeader represents utility for getting
// the object header from a remote host.
type RemoteHeader struct {
	keyStorage *util.KeyStorage

	clientCache ClientConstructor
}

// RemoteHeadPrm groups remote header operation parameters.
type RemoteHeadPrm struct {
	commonHeadPrm *Prm

	node *netmap.NodeInfo
}

var ErrNotFound = errors.New("object header not found")

// NewRemoteHeader creates, initializes and returns new RemoteHeader instance.
func NewRemoteHeader(keyStorage *util.KeyStorage, cache ClientConstructor) *RemoteHeader {
	return &RemoteHeader{
		keyStorage:  keyStorage,
		clientCache: cache,
	}
}

// WithNodeInfo sets information about the remote node.
func (p *RemoteHeadPrm) WithNodeInfo(v *netmap.NodeInfo) *RemoteHeadPrm {
	if p != nil {
		p.node = v
	}

	return p
}

// WithObjectAddress sets object address.
func (p *RemoteHeadPrm) WithObjectAddress(v *objectSDK.Address) *RemoteHeadPrm {
	if p != nil {
		p.commonHeadPrm = new(Prm).WithAddress(v)
	}

	return p
}

// Head requests object header from the remote node.
func (h *RemoteHeader) Head(ctx context.Context, prm *RemoteHeadPrm) (*object.Object, error) {
	key, err := h.keyStorage.GetKey(nil)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not receive private key: %w", h, err)
	}

	var info clientcore.NodeInfo

	err = clientcore.NodeInfoFromRawNetmapElement(&info, prm.node)
	if err != nil {
		return nil, fmt.Errorf("parse client node info: %w", err)
	}

	c, err := h.clientCache.Get(info)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not create SDK client %s: %w", h, info.AddressGroup(), err)
	}

	var headPrm internalclient.HeadObjectPrm

	headPrm.SetContext(ctx)
	headPrm.SetClient(c)
	headPrm.SetPrivateKey(key)
	headPrm.SetAddress(prm.commonHeadPrm.addr)
	headPrm.SetTTL(1) // FIXME: use constant

	res, err := internalclient.HeadObject(headPrm)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not head object in %s: %w", h, info.AddressGroup(), err)
	}

	return object.NewFromSDK(res.Header()), nil
}
