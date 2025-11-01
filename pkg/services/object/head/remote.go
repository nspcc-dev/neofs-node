package headsvc

import (
	"context"
	"fmt"
	"io"
	"math"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type ClientConstructor interface {
	Get(clientcore.NodeInfo) (clientcore.Client, error)
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

	node netmap.NodeInfo

	xs []string

	checkOID bool
}

// NewRemoteHeader creates, initializes and returns new RemoteHeader instance.
func NewRemoteHeader(keyStorage *util.KeyStorage, cache ClientConstructor) *RemoteHeader {
	return &RemoteHeader{
		keyStorage:  keyStorage,
		clientCache: cache,
	}
}

// WithNodeInfo sets information about the remote node.
func (p *RemoteHeadPrm) WithNodeInfo(v netmap.NodeInfo) *RemoteHeadPrm {
	if p != nil {
		p.node = v
	}

	return p
}

// WithObjectAddress sets object address.
func (p *RemoteHeadPrm) WithObjectAddress(v oid.Address) *RemoteHeadPrm {
	if p != nil {
		p.commonHeadPrm = new(Prm).WithAddress(v)
	}

	return p
}

// WithXHeaders sets X-headers.
func (p *RemoteHeadPrm) WithXHeaders(xs []string) {
	p.xs = xs
}

// WithIDVerification specifies whether to check object ID match.
func (p *RemoteHeadPrm) WithIDVerification(f bool) {
	p.checkOID = f
}

// Head requests object header from the remote node. Returns:
//   - [apistatus.ErrObjectNotFound] error if the requested object is missing
//   - [apistatus.ErrNodeUnderMaintenance] error if remote node is currently under maintenance
func (h *RemoteHeader) Head(ctx context.Context, prm *RemoteHeadPrm) (*object.Object, error) {
	key, err := h.keyStorage.GetKey(nil)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not receive private key: %w", h, err)
	}

	var info clientcore.NodeInfo

	err = clientcore.NodeInfoFromRawNetmapElement(&info, netmapCore.Node(prm.node))
	if err != nil {
		return nil, fmt.Errorf("parse client node info: %w", err)
	}

	c, err := h.clientCache.Get(info)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not create SDK client %s: %w", h, info.AddressGroup(), err)
	}

	var opts client.PrmObjectHead
	opts.MarkLocal()
	opts.WithXHeaders(prm.xs...)
	if !prm.checkOID {
		opts.SkipChecksumVerification()
	}

	res, err := c.ObjectHead(ctx, prm.commonHeadPrm.addr.Container(), prm.commonHeadPrm.addr.Object(), user.NewAutoIDSigner(*key), opts)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not head object in %s: read object header from NeoFS: %w", h, info.AddressGroup(), err)
	}

	return res, nil
}

// GetRange requests object payload range from the remote node.
func (h *RemoteHeader) GetRange(ctx context.Context, node netmap.NodeInfo, cnr cid.ID, id oid.ID, ln, off uint64, xs []string) (io.ReadCloser, error) {
	key, err := h.keyStorage.GetKey(nil)
	if err != nil {
		return nil, fmt.Errorf("get local SN private key: %w", err)
	}

	var info clientcore.NodeInfo

	err = clientcore.NodeInfoFromRawNetmapElement(&info, netmapCore.Node(node))
	if err != nil {
		return nil, fmt.Errorf("parse client node info: %w", err)
	}

	conn, err := h.clientCache.Get(info)
	if err != nil {
		return nil, fmt.Errorf("get conn: %w", err)
	}

	// TODO: Use GetRange after https://github.com/nspcc-dev/neofs-node/issues/3547.
	var opts client.PrmObjectGet
	opts.MarkLocal()
	opts.SkipChecksumVerification() // TODO: see same place in GET service
	opts.WithXHeaders(xs...)

	hdr, rc, err := conn.ObjectGetInit(ctx, cnr, id, user.NewAutoIDSigner(*key), opts)
	if err != nil {
		return nil, fmt.Errorf("call Get API: %w", err)
	}

	if ln == 0 && off == 0 {
		return rc, nil
	}

	full := hdr.PayloadSize()
	if off == 0 && ln == full {
		return rc, nil
	}

	if off >= full || full-off < ln {
		rc.Close()
		return nil, apistatus.ErrObjectOutOfRange
	}

	if off > math.MaxInt64 || ln > math.MaxInt64 {
		rc.Close()
		return nil, fmt.Errorf("too big range for this server: off=%d,len=%d", off, ln)
	}

	if off > 0 {
		if _, err := io.CopyN(io.Discard, rc, int64(off)); err != nil {
			rc.Close()
			return nil, fmt.Errorf("seek offset in Get API stream: %w", err)
		}
	}

	return struct {
		io.Reader
		io.Closer
	}{
		Reader: io.LimitReader(rc, int64(ln)),
		Closer: rc,
	}, nil
}
