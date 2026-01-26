package putsvc

import (
	"context"
	"fmt"
	"io"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

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

func putObjectToNode(ctx context.Context, nodeInfo clientcore.NodeInfo, obj *object.Object,
	keyStorage *util.KeyStorage, clientConstructor ClientConstructor, commonPrm *util.CommonPrm) error {
	var opts client.PrmObjectPutInit
	opts.MarkLocal()

	key, err := keyStorage.GetKey(nil)
	if err != nil {
		return fmt.Errorf("could not receive local node's private key: %w", err)
	}

	if tokV2 := commonPrm.SessionTokenV2(); tokV2 != nil {
		// For V2 tokens, the key is stored as the subjects
		if keyForSession, err := keyStorage.GetKeyBySubjects(tokV2.Issuer(), tokV2.Subjects()); err == nil {
			key = keyForSession
		}
		opts.WithinSessionV2(*tokV2)
	} else if tok := commonPrm.SessionToken(); tok != nil {
		sessionInfo := &util.SessionInfo{
			ID:    tok.ID(),
			Owner: tok.Issuer(),
		}
		key, err = keyStorage.GetKey(sessionInfo)
		if err != nil {
			return fmt.Errorf("could not receive private key: %w", err)
		}
		opts.WithinSession(*tok)
	}

	c, err := clientConstructor.Get(ctx, nodeInfo)
	if err != nil {
		return fmt.Errorf("could not create SDK client %s: %w", nodeInfo, err)
	}

	if bt := commonPrm.BearerToken(); bt != nil {
		opts.WithBearerToken(*bt)
	}
	opts.WithXHeaders(commonPrm.XHeaders()...)

	w, err := c.ObjectPutInit(ctx, *obj, user.NewAutoIDSigner(*key), opts)
	if err != nil {
		return fmt.Errorf("could not put object to %s: init object writing on client: %w", nodeInfo.AddressGroup(), err)
	}

	_, err = w.Write(obj.Payload())
	if err != nil {
		return fmt.Errorf("could not put object to %s: write object payload into stream: %w", nodeInfo.AddressGroup(), err)
	}

	err = w.Close()
	if err != nil {
		if ce, ok := c.(interface {
			ReportError(error)
		}); ok {
			ce.ReportError(err)
		}
		return fmt.Errorf("could not put object to %s: finish object stream: %w", nodeInfo.AddressGroup(), err)
	}

	return nil
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
	var nodeInfo clientcore.NodeInfo
	err := clientcore.NodeInfoFromRawNetmapElement(&nodeInfo, netmapCore.Node(p.node))
	if err != nil {
		return fmt.Errorf("parse client node info: %w", err)
	}

	err = putObjectToNode(ctx, nodeInfo, p.obj, s.keyStorage, s.clientConstructor, nil)
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

	c, err := s.clientConstructor.Get(ctx, nodeInfoForCons)
	if err != nil {
		return fmt.Errorf("init NeoFS API client of the remote node: %w", err)
	}

	_, err = c.ReplicateObject(ctx, id, src, (*neofsecdsa.Signer)(key), false)
	if err != nil {
		return fmt.Errorf("copy object using NeoFS API client of the remote node: %w", err)
	}

	return nil
}
