package putsvc

import (
	"context"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
)

// RemoteSender represents utility for
// sending an object to a remote host.
type RemoteSender struct {
	keyStorage *util.KeyStorage

	clientConstructor ClientConstructor
}

func putObjectToNode(ctx context.Context, nodeInfo netmap.NodeInfo, obj *object.Object,
	keyStorage *util.KeyStorage, clientConstructor ClientConstructor, commonPrm *util.CommonPrm) error {
	var opts client.PrmObjectPutInit
	opts.MarkLocal()

	key, err := keyStorage.GetKey(nil)
	if err != nil {
		return fmt.Errorf("could not receive local node's private key: %w", err)
	}

	if tokV2 := commonPrm.SessionTokenV2(); tokV2 != nil {
		// For V2 tokens, the key is stored as the subjects
		if keyForSession, err := keyStorage.GetKeyBySubjects(tokV2.Subjects()); err == nil {
			key = keyForSession
		}
		opts.WithinSessionV2(*tokV2)
	} else if tok := commonPrm.SessionToken(); tok != nil {
		authUser, err := tok.AuthUser()
		if err != nil {
			return fmt.Errorf("could not get session auth user: %w", err)
		}
		key, err = keyStorage.GetKey(&authUser)
		if err != nil {
			return fmt.Errorf("could not receive private key: %w", err)
		}
		opts.WithinSession(*tok)
	}

	c, err := clientConstructor.Get(ctx, nodeInfo)
	if err != nil {
		return fmt.Errorf("could not create SDK client %s: %w", addressLogString(nodeInfo), err)
	}

	if bt := commonPrm.BearerToken(); bt != nil {
		opts.WithBearerToken(*bt)
	}
	opts.WithXHeaders(commonPrm.XHeaders()...)

	w, err := c.ObjectPutInit(ctx, *obj, user.NewAutoIDSigner(*key), opts)
	if err != nil {
		return fmt.Errorf("could not put object to %s: init object writing on client: %w", addressLogString(nodeInfo), err)
	}

	_, err = w.Write(obj.Payload())
	if err != nil {
		return fmt.Errorf("could not put object to %s: write object payload into stream: %w", addressLogString(nodeInfo), err)
	}

	err = w.Close()
	if err != nil {
		if ce, ok := c.(interface {
			ReportError(error)
		}); ok {
			ce.ReportError(err)
		}
		return fmt.Errorf("could not put object to %s: finish object stream: %w", addressLogString(nodeInfo), err)
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

// ReplicateObjectToNode copies binary-encoded NeoFS object from the given
// [io.ReadSeeker] into local storage of the node described by specified
// [netmap.NodeInfo].
func (s *RemoteSender) ReplicateObjectToNode(ctx context.Context, id oid.ID, src io.ReadSeeker, nodeInfo netmap.NodeInfo) error {
	key, err := s.keyStorage.GetKey(nil)
	if err != nil {
		return fmt.Errorf("fetch local node's private key: %w", err)
	}

	c, err := s.clientConstructor.Get(ctx, nodeInfo)
	if err != nil {
		return fmt.Errorf("init NeoFS API client of the remote node: %w", err)
	}

	_, err = c.ReplicateObject(ctx, id, src, (*neofsecdsa.Signer)(key), false)
	if err != nil {
		return fmt.Errorf("copy object using NeoFS API client of the remote node: %w", err)
	}

	return nil
}

func sendReplicationRequestToNode(ctx context.Context, clientConstructor ClientConstructor, req []byte, node netmap.NodeInfo) ([]byte, error) {
	conn, err := clientConstructor.Get(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("connect to remote node: %w", err)
	}

	var res []byte
	return res, conn.ForAnyGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		// this will be changed during NeoFS API Go deprecation. Code most likely be
		// placed in SDK
		var resp protoobject.ReplicateResponse
		err := conn.Invoke(ctx, protoobject.ObjectService_Replicate_FullMethodName, req, &resp, binaryMessageOnly)
		if err != nil {
			return fmt.Errorf("API transport (op=%s): %w", protoobject.ObjectService_Replicate_FullMethodName, err)
		}
		res, err = replicationResultFromResponse(&resp)
		return err
	})
}

// [encoding.Codec] making Marshal to accept and forward []byte messages only.
var binaryMessageOnly = grpc.ForceCodecV2(protoCodecBinaryRequestOnly{})

type protoCodecBinaryRequestOnly struct{}

func (protoCodecBinaryRequestOnly) Name() string {
	// may be any non-empty, conflicts are unlikely to arise
	return "neofs_binary_sender"
}

func (protoCodecBinaryRequestOnly) Marshal(msg any) (mem.BufferSlice, error) {
	bMsg, ok := msg.([]byte)
	if ok {
		return mem.BufferSlice{mem.SliceBuffer(bMsg)}, nil
	}

	return nil, fmt.Errorf("message is not of type %T", bMsg)
}

func (protoCodecBinaryRequestOnly) Unmarshal(data mem.BufferSlice, msg any) error {
	return encoding.GetCodecV2(proto.Name).Unmarshal(data, msg)
}

func replicationResultFromResponse(m *protoobject.ReplicateResponse) ([]byte, error) {
	err := apistatus.ToError(m.GetStatus())
	if err != nil {
		return nil, err
	}

	return m.GetObjectSignature(), nil
}
