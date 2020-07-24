package storage

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/sha256"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/services/id"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/replication"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/verifier"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	objectValidator struct {
		as       AddressStore
		ls       localstore.Localstore
		executor transport.SelectiveContainerExecutor
		log      *zap.Logger

		saltSize   int
		maxRngSize uint64
		rangeCount int
		sltr       Salitor
		verifier   verifier.Verifier
	}

	// Salitor is a salting data function.
	Salitor func(data, salt []byte) []byte

	// ObjectValidatorParams groups th
	ObjectValidatorParams struct {
		AddressStore               AddressStore
		Localstore                 localstore.Localstore
		SelectiveContainerExecutor transport.SelectiveContainerExecutor
		Logger                     *zap.Logger

		Salitor             Salitor
		SaltSize            int
		MaxPayloadRangeSize uint64
		PayloadRangeCount   int

		Verifier verifier.Verifier
	}

	localHeadIntegrityVerifier struct {
	}

	payloadVerifier struct {
	}

	localIntegrityVerifier struct {
		headVerifier    verifier.Verifier
		payloadVerifier verifier.Verifier
	}
)

const (
	objectValidatorInstanceFailMsg = "could not create object validator"

	defaultSaltSize            = 64 // bytes
	defaultPayloadRangeCount   = 3
	defaultMaxPayloadRangeSize = 64
)

var (
	errEmptyLocalstore     = errors.New("empty local storage")
	errEmptyObjectVerifier = errors.New("empty object verifier")
)

var (
	errBrokenHeaderStructure = errors.New("broken header structure")

	errMissingPayloadChecksumHeader = errors.New("missing payload checksum header")
	errWrongPayloadChecksum         = errors.New("wrong payload checksum")
)

func (s *objectValidator) Verify(ctx context.Context, params *replication.ObjectVerificationParams) bool {
	selfAddr, err := s.as.SelfAddr()
	if err != nil {
		s.log.Debug("receive self address failure", zap.Error(err))
		return false
	}

	if params.Node == nil || params.Node.Equal(selfAddr) {
		return s.verifyLocal(ctx, params.Address)
	}

	return s.verifyRemote(ctx, params)
}

func (s *objectValidator) verifyLocal(ctx context.Context, addr refs.Address) bool {
	var (
		err error
		obj *object.Object
	)

	if obj, err = s.ls.Get(addr); err != nil {
		s.log.Debug("get local meta information failure", zap.Error(err))
		return false
	} else if err = s.verifier.Verify(ctx, obj); err != nil {
		s.log.Debug("integrity check failure", zap.Error(err))
	}

	return err == nil
}

func (s *objectValidator) verifyRemote(ctx context.Context, params *replication.ObjectVerificationParams) bool {
	var (
		receivedObj *object.Object
		valid       bool
	)

	defer func() {
		if params.Handler != nil && receivedObj != nil {
			params.Handler(valid, receivedObj)
		}
	}()

	p := &transport.HeadParams{
		GetParams: transport.GetParams{
			SelectiveParams: transport.SelectiveParams{
				CID:    params.CID,
				Nodes:  []multiaddr.Multiaddr{params.Node},
				TTL:    service.NonForwardingTTL,
				IDList: []object.ID{params.ObjectID},
				Raw:    true,
			},
			Handler: func(_ multiaddr.Multiaddr, obj *object.Object) {
				receivedObj = obj
				valid = s.verifier.Verify(ctx, obj) == nil
			},
		},
		FullHeaders: true,
	}

	if err := s.executor.Head(ctx, p); err != nil || !valid {
		return false
	} else if receivedObj.SystemHeader.PayloadLength <= 0 || receivedObj.IsLinking() {
		return true
	}

	if !params.LocalInvalid {
		has, err := s.ls.Has(params.Address)
		if err == nil && has {
			obj, err := s.ls.Get(params.Address)
			if err == nil {
				return s.verifyThroughHashes(ctx, obj, params.Node)
			}
		}
	}

	valid = false
	_ = s.executor.Get(ctx, &p.GetParams)

	return valid
}

func (s *objectValidator) verifyThroughHashes(ctx context.Context, obj *object.Object, node multiaddr.Multiaddr) (valid bool) {
	var (
		salt = generateSalt(s.saltSize)
		rngs = generateRanges(obj.SystemHeader.PayloadLength, s.maxRngSize, s.rangeCount)
	)

	_ = s.executor.RangeHash(ctx, &transport.RangeHashParams{
		SelectiveParams: transport.SelectiveParams{
			CID:    obj.SystemHeader.CID,
			Nodes:  []multiaddr.Multiaddr{node},
			TTL:    service.NonForwardingTTL,
			IDList: []object.ID{obj.SystemHeader.ID},
		},
		Ranges: rngs,
		Salt:   salt,
		Handler: func(node multiaddr.Multiaddr, hashes []hash.Hash) {
			valid = compareHashes(s.sltr, obj.Payload, salt, rngs, hashes)
		},
	})

	return
}

func compareHashes(sltr Salitor, payload, salt []byte, rngs []object.Range, hashes []hash.Hash) bool {
	if len(rngs) != len(hashes) {
		return false
	}

	for i := range rngs {
		saltPayloadPart := sltr(payload[rngs[i].Offset:rngs[i].Offset+rngs[i].Length], salt)
		if !hashes[i].Equal(hash.Sum(saltPayloadPart)) {
			return false
		}
	}

	return true
}

func generateRanges(payloadSize, maxRangeSize uint64, count int) []object.Range {
	res := make([]object.Range, count)

	l := min(payloadSize, maxRangeSize)

	for i := 0; i < count; i++ {
		res[i].Length = l
		res[i].Offset = rand.Uint64(rand.New(), int64(payloadSize-l))
	}

	return res
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}

func generateSalt(saltSize int) []byte {
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil
	}

	return salt
}

// NewObjectValidator constructs universal replication.ObjectVerifier.
func NewObjectValidator(p *ObjectValidatorParams) (replication.ObjectVerifier, error) {
	switch {
	case p.Logger == nil:
		return nil, errors.Wrap(logger.ErrNilLogger, objectValidatorInstanceFailMsg)
	case p.AddressStore == nil:
		return nil, errors.Wrap(errEmptyAddressStore, objectValidatorInstanceFailMsg)
	case p.Localstore == nil:
		return nil, errors.Wrap(errEmptyLocalstore, objectValidatorInstanceFailMsg)
	case p.Verifier == nil:
		return nil, errors.Wrap(errEmptyObjectVerifier, objectValidatorInstanceFailMsg)
	}

	if p.SaltSize <= 0 {
		p.SaltSize = defaultSaltSize
	}

	if p.PayloadRangeCount <= 0 {
		p.PayloadRangeCount = defaultPayloadRangeCount
	}

	if p.MaxPayloadRangeSize <= 0 {
		p.MaxPayloadRangeSize = defaultMaxPayloadRangeSize
	}

	return &objectValidator{
		as:         p.AddressStore,
		ls:         p.Localstore,
		executor:   p.SelectiveContainerExecutor,
		log:        p.Logger,
		saltSize:   p.SaltSize,
		maxRngSize: p.MaxPayloadRangeSize,
		rangeCount: p.PayloadRangeCount,
		sltr:       p.Salitor,
		verifier:   p.Verifier,
	}, nil
}

// NewLocalHeadIntegrityVerifier constructs local object head verifier and returns objutil.Verifier interface.
func NewLocalHeadIntegrityVerifier() (verifier.Verifier, error) {
	return new(localHeadIntegrityVerifier), nil
}

// NewLocalIntegrityVerifier constructs local object verifier and returns objutil.Verifier interface.
func NewLocalIntegrityVerifier() (verifier.Verifier, error) {
	return &localIntegrityVerifier{
		headVerifier:    new(localHeadIntegrityVerifier),
		payloadVerifier: new(payloadVerifier),
	}, nil
}

// NewPayloadVerifier constructs object payload verifier and returns objutil.Verifier.
func NewPayloadVerifier() verifier.Verifier {
	return new(payloadVerifier)
}

type hdrOwnerKeyContainer struct {
	owner refs.OwnerID
	key   []byte
}

func (s hdrOwnerKeyContainer) GetOwnerID() refs.OwnerID {
	return s.owner
}

func (s hdrOwnerKeyContainer) GetOwnerKey() []byte {
	return s.key
}

func (s *localHeadIntegrityVerifier) Verify(ctx context.Context, obj *object.Object) error {
	var (
		checkKey    *ecdsa.PublicKey
		ownerKeyCnr id.OwnerKeyContainer
	)

	if _, h := obj.LastHeader(object.HeaderType(object.TokenHdr)); h != nil {
		token := h.GetValue().(*object.Header_Token).Token

		if err := service.VerifySignatureWithKey(
			crypto.UnmarshalPublicKey(token.GetOwnerKey()),
			service.NewVerifiedSessionToken(token),
		); err != nil {
			return err
		}

		ownerKeyCnr = token

		checkKey = crypto.UnmarshalPublicKey(token.GetSessionKey())
	} else if _, h := obj.LastHeader(object.HeaderType(object.PublicKeyHdr)); h != nil {
		pkHdr := h.GetValue().(*object.Header_PublicKey)
		if pkHdr != nil && pkHdr.PublicKey != nil {
			val := pkHdr.PublicKey.GetValue()

			ownerKeyCnr = &hdrOwnerKeyContainer{
				owner: obj.GetSystemHeader().OwnerID,
				key:   val,
			}

			checkKey = crypto.UnmarshalPublicKey(val)
		}
	}

	if ownerKeyCnr == nil {
		return id.ErrNilOwnerKeyContainer
	} else if err := id.VerifyKey(ownerKeyCnr); err != nil {
		return err
	}

	return verifyObjectIntegrity(obj, checkKey)
}

// verifyObjectIntegrity verifies integrity of object header.
// Returns error if object
//  - does not contains integrity header;
//  - integrity header is not a last header in object;
//  - integrity header signature is broken.
func verifyObjectIntegrity(obj *object.Object, key *ecdsa.PublicKey) error {
	n, h := obj.LastHeader(object.HeaderType(object.IntegrityHdr))

	if l := len(obj.Headers); l <= 0 || n != l-1 {
		return errBrokenHeaderStructure
	}

	integrityHdr := h.Value.(*object.Header_Integrity).Integrity
	if integrityHdr == nil {
		return errBrokenHeaderStructure
	}

	data, err := verifier.MarshalHeaders(obj, n)
	if err != nil {
		return err
	}

	hdrChecksum := sha256.Sum256(data)

	return crypto.Verify(key, hdrChecksum[:], integrityHdr.ChecksumSignature)
}

func (s *payloadVerifier) Verify(_ context.Context, obj *object.Object) error {
	if _, h := obj.LastHeader(object.HeaderType(object.PayloadChecksumHdr)); h == nil {
		return errMissingPayloadChecksumHeader
	} else if checksum := sha256.Sum256(obj.Payload); !bytes.Equal(
		checksum[:],
		h.Value.(*object.Header_PayloadChecksum).PayloadChecksum,
	) {
		return errWrongPayloadChecksum
	}

	return nil
}

func (s *localIntegrityVerifier) Verify(ctx context.Context, obj *object.Object) error {
	if err := s.headVerifier.Verify(ctx, obj); err != nil {
		return err
	}

	return s.payloadVerifier.Verify(ctx, obj)
}
