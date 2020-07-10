package implementations

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
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/objutil"
	"github.com/nspcc-dev/neofs-node/lib/rand"
	"github.com/nspcc-dev/neofs-node/lib/replication"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	objectValidator struct {
		as       AddressStore
		ls       localstore.Localstore
		executor SelectiveContainerExecutor
		log      *zap.Logger

		saltSize   int
		maxRngSize uint64
		rangeCount int
		sltr       Salitor
		verifier   objutil.Verifier
	}

	// Salitor is a salting data function.
	Salitor func(data, salt []byte) []byte

	// ObjectValidatorParams groups th
	ObjectValidatorParams struct {
		AddressStore               AddressStore
		Localstore                 localstore.Localstore
		SelectiveContainerExecutor SelectiveContainerExecutor
		Logger                     *zap.Logger

		Salitor             Salitor
		SaltSize            int
		MaxPayloadRangeSize uint64
		PayloadRangeCount   int

		Verifier objutil.Verifier
	}

	localHeadIntegrityVerifier struct {
		keyVerifier core.OwnerKeyVerifier
	}

	payloadVerifier struct {
	}

	localIntegrityVerifier struct {
		headVerifier    objutil.Verifier
		payloadVerifier objutil.Verifier
	}
)

const (
	objectValidatorInstanceFailMsg = "could not create object validator"
	errEmptyLocalstore             = internal.Error("empty local storage")
	errEmptyObjectVerifier         = internal.Error("empty object verifier")

	defaultSaltSize            = 64 // bytes
	defaultPayloadRangeCount   = 3
	defaultMaxPayloadRangeSize = 64
)

const (
	errBrokenHeaderStructure = internal.Error("broken header structure")

	errMissingPayloadChecksumHeader = internal.Error("missing payload checksum header")
	errWrongPayloadChecksum         = internal.Error("wrong payload checksum")
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

func (s *objectValidator) verifyLocal(ctx context.Context, addr Address) bool {
	var (
		err error
		obj *Object
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
		receivedObj *Object
		valid       bool
	)

	defer func() {
		if params.Handler != nil && receivedObj != nil {
			params.Handler(valid, receivedObj)
		}
	}()

	p := &HeadParams{
		GetParams: GetParams{
			SelectiveParams: SelectiveParams{
				CID:    params.CID,
				Nodes:  []multiaddr.Multiaddr{params.Node},
				TTL:    service.NonForwardingTTL,
				IDList: []ObjectID{params.ObjectID},
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

func (s *objectValidator) verifyThroughHashes(ctx context.Context, obj *Object, node multiaddr.Multiaddr) (valid bool) {
	var (
		salt = generateSalt(s.saltSize)
		rngs = generateRanges(obj.SystemHeader.PayloadLength, s.maxRngSize, s.rangeCount)
	)

	_ = s.executor.RangeHash(ctx, &RangeHashParams{
		SelectiveParams: SelectiveParams{
			CID:    obj.SystemHeader.CID,
			Nodes:  []multiaddr.Multiaddr{node},
			TTL:    service.NonForwardingTTL,
			IDList: []ObjectID{obj.SystemHeader.ID},
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
		return nil, errors.Wrap(errEmptyLogger, objectValidatorInstanceFailMsg)
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
func NewLocalHeadIntegrityVerifier(keyVerifier core.OwnerKeyVerifier) (objutil.Verifier, error) {
	if keyVerifier == nil {
		return nil, core.ErrNilOwnerKeyVerifier
	}

	return &localHeadIntegrityVerifier{
		keyVerifier: keyVerifier,
	}, nil
}

// NewLocalIntegrityVerifier constructs local object verifier and returns objutil.Verifier interface.
func NewLocalIntegrityVerifier(keyVerifier core.OwnerKeyVerifier) (objutil.Verifier, error) {
	if keyVerifier == nil {
		return nil, core.ErrNilOwnerKeyVerifier
	}

	return &localIntegrityVerifier{
		headVerifier: &localHeadIntegrityVerifier{
			keyVerifier: keyVerifier,
		},
		payloadVerifier: new(payloadVerifier),
	}, nil
}

// NewPayloadVerifier constructs object payload verifier and returns objutil.Verifier.
func NewPayloadVerifier() objutil.Verifier {
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

func (s *localHeadIntegrityVerifier) Verify(ctx context.Context, obj *Object) error {
	var (
		checkKey    *ecdsa.PublicKey
		ownerKeyCnr core.OwnerKeyContainer
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
		return core.ErrNilOwnerKeyContainer
	} else if err := s.keyVerifier.VerifyKey(ctx, ownerKeyCnr); err != nil {
		return err
	}

	return verifyObjectIntegrity(obj, checkKey)
}

// verifyObjectIntegrity verifies integrity of object header.
// Returns error if object
//  - does not contains integrity header;
//  - integrity header is not a last header in object;
//  - integrity header signature is broken.
func verifyObjectIntegrity(obj *Object, key *ecdsa.PublicKey) error {
	n, h := obj.LastHeader(object.HeaderType(object.IntegrityHdr))

	if l := len(obj.Headers); l <= 0 || n != l-1 {
		return errBrokenHeaderStructure
	}

	integrityHdr := h.Value.(*object.Header_Integrity).Integrity
	if integrityHdr == nil {
		return errBrokenHeaderStructure
	}

	data, err := objutil.MarshalHeaders(obj, n)
	if err != nil {
		return err
	}

	hdrChecksum := sha256.Sum256(data)

	return crypto.Verify(key, hdrChecksum[:], integrityHdr.ChecksumSignature)
}

func (s *payloadVerifier) Verify(_ context.Context, obj *Object) error {
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

func (s *localIntegrityVerifier) Verify(ctx context.Context, obj *Object) error {
	if err := s.headVerifier.Verify(ctx, obj); err != nil {
		return err
	}

	return s.payloadVerifier.Verify(ctx, obj)
}
