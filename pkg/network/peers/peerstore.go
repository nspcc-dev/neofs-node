package peers

import (
	"crypto/ecdsa"
	"crypto/elliptic"

	"github.com/multiformats/go-multiaddr"
	crypto "github.com/nspcc-dev/neofs-crypto"
	netmap "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// Store is an interface to storage of all p2p connections
	Store interface {
		SelfIDReceiver
		PublicKeyStore
		AddressIDReceiver
		AddPeer(multiaddr.Multiaddr, *ecdsa.PublicKey, *ecdsa.PrivateKey) (ID, error)
		DeletePeer(ID)
		PeerNetAddressStore
		GetPrivateKey(ID) (*ecdsa.PrivateKey, error)
		Update(*netmap.NetMap) error
		Sign([]byte) ([]byte, error)
		Verify(id ID, data, sign []byte) error
		Check(min int) error
	}

	// PublicKeyStore is an interface of the storage of peer's public keys.
	PublicKeyStore interface {
		GetPublicKey(ID) (*ecdsa.PublicKey, error)
	}

	// SelfIDReceiver is an interface of local peer ID value with read access.
	SelfIDReceiver interface {
		SelfID() ID
	}

	// AddressIDReceiver is an interface of Multiaddr to ID converter.
	AddressIDReceiver interface {
		AddressID(multiaddr.Multiaddr) (ID, error)
	}

	// PeerNetAddressStore is an interface of ID to Multiaddr converter.
	PeerNetAddressStore interface {
		GetAddr(ID) (multiaddr.Multiaddr, error)
	}

	// StoreParams for creating new Store.
	StoreParams struct {
		Addr     multiaddr.Multiaddr
		Key      *ecdsa.PrivateKey
		Storage  Storage
		StoreCap int
		Logger   *zap.Logger
	}

	store struct {
		self    ID
		addr    multiaddr.Multiaddr
		storage Storage
		log     *zap.Logger
		key     *ecdsa.PrivateKey
	}
)

const defaultMinimalSignaturesCount = 3

var errPeerNotFound = errors.New("peer not found")

func (p *store) AddressID(addr multiaddr.Multiaddr) (ID, error) {
	if p.addr.Equal(addr) {
		return p.self, nil
	}

	res := p.storage.Filter(maddrFilter(addr))
	if len(res) == 0 {
		return "", errPeerNotFound
	}

	return res[0], nil
}

func maddrFilter(addr multiaddr.Multiaddr) PeerFilter {
	return func(p Peer) bool { return addr.Equal(p.Address()) }
}

// SelfID return ID of current Node.
func (p *store) SelfID() ID {
	return p.self
}

// AddPeer to store..
// Try to get PeerID from PublicKey, or return error
// Store Address and PublicKey for that PeerID.
func (p *store) AddPeer(addr multiaddr.Multiaddr, pub *ecdsa.PublicKey, key *ecdsa.PrivateKey) (ID, error) {
	item := NewPeer(addr, pub, key)
	if err := p.storage.Set(item.ID(), item); err != nil {
		return "", err
	}

	return item.ID(), nil
}

// DeletePeer from store.
func (p *store) DeletePeer(id ID) {
	if err := p.storage.Rem(id); err != nil {
		p.log.Error("could not delete peer",
			zap.Stringer("id", id),
			zap.Error(err))
	}
}

// Update update Store by new network map.
func (p *store) Update(nm *netmap.NetMap) error {
	if err := p.storage.Update(nm); err != nil {
		return err
	}

	// we must provide our PrivateKey, after updating
	if peer, err := p.storage.Get(p.self); err != nil {
		peer = NewPeer(p.addr, &p.key.PublicKey, p.key)
		return p.storage.Set(p.self, peer)
	} else if err := peer.SetPrivateKey(p.key); err != nil {
		return errors.Wrapf(err, "could not update private key (%s)", p.self.String())
	} else if err := p.storage.Set(p.self, peer); err != nil {
		return errors.Wrapf(err, "could not save peer(%s)", p.self.String())
	}

	return nil
}

// GetAddr by PeerID.
func (p *store) GetAddr(id ID) (multiaddr.Multiaddr, error) {
	n, err := p.storage.Get(id)
	if err != nil {
		return nil, err
	}

	return n.Address(), nil
}

// GetPublicKey by PeerID.
func (p *store) GetPublicKey(id ID) (*ecdsa.PublicKey, error) {
	n, err := p.storage.Get(id)
	if err != nil {
		return nil, err
	}

	return n.PublicKey(), nil
}

// GetPrivateKey by PeerID.
func (p *store) GetPrivateKey(id ID) (*ecdsa.PrivateKey, error) {
	n, err := p.storage.Get(id)
	if err != nil {
		return nil, err
	}

	return n.PrivateKey()
}

// Sign signs a data using the private key. If the data is longer than
// the bit-length of the private key's curve order, the hash will be
// truncated to that length. It returns the signature as slice bytes.
// The security of the private key depends on the entropy of rand.
func (p *store) Sign(data []byte) ([]byte, error) {
	return crypto.Sign(p.key, data)
}

// Verify verifies the signature in r, s of hash using the public key, pub. Its
// return value records whether the signature is valid.
// If store doesn't contains public key for ID,
// returns error about that
// TODO we must provide same method, but for IR list, to check,
//      that we have valid signatures of needed IR members
func (p *store) Verify(id ID, data, sign []byte) error {
	if pub, err := p.GetPublicKey(id); err != nil {
		return errors.Wrap(err, "could not get PublicKey")
	} else if err := crypto.Verify(pub, data, sign); err != nil {
		return errors.Wrapf(err, "could not verify signature: sign(`%x`) & data(`%x`)", sign, data)
	}

	return nil
}

// Neighbours peers that which are distributed by hrw(id).
func (p *store) Neighbours(seed int64, count int) ([]ID, error) {
	return p.storage.List(p.self, seed, count)
}

// Check validate signatures count
// TODO replace with settings or something else.
//      We can fetch min-count from settings, or
//      use another method for validate this..
func (p *store) Check(min int) error {
	if min <= defaultMinimalSignaturesCount {
		return errors.Errorf("invalid count of valid signatures: minimum %d, actual %d",
			defaultMinimalSignaturesCount,
			min,
		)
	}

	return nil
}

// NewStore creates new store by params.
func NewStore(p StoreParams) (Store, error) {
	var storage Storage

	if p.Key == nil || p.Key.Curve != elliptic.P256() {
		return nil, crypto.ErrEmptyPrivateKey
	}

	if p.Addr == nil {
		return nil, errNilMultiaddr
	}

	if storage = p.Storage; storage == nil {
		storage = NewSimpleStorage(p.StoreCap, p.Logger)
	}

	id := IDFromPublicKey(&p.Key.PublicKey)
	peer := NewPeer(p.Addr, &p.Key.PublicKey, p.Key)

	if err := storage.Set(id, peer); err != nil {
		return nil, err
	}

	return &store{
		self:    id,
		storage: storage,
		key:     p.Key,
		addr:    p.Addr,
		log:     p.Logger,
	}, nil
}
