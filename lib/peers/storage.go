package peers

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"sync"

	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/nspcc-dev/hrw"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

type (
	// Peer is value, that stores in Store storage
	Peer interface {
		ID() ID
		Address() multiaddr.Multiaddr
		PublicKey() *ecdsa.PublicKey
		PrivateKey() (*ecdsa.PrivateKey, error)
		SetPrivateKey(*ecdsa.PrivateKey) error

		// TODO implement marshal/unmarshal binary.
		//      Not sure that this method need for now,
		//      that's why let's leave it on future
		// encoding.BinaryMarshaler
		// encoding.BinaryUnmarshaler
	}

	peer struct {
		id   ID
		pub  *ecdsa.PublicKey
		key  *ecdsa.PrivateKey
		addr multiaddr.Multiaddr
	}

	// ID is a type of peer identification
	ID string

	storage struct {
		log *zap.Logger

		mu    *sync.RWMutex
		items map[ID]Peer
	}

	// PeerFilter is a Peer filtering function.
	PeerFilter func(Peer) bool

	// Storage is storage interface for Store
	Storage interface {
		Get(ID) (Peer, error)
		Set(ID, Peer) error
		Has(ID) bool
		Rem(ID) error
		List(ID, int64, int) ([]ID, error)
		Filter(PeerFilter) []ID
		Update(*netmap.NetMap) error
	}
)

const defaultStoreCapacity = 100

var (
	errUnknownPeer  = errors.New("unknown peer")
	errBadPublicKey = errors.New("bad public key")
)

var errNilNetMap = errors.New("netmap is nil")

// Hash method used in HRW-library.
func (i ID) Hash() uint64 {
	return murmur3.Sum64(i.Bytes())
}

// NewLocalPeer creates new peer instance.
func NewLocalPeer(addr multiaddr.Multiaddr, key *ecdsa.PrivateKey) Peer {
	pub := &key.PublicKey

	return &peer{
		id:   IDFromPublicKey(pub),
		pub:  pub,
		key:  key,
		addr: addr,
	}
}

// NewPeer creates new peer instance.
func NewPeer(addr multiaddr.Multiaddr, pub *ecdsa.PublicKey, key *ecdsa.PrivateKey) Peer {
	return &peer{
		id:   IDFromPublicKey(pub),
		pub:  pub,
		key:  key,
		addr: addr,
	}
}

func (p *peer) SetPrivateKey(key *ecdsa.PrivateKey) error {
	if key == nil || key.Curve != elliptic.P256() {
		return crypto.ErrEmptyPrivateKey
	}

	p.key = key

	return nil
}

// ID of peer.
func (p peer) ID() ID {
	return p.id
}

// Address of peer.
func (p peer) Address() multiaddr.Multiaddr {
	return p.addr
}

// PublicKey returns copy of peer public key.
func (p peer) PublicKey() *ecdsa.PublicKey {
	return p.pub
}

func (p peer) PrivateKey() (*ecdsa.PrivateKey, error) {
	if p.key == nil {
		return nil, crypto.ErrEmptyPrivateKey
	}

	return p.key, nil
}

// String returns string representation of PeerID.
func (i ID) String() string {
	return string(i)
}

// -- -- //

// Bytes returns bytes representation of PeerID.
func (i ID) Bytes() []byte {
	return []byte(i)
}

// Equal checks that both id's are identical.
func (i ID) Equal(id ID) bool {
	return i == id
}

// IDFromPublicKey returns peer ID for host with given public key.
func IDFromPublicKey(pk *ecdsa.PublicKey) ID {
	if pk == nil {
		return ""
	}

	return IDFromBinary(crypto.MarshalPublicKey(pk))
}

// IDFromBinary returns peer ID for host with given slice of byte.
func IDFromBinary(b []byte) ID {
	bytes := sha256.Sum256(b)
	hash, _ := multihash.Encode(bytes[:], multihash.IDENTITY)
	ident := multihash.Multihash(hash)

	return ID(ident.B58String())
}

// NewSimpleStorage is implementation over map.
func NewSimpleStorage(capacity int, l *zap.Logger) Storage {
	if capacity <= 0 {
		capacity = defaultStoreCapacity
	}

	return &storage{
		log:   l,
		mu:    new(sync.RWMutex),
		items: make(map[ID]Peer, capacity),
	}
}

// List peers that which are distributed by hrw(seed).
func (s *storage) List(id ID, seed int64, count int) ([]ID, error) {
	s.mu.RLock()
	items := make([]ID, 0, len(s.items))

	for key := range s.items {
		// ignore ourselves
		if id.Equal(key) {
			continue
		}

		items = append(items, key)
	}
	s.mu.RUnlock()

	// distribute keys by hrw(seed)
	hrw.SortSliceByValue(items,
		uint64(seed))

	return items[:count], nil
}

// Get peer by ID.
func (s *storage) Get(id ID) (Peer, error) {
	s.mu.RLock()
	p, ok := s.items[id]
	s.mu.RUnlock()

	if ok {
		return p, nil
	}

	return nil, errors.Wrapf(errUnknownPeer, "peer(%s)", id)
}

// Set peer by id.
func (s *storage) Set(id ID, p Peer) error {
	s.mu.Lock()
	s.items[id] = p
	s.mu.Unlock()

	return nil
}

// Has checks peer exists by id.
func (s *storage) Has(id ID) bool {
	s.mu.RLock()
	_, ok := s.items[id]
	s.mu.RUnlock()

	return ok
}

// Rem peer by id.
func (s *storage) Rem(id ID) error {
	s.mu.Lock()
	delete(s.items, id)
	s.mu.Unlock()

	return nil
}

// Update storage by network map.
func (s *storage) Update(nm *netmap.NetMap) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	list := nm.ItemsCopy()
	if len(list) == 0 {
		return errNilNetMap
	}

	items := make(map[ID]Peer, len(s.items))

	for i := range list {
		addr, err := multiaddr.NewMultiaddr(list[i].Address)
		if err != nil {
			return errors.Wrapf(err, "address=`%s`", list[i].Address)
		}

		pk := crypto.UnmarshalPublicKey(list[i].PubKey)
		if pk == nil && list[i].PubKey != nil {
			return errors.Wrapf(errBadPublicKey, "pubkey=`%x`", list[i].PubKey)
		}

		id := IDFromPublicKey(pk)
		if pv, ok := s.items[id]; ok {
			if pv.Address() != nil && pv.Address().Equal(addr) {
				items[id] = pv
				continue
			}
		}

		items[id] = NewPeer(addr, pk, nil)
	}

	s.items = items

	return nil
}

func (s *storage) Filter(filter PeerFilter) (res []ID) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for id, peer := range s.items {
		if filter(peer) {
			res = append(res, id)
		}
	}

	return
}
