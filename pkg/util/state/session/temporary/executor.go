package temporary

import (
	"crypto/ecdsa"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/pkg/util/state/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// Store saves parameterized private key in-memory.
func (s *TokenStore) Store(sk ecdsa.PrivateKey, usr user.ID, id []byte, exp uint64) error {
	s.mtx.Lock()
	s.tokens[key{
		tokenID: base58.Encode(id),
		ownerID: base58.Encode(usr[:]),
	}] = session.NewPrivateToken(&sk, exp)
	s.mtx.Unlock()
	return nil
}

func (s *TokenStore) Close() error {
	return nil
}
