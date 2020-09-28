package rangehashsvc

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/pkg/errors"
)

type localHasher struct {
	storage *localstore.Storage
}

func (h *localHasher) hashRange(ctx context.Context, prm *Prm, handler func([][]byte)) error {
	obj, err := h.storage.Get(prm.addr)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get object from local storage", h)
	}

	payload := obj.GetPayload()
	hashes := make([][]byte, 0, len(prm.rngs))

	var hasher hash.Hash
	switch prm.typ {
	default:
		panic(fmt.Sprintf("unexpected checksum type %v", prm.typ))
	case pkg.ChecksumSHA256:
		hasher = sha256.New()
	case pkg.ChecksumTZ:
		hasher = tz.New()
	}

	for i := range prm.rngs {
		left := prm.rngs[i].GetOffset()
		right := left + prm.rngs[i].GetLength()

		if ln := uint64(len(payload)); ln < right {
			return errors.Errorf("(%T) object range is out-of-boundaries (size %d, range [%d:%d]", h, ln, left, right)
		}

		hasher.Reset()

		hasher.Write(util.SaltXOR(payload[left:right], prm.salt))

		hashes = append(hashes, hasher.Sum(nil))
	}

	handler(hashes)

	return nil
}
