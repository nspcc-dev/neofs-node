package rangehashsvc

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	rangesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/range"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/pkg/errors"
)

type Service struct {
	*cfg
}

type Option func(*cfg)

type cfg struct {
	keyStorage *objutil.KeyStorage

	localStore *localstore.Storage

	cnrSrc container.Source

	netMapSrc netmap.Source

	workerPool util.WorkerPool

	localAddrSrc network.LocalAddressSource

	headSvc *headsvc.Service

	rangeSvc *rangesvc.Service
}

func defaultCfg() *cfg {
	return &cfg{
		workerPool: new(util.SyncWorkerPool),
	}
}

func NewService(opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg: c,
	}
}

func (s *Service) GetRangeHash(ctx context.Context, prm *Prm) (*Response, error) {
	headResult, err := s.headSvc.Head(ctx, new(headsvc.Prm).
		WithAddress(prm.addr).
		WithCommonPrm(prm.common),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive Head result", s)
	}

	origin := headResult.Header()

	originSize := origin.GetPayloadSize()

	var minLeft, maxRight uint64
	for i := range prm.rngs {
		left := prm.rngs[i].GetOffset()
		right := left + prm.rngs[i].GetLength()

		if originSize < right {
			return nil, errors.Errorf("(%T) requested payload range is out-of-bounds", s)
		}

		if left < minLeft {
			minLeft = left
		}

		if right > maxRight {
			maxRight = right
		}
	}

	right := headResult.RightChild()
	if right == nil {
		right = origin
	}

	borderRng := new(object.Range)
	borderRng.SetOffset(minLeft)
	borderRng.SetLength(maxRight - minLeft)

	return s.getHashes(ctx, prm, objutil.NewRangeTraverser(originSize, right, borderRng))
}

func (s *Service) getHashes(ctx context.Context, prm *Prm, traverser *objutil.RangeTraverser) (*Response, error) {
	addr := object.NewAddress()
	addr.SetContainerID(prm.addr.GetContainerID())

	resp := &Response{
		hashes: make([][]byte, 0, len(prm.rngs)),
	}

	for _, rng := range prm.rngs {
		for {
			nextID, nextRng := traverser.Next()
			if nextRng != nil {
				break
			}

			addr.SetObjectID(nextID)

			head, err := s.headSvc.Head(ctx, new(headsvc.Prm).
				WithAddress(addr).
				WithCommonPrm(prm.common),
			)
			if err != nil {
				return nil, errors.Wrapf(err, "(%T) could not receive object header", s)
			}

			traverser.PushHeader(head.Header())
		}

		traverser.SetSeekRange(rng)

		var hasher hasher

		for {
			nextID, nextRng := traverser.Next()

			if hasher == nil {
				if nextRng.GetLength() == rng.GetLength() {
					hasher = new(singleHasher)
				} else {
					switch prm.typ {
					default:
						panic(fmt.Sprintf("unexpected checksum type %v", prm.typ))
					case pkg.ChecksumSHA256:
						hasher = &commonHasher{h: sha256.New()}
					case pkg.ChecksumTZ:
						hasher = &tzHasher{
							hashes: make([][]byte, 0, 10),
						}
					}
				}
			}

			if nextRng.GetLength() == 0 {
				break
			}

			addr.SetObjectID(nextID)

			if prm.typ == pkg.ChecksumSHA256 && nextRng.GetLength() != rng.GetLength() {
				// here we cannot receive SHA256 checksum through GetRangeHash service
				// since SHA256 is not homomorphic
				res, err := s.rangeSvc.GetRange(ctx, new(rangesvc.Prm).
					WithAddress(addr).
					WithRange(nextRng).
					WithCommonPrm(prm.common),
				)
				if err != nil {
					return nil, errors.Wrapf(err, "(%T) could not receive payload range for %v checksum", s, prm.typ)
				}

				for stream := res.Stream(); ; {
					resp, err := stream.Recv()
					if errors.Is(errors.Cause(err), io.EOF) {
						break
					}

					hasher.add(resp.PayloadChunk())
				}
			} else {
				resp, err := (&distributedHasher{
					cfg: s.cfg,
				}).head(ctx, new(Prm).
					WithAddress(addr).
					WithChecksumType(prm.typ).
					FromRanges(nextRng).
					WithCommonPrm(prm.common),
				)
				if err != nil {
					return nil, errors.Wrapf(err, "(%T) could not receive %v checksum", s, prm.typ)
				}

				hs := resp.Hashes()
				if ln := len(hs); ln != 1 {
					return nil, errors.Errorf("(%T) unexpected %v hashes amount %d", s, prm.typ, ln)
				}

				hasher.add(hs[0])
			}

			traverser.PushSuccessSize(nextRng.GetLength())
		}

		sum, err := hasher.sum()
		if err != nil {
			return nil, errors.Wrapf(err, "(%T) could not calculate %v checksum", s, prm.typ)
		}

		resp.hashes = append(resp.hashes, sum)
	}

	return resp, nil
}

func WithKeyStorage(v *objutil.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStorage = v
	}
}

func WithLocalStorage(v *localstore.Storage) Option {
	return func(c *cfg) {
		c.localStore = v
	}
}

func WithContainerSource(v container.Source) Option {
	return func(c *cfg) {
		c.cnrSrc = v
	}
}

func WithNetworkMapSource(v netmap.Source) Option {
	return func(c *cfg) {
		c.netMapSrc = v
	}
}

func WithWorkerPool(v util.WorkerPool) Option {
	return func(c *cfg) {
		c.workerPool = v
	}
}

func WithLocalAddressSource(v network.LocalAddressSource) Option {
	return func(c *cfg) {
		c.localAddrSrc = v
	}
}

func WithHeadService(v *headsvc.Service) Option {
	return func(c *cfg) {
		c.headSvc = v
	}
}

func WithRangeService(v *rangesvc.Service) Option {
	return func(c *cfg) {
		c.rangeSvc = v
	}
}
