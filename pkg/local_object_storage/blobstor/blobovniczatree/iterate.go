package blobovniczatree

import (
	"fmt"
	"path/filepath"

	"github.com/nspcc-dev/hrw"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Iterate iterates over all objects in b.
func (b *Blobovniczas) Iterate(prm common.IteratePrm) (common.IterateRes, error) {
	return common.IterateRes{}, b.iterateBlobovniczas(prm.IgnoreErrors, func(p string, blz *blobovnicza.Blobovnicza) error {
		var subPrm blobovnicza.IteratePrm
		subPrm.SetHandler(func(elem blobovnicza.IterationElement) error {
			data, err := b.compression.Decompress(elem.ObjectData())
			if err != nil {
				if prm.IgnoreErrors {
					if prm.ErrorHandler != nil {
						return prm.ErrorHandler(elem.Address(), err)
					}
					return nil
				}
				return fmt.Errorf("could not decompress object data: %w", err)
			}

			if prm.Handler != nil {
				return prm.Handler(common.IterationElement{
					Address:    elem.Address(),
					ObjectData: data,
					StorageID:  []byte(p),
				})
			}
			return prm.LazyHandler(elem.Address(), func() ([]byte, error) {
				return data, err
			})
		})
		subPrm.DecodeAddresses()

		_, err := blz.Iterate(subPrm)
		return err
	})
}

// iterator over all Blobovniczas in unsorted order. Break on f's error return.
func (b *Blobovniczas) iterateBlobovniczas(ignoreErrors bool, f func(string, *blobovnicza.Blobovnicza) error) error {
	return b.iterateLeaves(func(p string) (bool, error) {
		blz, err := b.openBlobovnicza(p)
		if err != nil {
			if ignoreErrors {
				return false, nil
			}
			return false, fmt.Errorf("could not open blobovnicza %s: %w", p, err)
		}

		err = f(p, blz)

		return err != nil, err
	})
}

// iterator over the paths of Blobovniczas sorted by weight.
func (b *Blobovniczas) iterateSortedLeaves(addr *oid.Address, f func(string) (bool, error)) error {
	_, err := b.iterateSorted(
		addr,
		make([]string, 0, b.blzShallowDepth),
		b.blzShallowDepth,
		func(p []string) (bool, error) { return f(filepath.Join(p...)) },
	)

	return err
}

// iterator over directories with Blobovniczas sorted by weight.
func (b *Blobovniczas) iterateDeepest(addr oid.Address, f func(string) (bool, error)) error {
	depth := b.blzShallowDepth
	if depth > 0 {
		depth--
	}

	_, err := b.iterateSorted(
		&addr,
		make([]string, 0, depth),
		depth,
		func(p []string) (bool, error) { return f(filepath.Join(p...)) },
	)

	return err
}

// iterator over particular level of directories.
func (b *Blobovniczas) iterateSorted(addr *oid.Address, curPath []string, execDepth uint64, f func([]string) (bool, error)) (bool, error) {
	indices := indexSlice(b.blzShallowWidth)

	hrw.SortSliceByValue(indices, addressHash(addr, filepath.Join(curPath...)))

	exec := uint64(len(curPath)) == execDepth

	for i := range indices {
		if i == 0 {
			curPath = append(curPath, u64ToHexString(indices[i]))
		} else {
			curPath[len(curPath)-1] = u64ToHexString(indices[i])
		}

		if exec {
			if stop, err := f(curPath); err != nil {
				return false, err
			} else if stop {
				return true, nil
			}
		} else if stop, err := b.iterateSorted(addr, curPath, execDepth, f); err != nil {
			return false, err
		} else if stop {
			return true, nil
		}
	}

	return false, nil
}

// iterator over the paths of Blobovniczas in random order.
func (b *Blobovniczas) iterateLeaves(f func(string) (bool, error)) error {
	return b.iterateSortedLeaves(nil, f)
}

// makes slice of uint64 values from 0 to number-1.
func indexSlice(number uint64) []uint64 {
	s := make([]uint64, number)

	for i := range s {
		s[i] = uint64(i)
	}

	return s
}
