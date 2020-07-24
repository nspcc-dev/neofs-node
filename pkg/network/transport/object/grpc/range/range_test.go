package _range

import (
	"context"
	"crypto/rand"
	"io"
	"sync"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	addressSet struct {
		*sync.RWMutex
		items []RangeDescriptor
		data  [][]byte
	}

	testReader struct {
		pr object.PositionReader
		ct ChopperTable
	}
)

func (r testReader) Read(ctx context.Context, rd RangeDescriptor, rc RCType) ([]byte, error) {
	chopper, err := r.ct.GetChopper(rd.Addr, rc)
	if err != nil {
		return nil, errors.Wrap(err, "testReader.Read failed on get range chopper")
	}

	rngs, err := chopper.Chop(ctx, rd.Size, rd.Offset, true)
	if err != nil {
		return nil, errors.Wrap(err, "testReader.Read failed on chopper.Chop")
	}

	var sz int64
	for i := range rngs {
		sz += rngs[i].Size
	}

	res := make([]byte, 0, sz)

	for i := range rngs {
		data, err := r.pr.PRead(ctx, rngs[i].Addr, object.Range{
			Offset: uint64(rngs[i].Offset),
			Length: uint64(rngs[i].Size),
		})
		if err != nil {
			return nil, errors.Wrapf(err, "testReader.Read failed on PRead of range #%d", i)
		}

		res = append(res, data...)
	}

	return res, nil
}

func (as addressSet) PRead(ctx context.Context, addr refs.Address, rng object.Range) ([]byte, error) {
	as.RLock()
	defer as.RUnlock()

	for i := range as.items {
		if as.items[i].Addr.CID.Equal(addr.CID) && as.items[i].Addr.ObjectID.Equal(addr.ObjectID) {
			return as.data[i][rng.Offset : rng.Offset+rng.Length], nil
		}
	}

	return nil, errors.New("pread failed")
}

func (as addressSet) List(ctx context.Context, parent Address) ([]RangeDescriptor, error) {
	return as.items, nil
}

func (as addressSet) Base(ctx context.Context, addr Address) (RangeDescriptor, error) {
	return as.items[0], nil
}

func (as addressSet) Neighbor(ctx context.Context, addr Address, left bool) (RangeDescriptor, error) {
	as.Lock()
	defer as.Unlock()

	ind := -1
	for i := range as.items {
		if as.items[i].Addr.CID.Equal(addr.CID) && as.items[i].Addr.ObjectID.Equal(addr.ObjectID) {
			ind = i
			break
		}
	}

	if ind == -1 {
		return RangeDescriptor{}, errors.New("range not found")
	}

	if left {
		if ind > 0 {
			ind--
		} else {
			return RangeDescriptor{}, io.EOF
		}
	} else {
		if ind < len(as.items)-1 {
			ind++
		} else {
			return RangeDescriptor{}, io.EOF
		}
	}

	return as.items[ind], nil
}

func newTestNeighbor(rngs []RangeDescriptor, data [][]byte) *addressSet {
	return &addressSet{
		RWMutex: new(sync.RWMutex),
		items:   rngs,
		data:    data,
	}
}

func rangeSize(rngs []RangeDescriptor) (res int64) {
	for i := range rngs {
		res += rngs[i].Size
	}
	return
}

func TestScylla(t *testing.T) {
	var (
		cid              = [refs.CIDSize]byte{1}
		rngs             = make([]RangeDescriptor, 0, 10)
		pieceSize  int64 = 100
		pieceCount int64 = 99
		fullSize         = pieceCount * pieceSize
	)

	for i := int64(0); i < pieceCount; i++ {
		oid, err := refs.NewObjectID()
		require.NoError(t, err)

		rngs = append(rngs, RangeDescriptor{
			Size:   pieceSize,
			Offset: 0,
			Addr: Address{
				ObjectID: oid,
				CID:      cid,
			},
			LeftBound:  i == 0,
			RightBound: i == pieceCount-1,
		})
	}

	oid, err := refs.NewObjectID()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("Zero values in scylla notch/chop", func(t *testing.T) {
		scylla, err := NewScylla(&ChopperParams{
			RelativeReceiver: newTestNeighbor(rngs, nil),
			Addr: Address{
				ObjectID: oid,
				CID:      cid,
			},
		})
		require.NoError(t, err)

		res, err := scylla.Chop(ctx, 0, 0, true)
		require.NoError(t, err)
		require.Len(t, res, 0)
	})

	t.Run("Common scylla operations in both directions", func(t *testing.T) {
		var (
			off    = fullSize / 2
			length = fullSize / 4
		)

		scylla, err := NewScylla(&ChopperParams{
			RelativeReceiver: newTestNeighbor(rngs, nil),
			Addr: Address{
				ObjectID: oid,
				CID:      cid,
			},
		})
		require.NoError(t, err)

		choppedCount := int((length-1)/pieceSize + 1)

		if pieceCount > 1 && off%pieceSize > 0 {
			choppedCount++
		}

		res, err := scylla.Chop(ctx, fullSize, 0, true)
		require.NoError(t, err)
		require.Len(t, res, int(pieceCount))
		require.Equal(t, rangeSize(res), fullSize)
		require.Equal(t, res, rngs)

		res, err = scylla.Chop(ctx, length, off, true)
		require.NoError(t, err)
		require.Len(t, res, choppedCount)

		for i := int64(0); i < int64(choppedCount); i++ {
			require.Equal(t, res[i].Addr.ObjectID, rngs[pieceCount/2+i].Addr.ObjectID)
		}

		require.Equal(t, rangeSize(res), length)

		res, err = scylla.Chop(ctx, length, -length, false)
		require.NoError(t, err)
		require.Len(t, res, choppedCount)

		for i := int64(0); i < int64(choppedCount); i++ {
			require.Equal(t, res[i].Addr.ObjectID, rngs[pieceCount/4+i].Addr.ObjectID)
		}

		require.Equal(t, rangeSize(res), length)
	})

	t.Run("Border scylla Chop", func(t *testing.T) {
		var (
			err error
			res []RangeDescriptor
		)

		scylla, err := NewScylla(&ChopperParams{
			RelativeReceiver: newTestNeighbor(rngs, nil),
			Addr: Address{
				ObjectID: oid,
				CID:      cid,
			},
		})
		require.NoError(t, err)

		res, err = scylla.Chop(ctx, fullSize, 0, false)
		require.NoError(t, err)
		require.Equal(t, res, rngs)

		res, err = scylla.Chop(ctx, fullSize, -100, false)
		require.NoError(t, err)
		require.Equal(t, res, rngs)

		res, err = scylla.Chop(ctx, fullSize, 1, false)
		require.Error(t, err)

		res, err = scylla.Chop(ctx, fullSize, -fullSize, false)
		require.NoError(t, err)
		require.Equal(t, rangeSize(res), fullSize)
	})
}

func TestCharybdis(t *testing.T) {
	var (
		cid              = [refs.CIDSize]byte{1}
		rngs             = make([]RangeDescriptor, 0, 10)
		pieceSize  int64 = 100
		pieceCount int64 = 99
		fullSize         = pieceCount * pieceSize
		data             = make([]byte, fullSize)
		dataChunks       = make([][]byte, 0, pieceCount)
	)

	_, err := rand.Read(data)
	require.NoError(t, err)

	for i := int64(0); i < pieceCount; i++ {
		oid, err := refs.NewObjectID()
		require.NoError(t, err)

		dataChunks = append(dataChunks, data[i*pieceSize:(i+1)*pieceSize])

		rngs = append(rngs, RangeDescriptor{
			Size:   pieceSize,
			Offset: 0,
			Addr: Address{
				ObjectID: oid,
				CID:      cid,
			},
		})
	}

	oid, err := refs.NewObjectID()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("Zero values in scylla notch/chop", func(t *testing.T) {
		charybdis, err := NewCharybdis(&CharybdisParams{
			ChildLister: newTestNeighbor(rngs, nil),
			Addr: Address{
				ObjectID: oid,
				CID:      cid,
			},
		})
		require.NoError(t, err)

		res, err := charybdis.Chop(ctx, 0, 0, false)
		require.NoError(t, err)
		require.Len(t, res, 0)
	})

	t.Run("Common charybdis operations in both directions", func(t *testing.T) {
		var (
			off    = fullSize / 2
			length = fullSize / 4
		)

		charybdis, err := NewCharybdis(&CharybdisParams{
			ChildLister: newTestNeighbor(rngs, nil),
			Addr: Address{
				ObjectID: oid,
				CID:      cid,
			},
		})
		require.NoError(t, err)

		choppedCount := int((length-1)/pieceSize + 1)

		if pieceCount > 1 && off%pieceSize > 0 {
			choppedCount++
		}

		res, err := charybdis.Chop(ctx, fullSize, 0, false)
		require.NoError(t, err)
		require.Len(t, res, int(pieceCount))
		require.Equal(t, rangeSize(res), fullSize)
		require.Equal(t, res, rngs)

		res, err = charybdis.Chop(ctx, length, off, false)
		require.NoError(t, err)
		require.Len(t, res, choppedCount)

		for i := int64(0); i < int64(choppedCount); i++ {
			require.Equal(t, res[i].Addr.ObjectID, rngs[pieceCount/2+i].Addr.ObjectID)
		}

		require.Equal(t, rangeSize(res), length)

		res, err = charybdis.Chop(ctx, length, -length, false)
		require.NoError(t, err)
		require.Len(t, res, choppedCount)

		for i := int64(0); i < int64(choppedCount); i++ {
			require.Equal(t, res[i].Addr.ObjectID, rngs[pieceCount/4+i].Addr.ObjectID)
		}

		require.Equal(t, rangeSize(res), length)
	})

	t.Run("Border charybdis Chop", func(t *testing.T) {
		var (
			err error
			res []RangeDescriptor
		)

		charybdis, err := NewCharybdis(&CharybdisParams{
			ChildLister: newTestNeighbor(rngs, nil),
			Addr: Address{
				ObjectID: oid,
				CID:      cid,
			},
		})
		require.NoError(t, err)

		res, err = charybdis.Chop(ctx, fullSize, 0, false)
		require.NoError(t, err)
		require.Equal(t, res, rngs)

		res, err = charybdis.Chop(ctx, fullSize, -100, false)
		require.NoError(t, err)
		require.Equal(t, res, rngs)

		res, err = charybdis.Chop(ctx, fullSize, 1, false)
		require.Error(t, err)

		res, err = charybdis.Chop(ctx, fullSize, -fullSize, false)
		require.NoError(t, err)
		require.Equal(t, rangeSize(res), fullSize)
	})
}
