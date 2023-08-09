package loadstorage

import (
	"sort"
	"sync"

	loadcontroller "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/controller"
	"github.com/nspcc-dev/neofs-sdk-go/container"
)

type usedSpaceEstimations struct {
	announcement container.SizeEstimation

	sizes []uint64
}

type storageKey struct {
	epoch uint64

	cid string
}

// Storage represents in-memory storage of
// container.SizeEstimation values.
//
// The write operation has the usual behavior - to save
// the next number of used container space for a specific epoch.
// All values related to one key (epoch, container ID) are stored
// as a list.
//
// Storage also provides an iterator interface, into the handler
// of which the final score is passed, built on all values saved
// at the time of the call. Currently the only possible estimation
// formula is used - the average between 10th and 90th percentile.
//
// For correct operation, Storage must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// Storage is immediately ready to work through API.
type Storage struct {
	mtx sync.RWMutex

	estLifeCycle uint64
	mItems       map[storageKey]*usedSpaceEstimations
}

// New creates a new instance of the Storage.
//
// The created Storage does not require additional
// initialization and is completely ready for work.
//
// estimationsLifeCycle is a longevity (in epochs) of estimations
// that are kept in the [Storage] instance. Note, current epoch
// is controlled with [Storage.EpochEvent].
func New(estimationsLifeCycle uint64) *Storage {
	return &Storage{
		mItems:       make(map[storageKey]*usedSpaceEstimations),
		estLifeCycle: estimationsLifeCycle,
	}
}

// Put appends the next value of the occupied container space for the epoch
// to the list of already saved values.
//
// Always returns nil error.
func (s *Storage) Put(a container.SizeEstimation) error {
	s.mtx.Lock()

	{
		key := storageKey{
			epoch: a.Epoch(),
			cid:   a.Container().EncodeToString(),
		}

		estimations, ok := s.mItems[key]
		if !ok {
			estimations = &usedSpaceEstimations{
				announcement: a,
				sizes:        make([]uint64, 0, 1),
			}

			s.mItems[key] = estimations
		}

		estimations.sizes = append(estimations.sizes, a.Value())
	}

	s.mtx.Unlock()

	return nil
}

func (s *Storage) Close() error {
	return nil
}

// Iterate goes through all the lists with the key (container ID, epoch),
// calculates the final grade for all values, and passes it to the handler.
//
// Final grade is the average between 10th and 90th percentiles.
func (s *Storage) Iterate(f loadcontroller.UsedSpaceFilter, h loadcontroller.UsedSpaceHandler) (err error) {
	s.mtx.RLock()

	{
		for _, v := range s.mItems {
			if f(v.announcement) {
				// calculate estimation based on 90th percentile
				v.announcement.SetValue(finalEstimation(v.sizes))

				if err = h(v.announcement); err != nil {
					break
				}
			}
		}
	}

	s.mtx.RUnlock()

	return
}

// EpochEvent notifies [Storage] about epoch counter updating.
// Used to remove unused estimations. See [Prm.EstimationsLifeCycle].
// Blocking operation.
func (s *Storage) EpochEvent(e uint64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for k := range s.mItems {
		if k.epoch+s.estLifeCycle < e {
			delete(s.mItems, k)
		}
	}
}

func finalEstimation(vals []uint64) uint64 {
	sort.Slice(vals, func(i, j int) bool {
		return vals[i] < vals[j]
	})

	const (
		lowerRank = 10
		upperRank = 90
	)

	if len(vals) >= lowerRank {
		lowerInd := percentile(lowerRank, vals)
		upperInd := percentile(upperRank, vals)

		vals = vals[lowerInd:upperInd]
	}

	sum := uint64(0)

	for i := range vals {
		sum += vals[i]
	}

	return sum / uint64(len(vals))
}

func percentile(rank int, vals []uint64) int {
	p := len(vals) * rank / 100
	return p
}
