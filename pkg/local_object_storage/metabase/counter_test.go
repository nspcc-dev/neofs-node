package meta_test

import (
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

const objCount = 10

func TestCounters(t *testing.T) {
	db := newDB(t)

	var c meta.ObjectCounters
	var err error

	t.Run("defaults", func(t *testing.T) {
		c, err = db.ObjectCounters()
		require.NoError(t, err)
		require.Zero(t, c.Phy)
		require.Zero(t, c.Root)
		require.Zero(t, c.TS)
		require.Zero(t, c.Lock)
		require.Zero(t, c.Link)
		require.Zero(t, c.GC)
	})

	t.Run("put", func(t *testing.T) {
		for _, typ := range []object.Type{object.TypeRegular, object.TypeTombstone, object.TypeLock, object.TypeLink} {
			const payloadSize = 1024

			t.Run(typ.String(), func(t *testing.T) {
				oo := make([]*object.Object, 0, objCount)
				for range objCount {
					oo = append(oo, generateTypedObject(t, typ, payloadSize))
				}

				for i := range objCount {
					err = db.Put(oo[i])
					require.NoError(t, err)

					c, err = db.ObjectCounters()
					require.NoError(t, err)

					require.EqualValues(t, i+1, c.Phy)
					require.EqualValues(t, payloadSize*(i+1), c.Payload)

					switch typ {
					case object.TypeRegular:
						require.EqualValues(t, i+1, c.Root)

						require.Zero(t, c.TS)
						require.Zero(t, c.Lock)
						require.Zero(t, c.Link)
						require.Zero(t, c.GC)
					case object.TypeTombstone:
						require.EqualValues(t, i+1, c.TS)

						require.Zero(t, c.Root)
						require.Zero(t, c.Lock)
						require.Zero(t, c.Link)
						require.Zero(t, c.GC)
					case object.TypeLock:
						require.EqualValues(t, i+1, c.Lock)

						require.Zero(t, c.Root)
						require.Zero(t, c.TS)
						require.Zero(t, c.Link)
						require.Zero(t, c.GC)
					case object.TypeLink:
						require.EqualValues(t, i+1, c.Link)

						require.Zero(t, c.Root)
						require.Zero(t, c.Lock)
						require.Zero(t, c.TS)
						require.Zero(t, c.GC)
					default:
					}
				}

				require.NoError(t, db.Reset())
			})
		}
	})

	t.Run("delete", func(t *testing.T) {
		const payloadSize = 1024

		for _, typ := range []object.Type{object.TypeRegular, object.TypeTombstone, object.TypeLock, object.TypeLink} {
			t.Run(typ.String(), func(t *testing.T) {
				oo := putTypedObjs(t, db, typ, objCount, false, payloadSize)

				for i := objCount - 1; i >= 0; i-- {
					_, diff, err := db.Delete(oo[i].GetContainerID(), []oid.ID{oo[i].GetID()})
					require.NoError(t, err)
					require.Equal(t, -1, diff.Phy)

					c, err = db.ObjectCounters()
					require.NoError(t, err)

					require.EqualValues(t, i, c.Phy)
					require.EqualValues(t, i*(payloadSize), c.Payload)
					require.Zero(t, c.GC)
					switch typ {
					case object.TypeRegular:
						require.EqualValues(t, i, c.Root)

						require.Zero(t, c.TS)
						require.Zero(t, c.Lock)
						require.Zero(t, c.Link)
					case object.TypeTombstone:
						require.EqualValues(t, i, c.TS)

						require.Zero(t, c.Lock)
						require.Zero(t, c.Link)
					case object.TypeLock:
						require.EqualValues(t, i, c.Lock)

						require.Zero(t, c.TS)
						require.Zero(t, c.Link)
					case object.TypeLink:
						require.EqualValues(t, i, c.Link)

						require.Zero(t, c.TS)
						require.Zero(t, c.Lock)
					default:
					}
				}

				require.NoError(t, db.Reset())
			})
		}
	})

	t.Run("inhume", func(t *testing.T) {
		t.Run("object", func(t *testing.T) {
			const payloadSize = 1024

			oo := putTypedObjs(t, db, object.TypeRegular, objCount, false, payloadSize)

			inhumedObjs := make([]oid.Address, objCount/2)

			for i, o := range oo {
				if i == len(inhumedObjs) {
					break
				}

				inhumedObjs[i] = o.Address()
			}

			for _, addr := range inhumedObjs {
				err := db.Put(createTSForObject(addr.Container(), addr.Object()))
				require.NoError(t, err)
			}

			c, err = db.ObjectCounters()
			require.NoError(t, err)

			require.EqualValues(t, objCount+len(inhumedObjs), c.Phy)
			require.EqualValues(t, payloadSize*(objCount-len(inhumedObjs)), c.Payload)
			require.EqualValues(t, objCount, c.Root)
			require.EqualValues(t, len(inhumedObjs), c.TS)
			require.EqualValues(t, len(inhumedObjs), c.GC)

			require.Zero(t, c.Link)
			require.Zero(t, c.Lock)

			require.NoError(t, db.Reset())
		})

		t.Run("container", func(t *testing.T) {
			var (
				oo  []*object.Object
				cnr = cidtest.ID()
			)
			for range objCount {
				o := generateObjectWithCID(t, cnr)
				require.NoError(t, db.Put(o))

				oo = append(oo, o)
			}

			// drop single obj to have non-zero TS counter
			err := db.Put(createTSForObject(oo[0].GetContainerID(), oo[0].GetID()))
			require.NoError(t, err)

			c, err = db.ObjectCounters()
			require.NoError(t, err)

			require.EqualValues(t, objCount+1, c.Phy)
			require.EqualValues(t, objCount, c.Root)
			require.EqualValues(t, 1, c.TS)
			require.EqualValues(t, 1, c.GC)
			require.Zero(t, c.Lock)
			require.Zero(t, c.Link)

			diff, err := db.InhumeContainer(oo[0].GetContainerID())
			require.NoError(t, err)

			require.EqualValues(t, -(objCount + 1), diff.Phy)
			require.EqualValues(t, -objCount, diff.Root)
			require.EqualValues(t, -1, diff.TS)
			require.EqualValues(t, (c.Phy)-1, diff.GC)
			require.EqualValues(t, 0, diff.Lock)
			require.EqualValues(t, 0, diff.Link)

			c, err = db.ObjectCounters()
			require.NoError(t, err)

			require.Zero(t, c.Phy)
			require.Zero(t, c.Root)
			require.Zero(t, c.TS)
			require.EqualValues(t, objCount+1, c.GC)
			require.Zero(t, c.Lock)
			require.Zero(t, c.Link)

			require.NoError(t, db.Reset())
		})
	})

	t.Run("split", func(t *testing.T) {
		const (
			childrenCount     = objCount
			phyObjectsInTotal = childrenCount + 1 // +link
		)

		t.Run("put", func(t *testing.T) {
			cnr := cidtest.ID()
			_, chain := generateChain(t, childrenCount, cnr)

			for _, o := range chain {
				require.NoError(t, db.Put(o))
			}

			c, err = db.ObjectCounters()
			require.NoError(t, err)

			require.EqualValues(t, phyObjectsInTotal, c.Phy)
			require.EqualValues(t, 1, c.Root)
			require.EqualValues(t, 1, c.Link)
			require.Zero(t, c.TS)
			require.Zero(t, c.Lock)
			require.Zero(t, c.GC)

			require.NoError(t, db.Reset())
		})

		t.Run("inhume", func(t *testing.T) {
			cnr := cidtest.ID()
			par, chain := generateChain(t, childrenCount, cnr)
			for _, o := range chain {
				require.NoError(t, db.Put(o))
			}

			c, err = db.ObjectCounters()
			require.NoError(t, err)

			require.EqualValues(t, phyObjectsInTotal, c.Phy)
			require.EqualValues(t, 1, c.Root)
			require.EqualValues(t, 1, c.Link)
			require.Zero(t, c.TS)
			require.Zero(t, c.Lock)
			require.Zero(t, c.GC)

			_, err := db.MarkGarbage(par.GetContainerID(), []oid.ID{par.GetID()})
			require.NoError(t, err)

			c, err = db.ObjectCounters()
			require.NoError(t, err)

			require.EqualValues(t, phyObjectsInTotal, c.Phy)
			require.EqualValues(t, phyObjectsInTotal+1, c.GC) // parent also gets GC mark
			require.EqualValues(t, 1, c.Root)
			require.EqualValues(t, 1, c.Link)
			require.Zero(t, c.TS)
			require.Zero(t, c.Lock)

			require.NoError(t, db.Reset())
		})

		t.Run("delete", func(t *testing.T) {
			cnr := cidtest.ID()
			_, chain := generateChain(t, childrenCount, cnr)
			for _, o := range chain {
				require.NoError(t, db.Put(o))
			}

			// no parent removals, `Delete` is not expected to face parents
			chainAddrs := make([]oid.ID, 0, len(chain))
			for _, ch := range chain {
				chainAddrs = append(chainAddrs, ch.GetID())
			}

			_, _, err = db.Delete(cnr, chainAddrs)
			require.NoError(t, err)

			c, err = db.ObjectCounters()
			require.NoError(t, err)

			require.Zero(t, c.Phy)
			require.Zero(t, c.Root)
			require.Zero(t, c.TS)
			require.Zero(t, c.Lock)
			require.Zero(t, c.Link)
			require.Zero(t, c.GC)
		})
	})
}

func putTypedObjs(t *testing.T, db *meta.DB, typ object.Type, count int, withParent bool, size uint64) []*object.Object {
	var err error
	parent := generateObject(t)

	oo := make([]*object.Object, 0, count)
	for i := range count {
		o := generateTypedObject(t, typ, size)
		if withParent {
			o.SetParent(parent)
		}

		oo = append(oo, o)

		err = db.Put(o)
		require.NoError(t, err)

		c, err := db.ObjectCounters()
		require.NoError(t, err)

		require.EqualValues(t, i+1, c.Phy)
		require.EqualValues(t, int(size)*(i+1), c.Payload)

		switch typ {
		case object.TypeRegular:
			require.EqualValues(t, i+1, c.Root)
		case object.TypeTombstone:
			require.EqualValues(t, i+1, c.TS)
		case object.TypeLock:
			require.EqualValues(t, i+1, c.Lock)
		case object.TypeLink:
			require.EqualValues(t, i+1, c.Link)
		default:
		}
	}

	return oo
}

func generateChain(t *testing.T, ln int, cnr cid.ID) (*object.Object, []*object.Object) {
	par := generateObjectWithCID(t, cnr)
	par.ResetRelations()
	var chain []*object.Object
	for range ln {
		chain = append(chain, generateObjectWithCID(t, cnr))
	}
	for i := range chain {
		if i == 0 {
			chain[i].SetParent(par)
			chain[i].SetParentID(oid.ID{})
			continue
		}

		chain[i].SetFirstID(chain[0].GetID())
		chain[i].SetPreviousID(chain[i-1].GetID())

		if i == len(chain)-1 {
			chain[i].SetParent(par)
			chain[i].SetParentID(par.GetID())
		}
	}

	link := generateObjectWithCID(t, cnr)
	link.SetType(object.TypeLink)
	link.SetParent(par)
	link.SetParentID(par.GetID())
	link.SetFirstID(chain[0].GetID())

	chain = append(chain, link)

	return par, chain
}
