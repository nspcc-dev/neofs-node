package object

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testTraverseEntity struct {
		// Set of interfaces which testCommonEntity must implement, but some methods from those does not call.
		serviceRequest
		Placer

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var (
	_ Placer           = (*testTraverseEntity)(nil)
	_ placementBuilder = (*testTraverseEntity)(nil)
)

func (s *testTraverseEntity) GetNodes(ctx context.Context, a Address, p bool, e ...multiaddr.Multiaddr) ([]multiaddr.Multiaddr, error) {
	if s.f != nil {
		s.f(a, p, e)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]multiaddr.Multiaddr), nil
}

func (s *testTraverseEntity) buildPlacement(_ context.Context, addr Address, excl ...multiaddr.Multiaddr) ([]multiaddr.Multiaddr, error) {
	if s.f != nil {
		s.f(addr, excl)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.res.([]multiaddr.Multiaddr), nil
}

func Test_coreCnrAffChecker_buildPlacement(t *testing.T) {
	ctx := context.TODO()
	addr := testObjectAddress(t)
	nodes := testNodeList(t, 2)

	t.Run("correct placer params", func(t *testing.T) {
		s := &corePlacementUtil{
			prevNetMap: true,
			placementBuilder: &testTraverseEntity{
				f: func(items ...interface{}) {
					require.Equal(t, addr, items[0].(Address))
					require.True(t, items[1].(bool))
					require.Equal(t, nodes, items[2].([]multiaddr.Multiaddr))
				},
				err: internal.Error(""), // just to prevent panic
			},
			log: zap.L(),
		}

		s.buildPlacement(ctx, addr, nodes...)
	})

	t.Run("correct result", func(t *testing.T) {
		t.Run("placer error", func(t *testing.T) {
			s := &corePlacementUtil{
				placementBuilder: &testTraverseEntity{
					err: internal.Error(""), // force Placer to return some error
				},
				log: zap.L(),
			}

			res, err := s.buildPlacement(ctx, addr)
			require.Error(t, err)
			require.Empty(t, res)
		})

		t.Run("placer success", func(t *testing.T) {
			s := &corePlacementUtil{
				placementBuilder: &testTraverseEntity{
					res: nodes, // force Placer to return nodes
				},
				log: zap.L(),
			}

			res, err := s.buildPlacement(ctx, addr)
			require.NoError(t, err)
			require.Equal(t, nodes, res)
		})
	})
}

func Test_coreTraverser(t *testing.T) {
	ctx := context.TODO()

	t.Run("new", func(t *testing.T) {
		addr := testObjectAddress(t)
		pl := new(testTraverseEntity)

		v := newContainerTraverser(&traverseParams{
			tryPrevNM:            true,
			addr:                 addr,
			curPlacementBuilder:  pl,
			prevPlacementBuilder: pl,
			maxRecycleCount:      10,
		})

		res := v.(*coreTraverser)

		require.NotNil(t, res.RWMutex)
		require.Equal(t, addr, res.addr)
		require.True(t, res.tryPrevNM)
		require.False(t, res.usePrevNM)
		require.NotNil(t, res.mDone)
		require.Empty(t, res.mDone)
		require.Empty(t, res.failed)
		require.Equal(t, 10, res.maxRecycleCount)
		require.Equal(t, pl, res.curPlacementBuilder)
		require.Equal(t, pl, res.prevPlacementBuilder)
		require.Equal(t, 0, res.stopCount)
	})

	t.Run("close", func(t *testing.T) {
		v := newContainerTraverser(&traverseParams{
			curPlacementBuilder: &testTraverseEntity{
				res: make([]multiaddr.Multiaddr, 1),
			},
		})

		v.close()

		require.Empty(t, v.Next(ctx))
		require.True(t, v.(*coreTraverser).isClosed())
	})

	t.Run("done", func(t *testing.T) {
		nodes := testNodeList(t, 3)
		v := newContainerTraverser(&traverseParams{})

		v.add(nodes[0], true)
		require.True(t, v.done(nodes[0]))

		v.add(nodes[1], false)
		require.False(t, v.done(nodes[1]))

		require.False(t, v.done(nodes[2]))
	})

	t.Run("finished", func(t *testing.T) {

		t.Run("zero stop count", func(t *testing.T) {
			containerTraverser := &coreTraverser{
				RWMutex:        new(sync.RWMutex),
				traverseParams: traverseParams{stopCount: 0},
			}
			require.False(t, containerTraverser.finished())
		})

		t.Run("positive stop count", func(t *testing.T) {
			containerTraverser := &coreTraverser{
				RWMutex:        new(sync.RWMutex),
				mDone:          make(map[string]struct{}),
				traverseParams: traverseParams{stopCount: 3},
			}

			for i := 0; i < containerTraverser.stopCount-1; i++ {
				containerTraverser.mDone[strconv.Itoa(i)] = struct{}{}
			}

			require.False(t, containerTraverser.finished())

			containerTraverser.mDone["last node"] = struct{}{}

			require.True(t, containerTraverser.finished())
		})
	})

	t.Run("add result", func(t *testing.T) {
		mAddr := testNode(t, 0)

		containerTraverser := &coreTraverser{
			RWMutex: new(sync.RWMutex),
			mDone:   make(map[string]struct{}),
		}

		containerTraverser.add(mAddr, true)
		_, ok := containerTraverser.mDone[mAddr.String()]
		require.True(t, ok)

		containerTraverser.add(mAddr, false)
		require.Contains(t, containerTraverser.failed, mAddr)
	})

	t.Run("reset", func(t *testing.T) {
		initRecycleNum := 1

		s := &coreTraverser{
			failed:     testNodeList(t, 1),
			usePrevNM:  true,
			recycleNum: initRecycleNum,
		}

		s.reset()

		require.Empty(t, s.failed)
		require.False(t, s.usePrevNM)
		require.Equal(t, initRecycleNum+1, s.recycleNum)
	})

	t.Run("next", func(t *testing.T) {

		t.Run("exclude done nodes from result", func(t *testing.T) {
			nodes := testNodeList(t, 5)
			done := make([]multiaddr.Multiaddr, 2)
			copy(done, nodes)

			pl := &testTraverseEntity{res: nodes}
			tr := newContainerTraverser(&traverseParams{curPlacementBuilder: pl})

			for i := range done {
				tr.add(done[i], true)
			}

			res := tr.Next(ctx)
			for i := range done {
				require.NotContains(t, res, done[i])
			}

		})

		t.Run("stop count initialization", func(t *testing.T) {
			nodes := testNodeList(t, 5)

			pl := &testTraverseEntity{res: nodes}

			tr := newContainerTraverser(&traverseParams{curPlacementBuilder: pl})

			_ = tr.Next(ctx)
			require.Equal(t, len(nodes), tr.(*coreTraverser).stopCount)
		})

		t.Run("all nodes are done", func(t *testing.T) {
			nodes := testNodeList(t, 5)
			pl := &testTraverseEntity{res: nodes}
			tr := newContainerTraverser(&traverseParams{curPlacementBuilder: pl})

			require.Equal(t, nodes, tr.Next(ctx))

			for i := range nodes {
				tr.add(nodes[i], true)
			}

			require.Empty(t, tr.Next(ctx))
		})

		t.Run("failed nodes accounting", func(t *testing.T) {
			nodes := testNodeList(t, 5)
			failed := nodes[:len(nodes)-2]
			_ = failed
			addr := testObjectAddress(t)

			pl := &testTraverseEntity{
				f: func(items ...interface{}) {
					t.Run("correct placer params", func(t *testing.T) {
						require.Equal(t, addr, items[0].(Address))
						require.Equal(t, failed, items[1].([]multiaddr.Multiaddr))
					})
				},
				res: nodes,
			}

			tr := newContainerTraverser(&traverseParams{
				addr:                addr,
				curPlacementBuilder: pl,
			})

			for i := range failed {
				tr.add(failed[i], false)
			}

			_ = tr.Next(ctx)
		})

		t.Run("placement build failure", func(t *testing.T) {

			t.Run("forbid previous network map", func(t *testing.T) {
				pl := &testTraverseEntity{res: make([]multiaddr.Multiaddr, 0)}

				tr := newContainerTraverser(&traverseParams{curPlacementBuilder: pl})

				require.Empty(t, tr.Next(ctx))
			})

			t.Run("allow previous network map", func(t *testing.T) {

				t.Run("failure", func(t *testing.T) {
					pl := &testTraverseEntity{
						res: make([]multiaddr.Multiaddr, 0),
					}

					tr := newContainerTraverser(&traverseParams{
						tryPrevNM:            true,
						curPlacementBuilder:  pl,
						prevPlacementBuilder: pl,
					})

					require.Empty(t, tr.Next(ctx))
				})

				t.Run("success", func(t *testing.T) {
					nodes := testNodeList(t, 5)

					tr := newContainerTraverser(&traverseParams{
						tryPrevNM: true,
						curPlacementBuilder: &testTraverseEntity{
							res: make([]multiaddr.Multiaddr, 0),
						},
						prevPlacementBuilder: &testTraverseEntity{
							res: nodes,
						},
					})

					require.Equal(t, nodes, tr.Next(ctx))
				})
			})

			t.Run("recycle", func(t *testing.T) {
				recycleCount := 5

				curNetMapCallCounter, prevNetMapCallCounter := 0, 0

				tr := newContainerTraverser(&traverseParams{
					tryPrevNM: true,
					curPlacementBuilder: &testTraverseEntity{
						f: func(items ...interface{}) {
							curNetMapCallCounter++
						},
						res: make([]multiaddr.Multiaddr, 0),
					},
					prevPlacementBuilder: &testTraverseEntity{
						f: func(items ...interface{}) {
							prevNetMapCallCounter++
						},
						res: make([]multiaddr.Multiaddr, 0),
					},
					maxRecycleCount: recycleCount,
				})

				_ = tr.Next(ctx)
				require.Equal(t, recycleCount+1, prevNetMapCallCounter)
				require.Equal(t, recycleCount+1, curNetMapCallCounter)
			})
		})
	})
}

func testNodeList(t *testing.T, count int) (res []multiaddr.Multiaddr) {
	for i := 0; i < count; i++ {
		res = append(res, testNode(t, i))
	}
	return
}
