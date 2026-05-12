package engine

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
)

const (
	benchmarkObjectsPerShard    = 1000
	benchmarkRawMatchesPerShard = 16
	benchmarkExistsMissCount    = 256

	// benchmarkDropCachesEnabled controls whether OS caches are dropped during benchmarks.
	// Set to true and run benchmarks with root privileges to enable cache dropping.
	benchmarkDropCachesEnabled = false
)

var benchmarkShardCounts = []int{1, 2, 4, 8, 16, 32}

type benchmarkEngineFixture struct {
	engine     *StorageEngine
	shards     []shardWrapper
	shardCount int
}

type searchBenchmarkScenario struct {
	container  cid.ID
	hitFS      []objectcore.SearchFilter
	hitCursor  *objectcore.SearchCursor
	hitAttrs   []string
	missFS     []objectcore.SearchFilter
	missCursor *objectcore.SearchCursor
	missAttrs  []string
}

type collectRawBenchmarkScenario struct {
	container cid.ID
	attr      string
	hitValue  []byte
	missValue []byte
}

type existsBenchmarkScenario struct {
	hitFirst oid.Address
	hitLast  oid.Address
	misses   []oid.Address
}

func BenchmarkSearch(b *testing.B) {
	forEachBenchmarkFixture(b, func(b *testing.B, _ int, fx *benchmarkEngineFixture) {
		sc := prepareSearchBenchmarkScenario(b, fx)
		b.Run("hit", func(b *testing.B) {
			benchmarkSearch(b, fx, sc.container, sc.hitFS, sc.hitAttrs, sc.hitCursor)
		})
		b.Run("miss", func(b *testing.B) {
			benchmarkSearch(b, fx, sc.container, sc.missFS, sc.missAttrs, sc.missCursor)
		})
	})
}

func BenchmarkListWithCursor(b *testing.B) {
	forEachBenchmarkFixture(b, func(b *testing.B, _ int, fx *benchmarkEngineFixture) {
		prepareListBenchmarkScenario(b, fx)
		for _, batchSize := range []uint32{1, 10, 100} {
			b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
				benchmarkListWithCursor(b, fx, batchSize)
			})
		}
	})
}

func BenchmarkCollectRawWithAttribute(b *testing.B) {
	forEachBenchmarkFixture(b, func(b *testing.B, shardCount int, fx *benchmarkEngineFixture) {
		sc := prepareCollectRawBenchmarkScenario(b, fx)
		b.Run("hit", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				dropCachesIfEnabled(b)
				ids, err := fx.engine.collectRawWithAttribute(sc.container, sc.attr, sc.hitValue)
				if err != nil {
					b.Fatal(err)
				}
				if len(ids) != shardCount*benchmarkRawMatchesPerShard {
					b.Fatalf("unexpected hit result len %d", len(ids))
				}
			}
		})
		b.Run("miss", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				dropCachesIfEnabled(b)
				ids, err := fx.engine.collectRawWithAttribute(sc.container, sc.attr, sc.missValue)
				if err != nil {
					b.Fatal(err)
				}
				if len(ids) != 0 {
					b.Fatalf("unexpected miss result len %d", len(ids))
				}
			}
		})
	})
}

func BenchmarkExistsPhysical(b *testing.B) {
	forEachBenchmarkFixture(b, func(b *testing.B, _ int, fx *benchmarkEngineFixture) {
		sc := prepareExistsBenchmarkScenario(b, fx)
		b.Run("hit-first", func(b *testing.B) {
			benchmarkExists(b, fx, []oid.Address{sc.hitFirst}, true)
		})
		b.Run("hit-last", func(b *testing.B) {
			benchmarkExists(b, fx, []oid.Address{sc.hitLast}, true)
		})
		b.Run("miss-rotating", func(b *testing.B) {
			benchmarkExists(b, fx, sc.misses, false)
		})
	})
}

func benchmarkSearch(b *testing.B, fx *benchmarkEngineFixture, container cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor) {
	b.ReportAllocs()
	for b.Loop() {
		dropCachesIfEnabled(b)
		_, nextCursor, err := fx.engine.Search(container, fs, attrs, cursor, 1000)
		if err != nil {
			b.Fatal(err)
		}
		if len(nextCursor) != 0 {
			b.Fatalf("unexpected cursor len %d", len(nextCursor))
		}
	}
}

func benchmarkListWithCursor(b *testing.B, fx *benchmarkEngineFixture, batchSize uint32) {
	b.ReportAllocs()
	for b.Loop() {
		dropCachesIfEnabled(b)
		var cursor *Cursor
		var err error
		for {
			_, cursor, err = fx.engine.ListWithCursor(batchSize, cursor)
			if errors.Is(err, ErrEndOfListing) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func benchmarkExists(b *testing.B, fx *benchmarkEngineFixture, addrs []oid.Address, expected bool) {
	var i int
	b.ReportAllocs()
	for b.Loop() {
		ok, err := fx.engine.existsPhysical(addrs[i%len(addrs)])
		if err != nil {
			b.Fatal(err)
		}
		if ok != expected {
			b.Fatalf("unexpected exists=%t expected=%t", ok, expected)
		}
		i++
	}
}

func newBenchmarkEngineFixture(tb testing.TB, shardCount int) *benchmarkEngineFixture {
	engine := benchmarkNewEngineWithShardNum(tb, shardCount)
	return &benchmarkEngineFixture{
		engine:     engine,
		shards:     engine.unsortedShards(),
		shardCount: shardCount,
	}
}

func forEachBenchmarkFixture(b *testing.B, bench func(*testing.B, int, *benchmarkEngineFixture)) {
	for _, shardCount := range benchmarkShardCounts {
		b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
			fx := newBenchmarkEngineFixture(b, shardCount)
			b.Cleanup(func() {
				_ = fx.engine.Close()
			})
			bench(b, shardCount, fx)
		})
	}
}

func prepareSearchBenchmarkScenario(tb testing.TB, fx *benchmarkEngineFixture) searchBenchmarkScenario {
	started := time.Now()
	defer func() {
		tb.Logf("search setup wall=%s shards=%d objects_per_shard=%d", time.Since(started), fx.shardCount, benchmarkObjectsPerShard)
	}()

	const searchAttr = "bench_search_key"
	const hitValue = "value-hit"
	const missValue = "value-miss"

	sc := searchBenchmarkScenario{
		container: cidtest.ID(),
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(fx.shards))

	for shardIdx, sh := range fx.shards {
		wg.Add(1)
		go func(shardIdx int, sh shardWrapper) {
			defer wg.Done()
			for objIdx := range benchmarkObjectsPerShard {
				obj := generateObjectWithCID(sc.container)
				obj.SetAttributes(
					object.NewAttribute(searchAttr, fmt.Sprintf("value-%02d-%03d", shardIdx, objIdx)),
				)

				if shardIdx == 0 && objIdx == 0 {
					obj.SetAttributes(
						object.NewAttribute(searchAttr, hitValue),
					)
				}

				if err := sh.Put(obj, nil); err != nil {
					errCh <- err
					return
				}
			}
		}(shardIdx, sh)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			tb.Fatal(err)
		}
	}

	sc.hitFS, sc.hitAttrs, sc.hitCursor = benchmarkSearchQuery(tb, searchAttr, hitValue)
	sc.missFS, sc.missAttrs, sc.missCursor = benchmarkSearchQuery(tb, searchAttr, missValue)
	return sc
}

func prepareListBenchmarkScenario(tb testing.TB, fx *benchmarkEngineFixture) {
	started := time.Now()
	defer func() {
		tb.Logf("list setup wall=%s shards=%d objects_per_shard=%d", time.Since(started), fx.shardCount, benchmarkObjectsPerShard)
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, len(fx.shards))

	for _, sh := range fx.shards {
		wg.Add(1)
		go func(sh shardWrapper) {
			defer wg.Done()
			for range benchmarkObjectsPerShard {
				obj := generateObjectWithCID(cidtest.ID())
				if err := sh.Put(obj, nil); err != nil {
					errCh <- err
					return
				}
			}
		}(sh)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			tb.Fatal(err)
		}
	}
}

func prepareCollectRawBenchmarkScenario(tb testing.TB, fx *benchmarkEngineFixture) collectRawBenchmarkScenario {
	started := time.Now()
	defer func() {
		tb.Logf("collectRaw setup wall=%s shards=%d matches_per_shard=%d", time.Since(started), fx.shardCount, benchmarkRawMatchesPerShard)
	}()

	sc := collectRawBenchmarkScenario{
		container: cidtest.ID(),
		attr:      "bench_raw_group",
		hitValue:  []byte("target"),
		missValue: []byte("missing"),
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(fx.shards))

	for _, sh := range fx.shards {
		wg.Add(1)
		go func(sh shardWrapper) {
			defer wg.Done()
			for range benchmarkRawMatchesPerShard {
				obj := generateObjectWithCID(sc.container)
				obj.SetAttributes(
					object.NewAttribute(sc.attr, string(sc.hitValue)),
				)

				if err := sh.Put(obj, nil); err != nil {
					errCh <- err
					return
				}
			}
		}(sh)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			tb.Fatal(err)
		}
	}

	return sc
}

func prepareExistsBenchmarkScenario(tb testing.TB, fx *benchmarkEngineFixture) existsBenchmarkScenario {
	started := time.Now()
	defer func() {
		tb.Logf("exists setup wall=%s shards=%d miss_count=%d", time.Since(started), fx.shardCount, benchmarkExistsMissCount)
	}()

	sc := existsBenchmarkScenario{
		misses: make([]oid.Address, benchmarkExistsMissCount),
	}

	for i := range sc.misses {
		sc.misses[i] = oidtest.Address()
	}

	sc.hitFirst = benchmarkExistsObject(tb, fx.engine, true)
	sc.hitLast = benchmarkExistsObject(tb, fx.engine, false)
	return sc
}

func benchmarkNewEngineWithShardNum(tb testing.TB, shardCount int) *StorageEngine {
	shards := make([]*shard.Shard, 0, shardCount)
	for i := range shardCount {
		shards = append(shards, testNewShard(tb, i))
	}
	return testNewEngineWithShards(shards...)
}

func benchmarkSearchQuery(tb testing.TB, attr, val string) ([]objectcore.SearchFilter, []string, *objectcore.SearchCursor) {
	fs := object.SearchFilters{}
	fs.AddFilter(attr, val, object.MatchStringEqual)

	attrs := []string{attr}
	resFS, cursor, err := objectcore.PreprocessSearchQuery(fs, attrs, "")
	if err != nil {
		tb.Fatal(err)
	}
	return resFS, attrs, cursor
}

func dropCachesIfEnabled(b *testing.B) {
	if !benchmarkDropCachesEnabled {
		return
	}
	b.StopTimer()

	_ = exec.Command("sync").Run()
	err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0200)
	if err != nil {
		b.Skipf("can't drop caches: %v", err)
	}

	b.StartTimer()
}

func benchmarkExistsObject(tb testing.TB, engine *StorageEngine, first bool) oid.Address {
	obj := generateObjectWithCID(cidtest.ID())
	sorted := engine.sortedShards(obj.GetID())
	if len(sorted) == 0 {
		tb.Fatal("no shards")
	}

	target := sorted[len(sorted)-1]
	if first {
		target = sorted[0]
	}

	if err := target.Put(obj, nil); err != nil {
		tb.Fatal(err)
	}
	return obj.Address()
}
