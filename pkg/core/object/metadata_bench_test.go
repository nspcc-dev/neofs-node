package objectcore

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/signed256"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

const (
	benchSetLen       = 10_000
	benchDenseDataLen = 30_000

	benchOverlapShift = benchSetLen * 9 / 10

	benchSmallObjectSize        = 4 * 1024
	benchMediumObjectSize       = 512 * 1024
	benchLargeObjectSize  int64 = 8 * 1024 * 1024 * 1024
)

func BenchmarkPreprocessSearchQuery(b *testing.B) {
	const (
		minSigned256Dec      = "-115792089237316195423570985008687907853269984665640564039457584007913129639935"
		minSigned256PlusOne  = "-115792089237316195423570985008687907853269984665640564039457584007913129639934"
		maxSigned256Dec      = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
		maxSigned256MinusOne = "115792089237316195423570985008687907853269984665640564039457584007913129639934"
		maxUint64Dec         = "18446744073709551615"
		largePositive256Dec  = "340282366920938463463374607431768211455"
		largeNegative256Dec  = "-18446744073709551615"
	)

	var oidCursor oid.ID
	for i := range oidCursor {
		oidCursor[i] = byte(i + 1)
	}

	var intCursorID oid.ID
	for i := range intCursorID {
		intCursorID[i] = byte(255 - i)
	}

	cursorIntKey := make([]byte, len("attr")+attributeDelimiterLen+intValLen+oid.Size)
	off := copy(cursorIntKey, "attr")
	off += copy(cursorIntKey[off:], MetaAttributeDelimiter)
	v := signed256.NewInt(123)
	copy(cursorIntKey[off:], IntBytes(&v))
	copy(cursorIntKey[off+intValLen:], intCursorID[:])

	nonIntCursorKey := make([]byte, len("attr")+attributeDelimiterLen+len("hello")+attributeDelimiterLen+oid.Size)
	off = copy(nonIntCursorKey, "attr")
	off += copy(nonIntCursorKey[off:], MetaAttributeDelimiter)
	off += copy(nonIntCursorKey[off:], "hello")
	off += copy(nonIntCursorKey[off:], MetaAttributeDelimiter)
	copy(nonIntCursorKey[off:], intCursorID[:])

	for _, tc := range []struct {
		name      string
		fs        object.SearchFilters
		attrs     []string
		cursor    string
		wantError error
	}{
		{
			name:   "listing_initial",
			cursor: "",
		},
		{
			name:   "listing_cursor",
			cursor: base64.StdEncoding.EncodeToString(oidCursor[:]),
		},
		{
			name:  "string_eq_initial",
			fs:    stringFilters("attr", "hello", object.MatchStringEqual),
			attrs: []string{"attr"},
		},
		{
			name:  "string_prefix_initial",
			fs:    stringFilters("attr", "he", object.MatchCommonPrefix),
			attrs: []string{"attr"},
		},
		{
			name:  "int_small_ge_initial",
			fs:    numFilters("attr", "123", object.MatchNumGE),
			attrs: []string{"attr"},
		},
		{
			name:   "int_small_ge_cursor",
			fs:     numFilters("attr", "123", object.MatchNumGE),
			attrs:  []string{"attr"},
			cursor: base64.StdEncoding.EncodeToString(cursorIntKey),
		},
		{
			name:  "int_min_ge_automatch",
			fs:    numFilters("attr", minSigned256Dec, object.MatchNumGE),
			attrs: []string{"attr"},
		},
		{
			name:      "int_min_lt_unreachable",
			fs:        numFilters("attr", minSigned256Dec, object.MatchNumLT),
			attrs:     []string{"attr"},
			wantError: ErrUnreachableQuery,
		},
		{
			name:  "int_min_plus_one_ge",
			fs:    numFilters("attr", minSigned256PlusOne, object.MatchNumGE),
			attrs: []string{"attr"},
		},
		{
			name:  "int_max_le_automatch",
			fs:    numFilters("attr", maxSigned256Dec, object.MatchNumLE),
			attrs: []string{"attr"},
		},
		{
			name:      "int_max_gt_unreachable",
			fs:        numFilters("attr", maxSigned256Dec, object.MatchNumGT),
			attrs:     []string{"attr"},
			wantError: ErrUnreachableQuery,
		},
		{
			name:  "int_max_minus_one_le",
			fs:    numFilters("attr", maxSigned256MinusOne, object.MatchNumLE),
			attrs: []string{"attr"},
		},
		{
			name:  "int_max_uint64_ge",
			fs:    numFilters("attr", maxUint64Dec, object.MatchNumGE),
			attrs: []string{"attr"},
		},
		{
			name:  "int_large_positive_ge",
			fs:    numFilters("attr", largePositive256Dec, object.MatchNumGE),
			attrs: []string{"attr"},
		},
		{
			name:  "int_large_negative_ge",
			fs:    numFilters("attr", largeNegative256Dec, object.MatchNumGE),
			attrs: []string{"attr"},
		},
		{
			name:   "non_int_eq_cursor",
			fs:     stringFilters("attr", "hello", object.MatchStringEqual),
			attrs:  []string{"attr"},
			cursor: base64.StdEncoding.EncodeToString(nonIntCursorKey),
		},
		{
			name:  "mixed_numeric_same_attr",
			fs:    mixedNumericSameAttrFilters(),
			attrs: []string{"attr"},
		},
		{
			name:  "mixed_numeric_and_string",
			fs:    mixedNumericAndStringFilters(),
			attrs: []string{"attr"},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				_, _, err := PreprocessSearchQuery(tc.fs, tc.attrs, tc.cursor)
				if tc.wantError != nil {
					require.ErrorIs(b, err, tc.wantError)
				} else {
					require.NoError(b, err)
				}
			}
		})
	}
}

func numFilters(attr, val string, op object.SearchMatchType) object.SearchFilters {
	var fs object.SearchFilters
	fs.AddFilter(attr, val, op)
	return fs
}

func stringFilters(attr, val string, op object.SearchMatchType) object.SearchFilters {
	var fs object.SearchFilters
	fs.AddFilter(attr, val, op)
	return fs
}

func mixedNumericSameAttrFilters() object.SearchFilters {
	var fs object.SearchFilters
	fs.AddFilter("attr", "123456789", object.MatchNumGE)
	fs.AddFilter("attr", "999999999999999999", object.MatchNumLT)
	return fs
}

func mixedNumericAndStringFilters() object.SearchFilters {
	var fs object.SearchFilters
	fs.AddFilter("attr", "123456789", object.MatchNumGE)
	fs.AddFilter("tag", "hello", object.MatchStringEqual)
	return fs
}

func BenchmarkMergeSearchResults(b *testing.B) {
	for _, lim := range []uint16{1, 2, 1000, 10000} {
		b.Run(fmt.Sprintf("lim_%d", lim), func(b *testing.B) {
			for _, setCount := range []int{2, 4, 8, 16} {
				b.Run(fmt.Sprintf("set_count_%d", setCount), func(b *testing.B) {
					b.Run("id_identical", func(b *testing.B) {
						benchMergeSearchResults(b, mergeBenchCase{
							lim:     lim,
							sets:    makeIdenticalSets(setCount, makeIDOnlyItems(benchSetLen)),
							mores:   make([]bool, setCount),
							wantLen: min(int(lim), benchSetLen),
						})
					})

					b.Run("id_disjoint", func(b *testing.B) {
						benchMergeSearchResults(b, mergeBenchCase{
							lim:     lim,
							sets:    makeDisjointIDOnlySets(setCount, benchSetLen),
							mores:   make([]bool, setCount),
							wantLen: min(int(lim), benchSetLen),
						})
					})

					b.Run("id_intersecting", func(b *testing.B) {
						benchMergeSearchResults(b, mergeBenchCase{
							lim:     lim,
							sets:    makeSlidingIDOnlySets(setCount, benchSetLen, benchOverlapShift),
							mores:   make([]bool, setCount),
							wantLen: min(int(lim), benchSetLen),
						})
					})

					b.Run("size_numeric", func(b *testing.B) {
						benchMergeSearchResults(b, mergeBenchCase{
							lim:       lim,
							cmpInt:    true,
							sets:      makeNumericSizeSets(setCount, benchSetLen, benchOverlapShift),
							mores:     make([]bool, setCount),
							firstAttr: "Size",
							wantLen:   min(int(lim), benchSetLen),
						})
					})

					b.Run("common_prefix_dense", func(b *testing.B) {
						// S3 GW use-case: listing with a common path prefix
						benchMergeSearchResults(b, mergeBenchCase{
							lim:       lim,
							firstAttr: "FilePath",
							sets:      makeCommonPrefixDenseSets(setCount),
							mores:     makeDenseMores(setCount),
							wantLen:   min(int(lim), benchSetLen),
						})
					})

					b.Run("common_prefix_single_result", func(b *testing.B) {
						// S3 GW use-case: merged result may contain only one object
						benchMergeSearchResults(b, mergeBenchCase{
							lim:       lim,
							firstAttr: "FilePath",
							sets:      makeCommonPrefixSingleResultSets(setCount),
							mores:     makeSingleResultMores(setCount),
							wantLen:   1,
						})
					})

					b.Run("filepath_timestamp_dense", func(b *testing.B) {
						// REST GW use-case: querying FilePath and Timestamp attributes
						benchMergeSearchResults(b, mergeBenchCase{
							lim:       lim,
							firstAttr: "FilePath",
							sets:      makeFilePathTimestampDenseSets(setCount),
							mores:     makeDenseMores(setCount),
							wantLen:   min(int(lim), benchSetLen),
						})
					})

					b.Run("integer_attribute_dense", func(b *testing.B) {
						// NeoGo block fetcher use-case
						benchMergeSearchResults(b, mergeBenchCase{
							lim:       lim,
							firstAttr: "Height",
							cmpInt:    true,
							sets:      makeIntegerAttributeDenseSets(setCount),
							mores:     makeMixedMores(setCount),
							wantLen:   min(int(lim), benchSetLen),
						})
					})
				})
			}
		})
	}
}

type mergeBenchCase struct {
	lim       uint16
	firstAttr string
	cmpInt    bool
	sets      [][]client.SearchResultItem
	mores     []bool
	wantLen   int
}

func benchMergeSearchResults(b *testing.B, tc mergeBenchCase) {
	b.Helper()

	sets := make([][]client.SearchResultItem, len(tc.sets))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		copy(sets, tc.sets)

		res, _, err := MergeSearchResults(tc.lim, tc.firstAttr, tc.cmpInt, sets, tc.mores)
		require.NoError(b, err)
		require.Len(b, res, tc.wantLen)
	}
}

func makeIdenticalSets(n int, items []client.SearchResultItem) [][]client.SearchResultItem {
	res := make([][]client.SearchResultItem, n)
	for i := range res {
		res[i] = items
	}
	return res
}

func makeDisjointIDOnlySets(setCount, setLen int) [][]client.SearchResultItem {
	res := make([][]client.SearchResultItem, setCount)
	for i := range res {
		res[i] = makeIDOnlyItemsWithOffset(setLen, i*setLen)
	}
	return res
}

func makeSlidingIDOnlySets(setCount, setLen, shift int) [][]client.SearchResultItem {
	res := make([][]client.SearchResultItem, setCount)
	total := setLen + (setCount-1)*shift
	all := makeIDOnlyItems(total)
	for i := range res {
		start := i * shift
		res[i] = all[start : start+setLen]
	}
	return res
}

func makeNumericSizeSets(setCount, setLen, shift int) [][]client.SearchResultItem {
	res := make([][]client.SearchResultItem, setCount)
	total := setLen + (setCount-1)*shift
	all := make([]client.SearchResultItem, total)
	for i := range all {
		all[i] = client.SearchResultItem{
			ID:         benchOID(i),
			Attributes: []string{benchObjectSize(i)},
		}
	}
	for i := range res {
		start := i * shift
		res[i] = all[start : start+setLen]
	}
	return res
}

func makeCommonPrefixDenseSets(setCount int) [][]client.SearchResultItem {
	shared := makeCommonPrefixItems(benchDenseDataLen)
	sets := make([][]client.SearchResultItem, setCount)
	for i := range setCount {
		shift := 0
		switch i {
		case setCount - 2:
			shift = benchSetLen / 200
		case setCount - 1:
			shift = benchSetLen / 100
		}
		sets[i] = shared[shift : shift+benchSetLen]
	}
	return sets
}

const (
	tailLenA     = 20
	tailLenB     = 40
	extraOffsetA = 100_000
	extraOffsetB = 200_000
)

func makeCommonPrefixSingleResultSets(setCount int) [][]client.SearchResultItem {
	shared := makeCommonPrefixItems(1)
	sets := make([][]client.SearchResultItem, setCount)
	for i := range setCount {
		sets[i] = shared
	}
	return sets
}

func makeFilePathTimestampDenseSets(setCount int) [][]client.SearchResultItem {
	const smallShift = benchSetLen / 200

	shared := makeFilePathTimestampItems(benchDenseDataLen)
	sets := make([][]client.SearchResultItem, setCount)
	for i := range setCount {
		switch {
		case i < setCount-2:
			sets[i] = shared[:benchSetLen]
		case i == setCount-2:
			sets[i] = append(append([]client.SearchResultItem(nil), shared[:benchSetLen-smallShift]...),
				makeFilePathTimestampItemsWithOffset(smallShift, extraOffsetA)...)
		default:
			sets[i] = append(append([]client.SearchResultItem(nil), shared[:benchSetLen-2*smallShift]...),
				makeFilePathTimestampItemsWithOffset(2*smallShift, extraOffsetB)...)
		}
	}
	return sets
}

func makeIntegerAttributeDenseSets(setCount int) [][]client.SearchResultItem {
	shared := makeIntegerAttributeItems(benchDenseDataLen)
	sets := make([][]client.SearchResultItem, setCount)
	for i := range setCount {
		switch {
		case i < setCount-2:
			sets[i] = shared[:benchSetLen]
		case i == setCount-2:
			sets[i] = append(append([]client.SearchResultItem(nil), shared[:benchSetLen-tailLenA]...),
				makeIntegerAttributeItemsWithOffset(tailLenA+10, extraOffsetA)...)
		default:
			sets[i] = append(append([]client.SearchResultItem(nil), shared[:benchSetLen-tailLenB]...),
				makeIntegerAttributeItemsWithOffset(tailLenB+20, extraOffsetB)...)
		}
	}
	return sets
}

func makeDenseMores(setCount int) []bool {
	mores := make([]bool, setCount)
	for i := range mores {
		mores[i] = true
	}
	return mores
}

func makeMixedMores(setCount int) []bool {
	mores := makeDenseMores(setCount)
	if setCount > 0 {
		mores[setCount-1] = false
	}
	if setCount > 1 {
		mores[setCount-2] = false
	}
	return mores
}

func makeSingleResultMores(setCount int) []bool {
	mores := make([]bool, setCount)
	if setCount > 0 {
		mores[0] = true
	}
	return mores
}

func makeIDOnlyItems(n int) []client.SearchResultItem {
	return makeIDOnlyItemsWithOffset(n, 0)
}

func makeIDOnlyItemsWithOffset(n, off int) []client.SearchResultItem {
	res := make([]client.SearchResultItem, n)
	for i := range res {
		res[i] = client.SearchResultItem{ID: benchOID(off + i)}
	}
	return res
}

func makeCommonPrefixItems(n int) []client.SearchResultItem {
	return makeCommonPrefixItemsWithOffset(n, 0)
}

func makeCommonPrefixItemsWithOffset(n, off int) []client.SearchResultItem {
	res := make([]client.SearchResultItem, n)
	for i := range res {
		idx := off + i
		attrs := []string{
			fmt.Sprintf("/bucket/photos/img-%d.jpg", idx),
		}
		for j := range 10 {
			attrs = append(attrs, fmt.Sprintf("value_%d_%d", j, idx))
		}
		res[i] = client.SearchResultItem{ID: benchOID(idx), Attributes: attrs}
	}
	return res
}

func makeFilePathTimestampItems(n int) []client.SearchResultItem {
	return makeFilePathTimestampItemsWithOffset(n, 0)
}

func makeFilePathTimestampItemsWithOffset(n, off int) []client.SearchResultItem {
	res := make([]client.SearchResultItem, n)
	for i := range res {
		idx := off + i
		group := idx % 32
		res[i] = client.SearchResultItem{
			ID: benchOID(idx),
			Attributes: []string{
				fmt.Sprintf("album-%02d/photo-%06d.jpg", group, idx/32),
				strconv.FormatInt(1738760000+int64(idx%2048), 10),
			},
		}
	}
	return res
}

func makeIntegerAttributeItems(n int) []client.SearchResultItem {
	return makeIntegerAttributeItemsWithOffset(n, 0)
}

func makeIntegerAttributeItemsWithOffset(n, off int) []client.SearchResultItem {
	res := make([]client.SearchResultItem, n)
	for i := range res {
		idx := off + i
		res[i] = client.SearchResultItem{
			ID: benchOID(idx),
			Attributes: []string{
				strconv.Itoa(idx),
			},
		}
	}
	return res
}

func benchObjectSize(i int) string {
	switch {
	case i%1024 == 0:
		return strconv.FormatInt(benchLargeObjectSize, 10)
	case i%17 == 0:
		return strconv.Itoa(benchMediumObjectSize)
	default:
		return strconv.Itoa(benchSmallObjectSize)
	}
}

func benchOID(i int) oid.ID {
	var id oid.ID
	id[28] = byte(i >> 24)
	id[29] = byte(i >> 16)
	id[30] = byte(i >> 8)
	id[31] = byte(i)
	return id
}
