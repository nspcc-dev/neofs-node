package slices

import (
	stdslices "slices"
	"testing"
)

func TestRepeatElementNamedSlice(t *testing.T) {
	type integers []int
	got := RepeatElement[int, integers](3, 42)
	if !stdslices.Equal(got, integers{42, 42, 42}) {
		t.Fatalf("unexpected repeated values: %v", got)
	}
	if got := RepeatElement[int, integers](0, 42); got == nil || len(got) != 0 {
		t.Fatalf("expected allocated empty slice, got %v", got)
	}
}
