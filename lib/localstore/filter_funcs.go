package localstore

import (
	"context"
)

// SkippingFilterFunc is a FilterFunc that always returns result with
// CodePass code and nil error.
func SkippingFilterFunc(_ context.Context, _ *ObjectMeta) *FilterResult {
	return ResultPass()
}

// ContainerFilterFunc returns a FilterFunc that returns:
//  - result with CodePass code and nil error if CID of ObjectMeta if from the CID list;
//  - result with CodeFail code an nil error otherwise.
func ContainerFilterFunc(cidList []CID) FilterFunc {
	return func(_ context.Context, meta *ObjectMeta) *FilterResult {
		for i := range cidList {
			if meta.Object.SystemHeader.CID.Equal(cidList[i]) {
				return ResultPass()
			}
		}

		return ResultFail()
	}
}

// StoredEarlierThanFilterFunc returns a FilterFunc that returns:
//  - result with CodePass code and nil error if StoreEpoch is less that argument;
//  - result with CodeFail code and nil error otherwise.
func StoredEarlierThanFilterFunc(epoch uint64) FilterFunc {
	return func(_ context.Context, meta *ObjectMeta) *FilterResult {
		if meta.StoreEpoch < epoch {
			return ResultPass()
		}

		return ResultFail()
	}
}
