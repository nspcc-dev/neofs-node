package object

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/storagegroup"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/verifier"
	"github.com/pkg/errors"
)

type (
	filterParams struct {
		sgInfoRecv    storagegroup.InfoReceiver
		tsPresChecker tombstonePresenceChecker
		maxProcSize   uint64
		storageCap    uint64
		localStore    localstore.Localstore
		epochRecv     EpochReceiver
		verifier      verifier.Verifier

		maxPayloadSize uint64
	}

	filterConstructor func(p *filterParams) localstore.FilterFunc

	tombstonePresenceChecker interface {
		hasLocalTombstone(addr Address) (bool, error)
	}

	coreTSPresChecker struct {
		localStore localstore.Localstore
	}
)

const (
	ttlValue = "TTL"
)

const (
	commonObjectFN       = "OBJECTS_OVERALL"
	storageGroupFN       = "STORAGE_GROUP"
	tombstoneOverwriteFN = "TOMBSTONE_OVERWRITE"
	objSizeFN            = "OBJECT_SIZE"
	creationEpochFN      = "CREATION_EPOCH"
	objIntegrityFN       = "OBJECT_INTEGRITY"
	payloadSizeFN        = "PAYLOAD_SIZE"
)

var errObjectFilter = errors.New("incoming object has not passed filter")

var mFilters = map[string]filterConstructor{
	tombstoneOverwriteFN: tombstoneOverwriteFC,
	storageGroupFN:       storageGroupFC,
	creationEpochFN:      creationEpochFC,
	objIntegrityFN:       objectIntegrityFC,
	payloadSizeFN:        payloadSizeFC,
}

var mBasicFilters = map[string]filterConstructor{
	objSizeFN: objectSizeFC,
}

func newIncomingObjectFilter(p *Params) (Filter, error) {
	filter, err := newFilter(p, readyObjectsCheckpointFilterName, mFilters)
	if err != nil {
		return nil, err
	}

	return filter, nil
}

func newFilter(p *Params, name string, m map[string]filterConstructor) (Filter, error) {
	filter := localstore.NewFilter(&localstore.FilterParams{
		Name:       name,
		FilterFunc: localstore.SkippingFilterFunc,
	})

	fp := &filterParams{
		sgInfoRecv:    p.SGInfoReceiver,
		tsPresChecker: &coreTSPresChecker{localStore: p.LocalStore},
		maxProcSize:   p.MaxProcessingSize,
		storageCap:    p.StorageCapacity,
		localStore:    p.LocalStore,
		epochRecv:     p.EpochReceiver,
		verifier:      p.Verifier,

		maxPayloadSize: p.MaxPayloadSize,
	}

	items := make([]*localstore.FilterParams, 0, len(m))
	for fName, fCons := range m {
		items = append(items, &localstore.FilterParams{Name: fName, FilterFunc: fCons(fp)})
	}

	f, err := localstore.AllPassIncludingFilter(commonObjectFN, items...)
	if err != nil {
		return nil, err
	}

	if err := filter.PutSubFilter(localstore.SubFilterParams{
		PriorityFlag:   localstore.PriorityValue,
		FilterPipeline: f,
		OnFail:         localstore.CodeFail,
	}); err != nil {
		return nil, errors.Wrapf(err, "could not put filter %s in pipeline", f.GetName())
	}

	return filter, nil
}

func (s *coreTSPresChecker) hasLocalTombstone(addr Address) (bool, error) {
	m, err := s.localStore.Meta(addr)
	if err != nil {
		if errors.Is(errors.Cause(err), bucket.ErrNotFound) {
			return false, nil
		}

		return false, err
	}

	return m.Object.IsTombstone(), nil
}

func storageGroupFC(p *filterParams) localstore.FilterFunc {
	return func(ctx context.Context, meta *Meta) *localstore.FilterResult {
		if sgInfo, err := meta.Object.StorageGroup(); err != nil {
			return localstore.ResultPass()
		} else if group := meta.Object.Group(); len(group) == 0 {
			return localstore.ResultFail()
		} else if realSGInfo, err := p.sgInfoRecv.GetSGInfo(ctx, meta.Object.SystemHeader.CID, group); err != nil {
			return localstore.ResultWithError(localstore.CodeFail, err)
		} else if sgInfo.ValidationDataSize != realSGInfo.ValidationDataSize {
			return localstore.ResultWithError(
				localstore.CodeFail,
				&detailedError{
					error: errWrongSGSize,
					d:     sgSizeDetails(sgInfo.ValidationDataSize, realSGInfo.ValidationDataSize),
				},
			)
		} else if !sgInfo.ValidationHash.Equal(realSGInfo.ValidationHash) {
			return localstore.ResultWithError(
				localstore.CodeFail,
				&detailedError{
					error: errWrongSGHash,
					d:     sgHashDetails(sgInfo.ValidationHash, realSGInfo.ValidationHash),
				},
			)
		}

		return localstore.ResultPass()
	}
}

func tombstoneOverwriteFC(p *filterParams) localstore.FilterFunc {
	return func(ctx context.Context, meta *Meta) *localstore.FilterResult {
		if meta.Object.IsTombstone() {
			return localstore.ResultPass()
		} else if hasTombstone, err := p.tsPresChecker.hasLocalTombstone(*meta.Object.Address()); err != nil {
			return localstore.ResultFail()
		} else if hasTombstone {
			return localstore.ResultFail()
		}

		return localstore.ResultPass()
	}
}

func objectSizeFC(p *filterParams) localstore.FilterFunc {
	return func(ctx context.Context, meta *Meta) *localstore.FilterResult {
		if need := meta.Object.SystemHeader.PayloadLength; need > p.maxProcSize {
			return localstore.ResultWithError(
				localstore.CodeFail,
				&detailedError{ // // TODO: NSPCC-1048
					error: errProcPayloadSize,
					d:     maxProcPayloadSizeDetails(p.maxProcSize),
				},
			)
		} else if ctx.Value(ttlValue).(uint32) < service.NonForwardingTTL {
			if left := p.storageCap - uint64(p.localStore.Size()); need > left {
				return localstore.ResultWithError(
					localstore.CodeFail,
					errLocalStorageOverflow,
				)
			}
		}

		return localstore.ResultPass()
	}
}

func payloadSizeFC(p *filterParams) localstore.FilterFunc {
	return func(ctx context.Context, meta *Meta) *localstore.FilterResult {
		if meta.Object.SystemHeader.PayloadLength > p.maxPayloadSize {
			return localstore.ResultWithError(
				localstore.CodeFail,
				&detailedError{ // TODO: NSPCC-1048
					error: errObjectPayloadSize,
					d:     maxObjectPayloadSizeDetails(p.maxPayloadSize),
				},
			)
		}

		return localstore.ResultPass()
	}
}

func creationEpochFC(p *filterParams) localstore.FilterFunc {
	return func(_ context.Context, meta *Meta) *localstore.FilterResult {
		if current := p.epochRecv.Epoch(); meta.Object.SystemHeader.CreatedAt.Epoch > current {
			return localstore.ResultWithError(
				localstore.CodeFail,
				&detailedError{ // TODO: NSPCC-1048
					error: errObjectFromTheFuture,
					d:     objectCreationEpochDetails(current),
				},
			)
		}

		return localstore.ResultPass()
	}
}

func objectIntegrityFC(p *filterParams) localstore.FilterFunc {
	return func(ctx context.Context, meta *Meta) *localstore.FilterResult {
		if err := p.verifier.Verify(ctx, meta.Object); err != nil {
			return localstore.ResultWithError(
				localstore.CodeFail,
				&detailedError{
					error: errObjectHeadersVerification,
					d:     objectHeadersVerificationDetails(err),
				},
			)
		}

		return localstore.ResultPass()
	}
}

func basicFilter(p *Params) (Filter, error) {
	return newFilter(p, allObjectsCheckpointFilterName, mBasicFilters)
}
