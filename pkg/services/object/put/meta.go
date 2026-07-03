package putsvc

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (t *distributedTarget) encodeMetaIfRequested(obj object.Object) error {
	if t.metaCollection == nil {
		return nil
	}

	var err error
	t.metaCollection.metaTransaction, t.metaCollection.dataToSign, err = t.encodeObjectMetadata(obj)
	if err != nil {
		return fmt.Errorf("encode object metadata: %w", err)
	}

	return nil
}

func (t *distributedTarget) encodeObjectMetadata(obj object.Object) (*transaction.Transaction, []byte, error) {
	currBlock := t.metaSvc.Height()
	currEpochDuration := t.fsState.CurrentEpochDuration()
	expectedVUB := (uint64(currBlock)/currEpochDuration + 2) * currEpochDuration

	firstObj := obj.GetFirstID()
	if obj.HasParent() && firstObj.IsZero() {
		// object itself is the first one
		firstObj = obj.GetID()
	}

	var deletedObj oid.ID
	var lockedObj oid.ID
	typ := obj.Type()
	switch typ {
	case object.TypeTombstone:
		deletedObj = obj.AssociatedObject()
	case object.TypeLock:
		lockedObj = obj.AssociatedObject()
	default:
	}

	tx, dataToSign := objectcore.EncodeChainMetaInfo(len(t.containerNodes.PrimaryCounts())+len(t.containerNodes.ECRules()), obj.GetContainerID(), obj.GetID(), firstObj, obj.GetPreviousID(),
		obj.PayloadSize(), typ, deletedObj, lockedObj, expectedVUB, t.metaSvc.MagicNumber())
	err := t.metaSvc.TransactionTestInvocation(tx)
	if err != nil {
		return nil, nil, fmt.Errorf("matadata chain transaction test invocation fail: %w", err)
	}

	return tx, dataToSign, nil
}

func (t *distributedTarget) submitMetaCollection(o object.Object) error {
	if t.metaCollection == nil {
		return nil
	}
	t.metaCollection.signaturesMtx.RLock()
	defer t.metaCollection.signaturesMtx.RUnlock()

	var await bool
	switch t.metainfoConsistencyAttr {
	// TODO: there was no constant in SDK at the code creation moment
	case "strict":
		await = true
	case "optimistic":
		await = false
	default:
		return nil
	}

	var (
		objAcceptedCh chan struct{}
		addr          = o.Address()
	)
	if await {
		h := t.metaCollection.metaTransaction.Hash()

		var objCopy object.Object
		o.CutPayload().CopyTo(&objCopy)

		objAcceptedCh = make(chan struct{}, 1)
		t.metaSvc.NotifyObjectSuccess(objAcceptedCh, objCopy, h)
	}

	err := t.metaSvc.SubmitObjectPut(t.metaCollection.metaTransaction, t.metaCollection.signatures)
	if err != nil {
		if await {
			t.metaSvc.UnsubscribeFromObject(o.Address())
		}
		return fmt.Errorf("failed to submit %s object meta information: %w", addr, err)
	}

	if await {
		select {
		case <-t.opCtx.Done():
			t.metaSvc.UnsubscribeFromObject(addr)
			return fmt.Errorf("interrupted awaiting for %s object meta information: %w", addr, t.opCtx.Err())
		case <-objAcceptedCh:
		}
	}

	t.placementIterator.log.Debug("submitted object meta information", zap.Stringer("addr", addr))

	return nil
}
