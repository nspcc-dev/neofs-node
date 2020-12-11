package deletesvc

import (
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
)

type headSvcWrapper getsvc.Service

type searchSvcWrapper searchsvc.Service

type putSvcWrapper putsvc.Service

type simpleIDWriter struct {
	ids []*objectSDK.ID
}

func (w *headSvcWrapper) headAddress(exec *execCtx, addr *objectSDK.Address) (*object.Object, error) {
	wr := getsvc.NewSimpleObjectWriter()

	p := getsvc.HeadPrm{}
	p.SetPrivateKey(exec.key())
	p.SetCommonParameters(exec.commonParameters())
	p.SetHeaderWriter(wr)
	p.WithRawFlag(true)
	p.WithAddress(addr)
	p.SetRemoteCallOptions(exec.callOptions()...)

	err := (*getsvc.Service)(w).Head(exec.context(), p)
	if err != nil {
		return nil, err
	}

	return wr.Object(), nil
}

func (w *headSvcWrapper) splitInfo(exec *execCtx) (*objectSDK.SplitInfo, error) {
	_, err := w.headAddress(exec, exec.address())

	var errSplitInfo *objectSDK.SplitInfoError

	switch {
	case err == nil:
		return nil, nil
	case errors.As(err, &errSplitInfo):
		return errSplitInfo.SplitInfo(), nil
	default:
		return nil, err
	}
}

func (w *headSvcWrapper) children(exec *execCtx) ([]*objectSDK.ID, error) {
	a := exec.newAddress(exec.splitInfo.Link())

	linking, err := w.headAddress(exec, a)
	if err != nil {
		return nil, err
	}

	return linking.Children(), nil
}

func (w *headSvcWrapper) previous(exec *execCtx, id *objectSDK.ID) (*objectSDK.ID, error) {
	a := exec.newAddress(id)

	h, err := w.headAddress(exec, a)
	if err != nil {
		return nil, err
	}

	return h.PreviousID(), nil
}

func (w *searchSvcWrapper) splitMembers(exec *execCtx) ([]*objectSDK.ID, error) {
	fs := objectSDK.SearchFilters{}
	fs.AddSplitIDFilter(objectSDK.MatchStringEqual, exec.splitInfo.SplitID())

	wr := new(simpleIDWriter)

	p := searchsvc.Prm{}
	p.SetWriter(wr)
	p.SetCommonParameters(exec.commonParameters())
	p.SetPrivateKey(exec.key())
	p.WithContainerID(exec.containerID())
	p.WithSearchFilters(fs)
	p.SetRemoteCallOptions(exec.callOptions()...)

	err := (*searchsvc.Service)(w).Search(exec.context(), p)
	if err != nil {
		return nil, err
	}

	return wr.ids, nil
}

func (s *simpleIDWriter) WriteIDs(ids []*objectSDK.ID) error {
	s.ids = append(s.ids, ids...)

	return nil
}

func (w *putSvcWrapper) put(exec *execCtx, broadcast bool) (*objectSDK.ID, error) {
	streamer, err := (*putsvc.Service)(w).Put(exec.context())
	if err != nil {
		return nil, err
	}

	payload := exec.tombstoneObj.Payload()

	initPrm := new(putsvc.PutInitPrm).
		WithCommonPrm(exec.commonParameters()).
		WithObject(exec.tombstoneObj.CutPayload())

	if broadcast {
		initPrm.WithTraverseOption(placement.WithoutSuccessTracking())
	}

	err = streamer.Init(initPrm)
	if err != nil {
		return nil, err
	}

	err = streamer.SendChunk(new(putsvc.PutChunkPrm).WithChunk(payload))
	if err != nil {
		return nil, err
	}

	r, err := streamer.Close()
	if err != nil {
		return nil, err
	}

	return r.ObjectID(), nil
}
