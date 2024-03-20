package deletesvc

import (
	"errors"
	"fmt"

	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type headSvcWrapper getsvc.Service

type searchSvcWrapper searchsvc.Service

type putSvcWrapper putsvc.Service

type simpleIDWriter struct {
	ids []oid.ID
}

func (w *headSvcWrapper) headAddress(exec *execCtx, addr oid.Address) (*object.Object, error) {
	wr := getsvc.NewSimpleObjectWriter()

	p := getsvc.HeadPrm{}
	p.SetCommonParameters(exec.commonParameters())
	p.SetHeaderWriter(wr)
	p.WithRawFlag(true)
	p.WithAddress(addr)

	err := (*getsvc.Service)(w).Head(exec.context(), p)
	if err != nil {
		return nil, err
	}

	return wr.Object(), nil
}

func (w *headSvcWrapper) splitInfo(exec *execCtx) (*object.SplitInfo, error) {
	_, err := w.headAddress(exec, exec.address())

	var errSplitInfo *object.SplitInfoError

	switch {
	case err == nil:
		return nil, nil
	case errors.As(err, &errSplitInfo):
		return errSplitInfo.SplitInfo(), nil
	default:
		return nil, err
	}
}

func (w *headSvcWrapper) children(exec *execCtx) ([]oid.ID, error) {
	linkID, _ := exec.splitInfo.Link()

	if exec.splitInfo.SplitID() == nil {
		// V2 split
		linkObj, err := w.getChild(exec, linkID)
		if err != nil {
			return nil, fmt.Errorf("receiving link object: %w", err)
		}

		var link object.Link
		err = linkObj.ReadLink(&link)
		if err != nil {
			return nil, fmt.Errorf("parsing link object: %w", err)
		}

		res := make([]oid.ID, 0, len(link.Objects()))
		for _, child := range link.Objects() {
			res = append(res, child.ObjectID())
		}

		return res, nil
	}

	// V1 split

	a := exec.newAddress(linkID)

	linking, err := w.headAddress(exec, a)
	if err != nil {
		return nil, err
	}

	return linking.Children(), nil
}

func (w *headSvcWrapper) previous(exec *execCtx, id oid.ID) (*oid.ID, error) {
	a := exec.newAddress(id)

	h, err := w.headAddress(exec, a)
	if err != nil {
		return nil, err
	}

	prev, ok := h.PreviousID()
	if ok {
		return &prev, nil
	}

	return nil, nil
}

func (w *headSvcWrapper) getChild(exec *execCtx, oID oid.ID) (*object.Object, error) {
	a := exec.newAddress(oID)
	wr := getsvc.NewSimpleObjectWriter()

	p := getsvc.Prm{}
	p.SetCommonParameters(exec.commonParameters())
	p.SetObjectWriter(wr)
	p.WithRawFlag(true)
	p.WithAddress(a)

	err := (*getsvc.Service)(w).Get(exec.context(), p)
	if err != nil {
		return nil, err
	}

	return wr.Object(), nil
}

func (w *searchSvcWrapper) splitMembers(exec *execCtx) ([]oid.ID, error) {
	fs := object.SearchFilters{}
	if splitID := exec.splitInfo.SplitID(); splitID != nil {
		fs.AddSplitIDFilter(object.MatchStringEqual, *splitID)
	} else {
		fs.AddFilter(object.FilterSplitID, "", object.MatchStringEqual)
	}

	wr := new(simpleIDWriter)

	p := searchsvc.Prm{}
	p.SetWriter(wr)
	p.SetCommonParameters(exec.commonParameters())
	p.WithContainerID(exec.containerID())
	p.WithSearchFilters(fs)

	err := (*searchsvc.Service)(w).Search(exec.context(), p)
	if err != nil {
		return nil, err
	}

	return wr.ids, nil
}

func (s *simpleIDWriter) WriteIDs(ids []oid.ID) error {
	s.ids = append(s.ids, ids...)

	return nil
}

func (w *putSvcWrapper) put(exec *execCtx) (*oid.ID, error) {
	streamer, err := (*putsvc.Service)(w).Put(exec.context())
	if err != nil {
		return nil, err
	}

	payload := exec.tombstoneObj.Payload()

	initPrm := new(putsvc.PutInitPrm).
		WithCommonPrm(exec.commonParameters()).
		WithObject(exec.tombstoneObj.CutPayload())

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

	id := r.ObjectID()

	return &id, nil
}
