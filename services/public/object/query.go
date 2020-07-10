package object

import (
	"context"
	"fmt"
	"regexp"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/query"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"go.uber.org/zap"
)

type (
	queryVersionController struct {
		m map[int]localQueryImposer
	}

	coreQueryImposer struct {
		fCreator filterCreator
		lsLister localstore.Iterator

		log *zap.Logger
	}

	filterCreator interface {
		createFilter(query.Query) Filter
	}

	coreFilterCreator struct{}
)

const (
	queryFilterName = "QUERY_FILTER"

	pmUndefinedFilterType = "undefined filter type %d"

	errUnsupportedQueryVersion = internal.Error("unsupported query version number")
)

const errSearchQueryUnmarshal = internal.Error("query unmarshal failure")

const errLocalQueryImpose = internal.Error("local query imposing failure")

var (
	_ filterCreator     = (*coreFilterCreator)(nil)
	_ localQueryImposer = (*queryVersionController)(nil)
	_ localQueryImposer = (*coreQueryImposer)(nil)
)

func (s *queryVersionController) imposeQuery(ctx context.Context, c CID, d []byte, v int) ([]Address, error) {
	imp := s.m[v]
	if imp == nil {
		return nil, errUnsupportedQueryVersion
	}

	return imp.imposeQuery(ctx, c, d, v)
}

func (s *coreQueryImposer) imposeQuery(ctx context.Context, cid CID, qData []byte, _ int) (res []Address, err error) {
	defer func() {
		switch err {
		case nil, errSearchQueryUnmarshal:
		default:
			s.log.Error("local query imposing failure",
				zap.String("error", err.Error()),
			)

			err = errLocalQueryImpose
		}
	}()

	var q query.Query

	if err = q.Unmarshal(qData); err != nil {
		s.log.Error("could not unmarshal search query",
			zap.String("error", err.Error()),
		)

		return nil, errSearchQueryUnmarshal
	} else if err = mouldQuery(cid, &q); err != nil {
		return
	}

	err = s.lsLister.Iterate(
		s.fCreator.createFilter(q),
		func(meta *Meta) (stop bool) {
			res = append(res, Address{
				CID:      meta.Object.SystemHeader.CID,
				ObjectID: meta.Object.SystemHeader.ID,
			})
			return
		},
	)

	return res, err
}

func (s *coreFilterCreator) createFilter(q query.Query) Filter {
	f, err := localstore.AllPassIncludingFilter(queryFilterName, &localstore.FilterParams{
		FilterFunc: func(_ context.Context, o *Meta) *localstore.FilterResult {
			if !imposeQuery(q, o.Object) {
				return localstore.ResultFail()
			}
			return localstore.ResultPass()
		},
	})
	if err != nil {
		panic(err) // TODO: test panic occasion
	}

	return f
}

func mouldQuery(cid CID, q *query.Query) error {
	var (
		withCID bool
		cidStr  = cid.String()
	)

	for i := range q.Filters {
		if q.Filters[i].Name == KeyCID {
			if q.Filters[i].Value != cidStr {
				return errInvalidCIDFilter
			}

			withCID = true
		}
	}

	if !withCID {
		q.Filters = append(q.Filters, QueryFilter{
			Type:  query.Filter_Exact,
			Name:  KeyCID,
			Value: cidStr,
		})
	}

	return nil
}

func imposeQuery(q query.Query, o *Object) bool {
	fs := make(map[string]*QueryFilter)

	for i := range q.Filters {
		switch q.Filters[i].Name {
		case transport.KeyTombstone:
			if !o.IsTombstone() {
				return false
			}
		default:
			fs[q.Filters[i].Name] = &q.Filters[i]
		}
	}

	if !filterSystemHeader(fs, &o.SystemHeader) {
		return false
	}

	orphan := true

	for i := range o.Headers {
		var key, value string

		switch h := o.Headers[i].Value.(type) {
		case *object.Header_Link:
			switch h.Link.Type {
			case object.Link_Parent:
				delete(fs, transport.KeyHasParent)
				key = transport.KeyParent
				orphan = false
			case object.Link_Previous:
				key = KeyPrev
			case object.Link_Next:
				key = KeyNext
			case object.Link_Child:
				if _, ok := fs[transport.KeyNoChildren]; ok {
					return false
				}

				key = KeyChild
			default:
				continue
			}

			value = h.Link.ID.String()
		case *object.Header_UserHeader:
			key, value = h.UserHeader.Key, h.UserHeader.Value
		case *object.Header_StorageGroup:
			key = transport.KeyStorageGroup
		default:
			continue
		}

		if !applyFilter(fs, key, value) {
			return false
		}
	}

	if _, ok := fs[KeyRootObject]; ok && orphan { // we think that object without parents is a root or user's object
		delete(fs, KeyRootObject)
	}

	delete(fs, transport.KeyNoChildren)

	return len(fs) == 0
}

func filterSystemHeader(fs map[string]*QueryFilter, sysHead *SystemHeader) bool {
	return applyFilter(fs, KeyID, sysHead.ID.String()) &&
		applyFilter(fs, KeyCID, sysHead.CID.String()) &&
		applyFilter(fs, KeyOwnerID, sysHead.OwnerID.String())
}

func applyFilter(fs map[string]*QueryFilter, key, value string) bool {
	f := fs[key]
	if f == nil {
		return true
	}

	delete(fs, key)

	switch f.Type {
	case query.Filter_Exact:
		return value == f.Value
	case query.Filter_Regex:
		regex, err := regexp.Compile(f.Value)
		return err == nil && regex.MatchString(value)
	default:
		panic(fmt.Sprintf(pmUndefinedFilterType, f.Type))
	}
}
