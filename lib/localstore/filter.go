package localstore

import (
	"context"
	"math"
	"sort"
	"sync"

	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/pkg/errors"
)

type (
	// FilterCode is an enumeration of filter return codes.
	FilterCode int

	// PriorityFlag is an enumeration of priority flags.
	PriorityFlag int

	filterPipelineSet []FilterPipeline

	// FilterFunc is a function that checks whether an ObjectMeta matches a specific criterion.
	FilterFunc func(ctx context.Context, meta *ObjectMeta) *FilterResult

	// FilterResult groups of ObjectMeta filter result values.
	FilterResult struct {
		c FilterCode

		e error
	}

	// FilterPipeline is an interface of ObjectMeta filtering tool with sub-filters and priorities.
	FilterPipeline interface {
		Pass(ctx context.Context, meta *ObjectMeta) *FilterResult
		PutSubFilter(params SubFilterParams) error
		GetPriority() uint64
		SetPriority(uint64)
		GetName() string
	}

	// FilterParams groups the parameters of FilterPipeline constructor.
	FilterParams struct {
		Name       string
		Priority   uint64
		FilterFunc FilterFunc
	}

	// SubFilterParams groups the parameters of sub-filter registration.
	SubFilterParams struct {
		PriorityFlag
		FilterPipeline
		OnIgnore FilterCode
		OnPass   FilterCode
		OnFail   FilterCode
	}

	filterPipeline struct {
		*sync.RWMutex

		name     string
		pri      uint64
		filterFn FilterFunc

		maxSubPri  uint64
		mSubResult map[string]map[FilterCode]FilterCode
		subFilters []FilterPipeline
	}
)

const (
	// PriorityValue is a PriorityFlag of the sub-filter registration with GetPriority() value.
	PriorityValue PriorityFlag = iota

	// PriorityMax is a PriorityFlag of the sub-filter registration with maximum priority.
	PriorityMax

	// PriorityMin is a PriorityFlag of the sub-filter registration with minimum priority.
	PriorityMin
)

const (
	// CodeUndefined is a undefined FilterCode.
	CodeUndefined FilterCode = iota

	// CodePass is a FilterCode of filter passage.
	CodePass

	// CodeFail is a FilterCode of filter failure.
	CodeFail

	// CodeIgnore is a FilterCode of filter ignoring.
	CodeIgnore
)

var (
	rPass = &FilterResult{
		c: CodePass,
	}

	rFail = &FilterResult{
		c: CodeFail,
	}

	rIgnore = &FilterResult{
		c: CodeIgnore,
	}

	rUndefined = &FilterResult{
		c: CodeUndefined,
	}
)

// ResultPass returns the FilterResult with CodePass code and nil error.
func ResultPass() *FilterResult {
	return rPass
}

// ResultFail returns the FilterResult with CodeFail code and nil error.
func ResultFail() *FilterResult {
	return rFail
}

// ResultIgnore returns the FilterResult with CodeIgnore code and nil error.
func ResultIgnore() *FilterResult {
	return rIgnore
}

// ResultUndefined returns the FilterResult with CodeUndefined code and nil error.
func ResultUndefined() *FilterResult {
	return rUndefined
}

// ResultWithError returns the FilterResult with passed code and error.
func ResultWithError(c FilterCode, e error) *FilterResult {
	return &FilterResult{
		e: e,
		c: c,
	}
}

// Code returns the filter result code.
func (s *FilterResult) Code() FilterCode {
	return s.c
}

// Err returns the filter result error.
func (s *FilterResult) Err() error {
	return s.e
}

func (f filterPipelineSet) Len() int           { return len(f) }
func (f filterPipelineSet) Less(i, j int) bool { return f[i].GetPriority() > f[j].GetPriority() }
func (f filterPipelineSet) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }

func (r FilterCode) String() string {
	switch r {
	case CodePass:
		return "PASSED"
	case CodeFail:
		return "FAILED"
	case CodeIgnore:
		return "IGNORED"
	default:
		return "UNDEFINED"
	}
}

// NewFilter is a FilterPipeline constructor.
func NewFilter(p *FilterParams) FilterPipeline {
	return &filterPipeline{
		RWMutex:    new(sync.RWMutex),
		name:       p.Name,
		pri:        p.Priority,
		filterFn:   p.FilterFunc,
		mSubResult: make(map[string]map[FilterCode]FilterCode),
	}
}

// AllPassIncludingFilter returns FilterPipeline with sub-filters composed from parameters.
// Result filter fails with CodeFail code if any of the sub-filters returns not a CodePass code.
func AllPassIncludingFilter(name string, params ...*FilterParams) (FilterPipeline, error) {
	res := NewFilter(&FilterParams{
		Name:       name,
		FilterFunc: SkippingFilterFunc,
	})

	for i := range params {
		if err := res.PutSubFilter(SubFilterParams{
			FilterPipeline: NewFilter(params[i]),
			OnIgnore:       CodeFail,
			OnFail:         CodeFail,
		}); err != nil {
			return nil, errors.Wrap(err, "could not create all pass including filter")
		}
	}

	return res, nil
}

func (p *filterPipeline) Pass(ctx context.Context, meta *ObjectMeta) *FilterResult {
	p.RLock()
	defer p.RUnlock()

	for i := range p.subFilters {
		subResult := p.subFilters[i].Pass(ctx, meta)
		subName := p.subFilters[i].GetName()

		cSub := subResult.Code()

		if cSub <= CodeUndefined {
			return ResultUndefined()
		}

		if cFin := p.mSubResult[subName][cSub]; cFin != CodeIgnore {
			return ResultWithError(cFin, subResult.Err())
		}
	}

	if p.filterFn == nil {
		return ResultUndefined()
	}

	return p.filterFn(ctx, meta)
}

func (p *filterPipeline) PutSubFilter(params SubFilterParams) error {
	p.Lock()
	defer p.Unlock()

	if params.FilterPipeline == nil {
		return internal.Error("could not put sub filter: empty filter pipeline")
	}

	name := params.FilterPipeline.GetName()
	if _, ok := p.mSubResult[name]; ok {
		return errors.Errorf("filter %s is already in pipeline %s", name, p.GetName())
	}

	if params.PriorityFlag != PriorityMin {
		if pri := params.FilterPipeline.GetPriority(); pri < math.MaxUint64 {
			params.FilterPipeline.SetPriority(pri + 1)
		}
	} else {
		params.FilterPipeline.SetPriority(0)
	}

	switch pri := params.FilterPipeline.GetPriority(); params.PriorityFlag {
	case PriorityMax:
		if p.maxSubPri < math.MaxUint64 {
			p.maxSubPri++
		}

		params.FilterPipeline.SetPriority(p.maxSubPri)
	case PriorityValue:
		if pri > p.maxSubPri {
			p.maxSubPri = pri
		}
	}

	if params.OnFail <= 0 {
		params.OnFail = CodeIgnore
	}

	if params.OnIgnore <= 0 {
		params.OnIgnore = CodeIgnore
	}

	if params.OnPass <= 0 {
		params.OnPass = CodeIgnore
	}

	p.mSubResult[name] = map[FilterCode]FilterCode{
		CodePass:   params.OnPass,
		CodeIgnore: params.OnIgnore,
		CodeFail:   params.OnFail,
	}

	p.subFilters = append(p.subFilters, params.FilterPipeline)

	sort.Sort(filterPipelineSet(p.subFilters))

	return nil
}

func (p *filterPipeline) GetPriority() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.pri
}
func (p *filterPipeline) SetPriority(pri uint64) {
	p.Lock()
	p.pri = pri
	p.Unlock()
}

func (p *filterPipeline) GetName() string {
	p.RLock()
	defer p.RUnlock()

	if p.name == "" {
		return "FILTER_UNNAMED"
	}

	return p.name
}
