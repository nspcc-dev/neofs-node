package acl

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/acl"
	"github.com/stretchr/testify/require"
)

type testExtendedACLTable struct {
	records []acl.ExtendedACLRecord
}

type testRequestInfo struct {
	headers []acl.TypedHeader
	key     []byte
	opType  acl.OperationType
	target  acl.Target
}

type testEACLRecord struct {
	opType  acl.OperationType
	filters []acl.HeaderFilter
	targets []acl.ExtendedACLTarget
	action  acl.ExtendedACLAction
}

type testEACLTarget struct {
	target acl.Target
	keys   [][]byte
}

func (s testEACLTarget) Target() acl.Target {
	return s.target
}

func (s testEACLTarget) KeyList() [][]byte {
	return s.keys
}

func (s testEACLRecord) OperationType() acl.OperationType {
	return s.opType
}

func (s testEACLRecord) HeaderFilters() []acl.HeaderFilter {
	return s.filters
}

func (s testEACLRecord) TargetList() []acl.ExtendedACLTarget {
	return s.targets
}

func (s testEACLRecord) Action() acl.ExtendedACLAction {
	return s.action
}

func (s testRequestInfo) HeadersOfType(typ acl.HeaderType) ([]acl.Header, bool) {
	res := make([]acl.Header, 0, len(s.headers))

	for i := range s.headers {
		if s.headers[i].HeaderType() == typ {
			res = append(res, s.headers[i])
		}
	}

	return res, true
}

func (s testRequestInfo) Key() []byte {
	return s.key
}

func (s testRequestInfo) TypeOf(t acl.OperationType) bool {
	return s.opType == t
}

func (s testRequestInfo) TargetOf(t acl.Target) bool {
	return s.target == t
}

func (s testExtendedACLTable) Records() []acl.ExtendedACLRecord {
	return s.records
}

func TestExtendedACLChecker_Action(t *testing.T) {
	s := NewExtendedACLChecker()

	// nil ExtendedACLTable
	require.Equal(t, acl.ActionUndefined, s.Action(nil, nil))

	// create test ExtendedACLTable
	table := new(testExtendedACLTable)

	// nil RequestInfo
	require.Equal(t, acl.ActionUndefined, s.Action(table, nil))

	// create test RequestInfo
	req := new(testRequestInfo)

	// create test ExtendedACLRecord
	record := new(testEACLRecord)
	table.records = append(table.records, record)

	// set different OperationType
	record.opType = acl.OperationType(3)
	req.opType = record.opType + 1

	require.Equal(t, acl.ActionAllow, s.Action(table, req))

	// set equal OperationType
	req.opType = record.opType

	// create test ExtendedACLTarget through group
	target := new(testEACLTarget)
	record.targets = append(record.targets, target)

	// set not matching ExtendedACLTarget
	target.target = acl.Target(5)
	req.target = target.target + 1

	require.Equal(t, acl.ActionAllow, s.Action(table, req))

	// set matching ExtendedACLTarget
	req.target = target.target

	// create test HeaderFilter
	fHeader := new(testTypedHeader)
	hFilter := &testHeaderFilter{
		TypedHeader: fHeader,
	}
	record.filters = append(record.filters, hFilter)

	// create test TypedHeader
	header := new(testTypedHeader)
	req.headers = append(req.headers, header)

	// set not matching values
	header.t = hFilter.HeaderType() + 1

	require.Equal(t, acl.ActionAllow, s.Action(table, req))

	// set matching values
	header.k = "key"
	header.v = "value"

	fHeader.t = header.HeaderType()
	fHeader.k = header.Name()
	fHeader.v = header.Value()

	hFilter.t = acl.StringEqual

	// set ExtendedACLAction
	record.action = acl.ExtendedACLAction(7)

	require.Equal(t, record.action, s.Action(table, req))

	// set matching ExtendedACLTarget through key
	target.target = req.target + 1
	req.key = []byte{1, 2, 3}
	target.keys = append(target.keys, req.key)

	require.Equal(t, record.action, s.Action(table, req))
}
