package innerring

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep11"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	nnsrpc "github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/privatedomains"
)

// provides services of the NeoFS Name Service consumed by the Inner Ring node.
type neoFSNNS struct {
	invoker  nep11.Invoker
	contract *nnsrpc.ContractReader
}

// creates NeoFS Name Service provider working with the Neo smart contract
// deployed in the Neo network accessed through the specified [nep11.Invoker].
func newNeoFSNNS(contractAddress util.Uint160, contractCaller nep11.Invoker) *neoFSNNS {
	return &neoFSNNS{
		invoker:  contractCaller,
		contract: nnsrpc.NewReader(contractCaller, contractAddress),
	}
}

var errDomainNotFound = errors.New("domain not found")

type errWrongStackItemType struct {
	expected, got stackitem.Type
}

func wrongStackItemTypeError(expected, got stackitem.Type) errWrongStackItemType {
	return errWrongStackItemType{
		expected: expected,
		got:      got,
	}
}

func (x errWrongStackItemType) Error() string {
	return fmt.Sprintf("wrong type: expected %s, got %s", x.expected, x.got)
}

type errInvalidNumberOfStructFields struct {
	expected, got int
}

func invalidNumberOfStructFieldsError(expected, got int) errInvalidNumberOfStructFields {
	return errInvalidNumberOfStructFields{
		expected: expected,
		got:      got,
	}
}

func (x errInvalidNumberOfStructFields) Error() string {
	return fmt.Sprintf("invalid number of struct fields: expected %d, got %d", x.expected, x.got)
}

type errInvalidStructField struct {
	index uint
	cause error
}

func invalidStructFieldError(index uint, cause error) errInvalidStructField {
	return errInvalidStructField{
		index: index,
		cause: cause,
	}
}

func (x errInvalidStructField) Error() string {
	return fmt.Sprintf("invalid struct field #%d: %v", x.index, x.cause)
}

func (x errInvalidStructField) Unwrap() error {
	return x.cause
}

type errInvalidNNSDomainRecord struct {
	domain string
	record string
	cause  error
}

func invalidNNSDomainRecordError(domain, record string, cause error) errInvalidNNSDomainRecord {
	return errInvalidNNSDomainRecord{
		domain: domain,
		record: record,
		cause:  cause,
	}
}

func (x errInvalidNNSDomainRecord) Error() string {
	if x.record != "" {
		return fmt.Sprintf("invalid record %q of the NNS domain %q: %v", x.record, x.domain, x.cause)
	}

	return fmt.Sprintf("invalid record of the NNS domain %q: %v", x.domain, x.cause)
}

func (x errInvalidNNSDomainRecord) Unwrap() error {
	return x.cause
}

// CheckDomainRecord calls iterating 'getAllRecords' method of the parameterized
// Neo smart contract passing the given domain name. If contract throws 'token
// not found' exception, CheckDomainRecord returns errDomainNotFound. Each value
// in the resulting iterator is expected to be structure with at least 3 fields.
// If any value has the 2nd field is a number equal to 16 (TXT record type in
// the NNS) and the 3rd one is a string equal to the specified record,
// CheckDomainRecord returns nil. Otherwise,
// [privatedomains.ErrMissingDomainRecord] is returned.
func (x *neoFSNNS) CheckDomainRecord(domain string, record string) error {
	sessionID, iter, err := x.contract.GetAllRecords(domain)
	if err != nil {
		// Track https://github.com/nspcc-dev/neofs-node/issues/2583.
		if strings.Contains(err.Error(), "token not found") {
			return errDomainNotFound
		}

		return fmt.Errorf("get iterator over all records of the NNS domain %q: %w", domain, err)
	}

	defer func() {
		_ = x.invoker.TerminateSession(sessionID)
	}()

	hasRecords := false

	for {
		items, err := x.invoker.TraverseIterator(sessionID, &iter, 10)
		if err != nil {
			return fmt.Errorf("traverse iterator over all records of the NNS domain %q: %w", domain, err)
		}

		if len(items) == 0 {
			break
		}

		hasRecords = true

		for i := range items {
			fields, ok := items[i].Value().([]stackitem.Item)
			if !ok {
				return invalidNNSDomainRecordError(domain, "",
					wrongStackItemTypeError(stackitem.StructT, items[i].Type()))
			}

			if len(fields) < 3 {
				return invalidNNSDomainRecordError(domain, "",
					invalidNumberOfStructFieldsError(3, len(fields)))
			}

			_, err = fields[0].TryBytes()
			if err != nil {
				return invalidNNSDomainRecordError(domain, "",
					invalidStructFieldError(0, wrongStackItemTypeError(stackitem.ByteArrayT, fields[0].Type())))
			}

			typ, err := fields[1].TryInteger()
			if err != nil {
				return invalidNNSDomainRecordError(domain, "",
					invalidStructFieldError(1, wrongStackItemTypeError(stackitem.IntegerT, fields[1].Type())))
			}

			if typ.Cmp(nnsrpc.TXT) != 0 {
				continue
			}

			data, err := fields[2].TryBytes()
			if err != nil {
				return invalidNNSDomainRecordError(domain, "",
					invalidStructFieldError(2, wrongStackItemTypeError(stackitem.ByteArrayT, fields[2].Type())))
			}

			if string(data) == record {
				return nil
			}
		}
	}

	if hasRecords {
		return privatedomains.ErrMissingDomainRecord
	}

	return nil
}
