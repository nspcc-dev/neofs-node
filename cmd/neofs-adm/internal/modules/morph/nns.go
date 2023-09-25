package morph

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	nnsrpc "github.com/nspcc-dev/neofs-contract/rpc/nns"
)

// Various NeoFS NNS errors.
var (
	errDomainNotFound       = errors.New("domain not found")
	errMissingDomainRecords = errors.New("missing domain records")
)

func invalidNNSDomainRecordError(cause error) error {
	return fmt.Errorf("invalid domain record: %w", cause)
}

var errBreakIterator = errors.New("break iterator")

// iterates over text records of the specified NeoFS NNS domain and passes them
// into f. Breaks on any f's error and returns it (if f returns
// errBreakIterator, iterateNNSDomainTextRecords returns no error). Returns
// errDomainNotFound if domain is missing in the NNS. Returns
// errMissingDomainRecords if domain exists but has no records.
func iterateNNSDomainTextRecords(inv nnsrpc.Invoker, nnsContractAddr util.Uint160, domain string, f func(string) error) error {
	nnsContract := nnsrpc.NewReader(inv, nnsContractAddr)

	sessionID, iter, err := nnsContract.GetAllRecords(domain)
	if err != nil {
		// Track https://github.com/nspcc-dev/neofs-node/issues/2583.
		if strings.Contains(err.Error(), "token not found") {
			return errDomainNotFound
		}

		return fmt.Errorf("get all records of the NNS domain: %w", err)
	}

	defer func() {
		_ = inv.TerminateSession(sessionID)
	}()

	hasRecords := false

	for {
		items, err := inv.TraverseIterator(sessionID, &iter, 10)
		if err != nil {
			return fmt.Errorf("NNS domain records' iterator break: %w", err)
		}

		if len(items) == 0 {
			if hasRecords {
				return nil
			}

			return errMissingDomainRecords
		}

		hasRecords = true

		for i := range items {
			fields, ok := items[i].Value().([]stackitem.Item)
			if !ok {
				return invalidNNSDomainRecordError(
					fmt.Errorf("unexpected type %s instead of %s", stackitem.StructT, items[i].Type()))
			}

			if len(fields) < 3 {
				return invalidNNSDomainRecordError(
					fmt.Errorf("unsupported number of struct fields: expected at least 3, got %d", len(fields)))
			}

			_, err = fields[0].TryBytes()
			if err != nil {
				return invalidNNSDomainRecordError(
					fmt.Errorf("1st field is not a byte array: got %v", fields[0].Type()))
			}

			typ, err := fields[1].TryInteger()
			if err != nil {
				return invalidNNSDomainRecordError(fmt.Errorf("2nd field is not an integer: got %v", fields[1].Type()))
			}

			if typ.Cmp(nnsrpc.TXT) != 0 {
				continue
			}

			data, err := fields[2].TryBytes()
			if err != nil {
				return invalidNNSDomainRecordError(
					fmt.Errorf("3rd field is not a byte array: got %v", fields[2].Type()))
			}

			if err = f(string(data)); err != nil {
				if errors.Is(err, errBreakIterator) {
					return nil
				}

				return err
			}
		}
	}
}
