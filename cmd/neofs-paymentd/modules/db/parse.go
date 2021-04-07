package db

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

type transferEvent struct {
	from, to util.Uint160
	amount   int64
	details  []byte
}

const TransferEventName = "TransferX"

var (
	errParseTransferEvent = errors.New("transfer event does not contain array")
	errInvalidParamNumber = errors.New("transfer event does not contain 4 attributes")
)

func (db *HistoryDB) parseBlock(b *block.Block, cli *client.Client, key util.Uint160) error {
	for _, tx := range b.Transactions {
		appLog, err := cli.GetApplicationLog(tx.Hash(), nil)
		if err != nil {
			return fmt.Errorf("application log of tx %s is not available: %w", tx.Hash().StringLE(), err)
		}

		for _, exec := range appLog.Executions {
			for _, event := range exec.Events {
				err = db.parseEvent(event, key)
				if err != nil {
					return fmt.Errorf("can't parse event %s in %s: %w", event.Name, tx.Hash().StringLE(), err)
				}
			}
		}
	}

	return nil
}

func (db *HistoryDB) parseEvent(event state.NotificationEvent, target util.Uint160) error {
	if event.Name != TransferEventName {
		return nil
	}

	transferEvent, err := parseTransferXEvent(event)
	if err != nil {
		return err
	}

	if transferEvent.from.Equals(target) {
		db.AddOutcomeRecord(transferEvent.to, transferEvent.details, transferEvent.amount, util.Uint256{})
		return nil
	}

	if transferEvent.to.Equals(target) {
		db.AddIncomeRecord(transferEvent.from, transferEvent.details, transferEvent.amount, util.Uint256{})
		return nil
	}

	return nil
}

func parseTransferXEvent(event state.NotificationEvent) (*transferEvent, error) {
	arr, ok := event.Item.Value().([]stackitem.Item)
	if !ok {
		return nil, errParseTransferEvent
	}

	if len(arr) != 4 {
		return nil, errInvalidParamNumber
	}

	var from, to util.Uint160

	fromBytes, err := arr[0].TryBytes()
	if err == nil {
		from, err = util.Uint160DecodeBytesBE(fromBytes)
		if err != nil {
			return nil, fmt.Errorf("can't decode from as Uint160 value: %w", err)
		}
	}

	toBytes, err := arr[1].TryBytes()
	if err == nil {
		to, err = util.Uint160DecodeBytesBE(toBytes)
		if err != nil {
			return nil, fmt.Errorf("can't decode to as Uint160 value: %w", err)
		}
	}

	amountBig, err := arr[2].TryInteger()
	if err != nil {
		return nil, fmt.Errorf("can't parse amount attribute of TransferX event: %w", err)
	}

	details, err := arr[3].TryBytes()
	if err != nil {
		return nil, fmt.Errorf("can't parse details attribute of TransferX event: %w", err)
	}

	return &transferEvent{
		from:    from,
		to:      to,
		amount:  amountBig.Int64(),
		details: details,
	}, nil
}
