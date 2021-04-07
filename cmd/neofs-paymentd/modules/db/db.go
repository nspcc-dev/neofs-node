package db

import (
	"bytes"
	"fmt"
	"sync"
	"text/tabwriter"

	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

type (
	HistoryDB struct {
		mu      *sync.RWMutex
		height  uint32
		records []Record
	}

	Record struct {
		input   bool
		addr    util.Uint160
		amount  int64
		details []byte
		txHash  util.Uint256
	}
)

var (
	emptyAddress = util.Uint160{}
	bankAddress  = util.Uint160{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
)

func BuildDB(cli *client.Client, index uint32, key util.Uint160) (*HistoryDB, error) {
	result := &HistoryDB{
		mu:      new(sync.RWMutex),
		height:  index,
		records: []Record{},
	}

	err := result.Fill(cli, key)
	if err != nil {
		return nil, fmt.Errorf("can't build history data base: %w", err)
	}

	return result, nil
}

func (db *HistoryDB) Fill(cli *client.Client, key util.Uint160) error {
	height, err := cli.GetBlockCount()
	if err != nil {
		return fmt.Errorf("can't get chain height: %w", err)
	}

	err = db.fill(cli, db.height, height, key)
	if err != nil {
		return fmt.Errorf("can't fill history data base: %w", err)
	}

	return nil
}

func (db *HistoryDB) fill(cli *client.Client, from, to uint32, key util.Uint160) error {
	for i := from; i < to; i++ {
		chainBlock, err := cli.GetBlockByIndex(i)
		if err != nil {
			return fmt.Errorf("block %d is not available: %w", i, err)
		}

		err = db.parseBlock(chainBlock, cli, key)
		if err != nil {
			return fmt.Errorf("block %d cannot be parsed: %w", i, err)
		}
	}

	return nil
}

func (db *HistoryDB) AddIncomeRecord(addr util.Uint160, details []byte, amount int64, txHash util.Uint256) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.records = append(db.records, Record{
		input:   true,
		addr:    addr,
		amount:  amount,
		details: details,
		txHash:  txHash,
	})
}

func (db *HistoryDB) AddOutcomeRecord(addr util.Uint160, details []byte, amount int64, txHash util.Uint256) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.records = append(db.records, Record{
		input:   false,
		addr:    addr,
		amount:  amount,
		details: details,
		txHash:  txHash,
	})
}

func (db HistoryDB) String() string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	buf := bytes.NewBuffer(nil)

	w := tabwriter.NewWriter(buf, 0, 0, 1, ' ', 0)

	for _, record := range db.records {
		record.writeLine(w)
	}

	w.Flush()

	return buf.String()
}

func (r Record) writeLine(w *tabwriter.Writer) {
	var col1, col2, col3 string
	if r.input {
		col1 = "<- " + prettyFixed12(r.amount)
	} else {
		col1 = "-> " + prettyFixed12(r.amount)
	}

	if r.addr.Equals(emptyAddress) || r.addr.Equals(bankAddress) {
		col2 = "-"
	} else {
		col2 = address.Uint160ToString(r.addr)
	}

	col3 = prettyDetails(r.details)

	fmt.Fprintf(w, "%s\t%s\t%s\n", col1, col2, col3)
}
