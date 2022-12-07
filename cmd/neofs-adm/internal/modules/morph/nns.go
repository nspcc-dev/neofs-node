package morph

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-contract/nns"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type NNSState struct {
	Price   int64    `json:"price"`
	Domains []Domain `json:"domains"`
}

type Record struct {
	Name string         `json:"name"`
	Type nns.RecordType `json:"type"`
	ID   int64          `json:"id"`
	Data string         `json:"data"`
}

type Domain struct {
	Name       string    `json:"name"`
	SOA        SOA       `json:"soa"`
	Records    []*Record `json:"records"`
	IsRoot     bool      `json:"isRoot"`
	SubDomains []Domain  `json:"subDomains"`
}

type SOA struct {
	Name    string `json:"name"`
	Email   string `json:"email"`
	Serial  int64  `json:"serial"`
	Refresh int64  `json:"refresh"`
	Retry   int64  `json:"retry"`
	Expire  int64  `json:"expire"`
	TTL     int64  `json:"ttl"`
}

func (d *Domain) ParseSOA(record *Record) error {
	if record.Type != nns.SOA {
		return fmt.Errorf("record has wrong type %d", record.Type)
	}

	arr := strings.Split(record.Data, " ")
	if len(arr) != 7 {
		return fmt.Errorf("invalid soa format '%s'", record.Data)
	}

	if arr[0] != record.Name {
		return fmt.Errorf("invalid soa format: '%s'", record.Data)
	}

	serial, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid soa serial '%s': %w", arr[2], err)
	}
	refresh, err := strconv.ParseInt(arr[3], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid soa refresh '%s': %w", arr[3], err)
	}
	retry, err := strconv.ParseInt(arr[4], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid soa retry '%s': %w", arr[4], err)
	}
	expire, err := strconv.ParseInt(arr[5], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid soa expire '%s': %w", arr[5], err)
	}
	ttl, err := strconv.ParseInt(arr[6], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid soa ttl '%s': %w", arr[6], err)
	}

	d.SOA = SOA{
		Name:    arr[0],
		Email:   arr[1],
		Serial:  serial,
		Refresh: refresh,
		Retry:   retry,
		Expire:  expire,
		TTL:     ttl,
	}
	return nil
}

// FromStackItem implements stackitem.Convertible.
func (r *Record) FromStackItem(item stackitem.Item) error {
	arr, ok := item.Value().([]stackitem.Item)
	if !ok || len(arr) != 4 {
		return errors.New("invalid stack item type")
	}

	nameBytes, err := arr[0].TryBytes()
	if err != nil {
		return fmt.Errorf("invalid record name: %w", err)
	}
	integer, err := arr[1].TryInteger()
	if err != nil {
		return fmt.Errorf("invalid nns type: %w", err)
	}
	typeBytes := integer.Bytes()
	if len(typeBytes) != 1 {
		return errors.New("invalid nns type")
	}

	dataBytes, err := arr[2].TryBytes()
	if err != nil {
		return fmt.Errorf("invalid record data: %w", err)
	}

	idInt, err := arr[3].TryInteger()
	if err != nil {
		return fmt.Errorf("invalid record id: %w", err)
	}

	r.Name = string(nameBytes)
	r.Type = nns.RecordType(typeBytes[0])
	r.Data = string(dataBytes)
	r.ID = idInt.Int64()

	return nil
}

// IsDuplicate checks if current record the same as provided one (in terms of nns contract).
func (r *Record) IsDuplicate(rec *Record) bool {
	return r.Name == rec.Name && r.Type == rec.Type && r.Data == rec.Data
}

var errInvalidNNSResponse = errors.New("invalid response from NNS contract")

func dumpNNS(cmd *cobra.Command, _ []string) error {
	filename, err := cmd.Flags().GetString(nnsDumpFlag)
	if err != nil {
		return fmt.Errorf("invalid filename: %w", err)
	}

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	ch, err := getNNSContractHash(cmd, c)
	if err != nil {
		return fmt.Errorf("unable to get contaract hash: %w", err)
	}

	inv := invoker.New(c, nil)

	price, err := unwrap.Int64(inv.Call(ch, "getPrice"))
	if err != nil {
		return fmt.Errorf("unable to get renew price: %w", err)
	}

	tokens, err := getTokens(inv, ch)
	if err != nil {
		return fmt.Errorf("%w: %v", errInvalidNNSResponse, err)
	}

	tree := make(map[string][]string)
	domains := make(map[string]Domain, len(tokens))

	for _, token := range tokens {
		domain := Domain{Name: token}
		split := strings.Split(token, ".")
		domain.IsRoot = len(split) == 1

		parentDomain := strings.Join(split[1:], ".")
		subDomains := tree[parentDomain]
		tree[parentDomain] = append(subDomains, token)

		records, err := getAllDomainRecords(inv, ch, token)
		if err != nil {
			return fmt.Errorf("get all domain records '%s': %w", token, err)
		}

		soaIndex := -1
		for j, record := range records {
			if record.Type == nns.SOA {
				if soaIndex != -1 {
					return fmt.Errorf("%w: multiple soa records for '%s'", errInvalidNNSResponse, record.Name)
				}
				soaIndex = j
			}
		}
		if soaIndex == -1 {
			return fmt.Errorf("missed SOA records for '%s", token)
		}
		if err = domain.ParseSOA(records[soaIndex]); err != nil {
			return fmt.Errorf("%w: parse soa record '%s': %v", errInvalidNNSResponse, records[soaIndex].Name, err)
		}

		domain.Records = append(records[:soaIndex], records[soaIndex+1:]...)
		domains[token] = domain
	}

	nnsState := &NNSState{
		Price:   price,
		Domains: getSubDomains("", tree, domains),
	}

	out, err := json.Marshal(nnsState)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, out, 0o660)
}

func getSubDomains(domain string, tree map[string][]string, domains map[string]Domain) []Domain {
	subDomains := tree[domain]

	res := make([]Domain, len(subDomains))
	for i, subDomain := range subDomains {
		res[i] = domains[subDomain]
		res[i].SubDomains = getSubDomains(subDomain, tree, domains)
	}

	return res
}

func restoreNNS(cmd *cobra.Command, _ []string) error {
	filename, err := cmd.Flags().GetString(containerDumpFlag)
	if err != nil {
		return fmt.Errorf("invalid filename: %w", err)
	}

	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return err
	}
	defer wCtx.close()

	ch, err := getNNSContractHash(cmd, wCtx.Client)
	if err != nil {
		return fmt.Errorf("can't get NNS contract state: %w", err)
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("can't read dump file: %w", err)
	}

	nnsState := &NNSState{}
	err = json.Unmarshal(data, &nnsState)
	if err != nil {
		return fmt.Errorf("can't parse dump file: %w", err)
	}

	bw := io.NewBufBinWriter()

	emit.AppCall(bw.BinWriter, ch, "setPrice", callflag.All, nnsState.Price)
	if err = wCtx.sendCommitteeTx(bw.Bytes(), true); err != nil {
		return fmt.Errorf("can't set price: %w", err)
	}

	if err = restoreDomains(wCtx, nnsState.Domains, ch, bw); err != nil {
		return fmt.Errorf("can't restore root domains: %w", err)
	}

	return wCtx.awaitTx()
}

func restoreDomains(wCtx *initializeContext, domains []Domain, ch util.Uint160, bw *io.BufBinWriter) error {
	var existedRecs []*Record

	for _, domain := range domains {
		wCtx.Command.Printf("Restoring domain '%s'\n", domain.Name)

		isAvailable, err := unwrap.Bool(wCtx.ReadOnlyInvoker.Call(ch, "isAvailable", domain.Name))
		if err != nil {
			return fmt.Errorf("can't check if domain is already restored '%s': %w", domain.Name, err)
		}

		bw.Reset()

		if isAvailable {
			soa := domain.SOA
			emit.AppCall(bw.BinWriter, ch, "register", callflag.All,
				soa.Name, wCtx.ConsensusAcc.Contract.ScriptHash(), soa.Email, soa.Refresh, soa.Retry, soa.Expire, soa.TTL)
		} else {
			if len(domain.Records) != 0 {
				existedRecs, err = getAllDomainRecords(wCtx.ReadOnlyInvoker, ch, domain.Name)
				if err != nil {
					return fmt.Errorf("can't get all existed records '%s': %w", domain.Name, err)
				}
			}
			wCtx.Command.Printf("Root domain '%s' is already restored.\n", domain.Name)
		}

		filtered := filterRecords(domain.Records, existedRecs)
		if len(filtered) != 0 || isAvailable {
			for _, record := range filtered {
				emit.AppCall(bw.BinWriter, ch, "addRecord", callflag.All,
					record.Name, int64(record.Type), record.Data)
			}
			if bw.Err != nil {
				panic(bw.Err)
			}

			if err = wCtx.sendConsensusTx(bw.Bytes()); err != nil {
				return fmt.Errorf("can't send records consensus tx '%s': %w", domain.Name, err)
			}

			if err = wCtx.awaitTx(); err != nil {
				return fmt.Errorf("failed to await txs: %w", err)
			}
		}

		if err = restoreDomains(wCtx, domain.SubDomains, ch, bw); err != nil {
			return fmt.Errorf("can't restore subDomains for '%s': %w", domain.Name, err)
		}
	}
	return nil
}

func filterRecords(recordsToRestore, alreadyExistedRecords []*Record) []*Record {
	filtered := make([]*Record, 0, len(recordsToRestore))

LOOP:
	for _, record := range recordsToRestore {
		for _, existedRecord := range alreadyExistedRecords {
			if record.IsDuplicate(existedRecord) {
				continue LOOP
			}
		}
		filtered = append(filtered, record)
	}

	return filtered
}

func getNNSContractHash(cmd *cobra.Command, c Client) (util.Uint160, error) {
	s, err := cmd.Flags().GetString(nnsContractFlag)
	var ch util.Uint160
	if err == nil {
		ch, err = util.Uint160DecodeStringLE(s)
	}
	if err != nil {
		nnsCs, err := c.GetContractStateByID(1)
		if err != nil {
			return util.Uint160{}, fmt.Errorf("can't get NNS contract state: %w", err)
		}
		ch = nnsCs.Hash
	}
	return ch, nil
}

func getDomainsRoots(inv *invoker.Invoker, ch util.Uint160) ([]string, error) {
	r, err := inv.Call(ch, "roots")
	items, err := readOutIterator(inv, r, err)
	if err != nil {
		return nil, fmt.Errorf("read all from iterator: %w", err)
	}

	roots := make([]string, len(items))
	for i, item := range items {
		rootByte, err := item.TryBytes()
		if err != nil {
			return nil, fmt.Errorf("invalid root item type: %w", err)
		}

		roots[i] = string(rootByte)
	}

	return roots, nil
}

func getTokens(inv *invoker.Invoker, ch util.Uint160) ([]string, error) {
	r, err := inv.Call(ch, "tokens")
	items, err := readOutIterator(inv, r, err)
	if err != nil {
		return nil, fmt.Errorf("read all from iterator: %w", err)
	}

	roots := make([]string, len(items))
	for i, item := range items {
		rootByte, err := item.TryBytes()
		if err != nil {
			return nil, fmt.Errorf("invalid root item type: %w", err)
		}

		roots[i] = string(rootByte)
	}

	return roots, nil
}

func readOutIterator(inv *invoker.Invoker, r *result.Invoke, err error) ([]stackitem.Item, error) {
	sessionID, iterator, err := unwrap.SessionIterator(r, err)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidNNSResponse, err)
	}

	var shouldStop bool
	batchSize := 50

	results := make([]stackitem.Item, 0, batchSize)
	for !shouldStop {
		batch, err := inv.TraverseIterator(sessionID, &iterator, batchSize)
		if err != nil {
			return nil, err
		}

		results = append(results, batch...)
		shouldStop = len(batch) < batchSize
	}

	return results, nil
}

func getRecordsByItems(items []stackitem.Item) ([]*Record, error) {
	res := make([]*Record, len(items))
	for i, item := range items {
		res[i] = new(Record)

		if err := res[i].FromStackItem(item); err != nil {
			return nil, fmt.Errorf("record from stack item: %w", err)
		}
	}

	return res, nil
}

func getAllDomainRecords(inv *invoker.Invoker, ch util.Uint160, name string) ([]*Record, error) {
	r, err := inv.Call(ch, "getAllRecords", name)
	items, err := readOutIterator(inv, r, err)
	if err != nil {
		return nil, fmt.Errorf("read all from iterator: %w", err)
	}

	return getRecordsByItems(items)
}
