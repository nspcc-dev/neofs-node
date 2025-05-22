package fschain

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var errInvalidContainerResponse = errors.New("invalid response from container contract")

func getContainerContractHash(cmd *cobra.Command, inv *invoker.Invoker, c Client) (util.Uint160, error) {
	s, err := cmd.Flags().GetString(containerContractFlag)
	var ch util.Uint160
	if err == nil {
		ch, err = util.Uint160DecodeStringLE(s)
	}
	if err != nil {
		nnsReader, err := nns.NewInferredReader(c, inv)
		if err != nil {
			return ch, fmt.Errorf("can't find NNS contract: %w", err)
		}

		ch, err = nnsReader.ResolveFSContract(nns.NameContainer)
		if err != nil {
			return ch, fmt.Errorf("can't fetch container contract hash: %w", err)
		}
	}
	return ch, nil
}

func getContainersList(inv *invoker.Invoker, ch util.Uint160) ([][]byte, error) {
	res, err := unwrap.ArrayOfBytes(inv.Call(ch, "list", ""))
	if err != nil && !errors.Is(err, unwrap.ErrNull) {
		return nil, fmt.Errorf("%w: %w", errInvalidContainerResponse, err)
	}
	return res, nil
}

func dumpContainers(cmd *cobra.Command, _ []string) error {
	filename, err := cmd.Flags().GetString(containerDumpFlag)
	if err != nil {
		return fmt.Errorf("invalid filename: %w", err)
	}

	requestedIDs, err := getCIDs(cmd)
	if err != nil {
		return err
	}

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	inv := invoker.New(c, nil)

	ch, err := getContainerContractHash(cmd, inv, c)
	if err != nil {
		return fmt.Errorf("unable to get contaract hash: %w", err)
	}

	var containers []*Container
	b := smartcontract.NewBuilder()
	for id := range requestedIDs {
		b.Reset()
		b.InvokeMethod(ch, "get", id[:])
		b.InvokeMethod(ch, "eACL", id[:])

		script, err := b.Script()
		if err != nil {
			return fmt.Errorf("dumping '%s' container script: %w", id, err)
		}

		res, err := inv.Run(script)
		if err != nil {
			return fmt.Errorf("can't get container info: %w", err)
		}
		if len(res.Stack) != 2 {
			return fmt.Errorf("%w: expected 2 items on stack", errInvalidContainerResponse)
		}

		cnt := new(Container)
		err = cnt.FromStackItem(res.Stack[0])
		if err != nil {
			return fmt.Errorf("%w: %w", errInvalidContainerResponse, err)
		}

		ea := new(EACL)
		err = ea.FromStackItem(res.Stack[1])
		if err != nil {
			return fmt.Errorf("%w: %w", errInvalidContainerResponse, err)
		}
		if len(ea.Value) != 0 {
			cnt.EACL = ea
		}

		containers = append(containers, cnt)
	}

	out, err := json.Marshal(containers)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, out, 0o640)
}

func listContainers(cmd *cobra.Command, _ []string) error {
	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	inv := invoker.New(c, nil)

	ch, err := getContainerContractHash(cmd, inv, c)
	if err != nil {
		return fmt.Errorf("unable to get contaract hash: %w", err)
	}

	cids, err := getContainersList(inv, ch)
	if err != nil {
		return fmt.Errorf("%w: %w", errInvalidContainerResponse, err)
	}

	for _, id := range cids {
		var idCnr cid.ID
		err = idCnr.Decode(id)
		if err != nil {
			return fmt.Errorf("unable to decode container id: %w", err)
		}
		cmd.Println(idCnr)
	}
	return nil
}

func restoreContainers(cmd *cobra.Command, _ []string) error {
	filename, err := cmd.Flags().GetString(containerDumpFlag)
	if err != nil {
		return fmt.Errorf("invalid filename: %w", err)
	}

	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return err
	}
	defer wCtx.close()

	nnsReader, err := nns.NewInferredReader(wCtx.Client, wCtx.ReadOnlyInvoker)
	if err != nil {
		return fmt.Errorf("can't find NNS contract: %w", err)
	}

	ch, err := nnsReader.ResolveFSContract(nns.NameContainer)
	if err != nil {
		return fmt.Errorf("can't fetch container contract hash: %w", err)
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("can't read dump file: %w", err)
	}

	var containers []Container
	err = json.Unmarshal(data, &containers)
	if err != nil {
		return fmt.Errorf("can't parse dump file: %w", err)
	}

	requested, err := getCIDs(cmd)
	if err != nil {
		return err
	}

	var id cid.ID
	b := smartcontract.NewBuilder()
	for _, cnt := range containers {
		id = cid.NewFromMarshalledContainer(cnt.Value)
		if _, ok := requested[id]; !ok {
			continue
		}
		b.Reset()
		b.InvokeMethod(ch, "get", id[:])

		script, err := b.Script()
		if err != nil {
			return fmt.Errorf("reading container script: %w", err)
		}

		res, err := wCtx.Client.InvokeScript(script, nil)
		if err != nil {
			return fmt.Errorf("can't check if container is already restored: %w", err)
		}
		if len(res.Stack) == 0 {
			return errors.New("empty stack")
		}

		old := new(Container)
		if err := old.FromStackItem(res.Stack[0]); err != nil {
			return fmt.Errorf("%w: %w", errInvalidContainerResponse, err)
		}
		if len(old.Value) != 0 {
			cmd.Printf("Container %s is already deployed.\n", id)
			continue
		}

		b.Reset()
		b.InvokeMethod(ch, "put", cnt.Value, cnt.Signature, cnt.PublicKey, cnt.Token)
		if ea := cnt.EACL; ea != nil {
			b.InvokeMethod(ch, "setEACL", ea.Value, ea.Signature, ea.PublicKey, ea.Token)
		}

		script, err = b.Script()
		if err != nil {
			return fmt.Errorf("container update script: %w", err)
		}

		if err := wCtx.sendConsensusTx(script); err != nil {
			return err
		}
	}

	return wCtx.awaitTx()
}

// Container represents container struct in contract storage.
type Container struct {
	Value     []byte `json:"value"`
	Signature []byte `json:"signature"`
	PublicKey []byte `json:"public_key"`
	Token     []byte `json:"token"`
	EACL      *EACL  `json:"eacl"`
}

// EACL represents extended ACL struct in contract storage.
type EACL struct {
	Value     []byte `json:"value"`
	Signature []byte `json:"signature"`
	PublicKey []byte `json:"public_key"`
	Token     []byte `json:"token"`
}

// ToStackItem implements stackitem.Convertible.
func (c *Container) ToStackItem() (stackitem.Item, error) {
	return stackitem.NewStruct([]stackitem.Item{
		stackitem.NewByteArray(c.Value),
		stackitem.NewByteArray(c.Signature),
		stackitem.NewByteArray(c.PublicKey),
		stackitem.NewByteArray(c.Token),
	}), nil
}

// FromStackItem implements stackitem.Convertible.
func (c *Container) FromStackItem(item stackitem.Item) error {
	arr, ok := item.Value().([]stackitem.Item)
	if !ok || len(arr) != 4 {
		return errors.New("invalid stack item type")
	}

	value, err := arr[0].TryBytes()
	if err != nil {
		return fmt.Errorf("invalid container value: %w", err)
	}

	sig, err := arr[1].TryBytes()
	if err != nil {
		return fmt.Errorf("invalid container signature: %w", err)
	}

	pub, err := arr[2].TryBytes()
	if err != nil {
		return fmt.Errorf("invalid container public key: %w", err)
	}

	tok, err := arr[3].TryBytes()
	if err != nil {
		return fmt.Errorf("invalid container token: %w", err)
	}

	c.Value = value
	c.Signature = sig
	c.PublicKey = pub
	c.Token = tok
	return nil
}

// ToStackItem implements stackitem.Convertible.
func (c *EACL) ToStackItem() (stackitem.Item, error) {
	return stackitem.NewStruct([]stackitem.Item{
		stackitem.NewByteArray(c.Value),
		stackitem.NewByteArray(c.Signature),
		stackitem.NewByteArray(c.PublicKey),
		stackitem.NewByteArray(c.Token),
	}), nil
}

// FromStackItem implements stackitem.Convertible.
func (c *EACL) FromStackItem(item stackitem.Item) error {
	arr, ok := item.Value().([]stackitem.Item)
	if !ok || len(arr) != 4 {
		return errors.New("invalid stack item type")
	}

	value, err := arr[0].TryBytes()
	if err != nil {
		return fmt.Errorf("invalid eACL value: %w", err)
	}

	sig, err := arr[1].TryBytes()
	if err != nil {
		return fmt.Errorf("invalid eACL signature: %w", err)
	}

	pub, err := arr[2].TryBytes()
	if err != nil {
		return fmt.Errorf("invalid eACL public key: %w", err)
	}

	tok, err := arr[3].TryBytes()
	if err != nil {
		return fmt.Errorf("invalid eACL token: %w", err)
	}

	c.Value = value
	c.Signature = sig
	c.PublicKey = pub
	c.Token = tok
	return nil
}

// returns set of cid.ID specified via the 'cid' command flag. Function allows
// duplicates.
func getCIDs(cmd *cobra.Command) (map[cid.ID]struct{}, error) {
	rawIDs, err := cmd.Flags().GetStringSlice(containerIDsFlag)
	if err != nil {
		return nil, err
	}
	var id cid.ID
	res := make(map[cid.ID]struct{}, len(rawIDs))
	for i := range rawIDs {
		if err = id.DecodeString(rawIDs[i]); err != nil {
			return nil, fmt.Errorf("can't parse CID %s: %w", rawIDs[i], err)
		}
		res[id] = struct{}{}
	}
	return res, nil
}
