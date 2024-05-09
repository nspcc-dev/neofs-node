package morph

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
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
	res, err := inv.Call(ch, "list", "")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidContainerResponse, err)
	}
	itm, err := unwrap.Item(res, err)
	if _, ok := itm.(stackitem.Null); !ok {
		return unwrap.ArrayOfBytes(res, err)
	}
	return nil, nil
}

func dumpContainers(cmd *cobra.Command, _ []string) error {
	filename, err := cmd.Flags().GetString(containerDumpFlag)
	if err != nil {
		return fmt.Errorf("invalid filename: %w", err)
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

	cids, err := getContainersList(inv, ch)
	if err != nil {
		return fmt.Errorf("%w: %w", errInvalidContainerResponse, err)
	}

	isOK, err := getCIDFilterFunc(cmd)
	if err != nil {
		return err
	}

	var containers []*Container
	b := smartcontract.NewBuilder()
	for _, id := range cids {
		if !isOK(id) {
			continue
		}
		b.Reset()
		b.InvokeMethod(ch, "get", id)
		b.InvokeMethod(ch, "eACL", id)

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

	isOK, err := getCIDFilterFunc(cmd)
	if err != nil {
		return err
	}

	b := smartcontract.NewBuilder()
	for _, cnt := range containers {
		hv := hash.Sha256(cnt.Value)
		if !isOK(hv[:]) {
			continue
		}
		b.Reset()
		b.InvokeMethod(ch, "get", hv.BytesBE())

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
			var id cid.ID
			id.SetSHA256(hv)
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
		return errors.New("invalid container value")
	}

	sig, err := arr[1].TryBytes()
	if err != nil {
		return errors.New("invalid container signature")
	}

	pub, err := arr[2].TryBytes()
	if err != nil {
		return errors.New("invalid container public key")
	}

	tok, err := arr[3].TryBytes()
	if err != nil {
		return errors.New("invalid container token")
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
		return errors.New("invalid eACL value")
	}

	sig, err := arr[1].TryBytes()
	if err != nil {
		return errors.New("invalid eACL signature")
	}

	pub, err := arr[2].TryBytes()
	if err != nil {
		return errors.New("invalid eACL public key")
	}

	tok, err := arr[3].TryBytes()
	if err != nil {
		return errors.New("invalid eACL token")
	}

	c.Value = value
	c.Signature = sig
	c.PublicKey = pub
	c.Token = tok
	return nil
}

// getCIDFilterFunc returns filtering function for container IDs.
// Raw byte slices are used because it works with structures returned
// from contract.
func getCIDFilterFunc(cmd *cobra.Command) (func([]byte) bool, error) {
	rawIDs, err := cmd.Flags().GetStringSlice(containerIDsFlag)
	if err != nil {
		return nil, err
	}
	if len(rawIDs) == 0 {
		return func([]byte) bool { return true }, nil
	}

	for i := range rawIDs {
		err := new(cid.ID).DecodeString(rawIDs[i])
		if err != nil {
			return nil, fmt.Errorf("can't parse CID %s: %w", rawIDs[i], err)
		}
	}
	sort.Strings(rawIDs)
	return func(rawID []byte) bool {
		var v [32]byte
		copy(v[:], rawID)

		var id cid.ID
		id.SetSHA256(v)
		idStr := id.EncodeToString()
		n := sort.Search(len(rawIDs), func(i int) bool { return rawIDs[i] >= idStr })
		return n < len(rawIDs) && rawIDs[n] == idStr
	}, nil
}
