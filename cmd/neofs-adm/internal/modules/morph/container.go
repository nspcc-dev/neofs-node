package morph

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var errInvalidContainerResponse = errors.New("invalid response from container contract")

func dumpContainers(cmd *cobra.Command, _ []string) error {
	filename, err := cmd.Flags().GetString(containerDumpFlag)
	if err != nil {
		return fmt.Errorf("invalid filename: %w", err)
	}

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	nnsCs, err := c.GetContractStateByID(1)
	if err != nil {
		return fmt.Errorf("can't get NNS contract state: %w", err)
	}

	ch, err := nnsResolveHash(c, nnsCs.Hash, containerContract+".neofs")
	if err != nil {
		return fmt.Errorf("can't fetch container contract hash: %w", err)
	}

	res, err := c.InvokeFunction(ch, "list",
		[]smartcontract.Parameter{{Type: smartcontract.StringType, Value: ""}}, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", errInvalidContainerResponse, err)
	}

	var cids [][]byte
	arr, ok := res.Stack[0].Value().([]stackitem.Item)
	if !ok {
		return fmt.Errorf("%w: not a struct", errInvalidContainerResponse)
	}
	for _, item := range arr {
		id, err := item.TryBytes()
		if err != nil {
			return fmt.Errorf("%w: %v", errInvalidContainerResponse, err)
		}
		cids = append(cids, id)
	}

	var containers []*Container
	bw := io.NewBufBinWriter()
	for _, id := range cids {
		bw.Reset()
		emit.AppCall(bw.BinWriter, ch, "get", callflag.All, id)
		emit.AppCall(bw.BinWriter, ch, "eACL", callflag.All, id)
		res, err := c.InvokeScript(bw.Bytes(), nil)
		if err != nil {
			return fmt.Errorf("can't get container info: %w", err)
		}
		if len(res.Stack) != 2 {
			return fmt.Errorf("%w: expected 2 items on stack", errInvalidContainerResponse)
		}

		cnt := new(Container)
		err = cnt.FromStackItem(res.Stack[0])
		if err != nil {
			return fmt.Errorf("%w: %v", errInvalidContainerResponse, err)
		}

		ea := new(EACL)
		err = ea.FromStackItem(res.Stack[1])
		if err != nil {
			return fmt.Errorf("%w: %v", errInvalidContainerResponse, err)
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
	return ioutil.WriteFile(filename, out, 0o660)
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
