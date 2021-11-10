package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// ConfigArgs groups the arguments
// of get config value test invoke call.
type ConfigArgs struct {
	key []byte
}

// SetKey sets binary key to configuration parameter.
func (c *ConfigArgs) SetKey(v []byte) {
	c.key = v
}

// ConfigValues groups the stack parameters
// returned by get config test invoke.
type ConfigValues struct {
	val interface{}
}

// Value returns configuration value.
func (c ConfigValues) Value() interface{} {
	return c.val
}

// Config performs the test invoke of get config value
// method of NeoFS Netmap contract.
func (c *Client) Config(args ConfigArgs, assert func(stackitem.Item) (interface{}, error)) (*ConfigValues, error) {
	prm := client.TestInvokePrm{}

	prm.SetMethod(c.configMethod)
	prm.SetArgs(args.key)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			c.configMethod, err)
	}

	if ln := len(items); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d",
			c.configMethod, ln)
	}

	val, err := assert(items[0])
	if err != nil {
		return nil, fmt.Errorf("value type assertion failed: %w", err)
	}

	return &ConfigValues{
		val: val,
	}, nil
}

// SetConfigPrm groups parameters of SetConfig operation.
type SetConfigPrm struct {
	id    []byte
	key   []byte
	value interface{}

	client.InvokePrmOptional
}

// SetID sets ID of the config value.
func (s *SetConfigPrm) SetID(id []byte) {
	s.id = id
}

// SetKey sets key of the config value.
func (s *SetConfigPrm) SetKey(key []byte) {
	s.key = key
}

// SetValue sets value of the config value.
func (s *SetConfigPrm) SetValue(value interface{}) {
	s.value = value
}

// SetConfig invokes `setConfig` method of NeoFS Netmap contract.
func (c *Client) SetConfig(args SetConfigPrm) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.setConfigMethod)
	prm.SetArgs(args.id, args.key, args.value)
	prm.InvokePrmOptional = args.InvokePrmOptional

	return c.client.Invoke(prm)
}

func IntegerAssert(item stackitem.Item) (interface{}, error) {
	return client.IntFromStackItem(item)
}

func StringAssert(item stackitem.Item) (interface{}, error) {
	return client.StringFromStackItem(item)
}

// ListConfigArgs groups the arguments
// of config listing test invoke call.
type ListConfigArgs struct {
}

// ListConfigValues groups the stack parameters
// returned by config listing test invoke.
type ListConfigValues struct {
	rs []stackitem.Item
}

// IterateRecords iterates over all config records and passes them to f.
//
// Returns f's errors directly.
func (x ListConfigValues) IterateRecords(f func(key, value []byte) error) error {
	for i := range x.rs {
		fields, err := client.ArrayFromStackItem(x.rs[i])
		if err != nil {
			return fmt.Errorf("record fields: %w", err)
		}

		if ln := len(fields); ln != 2 {
			return fmt.Errorf("unexpected record fields number: %d", ln)
		}

		k, err := client.BytesFromStackItem(fields[0])
		if err != nil {
			return fmt.Errorf("record key: %w", err)
		}

		v, err := client.BytesFromStackItem(fields[1])
		if err != nil {
			return fmt.Errorf("record value: %w", err)
		}

		if err := f(k, v); err != nil {
			return err
		}
	}

	return nil
}

// ListConfig performs the test invoke of config listing method of NeoFS Netmap contract.
func (c *Client) ListConfig(_ ListConfigArgs) (*ListConfigValues, error) {
	prm := client.TestInvokePrm{}

	prm.SetMethod(c.configListMethod)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			c.configListMethod, err)
	}

	if ln := len(items); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d",
			c.configListMethod, ln)
	}

	arr, err := client.ArrayFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("record list (%s): %w",
			c.configListMethod, err)
	}

	return &ListConfigValues{
		rs: arr,
	}, nil
}
