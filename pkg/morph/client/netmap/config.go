package netmap

import (
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/encoding/bigint"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

const (
	maxObjectSizeConfig     = "MaxObjectSize"
	basicIncomeRateConfig   = "BasicIncomeRate"
	auditFeeConfig          = "AuditFee"
	epochDurationConfig     = "EpochDuration"
	containerFeeConfig      = "ContainerFee"
	containerAliasFeeConfig = "ContainerAliasFee"
	etIterationsConfig      = "EigenTrustIterations"
	etAlphaConfig           = "EigenTrustAlpha"
	irCandidateFeeConfig    = "InnerRingCandidateFee"
	withdrawFeeConfig       = "WithdrawFee"
)

// MaxObjectSize receives max object size configuration
// value through the Netmap contract call.
func (c *Client) MaxObjectSize() (uint64, error) {
	objectSize, err := c.readUInt64Config(maxObjectSizeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get epoch number: %w", c, err)
	}

	return objectSize, nil
}

// BasicIncomeRate returns basic income rate configuration value from network
// config in netmap contract.
func (c *Client) BasicIncomeRate() (uint64, error) {
	rate, err := c.readUInt64Config(basicIncomeRateConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get basic income rate: %w", c, err)
	}

	return rate, nil
}

// AuditFee returns audit fee configuration value from network
// config in netmap contract.
func (c *Client) AuditFee() (uint64, error) {
	fee, err := c.readUInt64Config(auditFeeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get audit fee: %w", c, err)
	}

	return fee, nil
}

// EpochDuration returns number of sidechain blocks per one NeoFS epoch.
func (c *Client) EpochDuration() (uint64, error) {
	epochDuration, err := c.readUInt64Config(epochDurationConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get epoch duration: %w", c, err)
	}

	return epochDuration, nil
}

// ContainerFee returns fee paid by container owner to each alphabet node
// for container registration.
func (c *Client) ContainerFee() (uint64, error) {
	fee, err := c.readUInt64Config(containerFeeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get container fee: %w", c, err)
	}

	return fee, nil
}

// ContainerAliasFee returns additional fee paid by container owner to each
// alphabet node for container nice name registration.
func (c *Client) ContainerAliasFee() (uint64, error) {
	fee, err := c.readUInt64Config(containerAliasFeeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get container alias fee: %w", c, err)
	}

	return fee, nil
}

// EigenTrustIterations returns global configuration value of iteration cycles
// for EigenTrust algorithm per epoch.
func (c *Client) EigenTrustIterations() (uint64, error) {
	iterations, err := c.readUInt64Config(etIterationsConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get eigen trust iterations: %w", c, err)
	}

	return iterations, nil
}

// EigenTrustAlpha returns global configuration value of alpha parameter.
// It receives the alpha as a string and tries to convert it to float.
func (c *Client) EigenTrustAlpha() (float64, error) {
	strAlpha, err := c.readStringConfig(etAlphaConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get eigen trust alpha: %w", c, err)
	}

	return strconv.ParseFloat(strAlpha, 64)
}

// InnerRingCandidateFee returns global configuration value of fee paid by
// node to be in inner ring candidates list.
func (c *Client) InnerRingCandidateFee() (uint64, error) {
	fee, err := c.readUInt64Config(irCandidateFeeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get inner ring candidate fee: %w", c, err)
	}

	return fee, nil
}

// WithdrawFee returns global configuration value of fee paid by user to
// withdraw assets from NeoFS contract.
func (c *Client) WithdrawFee() (uint64, error) {
	fee, err := c.readUInt64Config(withdrawFeeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get withdraw fee: %w", c, err)
	}

	return fee, nil
}

func (c *Client) readUInt64Config(key string) (uint64, error) {
	v, err := c.config([]byte(key), IntegerAssert)
	if err != nil {
		return 0, err
	}

	// IntegerAssert is guaranteed to return int64 if the error is nil.
	return uint64(v.(int64)), nil
}

func (c *Client) readStringConfig(key string) (string, error) {
	v, err := c.config([]byte(key), StringAssert)
	if err != nil {
		return "", err
	}

	// StringAssert is guaranteed to return string if the error is nil.
	return v.(string), nil
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

// SetConfig sets config field.
func (c *Client) SetConfig(p SetConfigPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(setConfigMethod)
	prm.SetArgs(p.id, p.key, p.value)
	prm.InvokePrmOptional = p.InvokePrmOptional

	return c.client.Invoke(prm)
}

// IterateConfigParameters iterates over configuration parameters stored in Netmap contract and passes them to f.
//
// Returns f's errors directly.
func (c *Client) IterateConfigParameters(f func(key, value []byte) error) error {
	prm := client.TestInvokePrm{}
	prm.SetMethod(configListMethod)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return fmt.Errorf("could not perform test invocation (%s): %w",
			configListMethod, err)
	}

	if ln := len(items); ln != 1 {
		return fmt.Errorf("unexpected stack item count (%s): %d", configListMethod, ln)
	}

	arr, err := client.ArrayFromStackItem(items[0])
	if err != nil {
		return fmt.Errorf("record list (%s): %w", configListMethod, err)
	}

	return iterateRecords(arr, func(key, value []byte) error {
		return f(key, value)
	})
}

// ConfigWriter is an interface of NeoFS network config writer.
type ConfigWriter interface {
	UnknownParameter(string, []byte)
	MaxObjectSize(uint64)
	BasicIncomeRate(uint64)
	AuditFee(uint64)
	EpochDuration(uint64)
	ContainerFee(uint64)
	ContainerAliasFee(uint64)
	EigenTrustIterations(uint64)
	EigenTrustAlpha(float64)
	InnerRingCandidateFee(uint64)
	WithdrawFee(uint64)
}

// WriteConfig writes NeoFS network configuration received via iterator.
//
// Returns iterator's errors directly.
func WriteConfig(dst ConfigWriter, iterator func(func(key, val []byte) error) error) error {
	return iterator(func(key, val []byte) error {
		switch k := string(key); k {
		default:
			dst.UnknownParameter(k, val)
		case maxObjectSizeConfig:
			dst.MaxObjectSize(bytesToUint64(val))
		case basicIncomeRateConfig:
			dst.BasicIncomeRate(bytesToUint64(val))
		case auditFeeConfig:
			dst.AuditFee(bytesToUint64(val))
		case epochDurationConfig:
			dst.EpochDuration(bytesToUint64(val))
		case containerFeeConfig:
			dst.ContainerFee(bytesToUint64(val))
		case containerAliasFeeConfig:
			dst.ContainerAliasFee(bytesToUint64(val))
		case etIterationsConfig:
			dst.EigenTrustIterations(bytesToUint64(val))
		case etAlphaConfig:
			v, err := strconv.ParseFloat(string(val), 64)
			if err != nil {
				return fmt.Errorf("prm %s: %v", etAlphaConfig, err)
			}

			dst.EigenTrustAlpha(v)
		case irCandidateFeeConfig:
			dst.InnerRingCandidateFee(bytesToUint64(val))
		case withdrawFeeConfig:
			dst.WithdrawFee(bytesToUint64(val))
		}

		return nil
	})
}

func bytesToUint64(val []byte) uint64 {
	if len(val) == 0 {
		return 0
	}
	return bigint.FromBytes(val).Uint64()
}

// config performs the test invoke of get config value
// method of NeoFS Netmap contract.
func (c *Client) config(key []byte, assert func(stackitem.Item) (interface{}, error)) (interface{}, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(configMethod)
	prm.SetArgs(key)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			configMethod, err)
	}

	if ln := len(items); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d",
			configMethod, ln)
	}

	return assert(items[0])
}

// IntegerAssert converts stack item to int64.
func IntegerAssert(item stackitem.Item) (interface{}, error) {
	return client.IntFromStackItem(item)
}

// StringAssert converts stack item to string.
func StringAssert(item stackitem.Item) (interface{}, error) {
	return client.StringFromStackItem(item)
}

// iterateRecords iterates over all config records and passes them to f.
//
// Returns f's errors directly.
func iterateRecords(arr []stackitem.Item, f func(key, value []byte) error) error {
	for i := range arr {
		fields, err := client.ArrayFromStackItem(arr[i])
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
