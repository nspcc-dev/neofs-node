package wrapper

import (
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
)

const (
	maxObjectSizeConfig   = "MaxObjectSize"
	basicIncomeRateConfig = "BasicIncomeRate"
	auditFeeConfig        = "AuditFee"
	epochDurationConfig   = "EpochDuration"
	containerFeeConfig    = "ContainerFee"
	etIterationsConfig    = "EigenTrustIterations"
	etAlphaConfig         = "EigenTrustAlpha"
	irCandidateFeeConfig  = "InnerRingCandidateFee"
	withdrawFeeConfig     = "WithdrawFee"
)

// MaxObjectSize receives max object size configuration
// value through the Netmap contract call.
func (w *Wrapper) MaxObjectSize() (uint64, error) {
	objectSize, err := w.readUInt64Config(maxObjectSizeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get epoch number: %w", w, err)
	}

	return objectSize, nil
}

// BasicIncomeRate returns basic income rate configuration value from network
// config in netmap contract.
func (w *Wrapper) BasicIncomeRate() (uint64, error) {
	rate, err := w.readUInt64Config(basicIncomeRateConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get basic income rate: %w", w, err)
	}

	return rate, nil
}

// AuditFee returns audit fee configuration value from network
// config in netmap contract.
func (w *Wrapper) AuditFee() (uint64, error) {
	fee, err := w.readUInt64Config(auditFeeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get audit fee: %w", w, err)
	}

	return fee, nil
}

// EpochDuration returns number of sidechain blocks per one NeoFS epoch.
func (w *Wrapper) EpochDuration() (uint64, error) {
	epochDuration, err := w.readUInt64Config(epochDurationConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get epoch duration: %w", w, err)
	}

	return epochDuration, nil
}

// ContainerFee returns fee paid by container owner to each alphabet node
// for container registration.
func (w *Wrapper) ContainerFee() (uint64, error) {
	fee, err := w.readUInt64Config(containerFeeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get container fee: %w", w, err)
	}

	return fee, nil
}

// EigenTrustIterations returns global configuration value of iteration cycles
// for EigenTrust algorithm per epoch.
func (w *Wrapper) EigenTrustIterations() (uint64, error) {
	iterations, err := w.readUInt64Config(etIterationsConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get eigen trust iterations: %w", w, err)
	}

	return iterations, nil
}

// EigenTrustAlpha returns global configuration value of alpha parameter.
// It receives the alpha as a string and tries to convert it to float.
func (w *Wrapper) EigenTrustAlpha() (float64, error) {
	strAlpha, err := w.readStringConfig(etAlphaConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get eigen trust alpha: %w", w, err)
	}

	return strconv.ParseFloat(strAlpha, 64)
}

// InnerRingCandidateFee returns global configuration value of fee paid by
// node to be in inner ring candidates list.
func (w *Wrapper) InnerRingCandidateFee() (uint64, error) {
	fee, err := w.readUInt64Config(irCandidateFeeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get inner ring candidate fee: %w", w, err)
	}

	return fee, nil
}

// WithdrawFee returns global configuration value of fee paid by user to
// withdraw assets from NeoFS contract.
func (w *Wrapper) WithdrawFee() (uint64, error) {
	fee, err := w.readUInt64Config(withdrawFeeConfig)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get withdraw fee: %w", w, err)
	}

	return fee, nil
}

func (w *Wrapper) readUInt64Config(key string) (uint64, error) {
	args := netmap.ConfigArgs{}
	args.SetKey([]byte(key))

	vals, err := w.client.Config(args, netmap.IntegerAssert)
	if err != nil {
		return 0, err
	}

	v := vals.Value()

	numeric, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("(%T) invalid value type %T", w, v)
	}

	return uint64(numeric), nil
}

func (w *Wrapper) readStringConfig(key string) (string, error) {
	args := netmap.ConfigArgs{}
	args.SetKey([]byte(key))

	vals, err := w.client.Config(args, netmap.StringAssert)
	if err != nil {
		return "", err
	}

	v := vals.Value()

	str, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("(%T) invalid value type %T", w, v)
	}

	return str, nil
}

// SetConfig sets config field.
func (w *Wrapper) SetConfig(id, key []byte, value interface{}) error {
	return w.client.SetConfig(id, key, value)
}
