package ec

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/klauspost/reedsolomon"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
)

// Erasure coding attributes.
const (
	AttributePrefix  = "__NEOFS__EC_"
	AttributeRuleIdx = AttributePrefix + "RULE_IDX"
	AttributePartIdx = AttributePrefix + "PART_IDX"
)

// Rule represents erasure coding rule for object payload's encoding and placement.
type Rule struct {
	DataPartNum   uint8
	ParityPartNum uint8
}

// String implements [fmt.Stringer].
func (x Rule) String() string {
	return strconv.FormatUint(uint64(x.DataPartNum), 10) + "/" + strconv.FormatUint(uint64(x.ParityPartNum), 10)
}

// Encode encodes given data according to specified EC rule and returns coded
// parts. First [Rule.DataPartNum] elements are data parts, other
// [Rule.ParityPartNum] ones are parity blocks.
//
// All parts are the same length. If data len is not divisible by
// [Rule.DataPartNum], last data part is aligned with zeros.
//
// If data is empty, all parts are nil.
func Encode(rule Rule, data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, rule.DataPartNum+rule.ParityPartNum), nil
	}

	// TODO: Explore reedsolomon.Option for performance improvement. https://github.com/nspcc-dev/neofs-node/issues/3501
	enc, err := newCoderForRule(rule)
	if err != nil {
		return nil, err
	}

	parts, err := enc.Split(data)
	if err != nil {
		return nil, fmt.Errorf("split data: %w", err)
	}

	if err := enc.Encode(parts); err != nil {
		return nil, fmt.Errorf("calculate Reed-Solomon parity: %w", err)
	}

	return parts, nil
}

// Decode decodes source data of known len from EC parts obtained by applying
// specified rule.
func Decode(rule Rule, dataLen uint64, parts [][]byte) ([]byte, error) {
	// TODO: Explore reedsolomon.Option for performance improvement. https://github.com/nspcc-dev/neofs-node/issues/3501
	dec, err := newCoderForRule(rule)
	if err != nil {
		return nil, err
	}

	required := make([]bool, rule.DataPartNum+rule.ParityPartNum)
	for i := range rule.DataPartNum {
		required[i] = true
	}

	if err := dec.ReconstructSome(parts, required); err != nil {
		return nil, fmt.Errorf("restore Reed-Solomon: %w", err)
	}

	if got := islices.TwoDimSliceElementCount(parts[:rule.DataPartNum]); uint64(got) < dataLen {
		return nil, fmt.Errorf("sum len of received data parts is less than full len: %d < %d", got, dataLen)
	}

	return ConcatDataParts(rule, dataLen, parts), nil
}

// ConcatDataParts returns a new slice of dataLen bytes originating given EC
// parts according to rule.
//
// Panics if there are less than [Rule.DataPartNum] parts.
func ConcatDataParts(rule Rule, dataLen uint64, parts [][]byte) []byte {
	// TODO: last part may be shorter, do not overallocate buffer.
	return slices.Concat(parts[:rule.DataPartNum]...)[:dataLen]
}

// DecodeRange decodes specified range of EC parts obtained by applying specified rule.
// If no error, parts[from:to+1] contains recovered data.
func DecodeRange(rule Rule, fromIdx, toIdx int, parts [][]byte) error {
	rs, err := newCoderForRule(rule)
	if err != nil {
		return err
	}

	required := make([]bool, rule.DataPartNum+rule.ParityPartNum)
	for i := fromIdx; i <= toIdx; i++ {
		required[i] = true
	}

	if err := rs.ReconstructSome(parts, required); err != nil {
		return fmt.Errorf("restore Reed-Solomon: %w", err)
	}

	return nil
}

// DecodeIndexes decodes specified EC parts obtained by applying specified rule.
func DecodeIndexes(rule Rule, parts [][]byte, idxs []int) error {
	rs, err := newCoderForRule(rule)
	if err != nil {
		return err
	}

	required := make([]bool, rule.DataPartNum+rule.ParityPartNum)
	for i := range idxs {
		required[idxs[i]] = true
	}

	if err := rs.ReconstructSome(parts, required); err != nil {
		return fmt.Errorf("restore Reed-Solomon: %w", err)
	}

	return nil
}

func newCoderForRule(rule Rule) (reedsolomon.Encoder, error) {
	enc, err := reedsolomon.New(int(rule.DataPartNum), int(rule.ParityPartNum))
	if err != nil { // should never happen with correct rule
		return nil, fmt.Errorf("init Reed-Solomon decoder: %w", err)
	}

	return enc, nil
}
