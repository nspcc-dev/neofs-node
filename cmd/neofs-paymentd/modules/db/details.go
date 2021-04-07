package db

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/mr-tron/base58"
)

// <- 50.0000 [deposit]

func prettyDetails(details []byte) string {
	var buf strings.Builder
	buf.WriteString("[")

	switch details[0] {
	case 0x01:
		buf.WriteString("deposit; ")
		buf.WriteString("mainnet tx:")
		buf.WriteString(hex.EncodeToString(details[1:]))
	case 0x02:
		buf.WriteString("burn; ")
		buf.WriteString("mainnet tx:")
		buf.WriteString(hex.EncodeToString(details[1:]))
	case 0x03:
		buf.WriteString("withdraw; ")
		buf.WriteString("mainnet tx:")
		buf.WriteString(hex.EncodeToString(details[1:]))
	case 0x04:
		buf.WriteString("failed withdraw;")
		// todo: parse epoch number there
	case 0x10:
		buf.WriteString("container fee; ")
		buf.WriteString("cid:")
		buf.WriteString(base58.Encode(details[1:]))
	case 0x40:
		buf.WriteString("audit settlement; ")
		epoch := binary.LittleEndian.Uint64(details[1:])
		buf.WriteString("epoch:")
		buf.WriteString(strconv.FormatUint(epoch, 10))
	case 0x41:
		buf.WriteString("basic income collect; ")
		epoch := binary.LittleEndian.Uint64(details[1:])
		buf.WriteString("epoch:")
		buf.WriteString(strconv.FormatUint(epoch, 10))
	case 0x42:
		buf.WriteString("basic income distribution; ")
		epoch := binary.LittleEndian.Uint64(details[1:])
		buf.WriteString("epoch:")
		buf.WriteString(strconv.FormatUint(epoch, 10))
	default:
		buf.WriteString("unknown transfer type")
	}

	buf.WriteString("]")

	return buf.String()
}

func prettyFixed12(n int64) string {
	a := n / 1_0000_0000_0000
	b := n % 1_0000_0000_0000

	return fmt.Sprintf("%d.%012d", a, b)
}
