package network

import (
	"fmt"
	"net"
	"strconv"
)

// ChangePort changes port in every address in the provided list to the
// specified value. Addresses should be in the "host:port" form.
func ChangePort(addrs []string, port uint16) ([]string, error) {
	res := make([]string, 0, len(addrs))
	for i := range addrs {
		host, _, err := net.SplitHostPort(addrs[i])
		if err != nil {
			return nil, fmt.Errorf("[%d] address ('%s') cannot be parsed: %w", i, addrs[i], err)
		}
		res = append(res, net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10)))
	}

	return res, nil
}
