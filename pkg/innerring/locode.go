package innerring

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	irlocode "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/locode"
)

func (s *Server) newLocodeValidator() (netmap.NodeValidator, error) {
	return irlocode.New(), nil
}
