package client

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

func (c *Client) SendObjectHeader(o object.Object) error {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return ErrConnectionLost
	}

	rawObject, err := o.Marshal()
	if err != nil {
		return fmt.Errorf("marshal object: %w", err)
	}

	w := io.NewBufBinWriter()
	emit.Bytes(w.BinWriter, rawObject)
	if w.Err != nil {
		return fmt.Errorf("creating transaction script: %w", w.Err)
	}

	script := w.Bytes()

	_, err = c.rpcActor.Wait(c.rpcActor.SendRun(script))
	return err
}
