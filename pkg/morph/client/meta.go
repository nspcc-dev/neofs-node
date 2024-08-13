package client

import (
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

func (c *Client) SendObjectHeader(o object.Object) error {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return ErrConnectionLost
	}

	rawObject := o.Marshal()

	w := io.NewBufBinWriter()
	emit.Bytes(w.BinWriter, rawObject)
	if w.Err != nil {
		return fmt.Errorf("creating transaction script: %w", w.Err)
	}

	script := w.Bytes()

	start := time.Now()
	tx, vub, err := c.rpcActor.SendRun(script)
	sendingTook := time.Since(start)

	start = time.Now()
	_, err = c.rpcActor.Wait(tx, vub, err)
	waitingTook := time.Since(start)

	c.logger.Info("meta time", zap.Duration("sending", sendingTook), zap.Duration("waiting", waitingTook))

	return err
}
