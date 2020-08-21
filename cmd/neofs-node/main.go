package main

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/grace"
)

func main() {
	ctx := grace.NewGracefulContext(nil)

	<-ctx.Done()
}
