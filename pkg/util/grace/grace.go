package grace

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

// NewGracefulContext returns grace context that cancelled by sigint,
// sigterm and sighup.
func NewGracefulContext(l *zap.Logger) context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		sig := <-ch
		if l != nil {
			l.Info("received signal",
				zap.String("signal", sig.String()))
		} else {
			fmt.Printf("received signal %s\n", sig)
		}
		cancel()
	}()

	return ctx
}
