package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type epochState struct {
}

func (epochState) CurrentEpoch() uint64 { return 0 }

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage ./migrate 1000")
		return
	}
	batchSize, err := strconv.ParseUint(os.Args[1], 10, 64)

	lc := zap.NewProductionConfig()
	lc.Level.SetLevel(zapcore.DebugLevel)
	lc.OutputPaths = []string{"/home/ll/log3"}
	lc.ErrorOutputPaths = []string{"/home/ll/log3"}
	l, err := lc.Build()
	if err != nil {
		log.Fatalf("init logger: %v\n", err)
	}
	mb := meta.New(
		meta.WithPath("./metabase1.cp"),
		meta.WithEpochState(epochState{}),
		meta.WithLogger(l),
	)
	if err := mb.Open(false); err != nil {
		log.Fatalf("open metabase1: %v\n", err)
	}
	defer func() {
		if err := mb.Close(); err != nil {
			l.Warn("failed close metabase", zap.Error(err))
		}
	}()
	st := time.Now()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := meta.MigrateFrom3Version(ctx, mb, uint(batchSize)); err != nil {
		l.Error("migration failed", zap.Error(err), zap.Stringer("took", time.Since(st)))
		return
	}
	l.Info("Metabase successfully migrated", zap.Stringer("took", time.Since(st)))
}
