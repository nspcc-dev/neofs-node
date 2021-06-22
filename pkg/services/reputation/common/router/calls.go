package router

import (
	"encoding/hex"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	"go.uber.org/zap"
)

// routeContext wraps context with additional passed
// route data. It is only used inside Router and is
// not passed in any external methods.
type routeContext struct {
	common.Context

	passedRoute []common.ServerInfo
}

// NewRouteContext wraps the main context of value passing with its traversal route and epoch.
func NewRouteContext(ctx common.Context, passed []common.ServerInfo) common.Context {
	return &routeContext{
		Context:     ctx,
		passedRoute: passed,
	}
}

type trustWriter struct {
	router *Router

	routeCtx *routeContext

	routeMtx sync.RWMutex
	mServers map[string]common.Writer
}

// InitWriter initializes and returns Writer that sends each value to its next route point.
//
// If ctx was created by NewRouteContext, then the traversed route is taken into account,
// and the value will be sent to its continuation. Otherwise, the route will be laid
// from scratch and the value will be sent to its primary point.
//
// After building a list of remote points of the next leg of the route, the value is sent
// sequentially to all of them. If any transmissions (even all) fail, an error will not
// be returned.
//
// Close of the composed Writer calls Close method on each internal Writer generated in
// runtime and never returns an error.
//
// Always returns nil error.
func (r *Router) InitWriter(ctx common.Context) (common.Writer, error) {
	var (
		routeCtx *routeContext
		ok       bool
	)

	if routeCtx, ok = ctx.(*routeContext); !ok {
		routeCtx = &routeContext{
			Context:     ctx,
			passedRoute: []common.ServerInfo{r.localSrvInfo},
		}
	}

	return &trustWriter{
		router:   r,
		routeCtx: routeCtx,
		mServers: make(map[string]common.Writer),
	}, nil
}

func (w *trustWriter) Write(t reputation.Trust) error {
	w.routeMtx.Lock()
	defer w.routeMtx.Unlock()

	route, err := w.router.routeBuilder.NextStage(w.routeCtx.Epoch(), t, w.routeCtx.passedRoute)
	if err != nil {
		return err
	} else if len(route) == 0 {
		route = []common.ServerInfo{nil}
	}

	for _, remoteInfo := range route {
		var key string

		if remoteInfo != nil {
			key = hex.EncodeToString(remoteInfo.PublicKey())
		}

		remoteWriter, ok := w.mServers[key]
		if !ok {
			provider, err := w.router.remoteProvider.InitRemote(remoteInfo)
			if err != nil {
				w.router.log.Debug("could not initialize writer provider",
					zap.String("error", err.Error()),
				)

				continue
			}

			// init writer with original context wrapped in routeContext
			remoteWriter, err = provider.InitWriter(w.routeCtx.Context)
			if err != nil {
				w.router.log.Debug("could not initialize writer",
					zap.String("error", err.Error()),
				)

				continue
			}

			w.mServers[key] = remoteWriter
		}

		err := remoteWriter.Write(t)
		if err != nil {
			w.router.log.Debug("could not write the value",
				zap.String("error", err.Error()),
			)
		}
	}

	return nil
}

func (w *trustWriter) Close() error {
	for key, wRemote := range w.mServers {
		err := wRemote.Close()
		if err != nil {
			w.router.log.Debug("could not close remote server writer",
				zap.String("key", key),
				zap.String("error", err.Error()),
			)
		}
	}

	return nil
}
