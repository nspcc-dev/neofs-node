package reputationroute

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	"go.uber.org/zap"
)

type routeContext struct {
	reputationcontroller.Context

	passedRoute []ServerInfo
}

// NewRouteContext wraps the main context of value passing with its traversal route and epoch.
func NewRouteContext(ctx reputationcontroller.Context, passed []ServerInfo) reputationcontroller.Context {
	return &routeContext{
		Context:     ctx,
		passedRoute: passed,
	}
}

type trustWriter struct {
	router *Router

	ctx *routeContext

	routeMtx sync.RWMutex
	mServers map[string]reputationcontroller.Writer
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
func (r *Router) InitWriter(ctx reputationcontroller.Context) (reputationcontroller.Writer, error) {
	var (
		routeCtx *routeContext
		ok       bool
	)

	if routeCtx, ok = ctx.(*routeContext); !ok {
		routeCtx = &routeContext{
			Context:     ctx,
			passedRoute: []ServerInfo{r.localSrvInfo},
		}
	}

	return &trustWriter{
		router:   r,
		ctx:      routeCtx,
		mServers: make(map[string]reputationcontroller.Writer),
	}, nil
}

func (w *trustWriter) Write(t reputation.Trust) error {
	w.routeMtx.Lock()
	defer w.routeMtx.Unlock()

	localPeerID := reputation.PeerIDFromBytes(w.router.localSrvInfo.PublicKey())

	route, err := w.router.routeBuilder.NextStage(w.ctx.Epoch(), localPeerID, w.ctx.passedRoute)
	if err != nil {
		return err
	} else if len(route) == 0 {
		route = []ServerInfo{nil}
	}

	for _, remoteInfo := range route {
		var endpoint string

		if remoteInfo != nil {
			endpoint = remoteInfo.Address()
		}

		remoteWriter, ok := w.mServers[endpoint]
		if !ok {
			provider, err := w.router.remoteProvider.InitRemote(remoteInfo)
			if err != nil {
				w.router.log.Debug("could not initialize writer provider",
					zap.String("error", err.Error()),
				)

				continue
			}

			remoteWriter, err = provider.InitWriter(w.ctx)
			if err != nil {
				w.router.log.Debug("could not initialize writer",
					zap.String("error", err.Error()),
				)

				continue
			}

			w.mServers[endpoint] = remoteWriter
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
	for endpoint, wRemote := range w.mServers {
		err := wRemote.Close()
		if err != nil {
			w.router.log.Debug("could not close remote server writer",
				zap.String("endpoint", endpoint),
				zap.String("error", err.Error()),
			)
		}
	}

	return nil
}
