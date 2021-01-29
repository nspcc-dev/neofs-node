package loadroute

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	loadcontroller "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/controller"
	"go.uber.org/zap"
)

type routeContext struct {
	context.Context

	passedRoute []ServerInfo
}

// NewRouteContext wraps the main context of value passing with its traversal route.
//
// Passing the result to Router.InitWriter method will allow you to continue this route.
func NewRouteContext(ctx context.Context, passed []ServerInfo) context.Context {
	return &routeContext{
		Context:     ctx,
		passedRoute: passed,
	}
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
func (r *Router) InitWriter(ctx context.Context) (loadcontroller.Writer, error) {
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

	return &loadWriter{
		router:   r,
		ctx:      routeCtx,
		mRoute:   make(map[routeKey]*valuesRoute),
		mServers: make(map[string]loadcontroller.Writer),
	}, nil
}

type routeKey struct {
	epoch uint64

	cid string
}

type valuesRoute struct {
	route []ServerInfo

	values []container.UsedSpaceAnnouncement
}

type loadWriter struct {
	router *Router

	ctx *routeContext

	routeMtx sync.RWMutex
	mRoute   map[routeKey]*valuesRoute

	mServers map[string]loadcontroller.Writer
}

func (w *loadWriter) Put(a container.UsedSpaceAnnouncement) error {
	w.routeMtx.Lock()
	defer w.routeMtx.Unlock()

	key := routeKey{
		epoch: a.Epoch(),
		cid:   a.ContainerID().String(),
	}

	routeValues, ok := w.mRoute[key]
	if !ok {
		route, err := w.router.routeBuilder.NextStage(a, w.ctx.passedRoute)
		if err != nil {
			return err
		} else if len(route) == 0 {
			route = []ServerInfo{nil}
		}

		routeValues = &valuesRoute{
			route:  route,
			values: []container.UsedSpaceAnnouncement{a},
		}

		w.mRoute[key] = routeValues
	}

	for _, remoteInfo := range routeValues.route {
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

				continue // best effort
			}

			remoteWriter, err = provider.InitWriter(w.ctx)
			if err != nil {
				w.router.log.Debug("could not initialize writer",
					zap.String("error", err.Error()),
				)

				continue // best effort
			}

			w.mServers[endpoint] = remoteWriter
		}

		err := remoteWriter.Put(a)
		if err != nil {
			w.router.log.Debug("could not put the value",
				zap.String("error", err.Error()),
			)
		}

		// continue best effort
	}

	return nil
}

func (w *loadWriter) Close() error {
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
