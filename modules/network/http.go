package network

import (
	"github.com/fasthttp/router"
	svc "github.com/nspcc-dev/neofs-node/modules/bootstrap"
	"github.com/valyala/fasthttp"
	"go.uber.org/dig"
)

type (
	handlerParams struct {
		dig.In

		Healthy svc.HealthyClient
	}
)

const (
	healthyState       = "NeoFS node is "
	defaultContentType = "text/plain; charset=utf-8"
)

func newHTTPHandler(p handlerParams) (fasthttp.RequestHandler, error) {
	r := router.New()
	r.RedirectTrailingSlash = true

	r.GET("/-/ready/", func(c *fasthttp.RequestCtx) {
		c.SetStatusCode(fasthttp.StatusOK)
		c.SetBodyString(healthyState + "ready")
	})

	r.GET("/-/healthy/", func(c *fasthttp.RequestCtx) {
		code := fasthttp.StatusOK
		msg := "healthy"

		err := p.Healthy.Healthy()
		if err != nil {
			code = fasthttp.StatusBadRequest
			msg = "unhealthy: " + err.Error()
		}

		c.Response.Reset()
		c.SetStatusCode(code)
		c.SetContentType(defaultContentType)
		c.SetBodyString(healthyState + msg)
	})

	return r.Handler, nil
}
