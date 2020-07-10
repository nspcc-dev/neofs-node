// About "github.com/nspcc-dev/neofs-node/lib/grpc"
// there's just alias for "google.golang.org/grpc"
// with Service-interface

package grpc

import (
	middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	gZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	// Service interface
	Service interface {
		Name() string
		Register(*grpc.Server)
	}

	// ServerParams to create gRPC-server
	// and provide service-handlers
	ServerParams struct {
		dig.In

		Services []Service
		Logger   *zap.Logger
		Viper    *viper.Viper
	}

	// ServicesResult ...
	ServicesResult struct {
		dig.Out

		Services []Service
	}

	// Server type-alias
	Server = grpc.Server

	// CallOption type-alias
	CallOption = grpc.CallOption

	// ClientConn type-alias
	ClientConn = grpc.ClientConn

	// ServerOption type-alias
	ServerOption = grpc.ServerOption
)

var (
	// DialContext func-alias
	DialContext = grpc.DialContext

	// WithBlock func-alias
	WithBlock = grpc.WithBlock

	// WithInsecure func-alias
	WithInsecure = grpc.WithInsecure
)

// NewServer creates a gRPC server which has no service registered and has not
// started to accept requests yet.
func NewServer(opts ...ServerOption) *Server {
	return grpc.NewServer(opts...)
}

// creates new gRPC server and attach handlers.
func routing(p ServerParams) *grpc.Server {
	var (
		options []ServerOption
		stream  []grpc.StreamServerInterceptor
		unary   []grpc.UnaryServerInterceptor
	)

	if p.Viper.GetBool("node.grpc.billing") {
		unary = append(unary, unaryBilling)
		stream = append(stream, streamBilling)
	}

	if p.Viper.GetBool("node.grpc.logging") {
		stream = append(stream, gZap.StreamServerInterceptor(p.Logger))
		unary = append(unary, gZap.UnaryServerInterceptor(p.Logger))
	}

	if p.Viper.GetBool("node.grpc.metrics") {
		stream = append(stream, prometheus.StreamServerInterceptor)
		unary = append(unary, prometheus.UnaryServerInterceptor)
	}

	// Add stream options:
	if len(stream) > 0 {
		options = append(options,
			grpc.StreamInterceptor(middleware.ChainStreamServer(stream...)),
		)
	}

	// Add unary options:
	if len(unary) > 0 {
		options = append(options,
			grpc.UnaryInterceptor(middleware.ChainUnaryServer(unary...)),
		)
	}

	g := grpc.NewServer(options...)

	// Service services here:
	for _, service := range p.Services {
		p.Logger.Info("register gRPC service",
			zap.String("service", service.Name()))
		service.Register(g)
	}

	return g
}
