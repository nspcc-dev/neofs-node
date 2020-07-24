package grpc

import (
	"google.golang.org/grpc"
)

type Server = grpc.Server

// Service interface
type Service interface {
	Name() string
	Register(*Server)
}
