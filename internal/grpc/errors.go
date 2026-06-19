package grpc

import "google.golang.org/grpc/mem"

// MemBufferSliceError allows to pass mem.BufferSlice as error.
type MemBufferSliceError mem.BufferSlice

// Error implements built-in [error] interface.
func (MemBufferSliceError) Error() string { return "" }
