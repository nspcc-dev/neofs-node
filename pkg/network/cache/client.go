package cache

// DefaultBufferSize describes default max GRPC message size. Unfortunately GRPC lib contains this const in private.
const DefaultBufferSize = 4 * 1024 * 1024 // 4MB
