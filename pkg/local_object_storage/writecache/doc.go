// Package writecache implements write-cache for objects.
//
// Write-cache uses file system tree for storing objects.
//
// Flushing from the writecache to the main storage is done in the background.
// To make it possible to serve Read requests after the object was flushed,
// we maintain an LRU cache containing addresses of all the objects that
// could be safely deleted. The actual deletion is done during eviction from this cache.
package writecache
