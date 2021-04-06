// Package writecache implements write-cache for objects.
//
// It contains in-memory cache of fixed size and underlying database
// (usually on SSD) for storing small objects.
// There are 3 places where object can be:
// 1. In-memory cache.
// 2. On-disk cache DB.
// 3. Main storage (blobstor).
//
// There are 2 types of background jobs:
// 1. Persisting objects from in-memory cache to database.
// 2. Flushing objects from database to blobstor.
//	  On flushing object address is put in in-memory LRU cache.
//	  The actual deletion from the DB is done when object
//	  is evicted from this cache.
//
// Putting objects to the main storage is done by multiple workers.
// Some of them prioritize flushing items, others prioritize putting new objects.
// The current ration is 50/50. This helps to make some progress even under load.
package writecache
