package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

const keyOffset = 8

var sessionsBucket = []byte("sessions")

func main() {
	var dbPath string
	flag.StringVar(&dbPath, "db", "", "path to persistent state DB")
	flag.Parse()

	if dbPath == "" {
		fmt.Fprintln(os.Stderr, "usage: state-fix -db /path/to/persistent-state.db")
		os.Exit(2)
	}

	db, err := bbolt.Open(dbPath, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open bbolt %q: %v\n", dbPath, err)
		os.Exit(1)
	}
	defer db.Close()

	var (
		deletedBuckets int
		deletedValues  int
		skippedBuckets int
	)

	err = db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket(sessionsBucket)
		if root == nil {
			fmt.Printf("bucket %q not found\n", sessionsBucket)
			return nil
		}

		var bucketKeys [][]byte
		c := root.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			switch {
			case v == nil:
				bucketKeys = append(bucketKeys, append([]byte(nil), k...))
			case len(v) < keyOffset:
				fmt.Printf("delete broken session value key=%s len=%d\n", hex.EncodeToString(k), len(v))
				if err := c.Delete(); err != nil {
					return fmt.Errorf("delete broken session value %s: %w", hex.EncodeToString(k), err)
				}
				deletedValues++
			}
		}

		for _, ownerKey := range bucketKeys {
			ownerID, format, err := decodeOwnerKey(ownerKey)
			if err != nil || format != "protobuf-bytes" {
				fmt.Printf("skip nested bucket key=%s unsupported owner key format\n", hex.EncodeToString(ownerKey))
				skippedBuckets++
				continue
			}

			ownerBucket := root.Bucket(ownerKey)
			if ownerBucket == nil {
				continue
			}

			stats := ownerBucket.Stats()
			fmt.Printf("delete protobuf owner bucket owner=%s key=%s child_keys=%d child_buckets=%d\n",
				ownerID, hex.EncodeToString(ownerKey), stats.KeyN, stats.BucketN)

			if err := root.DeleteBucket(ownerKey); err != nil {
				return fmt.Errorf("delete protobuf owner bucket %s: %w", hex.EncodeToString(ownerKey), err)
			}
			deletedBuckets++
		}

		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "fix db: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("done: deleted_buckets=%d deleted_broken_values=%d skipped_buckets=%d\n",
		deletedBuckets, deletedValues, skippedBuckets)
}

func decodeOwnerKey(k []byte) (user.ID, string, error) {
	var id user.ID

	switch {
	case len(k) == len(id):
		copy(id[:], k)
		return id, "raw", nil
	case len(k) == len(id)+2 && k[0] == 0x0a && int(k[1]) == len(id):
		copy(id[:], k[2:])
		return id, "protobuf-bytes", nil
	default:
		return user.ID{}, "", errors.New("unsupported owner key format")
	}
}
