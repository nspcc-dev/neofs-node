package common

import (
	"os"
	"strings"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

// PrintObjectHeader prints passed object's header fields via
// the passed cobra command. Does nothing with the payload.
func PrintObjectHeader(cmd *cobra.Command, h object.Object) {
	cmd.Println("Version:", h.Version())
	cmd.Println("Type:", h.Type())
	printContainerID(cmd, h.ContainerID)
	printObjectID(cmd, h.ID)
	cmd.Println("Owner:", h.OwnerID())
	cmd.Println("CreatedAt:", h.CreationEpoch())
	cmd.Println("PayloadSize:", h.PayloadSize())
	cmd.Println("Attributes:")
	for _, attr := range h.Attributes() {
		cmd.Printf("  %s: %s\n", attr.Key(), attr.Value())
	}
}

func printContainerID(cmd *cobra.Command, recv func() (cid.ID, bool)) {
	var val string

	id, ok := recv()
	if ok {
		val = id.String()
	} else {
		val = "<empty>"
	}

	cmd.Println("CID:", val)
}

func printObjectID(cmd *cobra.Command, recv func() (oid.ID, bool)) {
	var val string

	id, ok := recv()
	if ok {
		val = id.String()
	} else {
		val = "<empty>"
	}

	cmd.Println("ID:", val)
}

// WriteObjectToFile writes object to the provided path. Does nothing if
// the path is empty.
func WriteObjectToFile(cmd *cobra.Command, path string, data []byte, payloadOnly bool) {
	if path == "" {
		return
	}

	ExitOnErr(cmd, Errf("could not write file: %w",
		os.WriteFile(path, data, 0o644)))

	if payloadOnly {
		cmd.Printf("\nSaved payload to '%s' file\n", path)
		return
	}
	cmd.Printf("\nSaved object to '%s' file\n", path)
}

// PrintStorageObjectStatus prints object status.
func PrintStorageObjectStatus(cmd *cobra.Command, status engine.ObjectStatus) {
	for _, shard := range status.Shards {
		if len(shard.Shard.Blob.Substorages) != 0 {
			cmd.Printf("Shard ID:\t%s\n", shard.ID)
			for i, subblobs := range shard.Shard.Blob.Substorages {
				cmd.Printf("\tBlobstor substorage %d\n", i)
				cmd.Printf("\t\tStorage type:\t%s\n", subblobs.Type)
				cmd.Printf("\t\tStorage Path:\t%s\n", subblobs.Path)
				if subblobs.Error != nil {
					cmd.Printf("\t\tStorage Error:\t%s\n", subblobs.Error)
				}
			}

			cmd.Printf("\tMetabase\n")
			cmd.Printf("\t\tMetabase storage ID:\t%s\n", shard.Shard.Metabase.StorageID)
			cmd.Printf("\t\tMetabase path:\t%s\n", shard.Shard.Metabase.Path)
			cmd.Printf("\t\tMetabase object status:\t%s\n", strings.Join(shard.Shard.Metabase.State, " "))
			if shard.Shard.Metabase.Error != nil {
				cmd.Printf("\t\tMetabase object error:\t%v\n", shard.Shard.Metabase.Error)
			}
			if shard.Shard.Writecache.PathDB != "" || shard.Shard.Writecache.PathFSTree != "" {
				cmd.Printf("\tWritecache\n")
				cmd.Printf("\t\tWritecache DB path:\t%s\n", shard.Shard.Writecache.PathDB)
				cmd.Printf("\t\tWritecache FSTree path:\t%s\n", shard.Shard.Writecache.PathFSTree)
			}
			cmd.Println()
		}
	}
}
