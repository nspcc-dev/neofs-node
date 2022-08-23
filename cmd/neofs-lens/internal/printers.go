package common

import (
	"os"

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
func WriteObjectToFile(cmd *cobra.Command, path string, data []byte) {
	if path == "" {
		return
	}

	ExitOnErr(cmd, Errf("could not write file: %w",
		os.WriteFile(path, data, 0644)))

	cmd.Printf("\nSaved payload to '%s' file\n", path)
}
