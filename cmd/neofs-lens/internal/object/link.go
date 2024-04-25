package object

import (
	"errors"
	"fmt"
	"os"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/spf13/cobra"
)

var linkCMD = &cobra.Command{
	Use:   "link",
	Short: "Inspect link object",
	Args:  cobra.NoArgs,
	Run:   linkFunc,
}

func linkFunc(cmd *cobra.Command, _ []string) {
	if vPath == "" {
		common.ExitOnErr(cmd, errors.New("empty path to file"))
	}

	raw, err := os.ReadFile(vPath)
	common.ExitOnErr(cmd, common.Errf("reading file: %w", err))

	var link object.Link
	err = link.Unmarshal(raw)
	if err != nil {
		cmd.Printf("%q file does not contain raw link payload, trying to decode it as a full NeoFS object", vPath)

		var obj object.Object
		err = obj.Unmarshal(raw)
		common.ExitOnErr(cmd, common.Errf("decoding NeoFS object: %w", err))

		if typeGot := obj.Type(); typeGot != object.TypeLink {
			common.ExitOnErr(cmd, fmt.Errorf("unexpected object type (not %s): %s", object.TypeLink, typeGot))
		}

		err = obj.ReadLink(&link)
	}
	common.ExitOnErr(cmd, common.Errf("decoding link object: %w", err))

	if len(link.Objects()) == 0 {
		common.ExitOnErr(cmd, errors.New("empty children list"))
	}

	cmd.Println("Children (sorted according to the read payload):")

	for _, measuredObject := range link.Objects() {
		cmd.Printf("Size: %d, object ID: %s\n", measuredObject.ObjectSize(), measuredObject.ObjectID())
	}
}
