package object

import (
	"errors"
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/spf13/cobra"
)

var linkCMD = &cobra.Command{
	Use:   "link",
	Short: "Inspect link object",
	Args:  cobra.NoArgs,
	RunE:  linkFunc,
}

func linkFunc(cmd *cobra.Command, _ []string) error {
	if vPath == "" {
		return errors.New("empty path to file")
	}

	raw, err := os.ReadFile(vPath)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}

	var link object.Link
	err = link.Unmarshal(raw)
	if err != nil {
		cmd.Printf("%q file does not contain raw link payload, trying to decode it as a full NeoFS object", vPath)

		var obj object.Object
		err = obj.Unmarshal(raw)
		if err != nil {
			return fmt.Errorf("decoding NeoFS object: %w", err)
		}

		if typeGot := obj.Type(); typeGot != object.TypeLink {
			return fmt.Errorf("unexpected object type (not %s): %s", object.TypeLink, typeGot)
		}

		err = obj.ReadLink(&link)
	}
	if err != nil {
		return fmt.Errorf("decoding link object: %w", err)
	}

	if len(link.Objects()) == 0 {
		return errors.New("empty children list")
	}

	cmd.Println("Children (sorted according to the read payload):")

	for _, measuredObject := range link.Objects() {
		cmd.Printf("Size: %d, object ID: %s\n", measuredObject.ObjectSize(), measuredObject.ObjectID())
	}

	return nil
}
