package common

import (
	"bytes"
	"encoding/json"

	"github.com/spf13/cobra"
)

// PrettyPrintJSON prints m as an indented JSON to the cmd output.
func PrettyPrintJSON(cmd *cobra.Command, m json.Marshaler, entity string) {
	data, err := m.MarshalJSON()
	if err != nil {
		PrintVerbose(cmd, "Can't convert %s to json: %w", entity, err)
		return
	}
	buf := new(bytes.Buffer)
	if err := json.Indent(buf, data, "", "  "); err != nil {
		PrintVerbose(cmd, "Can't pretty print json: %w", err)
		return
	}
	cmd.Println(buf)
}
