package session

import (
	"testing"

	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestParseContexts(t *testing.T) {
	cnr := cidtest.ID().EncodeToString()
	obj := oidtest.ID().EncodeToString()

	for _, tc := range []struct {
		name  string
		spec  string
		valid bool
	}{
		{name: "container and verb", spec: cnr + ":GET", valid: true},
		{name: "wildcard container", spec: "0:CONTAINERPUT", valid: true},
		{name: "object ID", spec: cnr + ":GET:" + obj},
		{name: "trailing separator", spec: cnr + ":GET:"},
		{name: "no separator", spec: cnr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().StringArray(v2ContextFlag, nil, "")
			require.NoError(t, cmd.Flags().Set(v2ContextFlag, tc.spec))

			_, err := parseContexts(cmd)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "must be 'containerID:verbs'")
			}
		})
	}
}
