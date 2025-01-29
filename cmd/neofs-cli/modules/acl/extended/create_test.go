package extended

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/util"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/stretchr/testify/require"
)

func TestParseTable(t *testing.T) {
	tests := [...]struct {
		name       string // test name
		rule       string // input extended ACL rule
		jsonRecord string // produced record after successful parsing
	}{
		{
			name:       "valid rule with multiple filters",
			rule:       "deny get obj:a=b req:c=d others",
			jsonRecord: `{"operation":"GET","action":"DENY","filters":[{"headerType":"OBJECT","matchType":"STRING_EQUAL","key":"a","value":"b"},{"headerType":"REQUEST","matchType":"STRING_EQUAL","key":"c","value":"d"}],"targets":[{"role":"OTHERS","keys":[]}]}`,
		},
		{
			name:       "valid rule without filters",
			rule:       "allow put user",
			jsonRecord: `{"operation":"PUT","action":"ALLOW","filters":[],"targets":[{"role":"USER","keys":[]}]}`,
		},
		{
			name: "invalid rule with public key",
			rule: "deny getrange pubkey:036410abb260bbbda89f61c0cad65a4fa15ac5cb83b3c3abf8aee403856fcf65ed",
		},
		{
			name:       "valid rule with account",
			rule:       "deny getrange address:NWcSSzMkw5Vuq3gFmcih5yfCNxfXjWdKz8",
			jsonRecord: `{"operation":"GETRANGE","action":"DENY","filters":[],"targets":[{"role":"ROLE_UNSPECIFIED","keys":["NXVYp24sheVFxW8PazdLtZEvUM/gLAhtqQ=="]}]}`,
		},
		{
			name: "missing action",
			rule: "get obj:a=b others",
		},
		{
			name: "invalid action",
			rule: "permit get obj:a=b others",
		},
		{
			name: "missing op",
			rule: "deny obj:a=b others",
		},
		{
			name: "invalid op action",
			rule: "deny look obj:a=b others",
		},
		{
			name: "invalid filter type",
			rule: "deny get invalid:a=b others",
		},
		{
			name: "invalid target group",
			rule: "deny get obj:a=b helpers",
		},
		{
			name: "invalid public key",
			rule: "deny get obj:a=b pubkey:0123",
		},
		{
			name: "invalid account",
			rule: "deny get obj:a=b account:1234",
		},
	}

	var eaclTable eacl.Table

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := util.ParseEACLRule(&eaclTable, test.rule)
			ok := len(test.jsonRecord) > 0
			require.Equal(t, ok, err == nil, err)
			if ok {
				var expectedRecord eacl.Record
				err = expectedRecord.UnmarshalJSON([]byte(test.jsonRecord))
				require.NoError(t, err)

				actualRecord := eaclTable.Records()[len(eaclTable.Records())-1]

				equalRecords(t, &expectedRecord, &actualRecord)
			}
		})
	}
}

func equalRecords(t *testing.T, r1, r2 *eacl.Record) {
	require.Equal(t, r1.Marshal(), r2.Marshal())
}
