package object

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchHelpDocumentsFilters(t *testing.T) {
	f := objectSearchCmd.Flag("filters")
	require.NotNil(t, f)
	require.Equal(t, searchFiltersUsage, f.Usage)
	require.Contains(t, objectSearchCmd.Example, "FilePath EQ cat.jpg")
	require.Contains(t, objectSearchCmd.Example, "FileName NOPRESENT")
	require.Contains(t, objectSearchCmd.Example, "filters.json")
}

func TestSearchV2HelpDocumentsFilters(t *testing.T) {
	f := searchV2Cmd.Flag("filters")
	require.NotNil(t, f)
	require.Equal(t, searchFiltersUsage, f.Usage)
	require.Equal(t, searchV2Example, searchV2Cmd.Example)
}
