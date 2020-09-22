package attributes_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util/attributes"
	"github.com/stretchr/testify/require"
)

func TestParseV2Attributes(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		attrs, err := attributes.ParseV2Attributes(nil, nil)
		require.NoError(t, err)
		require.Len(t, attrs, 0)
	})

	t.Run("non unique bucket keys", func(t *testing.T) {
		good := []string{
			"StorageType:HDD/RPM:7200",
			"StorageType:HDD/SMR:True",
		}
		_, err := attributes.ParseV2Attributes(good, nil)
		require.NoError(t, err)

		bad := append(good, "StorageType:SSD/Cell:QLC")
		_, err = attributes.ParseV2Attributes(bad, nil)
		require.Error(t, err)

	})

	t.Run("malformed", func(t *testing.T) {
		_, err := attributes.ParseV2Attributes([]string{"..."}, nil)
		require.Error(t, err)

		_, err = attributes.ParseV2Attributes([]string{"a:b", ""}, nil)
		require.Error(t, err)

		_, err = attributes.ParseV2Attributes([]string{"//"}, nil)
		require.Error(t, err)
	})

	t.Run("unexpected", func(t *testing.T) {
		unexpectedBucket := []string{
			"Location:Europe/City:Moscow",
			"Price:100",
		}
		_, err := attributes.ParseV2Attributes(unexpectedBucket, []string{"Price"})
		require.Error(t, err)
	})

	t.Run("correct", func(t *testing.T) {
		from := []string{
			"/Location:Europe/Country:Sweden/City:Stockholm",
			"/StorageType:HDD/RPM:7200",
		}
		attrs, err := attributes.ParseV2Attributes(from, nil)
		require.NoError(t, err)
		require.Len(t, attrs, 5)
	})
}
