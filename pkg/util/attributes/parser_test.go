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

	t.Run("escape characters", func(t *testing.T) {
		from := []string{
			`/K\:ey1:V\/alue\\/Ke\/y2:Va\:lue`,
		}
		attrs, err := attributes.ParseV2Attributes(from, nil)
		require.NoError(t, err)
		require.Equal(t, `K:ey1`, attrs[0].Key())
		require.Equal(t, `V/alue\`, attrs[0].Value())
		require.Equal(t, `Ke/y2`, attrs[1].Key())
		require.Equal(t, `Va:lue`, attrs[1].Value())
	})

	t.Run("same attributes", func(t *testing.T) {
		from := []string{"/a:b", "/a:b"}
		attrs, err := attributes.ParseV2Attributes(from, nil)
		require.NoError(t, err)
		require.Len(t, attrs, 1)
	})

	t.Run("multiple parents", func(t *testing.T) {
		from := []string{
			"/parent1:x/child:x",
			"/parent2:x/child:x",
			"/parent2:x/child:x/subchild:x",
		}
		attrs, err := attributes.ParseV2Attributes(from, nil)
		require.NoError(t, err)

		var flag bool
		for _, attr := range attrs {
			if attr.Key() == "child" {
				flag = true
				require.Equal(t, []string{"parent1", "parent2"}, attr.ParentKeys())
			}
		}
		require.True(t, flag)
	})

	t.Run("consistent order in chain", func(t *testing.T) {
		from := []string{"/a:1/b:2/c:3"}

		for i := 0; i < 10000; i++ {
			attrs, err := attributes.ParseV2Attributes(from, nil)
			require.NoError(t, err)

			require.Equal(t, "a", attrs[0].Key())
			require.Equal(t, "1", attrs[0].Value())
			require.Equal(t, "b", attrs[1].Key())
			require.Equal(t, "2", attrs[1].Value())
			require.Equal(t, "c", attrs[2].Key())
			require.Equal(t, "3", attrs[2].Value())
		}
	})
}
