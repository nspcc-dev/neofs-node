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
		require.Equal(t, attrs[0].Key(), `K:ey1`)
		require.Equal(t, attrs[0].Value(), `V/alue\`)
		require.Equal(t, attrs[1].Key(), `Ke/y2`)
		require.Equal(t, attrs[1].Value(), `Va:lue`)
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
				require.Equal(t, attr.ParentKeys(), []string{"parent1", "parent2"})
			}
		}
		require.True(t, flag)
	})

	t.Run("consistent order in chain", func(t *testing.T) {
		from := []string{"/a:1/b:2/c:3"}

		for i := 0; i < 10000; i++ {
			attrs, err := attributes.ParseV2Attributes(from, nil)
			require.NoError(t, err)

			require.Equal(t, attrs[0].Key(), "a")
			require.Equal(t, attrs[0].Value(), "1")
			require.Equal(t, attrs[1].Key(), "b")
			require.Equal(t, attrs[1].Value(), "2")
			require.Equal(t, attrs[2].Key(), "c")
			require.Equal(t, attrs[2].Value(), "3")
		}
	})
}
