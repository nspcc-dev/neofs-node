package config_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestStringSlice(t *testing.T) {
	configtest.ForEachFileType("test/config", func(c *config.Config) {
		cStringSlice := c.Sub("string_slice")

		val := config.StringSlice(cStringSlice, "empty")
		require.Empty(t, val)

		val = config.StringSlice(cStringSlice, "filled")
		require.Equal(t, []string{
			"string1",
			"string2",
		}, val)

		require.Panics(t, func() {
			config.StringSlice(cStringSlice, "incorrect")
		})

		val = config.StringSliceSafe(cStringSlice, "incorrect")
		require.Nil(t, val)
	})
}

func TestString(t *testing.T) {
	configtest.ForEachFileType("test/config", func(c *config.Config) {
		c = c.Sub("string")

		val := config.String(c, "correct")
		require.Equal(t, "some string", val)

		require.Panics(t, func() {
			config.String(c, "incorrect")
		})

		val = config.StringSafe(c, "incorrect")
		require.Empty(t, val)
	})
}

func TestDuration(t *testing.T) {
	configtest.ForEachFileType("test/config", func(c *config.Config) {
		c = c.Sub("duration")

		val := config.Duration(c, "correct")
		require.Equal(t, 15*time.Minute, val)

		require.Panics(t, func() {
			config.Duration(c, "incorrect")
		})

		val = config.DurationSafe(c, "incorrect")
		require.Equal(t, time.Duration(0), val)
	})
}

func TestBool(t *testing.T) {
	configtest.ForEachFileType("test/config", func(c *config.Config) {
		c = c.Sub("bool")

		val := config.Bool(c, "correct")
		require.Equal(t, true, val)

		val = config.Bool(c, "correct_string")
		require.Equal(t, true, val)

		require.Panics(t, func() {
			config.Bool(c, "incorrect")
		})

		val = config.BoolSafe(c, "incorrect")
		require.Equal(t, false, val)
	})
}

func TestNumbers(t *testing.T) {
	configtest.ForEachFileType("test/config", func(c *config.Config) {
		c = c.Sub("number")

		const (
			intPos = "int_pos"
			intNeg = "int_neg"

			fractPos = "fract_pos"
			fractNeg = "fract_neg"

			incorrect = "incorrect"
		)

		require.EqualValues(t, 1, config.Int(c, intPos))
		require.EqualValues(t, 1, config.Uint(c, intPos))

		require.EqualValues(t, -1, config.Int(c, intNeg))
		require.Panics(t, func() { config.Uint(c, intNeg) })

		require.EqualValues(t, 2, config.Int(c, fractPos))
		require.EqualValues(t, 2, config.Uint(c, fractPos))

		require.EqualValues(t, -2, config.Int(c, fractNeg))
		require.Panics(t, func() { config.Uint(c, fractNeg) })

		require.Panics(t, func() { config.Int(c, incorrect) })
		require.Panics(t, func() { config.Uint(c, incorrect) })

		require.Zero(t, config.IntSafe(c, incorrect))
		require.Zero(t, config.UintSafe(c, incorrect))
	})
}

func TestSizeInBytes(t *testing.T) {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
	)
	configtest.ForEachFileType("test/config", func(c *config.Config) {
		c = c.Sub("sizes")
		require.EqualValues(t, kb, config.SizeInBytesSafe(c, "size_kb"))
		require.EqualValues(t, 2*kb, config.SizeInBytesSafe(c, "size_kb_no_space"))
		require.EqualValues(t, 12*mb, config.SizeInBytesSafe(c, "size_mb"))
		require.EqualValues(t, 4*gb, config.SizeInBytesSafe(c, "size_gb"))
		require.EqualValues(t, 5*tb, config.SizeInBytesSafe(c, "size_tb"))
		require.EqualValues(t, 12, config.SizeInBytesSafe(c, "size_i_am_not_very_clever"))
		require.EqualValues(t, tb/2, config.SizeInBytesSafe(c, "size_float"))
		require.EqualValues(t, uint64(14*gb+(gb*123/1000/mb*mb)), config.SizeInBytesSafe(c, "size_float_big"))
		require.EqualValues(t, 2048, config.SizeInBytesSafe(c, "size_bytes"))
		require.EqualValues(t, 123456, config.SizeInBytesSafe(c, "size_bytes_no_suffix"))
	})
}
