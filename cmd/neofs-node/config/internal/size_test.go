package internal

import (
	"strings"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/configutil"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const (
	kb = 1024
	mb = 1024 * kb
	gb = 1024 * mb
	tb = 1024 * gb
)

func TestSize_UnmarshalText(t *testing.T) {
	tests := []struct {
		input    string
		expected Size
	}{
		{"100G", Size(100 * gb)},
		{"2M", Size(2 * mb)},
		{"10 K", Size(10 * kb)},
		{"500", Size(500)},
		{"1g", Size(1 * gb)},

		{"", 0},
		{"abc", 0},
		{"10X", 0},
		{"-5G", 0},
		{"100G extra", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			s := Size(parseSizeInBytes(tt.input))

			if s != tt.expected {
				t.Errorf("UnmarshalText(%q) = %d, expected %d", tt.input, s, tt.expected)
			}
		})
	}
}

func TestViperUnmarshalSize(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")

	config := `
size_kb: 1 kb
size_kb_no_space: 2kb
size_mb: 12m
size_gb: 4g
size_tb: 5 TB
size_float: .5t
size_float_big: 14.123 gb
size_i_am_not_very_clever: 12.12345678
size_bytes: 2048b
size_bytes_single_char: 1 b
size_bytes_single_char_no_space: 1b
size_bytes_no_suffix: 123456
`
	err := v.ReadConfig(strings.NewReader(config))
	if err != nil {
		t.Fatalf("Failed to read config: %v", err)
	}

	var cfg Config
	err = configutil.Unmarshal(v, &cfg, EnvPrefix, SizeHook())
	if err != nil {
		t.Fatalf("UnmarshalExact failed: %v", err)
	}

	require.EqualValues(t, kb, cfg.SizeKB, "size_kb")
	require.EqualValues(t, 2*kb, cfg.SizeKBNoSpace, "size_kb_no_space")
	require.EqualValues(t, 12*mb, cfg.SizeMB, "size_mb")
	require.EqualValues(t, 4*gb, cfg.SizeGB, "size_gb")
	require.EqualValues(t, 5*tb, cfg.SizeTB, "size_tb")
	require.EqualValues(t, 0, cfg.SizeFloat, "size_float")
	require.EqualValues(t, 0, cfg.SizeFloatBig, "size_float_big")
	require.EqualValues(t, 0, cfg.SizeIAmNotVeryClever, "size_i_am_not_very_clever")
	require.EqualValues(t, 2048, cfg.SizeBytes, "size_bytes")
	require.EqualValues(t, 1, cfg.SizeBytesSingleChar, "size_bytes_single_char")
	require.EqualValues(t, 1, cfg.SizeBytesSingleCharNoSp, "size_bytes_single_char_no_space")
	require.EqualValues(t, 123456, cfg.SizeBytesNoSuffix, "size_bytes_no_suffix")
}

type Config struct {
	SizeKB                  Size `mapstructure:"size_kb,string"`
	SizeKBNoSpace           Size `mapstructure:"size_kb_no_space,string"`
	SizeMB                  Size `mapstructure:"size_mb,string"`
	SizeGB                  Size `mapstructure:"size_gb,string"`
	SizeTB                  Size `mapstructure:"size_tb,string"`
	SizeFloat               Size `mapstructure:"size_float,string"`
	SizeFloatBig            Size `mapstructure:"size_float_big,string"`
	SizeIAmNotVeryClever    Size `mapstructure:"size_i_am_not_very_clever,string"`
	SizeBytes               Size `mapstructure:"size_bytes,string"`
	SizeBytesSingleChar     Size `mapstructure:"size_bytes_single_char,string"`
	SizeBytesSingleCharNoSp Size `mapstructure:"size_bytes_single_char_no_space,string"`
	SizeBytesNoSuffix       Size `mapstructure:"size_bytes_no_suffix,string"`
}

func (c *Config) Set(key string)   {}
func (c *Config) Unset(key string) {}
