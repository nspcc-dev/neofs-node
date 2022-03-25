package blobstor

import (
	"github.com/klauspost/compress/zstd"
)

// zstdFrameMagic contains first 4 bytes of any compressed object
// https://github.com/klauspost/compress/blob/master/zstd/framedec.go#L58 .
var zstdFrameMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

func noOpCompressor(data []byte) []byte {
	return data
}

func noOpDecompressor(data []byte) ([]byte, error) {
	return data, nil
}

func zstdCompressor() (*zstd.Encoder, func([]byte) []byte, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, nil, err
	}

	return enc, func(data []byte) []byte {
		return enc.EncodeAll(data, make([]byte, 0, len(data)))
	}, nil
}

func zstdDecompressor() (*zstd.Decoder, func([]byte) ([]byte, error), error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, nil, err
	}

	return dec, func(data []byte) ([]byte, error) {
		return dec.DecodeAll(data, nil)
	}, nil
}
