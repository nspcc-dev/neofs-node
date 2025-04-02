package crypto_test

import (
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

var (
	// ECDSA private key used in tests.
	// privECDSA = ecdsa.PrivateKey{
	// 	PublicKey: ecdsa.PublicKey{
	// 		Curve: elliptic.P256(),
	// 		X: new(big.Int).SetBytes([]byte{206, 67, 193, 231, 254, 180, 127, 78, 101, 154, 23, 161, 134, 77, 122, 34, 234, 85,
	// 			149, 44, 32, 223, 244, 140, 28, 194, 76, 214, 239, 121, 174, 40}),
	// 		Y: new(big.Int).SetBytes([]byte{170, 190, 155, 176, 31, 11, 4, 14, 103, 210, 53, 0, 73, 46, 81, 129, 163, 217, 81, 51, 111,
	// 			135, 223, 253, 48, 104, 240, 197, 122, 37, 197, 78}),
	// 	},
	// 	D: new(big.Int).SetBytes([]byte{185, 97, 226, 151, 175, 3, 234, 11, 168, 211, 53, 141, 136, 102, 100, 222, 73, 174, 234, 157,
	// 		139, 192, 66, 145, 13, 173, 12, 120, 22, 134, 52, 180}),
	// }
	// corresponds to the private key.
	pubECDSA = []byte{2, 206, 67, 193, 231, 254, 180, 127, 78, 101, 154, 23, 161, 134, 77, 122, 34, 234, 85, 149, 44, 32, 223,
		244, 140, 28, 194, 76, 214, 239, 121, 174, 40}
	// corresponds to pubECDSA.
	issuer = user.ID{53, 57, 243, 96, 136, 255, 217, 227, 204, 13, 243, 228, 109, 31, 226, 226, 236, 62, 13, 190, 156, 135, 252, 236, 8}
)
