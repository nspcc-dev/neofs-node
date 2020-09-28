package util

// SaltXOR xors bits of data with salt
// repeating salt if necessary.
func SaltXOR(data, salt []byte) (result []byte) {
	result = make([]byte, len(data))
	ls := len(salt)
	if ls == 0 {
		copy(result, data)
		return
	}

	for i := range result {
		result[i] = data[i] ^ salt[i%ls]
	}
	return
}
