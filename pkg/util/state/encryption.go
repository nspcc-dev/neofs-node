package state

import (
	"crypto/rand"
	"fmt"
	"io"
)

func (p PersistentStorage) encrypt(value []byte) ([]byte, error) {
	nonce := make([]byte, p.gcm.NonceSize())

	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("could not init random nonce: %w", err)
	}

	return p.gcm.Seal(nonce, nonce, value, nil), nil
}

func (p PersistentStorage) decrypt(value []byte) ([]byte, error) {
	nonceSize := p.gcm.NonceSize()
	if len(value) < nonceSize {
		return nil, fmt.Errorf(
			"unexpected encrypted length: nonce length is %d, encrypted data length is %d",
			nonceSize,
			len(value),
		)
	}

	nonce, encryptedData := value[:nonceSize], value[nonceSize:]

	return p.gcm.Open(nil, nonce, encryptedData, nil)
}
