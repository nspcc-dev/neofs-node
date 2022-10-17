package persistent

import (
	"crypto/rand"
	"fmt"
	"io"
)

func (s *TokenStore) encrypt(value []byte) ([]byte, error) {
	nonce := make([]byte, s.gcm.NonceSize())

	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("could not init random nonce: %w", err)
	}

	return s.gcm.Seal(nonce, nonce, value, nil), nil
}

func (s *TokenStore) decrypt(value []byte) ([]byte, error) {
	nonceSize := s.gcm.NonceSize()
	if len(value) < nonceSize {
		return nil, fmt.Errorf(
			"unexpected encrypted length: nonce length is %d, encrypted data length is %d",
			nonceSize,
			len(value),
		)
	}

	nonce, encryptedData := value[:nonceSize], value[nonceSize:]

	return s.gcm.Open(nil, nonce, encryptedData, nil)
}
