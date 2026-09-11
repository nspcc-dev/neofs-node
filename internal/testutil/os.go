package testutil

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// AssertSingleDirFileData asserts that directory has given file only with
// specified content.
func AssertSingleDirFileData(t *testing.T, dir string, fileName string, data []byte) {
	AssertSingleDirFile(t, dir, fileName)
	AssertFileData(t, filepath.Join(dir, fileName), data)
}

// AssertSingleDirFile assert that directory has given file only.
func AssertSingleDirFile(t *testing.T, dir string, fileName string) {
	dirents, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, dirent := range dirents {
		require.False(t, dirent.IsDir(), dirent.Name())
		require.Equal(t, fileName, dirent.Name())
	}
}

// AssertFileData asserts file content.
func AssertFileData(t *testing.T, filePath string, data []byte) {
	gotData, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.EqualValues(t, len(data), len(gotData)) // bytes.Equal catches this but mislen is easier to explore
	require.True(t, bytes.Equal(gotData, data))
}

// AssertFileExists asserts file presence.
func AssertFileExists(t *testing.T, filePath string) {
	_, err := os.Stat(filePath)
	require.NoError(t, err)
}

// AssertFileNotExists asserts file absence.
func AssertFileNotExists(t *testing.T, filePath string) {
	_, err := os.Stat(filePath)
	require.ErrorIs(t, err, fs.ErrNotExist)
}

// AssertEmptyDir asserts dir emptiness.
func AssertEmptyDir(t *testing.T, dir string) {
	dirents, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, dirents)
}

// TouchFile creates empty file.
func TouchFile(t *testing.T, filePath string) {
	f, err := os.Create(filePath)
	require.NoError(t, err)
	_ = f.Close()
}
