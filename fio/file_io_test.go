package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func deleteFile(path string) {
	if err := os.Remove(path); err != nil {
		panic(err)
	}
}

func TestNewFileIOManger(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManger(path)
	defer deleteFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManger(path)
	defer deleteFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello"))
	assert.Equal(t, 5, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("world"))
	assert.Equal(t, 5, n)
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err = fio.Read(b, 0)
	assert.Equal(t, 5, n)
	assert.Nil(t, err)
	assert.Equal(t, "hello", string(b))

	b1 := make([]byte, 5)
	n, err = fio.Read(b1, 5)
	assert.Equal(t, 5, n)
	assert.Nil(t, err)
	assert.Equal(t, "world", string(b1))

}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManger(path)
	defer deleteFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello"))
	assert.Equal(t, 5, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("world"))
	assert.Equal(t, 5, n)
	assert.Nil(t, err)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManger(path)
	defer deleteFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello"))
	assert.Equal(t, 5, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("world"))
	assert.Equal(t, 5, n)
	assert.Nil(t, err)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManger(path)
	defer deleteFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello"))
	assert.Equal(t, 5, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("world"))
	assert.Equal(t, 5, n)
	assert.Nil(t, err)

	err = fio.Sync()
	assert.Nil(t, err)

	err = fio.Close()
	assert.Nil(t, err)
}
