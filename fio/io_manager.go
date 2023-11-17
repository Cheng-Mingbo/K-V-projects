package fio

const DataFileParm = 0644

// IOManager 封装了IO操作
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
}
