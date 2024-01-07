package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO，内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt // mmap.ReaderAt 是一个实现了 io.ReaderAt 接口的结构体
}

func (mmap *MMap) Read(bytes []byte, offset int64) (int, error) {
	//TODO implement me
	return mmap.readerAt.ReadAt(bytes, offset) // 从文件的给定位置读取对应的数据, 返回读取的字节数和错误
}

func (mmap *MMap) Write(bytes []byte) (int, error) {
	//TODO implement me
	panic("not implemented")
}

func (mmap *MMap) Sync() error {
	//TODO implement me
	panic("implement me")
}

func (mmap *MMap) Close() error {
	//TODO implement me
	return mmap.readerAt.Close() // 关闭文件
}

func (mmap *MMap) Size() (int64, error) {
	//TODO implement me
	return int64(mmap.readerAt.Len()), nil // 获取到文件大小
}

// NewMMapIOManager 初始化 MMap IO
func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm) // 创建文件
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName) // 打开文件
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil // 返回 MMap 结构体
}
