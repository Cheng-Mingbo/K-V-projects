package fio

import "os"

// FileIO 标准文件IO接口
type FileIO struct {
	fd *os.File // 系统文件描述符
}

func (fio *FileIO) Read(bytes []byte, offset int64) (int, error) {
	//TODO implement me
	return fio.fd.ReadAt(bytes, offset) // 从文件的给定位置读取对应的数据, 返回读取的字节数和错误
}

func (fio *FileIO) Write(bytes []byte) (int, error) {
	//TODO implement me
	return fio.fd.Write(bytes) // 写入字节数组到文件中, 返回写入的字节数和错误
}

func (fio *FileIO) Sync() error {
	//TODO implement me
	return fio.fd.Sync() // 持久化数据
}

func (fio *FileIO) Close() error {
	//TODO implement me
	return fio.fd.Close() // 关闭文件
}

func (fio *FileIO) Size() (int64, error) {
	//TODO implement me
	stat, err := fio.fd.Stat() // 获取到文件大小
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// NewFileIOManager 初始化标准文件IO
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}
