package data

import (
	"KV-Project/db/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	DataFileNameSuffix    = ".data"          // 数据文件后缀
	HintFileName          = "hint-index"     // hint 索引文件名, 用于存储 key 和数据文件的位置信息
	MergeFinishedFileName = "merge-finished" // merge 完成标识文件名
	SeqNoFileName         = "seq-no"         // 存储事务序列号的文件名
)

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io 读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

// OpenHintFile 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StandardFIO) // 返回数据文件
}

// OpenMergeFinishedFile 打开标识 merge 完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFIO) // 返回数据文件
}

// OpenSeqNoFile 存储事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StandardFIO) // 返回数据文件
}

// GetDataFileName 获取数据文件名
func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix) // 返回数据文件名
}

// newDataFile 初始化数据文件
func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord 根据 offset 从数据文件中读取 LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size() // 获取到文件大小
	if err != nil {
		return nil, 0, err
	}

	// 如果读取的最大 header 长度已经超过了文件的长度，则只需要读取到文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset // 读取的 header 长度
	}

	// 读取 Header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset) // 通过 offset 读取 header 信息
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf) // 解码 header 信息
	if header == nil {
		return nil, 0, io.EOF // 读取到了文件末尾
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF // 读取到了文件末尾
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize // 计算出 LogRecord 的大小

	logRecord := &LogRecord{Type: header.recordType} // 初始化 LogRecord
	// 开始读取用户实际存储的 key/value 数据
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize) // 读取 key/value 数据
		if err != nil {
			return nil, 0, err
		}
		// 解码 key/value 数据
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 对 LogRecord 的数据进行 crc 校验
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize]) // 计算 crc 校验值
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

// Write 写入数据
func (df *DataFile) Write(buf []byte) error {
	// 写入数据
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n) // 更新写入的位置
	return nil
}

// WriteHintRecord 写入 Hint 索引记录
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos), // 将 LogRecordPos 编码成字节数组
	}
	encRecord, _ := EncodeLogRecord(record) // 编码 LogRecord
	return df.Write(encRecord)              // 写入数据
}

// Sync 同步数据
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

// Close 关闭数据文件
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// SetIOManager 设置 IOManager
func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}

// readNBytes 从 offset 位置读取 n 个字节
func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	buf := make([]byte, n)
	_, err := df.IoManager.Read(buf, offset) // 从 offset 位置读取 n 个字节
	return buf, err
}
