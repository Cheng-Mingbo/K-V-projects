package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished // 事务结束标识
)

// crc type keySize valueSize
// 4 +  1  +  5   +   5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5 // 最大的 LogRecord 头部信息的长度

// LogRecord 写入到数据文件的记录
type LogRecord struct {
	Key   []byte        // 关键字
	Value []byte        // 值
	Type  LogRecordType // 类型
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// LogRecordPos 数据内存索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的哪个位置
	Size   uint32 // 标识数据在磁盘上的大小
}

// TransactionRecord 暂存的事务相关的数据
type TransactionRecord struct {
	Record *LogRecord    // 记录
	Pos    *LogRecordPos // 位置
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize) // 初始化一个 header 部分的字节数组

	// 第五个字节存储 Type
	header[4] = logRecord.Type
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))   // key 的长度
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value))) // value 的长度

	var size = index + len(logRecord.Key) + len(logRecord.Value) // 计算总长度
	encBytes := make([]byte, size)                               // 初始化一个总长度的字节数组

	copy(encBytes, header)                                     // 拷贝 header
	copy(encBytes[index:], logRecord.Key)                      // 拷贝 key
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value) // 拷贝 value

	// 计算 crc 校验值
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc) // 将 crc 校验值写入到 encBytes 的前四个字节中

	return encBytes, int64(size)
}

// EncodeLogRecordPos 对 LogRecordPos 进行编码，返回字节数组
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64) // 初始化一个最大长度的字节数组, 2 个 uint32 和 1 个 int64
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))  // 将 fid 写入到 buf 中
	index += binary.PutVarint(buf[index:], pos.Offset)      // 将 offset 写入到 buf 中
	index += binary.PutVarint(buf[index:], int64(pos.Size)) // 将 size 写入到 buf 中
	return buf[:index]
}

// DecodeLogRecordPos 对 LogRecordPos 进行解码，返回 LogRecordPos
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fid, n := binary.Varint(buf[index:]) // 从 buf 中解码出 fid
	index += n
	offset, n := binary.Varint(buf[index:]) // 从 buf 中解码出 offset
	index += n
	size, _ := binary.Varint(buf[index:]) // 从 buf 中解码出 size
	return &LogRecordPos{
		Fid:    uint32(fid),
		Offset: offset,
		Size:   uint32(size),
	}
}

// decodeLogRecordHeader 解码 LogRecord 的头部信息
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]), // 从 buf 中解码出 crc 校验值
		recordType: buf[4],                              // 从 buf 中解码出 recordType
	}

	var index = 5
	keySize, n := binary.Varint(buf[index:]) // 从 buf 中解码出 keySize
	index += n
	header.keySize = uint32(keySize)

	valueSize, n := binary.Varint(buf[index:]) // 从 buf 中解码出 valueSize
	index += n
	header.valueSize = uint32(valueSize)

	return header, int64(index)
}

// getLogRecordCRC 计算 LogRecord 的 crc 校验值
func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	if logRecord == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])                      // 计算 crc 校验值
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)   // 更新 crc 校验值
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value) // 更新 crc 校验值
	return crc
}
