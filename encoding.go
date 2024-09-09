package bigcache

import (
	"encoding/binary"
)

const (
	timestampSizeInBytes = 8                                                       // Number of bytes used for timestamp
	hashSizeInBytes      = 8                                                       // Number of bytes used for hash
	keySizeInBytes       = 2                                                       // Number of bytes used for size of entry key
	headersSizeInBytes   = timestampSizeInBytes + hashSizeInBytes + keySizeInBytes // Number of bytes used for all headers
)

// wrapEntry
//
//	@Description: zhmark 2024/8/20 核心存储结构
//
// blob:Binary Large Object,二进制大对象
// entry结构：timestamp + hashkey + keyLength + key + value
//
//	@param timestamp:
//	@param hash
//	@param key
//	@param entry
//	@param buffer
//	@return []byte
func wrapEntry(timestamp uint64, hash uint64, key string, value []byte, buffer *[]byte) []byte {
	keyLength := len(key)
	// headersSizeInBytes = timestampSizeInBytes + hashSizeInBytes + keySizeInBytes
	blobLength := headersSizeInBytes + keyLength + len(value)

	// 如果 buffer 的长度不足以容纳打包后的数据，重新分配一个足够大的字节切片
	if blobLength > len(*buffer) {
		*buffer = make([]byte, blobLength)
	}
	//将 buffer 指向的字节切片赋值给 blob，以便后续操作直接修改 blob
	blob := *buffer
	// zhmark 2024/8/21 blob结构：timestamp + hashkey + keyLength + key + value
	binary.LittleEndian.PutUint64(blob, timestamp)
	binary.LittleEndian.PutUint64(blob[timestampSizeInBytes:], hash)
	binary.LittleEndian.PutUint16(blob[timestampSizeInBytes+hashSizeInBytes:], uint16(keyLength))
	//使用 copy 函数将 key 和 value 复制到 blob 中
	copy(blob[headersSizeInBytes:], key)
	copy(blob[headersSizeInBytes+keyLength:], value)

	return blob[:blobLength]
}

func appendToWrappedEntry(timestamp uint64, wrappedEntry []byte, entry []byte, buffer *[]byte) []byte {
	blobLength := len(wrappedEntry) + len(entry)
	if blobLength > len(*buffer) {
		*buffer = make([]byte, blobLength)
	}

	blob := *buffer

	binary.LittleEndian.PutUint64(blob, timestamp)
	copy(blob[timestampSizeInBytes:], wrappedEntry[timestampSizeInBytes:])
	copy(blob[len(wrappedEntry):], entry)

	return blob[:blobLength]
}

func readEntry(data []byte) []byte {
	length := binary.LittleEndian.Uint16(data[timestampSizeInBytes+hashSizeInBytes:])

	// copy on read
	dst := make([]byte, len(data)-int(headersSizeInBytes+length))
	copy(dst, data[headersSizeInBytes+length:])

	return dst
}

func readTimestampFromEntry(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func readKeyFromEntry(data []byte) string {
	length := binary.LittleEndian.Uint16(data[timestampSizeInBytes+hashSizeInBytes:])

	// copy on read
	dst := make([]byte, length)
	copy(dst, data[headersSizeInBytes:headersSizeInBytes+length])

	return bytesToString(dst)
}

func compareKeyFromEntry(data []byte, key string) bool {
	length := binary.LittleEndian.Uint16(data[timestampSizeInBytes+hashSizeInBytes:])

	return bytesToString(data[headersSizeInBytes:headersSizeInBytes+length]) == key
}

func readHashFromEntry(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data[timestampSizeInBytes:])
}

// 将 data 切片中从 timestampSizeInBytes 位置开始的数据重置为 0。确保这些数据字段不再持有之前的引用，释放资源和清理数据
func resetHashFromEntry(data []byte) {
	binary.LittleEndian.PutUint64(data[timestampSizeInBytes:], 0)
}
