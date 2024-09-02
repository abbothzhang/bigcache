package queue

import (
	"encoding/binary"
	"log"
	"time"
)

const (
	// Number of bytes to encode 0 in uvarint format
	minimumHeaderSize = 17 // 1 byte blobsize + timestampSizeInBytes + hashSizeInBytes
	// Bytes before left margin are not used. Zero index means element does not exist in queue, useful while reading slice from index
	// 因为head、tail默认会被赋初始值0，为了区分是否默认赋值，初始化时将值设为leftMarginIndex=1
	leftMarginIndex = 1
)

var (
	errEmptyQueue       = &queueError{"Empty queue"}
	errInvalidIndex     = &queueError{"Index must be greater than zero. Invalid index."}
	errIndexOutOfBounds = &queueError{"Index out of range"}
)

// BytesQueue is a non-thread safe queue type of fifo based on bytes array.
// For every push operation index of entry is returned. It can be used to read the entry later
// BytesQueue 是一个用于管理字节数据的队列结构体
type BytesQueue struct {
	// full 表示队列是否已满
	full bool
	// array 存储队列中的实际数据
	array []byte
	// capacity 当前队列的容量，即可以容纳的最大字节数
	capacity int
	// maxCapacity 队列的最大容量，表示队列能够扩展到的最大字节数
	maxCapacity int
	// head 队列的头部索引，指示从哪个位置开始读取数据
	head int
	// tail 队列的尾部索引，指示在什么位置写入新数据
	tail int
	// count 当前队列中的元素数量
	count int
	// rightMargin 用于计算或管理队列中的边际空间
	rightMargin int
	// headerBuffer 可能用于存储额外的头部数据或作为缓存的一部分
	headerBuffer []byte
	// verbose 控制是否启用详细模式，用于调试和记录
	verbose bool
}

type queueError struct {
	message string
}

// getNeededSize returns the number of bytes an entry of length need in the queue
func getNeededSize(length int) int {
	var header int
	switch {
	case length < 127: // 1<<7-1
		header = 1
	case length < 16382: // 1<<14-2
		header = 2
	case length < 2097149: // 1<<21 -3
		header = 3
	case length < 268435452: // 1<<28 -4
		header = 4
	default:
		header = 5
	}

	return length + header
}

// NewBytesQueue initialize new bytes queue.
// capacity is used in bytes array allocation
// When verbose flag is set then information about memory allocation are printed
//
// NewBytesQueue initialize new bytes queue.
//
//	@Description: 初始化字节队列
//	@param capacity: 当前队列的容量，即可以容纳的最大字节数
//	@param maxCapacity: 队列的最大容量，表示队列能够扩展到的最大字节数
//	@param verbose
//	@return *BytesQueue
func NewBytesQueue(capacity int, maxCapacity int, verbose bool) *BytesQueue {
	return &BytesQueue{
		array:        make([]byte, capacity),
		capacity:     capacity,
		maxCapacity:  maxCapacity,
		headerBuffer: make([]byte, binary.MaxVarintLen32),
		tail:         leftMarginIndex,
		head:         leftMarginIndex,
		rightMargin:  leftMarginIndex,
		verbose:      verbose,
	}
}

// Reset removes all entries from queue
func (q *BytesQueue) Reset() {
	// Just reset indexes
	q.tail = leftMarginIndex
	q.head = leftMarginIndex
	q.rightMargin = leftMarginIndex
	q.count = 0
	q.full = false
}

// Push copies entry at the end of queue and moves tail pointer. Allocates more space if needed.
// Returns index for pushed data or error if maximum size queue limit is reached.

// Push copies entry at the end of queue and moves tail pointer. Allocates more space if needed.
// 将value放入队列中，并返回index
//
//	@Description:
//	@receiver q
//	@param data
//	@return int
//	@return error
//
// Push 方法向队列中添加数据，接收一个 []byte 类型的参数 data，并返回插入位置的索引和可能发生的错误。
/**
 zhmark 2024/8/30
 从队列头部0开始，慢慢往后插入数据
**/
func (q *BytesQueue) Push(data []byte) (int, error) {
	// 计算插入数据所需的大小
	neededSize := getNeededSize(len(data))

	// 检查队列是否能在尾部插入所需大小的数据
	if !q.canInsertAfterTail(neededSize) {
		// 如果不能在尾部插入数据，说明队列已经满了
		//检查是否可以在头部插入
		if q.canInsertBeforeHead(neededSize) {
			// 如果可以在头部前插入数据，调整尾部位置到队列的左边缘位置
			q.tail = leftMarginIndex
		} else if q.capacity+neededSize >= q.maxCapacity && q.maxCapacity > 0 {
			// 如果队列的当前容量加上需要插入的数据大小大于或等于最大容量限制，并且最大容量大于0，返回错误
			return -1, &queueError{"Full queue. Maximum size limit reached."}
		} else {
			// 如果不能在尾部或头部插入且没有达到最大容量限制，则分配额外的内存以容纳新数据
			q.allocateAdditionalMemory(neededSize)
		}
	}

	// 将当前尾部的位置赋值给 index，用于记录数据插入的位置
	index := q.tail

	// 调用 push 方法将数据实际插入到队列中
	q.push(data, neededSize)

	// 返回数据插入的位置索引和 nil 错误，表示操作成功
	return index, nil
}

// 重新扩容，建一个2倍大小的byte，将老数据复制过去
func (q *BytesQueue) allocateAdditionalMemory(minimum int) {
	start := time.Now()
	if q.capacity < minimum {
		q.capacity += minimum
	}
	q.capacity = q.capacity * 2
	if q.capacity > q.maxCapacity && q.maxCapacity > 0 {
		q.capacity = q.maxCapacity
	}

	oldArray := q.array
	q.array = make([]byte, q.capacity)

	if leftMarginIndex != q.rightMargin {
		//
		copy(q.array, oldArray[:q.rightMargin])

		if q.tail <= q.head {
			if q.tail != q.head {
				// created slice is slightly larger than need but this is fine after only the needed bytes are copied
				q.push(make([]byte, q.head-q.tail), q.head-q.tail)
			}

			q.head = leftMarginIndex
			q.tail = q.rightMargin
		}
	}

	q.full = false

	if q.verbose {
		log.Printf("Allocated new queue in %s; Capacity: %d \n", time.Since(start), q.capacity)
	}
}

func (q *BytesQueue) push(data []byte, len int) {
	headerEntrySize := binary.PutUvarint(q.headerBuffer, uint64(len))
	q.copy(q.headerBuffer, headerEntrySize)

	q.copy(data, len-headerEntrySize)

	if q.tail > q.head {
		q.rightMargin = q.tail
	}
	if q.tail == q.head {
		q.full = true
	}

	q.count++
}

func (q *BytesQueue) copy(data []byte, len int) {
	q.tail += copy(q.array[q.tail:], data[:len])
}

// Pop reads the oldest entry from queue and moves head pointer to the next one
func (q *BytesQueue) Pop() ([]byte, error) {
	data, blockSize, err := q.peek(q.head)
	if err != nil {
		return nil, err
	}

	q.head += blockSize
	q.count--

	if q.head == q.rightMargin {
		q.head = leftMarginIndex
		if q.tail == q.rightMargin {
			q.tail = leftMarginIndex
		}
		q.rightMargin = q.tail
	}

	q.full = false

	return data, nil
}

// Peek reads the oldest entry from list without moving head pointer
func (q *BytesQueue) Peek() ([]byte, error) {
	data, _, err := q.peek(q.head)
	return data, err
}

// Get reads entry from index
func (q *BytesQueue) Get(index int) ([]byte, error) {
	data, _, err := q.peek(index)
	return data, err
}

// CheckGet checks if an entry can be read from index
func (q *BytesQueue) CheckGet(index int) error {
	return q.peekCheckErr(index)
}

// Capacity returns number of allocated bytes for queue
func (q *BytesQueue) Capacity() int {
	return q.capacity
}

// Len returns number of entries kept in queue
func (q *BytesQueue) Len() int {
	return q.count
}

// Error returns error message
func (e *queueError) Error() string {
	return e.message
}

// peekCheckErr is identical to peek, but does not actually return any data
func (q *BytesQueue) peekCheckErr(index int) error {

	if q.count == 0 {
		return errEmptyQueue
	}

	if index <= 0 {
		return errInvalidIndex
	}

	if index >= len(q.array) {
		return errIndexOutOfBounds
	}
	return nil
}

// peek returns the data from index and the number of bytes to encode the length of the data in uvarint format
func (q *BytesQueue) peek(index int) ([]byte, int, error) {
	err := q.peekCheckErr(index)
	if err != nil {
		return nil, 0, err
	}

	blockSize, n := binary.Uvarint(q.array[index:])
	return q.array[index+n : index+int(blockSize)], int(blockSize), nil
}

// canInsertAfterTail returns true if it's possible to insert an entry of size of need after the tail of the queue

func (q *BytesQueue) canInsertAfterTail(need int) bool {
	if q.full {
		return false
	}
	// 如果尾部在头后面，说明还没重写，只要队列长度减去尾部位置大于需要的大小就行
	if q.tail >= q.head {
		return q.capacity-q.tail >= need
	}
	// 1. there is exactly need bytes between head and tail, so we do not need
	// to reserve extra space for a potential empty entry when realloc this queue
	// 2. still have unused space between tail and head, then we must reserve
	// at least headerEntrySize bytes so we can put an empty entry
	// 1. 头部和尾部之间的字节数正好满足需求，因此在重新分配这个队列时，我们不需要为潜在的空条目保留额外的空间。
	// 2. 头部和尾部之间仍然有未使用的空间，因此我们必须至少保留 headerEntrySize 字节，以便能够放置一个空条目
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}

// canInsertBeforeHead returns true if it's possible to insert an entry of size of need before the head of the queue
func (q *BytesQueue) canInsertBeforeHead(need int) bool {
	if q.full {
		return false
	}
	if q.tail >= q.head {
		// q.head-leftMarginIndex == need： 检查从头部 (q.head) 到队列的左边缘 (leftMarginIndex) 的空间是否恰好等于所需的大小 (need)
		// q.head-leftMarginIndex >= need+minimumHeaderSize： 为什么还要多这个判断，有点奇怪 todo
		return q.head-leftMarginIndex == need || q.head-leftMarginIndex >= need+minimumHeaderSize
	}
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}
