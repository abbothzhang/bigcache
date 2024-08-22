package bigcache

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/allegro/bigcache/v3/queue"
)

type onRemoveCallback func(wrappedEntry []byte, reason RemoveReason)

// Metadata contains information of a specific entry
type Metadata struct {
	RequestCount uint32
}

/*
*

	zhmark 2024/8/19

onRemove onRemoveCallback

这是一个回调函数，用于处理缓存条目被移除时的操作。它允许在条目被删除时执行自定义的逻辑，例如资源清理或通知外部系统。
isVerbose bool

这是一个布尔值，指示是否启用详细日志记录。启用详细日志记录时，会输出更多的调试信息，帮助排查问题。
statsEnabled bool

这是一个布尔值，指示是否启用统计信息收集。如果启用，它会收集和记录缓存的运行统计信息。
logger Logger

这是一个日志记录器，用于输出缓存操作的日志。Logger 是一个接口或结构体，用于记录和管理日志信息。
clock clock

这是一个时钟接口或结构体，用于获取当前时间。它可能用于跟踪条目的生命周期或执行时间相关的操作。
lifeWindow uint64

这是一个无符号整数，用于定义缓存条目的生命周期窗口。它表示缓存条目在被认为过期之前的有效时间长度。
hashmapStats map[uint64]uint32

这是一个用于收集哈希映射统计信息的字段。它可能记录了每个哈希值对应的条目数或其他相关统计数据。
stats Stats

这是一个结构体或接口，用于存储和管理缓存的运行统计信息。它可能包含缓存命中率、请求次数等统计数据。
cleanEnabled bool

这是一个布尔值，指示是否启用缓存清理操作。如果启用，缓存会定期执行清理操作，以移除过期或不再使用的条目
*
*/
type cacheShard struct {
	//哈希映射，用于存储缓存条目的索引。键是条目的哈希值，值是条目的位置索引。哈希映射提供了对缓存条目的快速查找
	hashmap map[uint64]uint64
	//字节队列，用于存储实际的缓存数据条目。BytesQueue 是一个队列数据结构，负责管理和存取缓存条目
	entries queue.BytesQueue
	//读写锁，用于保护对 cacheShard 内部数据结构的并发访问。读写锁允许多个读取操作并发进行，但写操作会阻塞所有读操作和其他写操作，从而保证数据的一致性
	lock sync.RWMutex
	//字节切片，用作缓存条目的临时缓冲区。它用于读取和写入条目时的数据缓存，减少内存分配和拷贝的开销
	entryBuffer []byte
	//回调函数，用于处理缓存条目被移除时的操作。它允许在条目被删除时执行自定义的逻辑，例如资源清理或通知外部系统
	onRemove onRemoveCallback
	//布尔值，指示是否启用详细日志记录。启用详细日志记录时，会输出更多的调试信息，帮助排查问题
	isVerbose bool
	//布尔值，指示是否启用统计信息收集。如果启用，它会收集和记录缓存的运行统计信息
	statsEnabled bool
	//日志记录器，支持自定义
	logger Logger
	//获取当前时间
	clock clock
	//无符号整数，用于定义缓存条目的生命周期窗口。它表示缓存条目在被认为过期之前的有效时间长度
	lifeWindow uint64
	//收集哈希映射统计信息的字段。记录了每个哈希值对应的条目数或其他相关统计数据
	hashmapStats map[uint64]uint32
	//存储和管理缓存的运行统计信息。它可能包含缓存命中率、请求次数等统计数据
	stats Stats
	//是否启用缓存清理操作。如果启用，缓存会定期执行清理操作，以移除过期或不再使用的条目
	cleanEnabled bool
}

func (s *cacheShard) getWithInfo(key string, hashedKey uint64) (entry []byte, resp Response, err error) {
	currentTime := uint64(s.clock.Epoch())
	s.lock.RLock()
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		s.lock.RUnlock()
		return nil, resp, err
	}
	if entryKey := readKeyFromEntry(wrappedEntry); key != entryKey {
		s.lock.RUnlock()
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, entryKey, hashedKey)
		}
		return nil, resp, ErrEntryNotFound
	}

	entry = readEntry(wrappedEntry)
	if s.isExpired(wrappedEntry, currentTime) {
		resp.EntryStatus = Expired
	}
	s.lock.RUnlock()
	s.hit(hashedKey)
	return entry, resp, nil
}

func (s *cacheShard) get(key string, hashedKey uint64) ([]byte, error) {
	s.lock.RLock()
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		s.lock.RUnlock()
		return nil, err
	}

	//zhmark 2024/8/21 有可能因为不同的key生成的hashedKey相同，虽然这次get时候的hashkey在map中存在，
	//但是实际上是其他key存入的，这个时候实际上是没有set过这个key的
	if entryKey := readKeyFromEntry(wrappedEntry); key != entryKey {
		s.lock.RUnlock()
		s.collision()
		if s.isVerbose {
			// 发生碰撞
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, entryKey, hashedKey)
		}
		return nil, ErrEntryNotFound
	}
	entry := readEntry(wrappedEntry)
	s.lock.RUnlock()
	s.hit(hashedKey)

	return entry, nil
}

func (s *cacheShard) getWrappedEntry(hashedKey uint64) ([]byte, error) {
	itemIndex := s.hashmap[hashedKey]

	if itemIndex == 0 {
		s.miss()
		return nil, ErrEntryNotFound
	}

	wrappedEntry, err := s.entries.Get(int(itemIndex))
	if err != nil {
		s.miss()
		return nil, err
	}

	return wrappedEntry, err
}

func (s *cacheShard) getValidWrapEntry(key string, hashedKey uint64) ([]byte, error) {
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		return nil, err
	}

	if !compareKeyFromEntry(wrappedEntry, key) {
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, readKeyFromEntry(wrappedEntry), hashedKey)
		}

		return nil, ErrEntryNotFound
	}
	s.hitWithoutLock(hashedKey)

	return wrappedEntry, nil
}

func (s *cacheShard) set(key string, hashedKey uint64, value []byte) error {
	currentTimestamp := uint64(s.clock.Epoch())

	s.lock.Lock()

	//检查 hashmap 中是否已有相同的 hashedKey。
	//如果有，获取先前的条目，并调用 resetHashFromEntry 函数重置该条目的哈希值（通常是为了清理之前的哈希映射）。
	//然后，从 hashmap 中删除旧的 hashedKey 条目
	if previousIndex := s.hashmap[hashedKey]; previousIndex != 0 {
		if previousEntry, err := s.entries.Get(int(previousIndex)); err == nil {
			//打上一个已处理的标记，保证数据在淘汰的时候不再去调用OnRemove的callback
			resetHashFromEntry(previousEntry)
			//remove hashkey
			delete(s.hashmap, hashedKey)
		}
	}

	//如果清理（淘汰）功能未启用，则检查缓存中是否存在最旧的条目（oldestEntry）。
	//如果存在，调用 s.onEvict 方法处理淘汰操作，将最旧的条目移除或进行其他处理
	if !s.cleanEnabled {
		if oldestEntry, err := s.entries.Peek(); err == nil {
			s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
		}
	}

	// 使用 wrapEntry 函数将条目封装起来。wrapEntry 函数创建一个新的条目对象，
	//包括时间戳、哈希键、原始键、条目数据以及一个缓冲区（entryBuffer），以便在缓存中进行存储
	w := wrapEntry(currentTimestamp, hashedKey, key, value, &s.entryBuffer)

	//使用循环将封装的条目 (w) 推入 entries 数据结构中。如果成功，更新 hashmap 中的索引，并解锁 lock，然后返回 nil 表示成功。
	//如果推入操作失败（例如由于空间不足），调用 s.removeOldestEntry(NoSpace) 尝试移除最旧的条目以腾出空间。
	//如果此操作仍未成功（即条目太大无法放入缓存），则解锁并返回一个错误，表示条目超出了最大缓存分片大小
	for {
		// 将包装过的value放入entries中
		if index, err := s.entries.Push(w); err == nil {
			s.hashmap[hashedKey] = uint64(index)
			s.lock.Unlock()
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			s.lock.Unlock()
			return errors.New("entry is bigger than max shard size")
		}
	}
}

func (s *cacheShard) addNewWithoutLock(key string, hashedKey uint64, entry []byte) error {
	currentTimestamp := uint64(s.clock.Epoch())

	if !s.cleanEnabled {
		if oldestEntry, err := s.entries.Peek(); err == nil {
			s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
		}
	}

	w := wrapEntry(currentTimestamp, hashedKey, key, entry, &s.entryBuffer)

	for {
		if index, err := s.entries.Push(w); err == nil {
			s.hashmap[hashedKey] = uint64(index)
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			return errors.New("entry is bigger than max shard size")
		}
	}
}

func (s *cacheShard) setWrappedEntryWithoutLock(currentTimestamp uint64, w []byte, hashedKey uint64) error {
	if previousIndex := s.hashmap[hashedKey]; previousIndex != 0 {
		if previousEntry, err := s.entries.Get(int(previousIndex)); err == nil {
			resetHashFromEntry(previousEntry)
		}
	}

	if !s.cleanEnabled {
		if oldestEntry, err := s.entries.Peek(); err == nil {
			s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
		}
	}

	for {
		if index, err := s.entries.Push(w); err == nil {
			s.hashmap[hashedKey] = uint64(index)
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			return errors.New("entry is bigger than max shard size")
		}
	}
}

func (s *cacheShard) append(key string, hashedKey uint64, entry []byte) error {
	s.lock.Lock()
	wrappedEntry, err := s.getValidWrapEntry(key, hashedKey)

	if err == ErrEntryNotFound {
		err = s.addNewWithoutLock(key, hashedKey, entry)
		s.lock.Unlock()
		return err
	}
	if err != nil {
		s.lock.Unlock()
		return err
	}

	currentTimestamp := uint64(s.clock.Epoch())

	w := appendToWrappedEntry(currentTimestamp, wrappedEntry, entry, &s.entryBuffer)

	err = s.setWrappedEntryWithoutLock(currentTimestamp, w, hashedKey)
	s.lock.Unlock()

	return err
}

func (s *cacheShard) del(hashedKey uint64) error {
	// Optimistic pre-check using only readlock
	s.lock.RLock()
	{
		itemIndex := s.hashmap[hashedKey]

		if itemIndex == 0 {
			s.lock.RUnlock()
			s.delmiss()
			return ErrEntryNotFound
		}

		if err := s.entries.CheckGet(int(itemIndex)); err != nil {
			s.lock.RUnlock()
			s.delmiss()
			return err
		}
	}
	s.lock.RUnlock()

	s.lock.Lock()
	{
		// After obtaining the writelock, we need to read the same again,
		// since the data delivered earlier may be stale now
		itemIndex := s.hashmap[hashedKey]

		if itemIndex == 0 {
			s.lock.Unlock()
			s.delmiss()
			return ErrEntryNotFound
		}

		wrappedEntry, err := s.entries.Get(int(itemIndex))
		if err != nil {
			s.lock.Unlock()
			s.delmiss()
			return err
		}

		delete(s.hashmap, hashedKey)
		s.onRemove(wrappedEntry, Deleted)
		if s.statsEnabled {
			delete(s.hashmapStats, hashedKey)
		}
		resetHashFromEntry(wrappedEntry)
	}
	s.lock.Unlock()

	s.delhit()
	return nil
}

func (s *cacheShard) onEvict(oldestEntry []byte, currentTimestamp uint64, evict func(reason RemoveReason) error) bool {
	if s.isExpired(oldestEntry, currentTimestamp) {
		evict(Expired)
		return true
	}
	return false
}

func (s *cacheShard) isExpired(oldestEntry []byte, currentTimestamp uint64) bool {
	oldestTimestamp := readTimestampFromEntry(oldestEntry)
	if currentTimestamp <= oldestTimestamp { // if currentTimestamp < oldestTimestamp, the result will out of uint64 limits;
		return false
	}
	return currentTimestamp-oldestTimestamp > s.lifeWindow
}

func (s *cacheShard) cleanUp(currentTimestamp uint64) {
	s.lock.Lock()
	for {
		if oldestEntry, err := s.entries.Peek(); err != nil {
			break
		} else if evicted := s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry); !evicted {
			break
		}
	}
	s.lock.Unlock()
}

func (s *cacheShard) getEntry(hashedKey uint64) ([]byte, error) {
	s.lock.RLock()

	entry, err := s.getWrappedEntry(hashedKey)
	// copy entry
	newEntry := make([]byte, len(entry))
	copy(newEntry, entry)

	s.lock.RUnlock()

	return newEntry, err
}

func (s *cacheShard) copyHashedKeys() (keys []uint64, next int) {
	s.lock.RLock()
	keys = make([]uint64, len(s.hashmap))

	for key := range s.hashmap {
		keys[next] = key
		next++
	}

	s.lock.RUnlock()
	return keys, next
}

func (s *cacheShard) removeOldestEntry(reason RemoveReason) error {
	oldest, err := s.entries.Pop()
	if err == nil {
		hash := readHashFromEntry(oldest)
		if hash == 0 {
			// entry has been explicitly deleted with resetHashFromEntry, ignore
			return nil
		}
		delete(s.hashmap, hash)
		s.onRemove(oldest, reason)
		if s.statsEnabled {
			delete(s.hashmapStats, hash)
		}
		return nil
	}
	return err
}

func (s *cacheShard) reset(config Config) {
	s.lock.Lock()
	s.hashmap = make(map[uint64]uint64, config.initialShardSize())
	s.entryBuffer = make([]byte, config.MaxEntrySize+headersSizeInBytes)
	s.entries.Reset()
	s.lock.Unlock()
}

func (s *cacheShard) resetStats() {
	s.lock.Lock()
	s.stats = Stats{}
	s.lock.Unlock()
}

func (s *cacheShard) len() int {
	s.lock.RLock()
	res := len(s.hashmap)
	s.lock.RUnlock()
	return res
}

func (s *cacheShard) capacity() int {
	s.lock.RLock()
	res := s.entries.Capacity()
	s.lock.RUnlock()
	return res
}

func (s *cacheShard) getStats() Stats {
	var stats = Stats{
		Hits:       atomic.LoadInt64(&s.stats.Hits),
		Misses:     atomic.LoadInt64(&s.stats.Misses),
		DelHits:    atomic.LoadInt64(&s.stats.DelHits),
		DelMisses:  atomic.LoadInt64(&s.stats.DelMisses),
		Collisions: atomic.LoadInt64(&s.stats.Collisions),
	}
	return stats
}

func (s *cacheShard) getKeyMetadataWithLock(key uint64) Metadata {
	s.lock.RLock()
	c := s.hashmapStats[key]
	s.lock.RUnlock()
	return Metadata{
		RequestCount: c,
	}
}

func (s *cacheShard) getKeyMetadata(key uint64) Metadata {
	return Metadata{
		RequestCount: s.hashmapStats[key],
	}
}

func (s *cacheShard) hit(key uint64) {
	atomic.AddInt64(&s.stats.Hits, 1)
	if s.statsEnabled {
		s.lock.Lock()
		s.hashmapStats[key]++
		s.lock.Unlock()
	}
}

func (s *cacheShard) hitWithoutLock(key uint64) {
	atomic.AddInt64(&s.stats.Hits, 1)
	if s.statsEnabled {
		s.hashmapStats[key]++
	}
}

// 将缓存未命中的计数器加一
func (s *cacheShard) miss() {
	atomic.AddInt64(&s.stats.Misses, 1)
}

func (s *cacheShard) delhit() {
	atomic.AddInt64(&s.stats.DelHits, 1)
}

func (s *cacheShard) delmiss() {
	atomic.AddInt64(&s.stats.DelMisses, 1)
}

func (s *cacheShard) collision() {
	atomic.AddInt64(&s.stats.Collisions, 1)
}

func initNewShard(config Config, callback onRemoveCallback, clock clock) *cacheShard {
	bytesQueueInitialCapacity := config.initialShardSize() * config.MaxEntrySize
	maximumShardSizeInBytes := config.maximumShardSizeInBytes()
	if maximumShardSizeInBytes > 0 && bytesQueueInitialCapacity > maximumShardSizeInBytes {
		bytesQueueInitialCapacity = maximumShardSizeInBytes
	}
	return &cacheShard{
		hashmap:      make(map[uint64]uint64, config.initialShardSize()),
		hashmapStats: make(map[uint64]uint32, config.initialShardSize()),
		entries:      *queue.NewBytesQueue(bytesQueueInitialCapacity, maximumShardSizeInBytes, config.Verbose),
		entryBuffer:  make([]byte, config.MaxEntrySize+headersSizeInBytes),
		onRemove:     callback,

		isVerbose:    config.Verbose,
		logger:       newLogger(config.Logger),
		clock:        clock,
		lifeWindow:   uint64(config.LifeWindow.Seconds()),
		statsEnabled: config.StatsEnabled,
		cleanEnabled: config.CleanWindow > 0,
	}
}
