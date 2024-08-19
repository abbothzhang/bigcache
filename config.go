package bigcache

import "time"

/**
 zhmark 2024/8/18
Shards: 缓存分片数量，必须是2的幂，用于提高并发性能。
LifeWindow: 缓存条目的有效期，过期后条目会被清除。
CleanWindow: 清理过期条目的时间间隔，设为0表示不自动清理。
MaxEntriesInWindow: 生命周期窗口内的最大条目数，用于计算分片的初始大小。
MaxEntrySize: 单个条目的最大大小（字节），用于计算分片的初始大小。
StatsEnabled: 是否启用统计功能，跟踪缓存资源的访问次数。
Verbose: 是否启用详细模式，打印内存分配信息。
Hasher: 用于将键映射为64位整数的哈希函数，默认使用fnv64哈希。
HardMaxCacheSize: 缓存的最大内存限制（MB），超过后会清除最旧的条目。
OnRemove: 条目被移除时的回调函数，无论是因过期、空间不足还是删除。
OnRemoveWithMetadata: 带有条目详细信息的移除回调。
OnRemoveWithReason: 带有移除原因的回调函数。
Logger: 日志记录接口，与 Verbose 结合使用
**/
// Config for BigCache
type Config struct {
	// Number of cache shards, value must be a power of two
	Shards int
	// Time after which entry can be evicted
	LifeWindow time.Duration
	// Interval between removing expired entries (clean up).
	// If set to <= 0 then no action is performed. Setting to < 1 second is counterproductive — bigcache has a one second resolution.
	CleanWindow time.Duration
	// Max number of entries in life window. Used only to calculate initial size for cache shards.
	// When proper value is set then additional memory allocation does not occur.
	MaxEntriesInWindow int
	// Max size of entry in bytes. Used only to calculate initial size for cache shards.
	MaxEntrySize int
	// StatsEnabled if true calculate the number of times a cached resource was requested.
	StatsEnabled bool
	// Verbose mode prints information about new memory allocation
	Verbose bool
	// Hasher used to map between string keys and unsigned 64bit integers, by default fnv64 hashing is used.
	Hasher Hasher
	// HardMaxCacheSize is a limit for BytesQueue size in MB.
	// It can protect application from consuming all available memory on machine, therefore from running OOM Killer.
	// Default value is 0 which means unlimited size. When the limit is higher than 0 and reached then
	// the oldest entries are overridden for the new ones. The max memory consumption will be bigger than
	// HardMaxCacheSize due to Shards' s additional memory. Every Shard consumes additional memory for map of keys
	// and statistics (map[uint64]uint32) the size of this map is equal to number of entries in
	// cache ~ 2×(64+32)×n bits + overhead or map itself.
	HardMaxCacheSize int
	// OnRemove is a callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	// ignored if OnRemoveWithMetadata is specified.
	OnRemove func(key string, entry []byte)
	// OnRemoveWithMetadata is a callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called. A structure representing details about that specific entry.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	OnRemoveWithMetadata func(key string, entry []byte, keyMetadata Metadata)
	// OnRemoveWithReason is a callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called. A constant representing the reason will be passed through.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	// Ignored if OnRemove is specified.
	OnRemoveWithReason func(key string, entry []byte, reason RemoveReason)

	onRemoveFilter int

	// Logger is a logging interface and used in combination with `Verbose`
	// Defaults to `DefaultLogger()`
	Logger Logger
}

// DefaultConfig initializes config with default values.
// When load for BigCache can be predicted in advance then it is better to use custom config.
func DefaultConfig(eviction time.Duration) Config {
	return Config{
		Shards:             1024,
		LifeWindow:         eviction,
		CleanWindow:        1 * time.Second,
		MaxEntriesInWindow: 1000 * 10 * 60,
		MaxEntrySize:       500,
		StatsEnabled:       false,
		Verbose:            true,
		Hasher:             newDefaultHasher(),
		HardMaxCacheSize:   0,
		Logger:             DefaultLogger(),
	}
}

// initialShardSize computes initial shard size
func (c Config) initialShardSize() int {
	return max(c.MaxEntriesInWindow/c.Shards, minimumEntriesInShard)
}

// maximumShardSizeInBytes computes maximum shard size in bytes
func (c Config) maximumShardSizeInBytes() int {
	maxShardSize := 0

	if c.HardMaxCacheSize > 0 {
		maxShardSize = convertMBToBytes(c.HardMaxCacheSize) / c.Shards
	}

	return maxShardSize
}

// OnRemoveFilterSet sets which remove reasons will trigger a call to OnRemoveWithReason.
// Filtering out reasons prevents bigcache from unwrapping them, which saves cpu.
func (c Config) OnRemoveFilterSet(reasons ...RemoveReason) Config {
	c.onRemoveFilter = 0
	for i := range reasons {
		c.onRemoveFilter |= 1 << uint(reasons[i])
	}

	return c
}
