package bigcache

// newDefaultHasher returns a new 64-bit FNV-1a Hasher which makes no memory allocations.
// Its Sum64 method will lay the value out in big-endian byte order.
// See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
func newDefaultHasher() Hasher {
	return fnv64a{}
}

type fnv64a struct{}

const (
	// offset64 FNVa offset basis. See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
	offset64 = 14695981039346656037
	// prime64 FNVa prime value. See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
	prime64 = 1099511628211
)

// Sum64 gets the string and returns its uint64 hash value.
// 这个 Sum64 方法实现了一个基于 FNV-1a 哈希算法的 fnv64a 类型的哈希计算。FNV-1a 是一种简单且有效的哈希算法，广泛用于计算哈希值
func (f fnv64a) Sum64(key string) uint64 {
	var hash uint64 = offset64
	for i := 0; i < len(key); i++ {
		//异或操作：通过将每个字符的字节值与当前的哈希值异或，增加了字符的影响。
		hash ^= uint64(key[i])
		//乘法扰动：通过乘以一个常量 prime64，进一步混合哈希值，增加散列的均匀性和随机性
		hash *= prime64
	}

	return hash
}
