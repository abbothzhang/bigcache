package bigcache

import "time"

// 提供当前时间戳
type clock interface {
	Epoch() int64
}

type systemClock struct {
}

func (c systemClock) Epoch() int64 {
	return time.Now().Unix()
}
