package serverplugin

import (
	"context"
	"time"

	"github.com/juju/ratelimit"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
)

// ReqRateLimitingPlugin can limit requests per unit time
type ReqRateLimitingPlugin struct {
	FillInterval time.Duration     // 间隔
	Capacity     int64             // 间隔放行的流量个数
	bucket       *ratelimit.Bucket // 限流桶
	block        bool              // blocks or return error if reach the limit
}

// NewReqRateLimitingPlugin creates a new RateLimitingPlugin
func NewReqRateLimitingPlugin(fillInterval time.Duration, capacity int64, block bool) *ReqRateLimitingPlugin {
	tb := ratelimit.NewBucket(fillInterval, capacity)

	return &ReqRateLimitingPlugin{
		FillInterval: fillInterval,
		Capacity:     capacity,
		bucket:       tb,
		block:        block,
	}
}

// PostReadRequest can limit request processing.
func (plugin *ReqRateLimitingPlugin) PostReadRequest(ctx context.Context, r *protocol.Message, e error) error {
	// 如果当前处于 block 那么进行等待
	if plugin.block {
		plugin.bucket.Wait(1)
		return nil
	}

	// 拿到令牌
	count := plugin.bucket.TakeAvailable(1)
	// 拿到令牌
	if count == 1 {
		return nil
	}
	// 当前被限流
	return server.ErrReqReachLimit
}
