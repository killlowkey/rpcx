package server

import (
	"crypto/tls"
	"time"

	"github.com/alitto/pond"
)

// OptionFn configures options of server.
// Option 在中间件设计中很常见，一般用于扩展点增强
type OptionFn func(*Server)

// // WithOptions sets multiple options.
// func WithOptions(ops map[string]interface{}) OptionFn {
// 	return func(s *Server) {
// 		for k, v := range ops {
// 			s.options[k] = v
// 		}
// 	}
// }

// WithTLSConfig sets tls.Config.
// TLS 配置
func WithTLSConfig(cfg *tls.Config) OptionFn {
	return func(s *Server) {
		s.tlsConfig = cfg
	}
}

// WithReadTimeout sets readTimeout.
// 设置读超时
func WithReadTimeout(readTimeout time.Duration) OptionFn {
	return func(s *Server) {
		s.readTimeout = readTimeout
	}
}

// WithWriteTimeout sets writeTimeout.
// 写超时
func WithWriteTimeout(writeTimeout time.Duration) OptionFn {
	return func(s *Server) {
		s.writeTimeout = writeTimeout
	}
}

// WithPool sets goroutine pool.
func WithPool(maxWorkers, maxCapacity int, options ...pond.Option) OptionFn {
	return func(s *Server) {
		s.pool = pond.New(maxWorkers, maxCapacity, options...)
	}
}

// WithCustomPool uses a custom goroutine pool.
func WithCustomPool(pool WorkerPool) OptionFn {
	return func(s *Server) {
		s.pool = pool
	}
}

// WithAsyncWrite sets AsyncWrite to true.
// 异步写
func WithAsyncWrite() OptionFn {
	return func(s *Server) {
		s.AsyncWrite = true
	}
}
