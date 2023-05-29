package server

import "time"

// WithTCPKeepAlivePeriod sets tcp keepalive period.
// 设置 tcp-Keepalive 间隔
func WithTCPKeepAlivePeriod(period time.Duration) OptionFn {
	return func(s *Server) {
		s.options["TCPKeepAlivePeriod"] = period
	}
}
