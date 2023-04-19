package serverplugin

import "net"

// BlacklistPlugin is a plugin that control only ip addresses in blacklist can **NOT** access services.
// 黑名单插件，命中 ip 的地址无法进行访问
type BlacklistPlugin struct {
	Blacklist     map[string]bool
	BlacklistMask []*net.IPNet // net.ParseCIDR("172.17.0.0/16") to get *net.IPNet
}

// HandleConnAccept check ip.
func (plugin *BlacklistPlugin) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	// 分割出 ip 地址
	ip, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn, true
	}
	// 黑名单中命中 ip
	if plugin.Blacklist[ip] {
		return conn, false
	}

	// 解析出远程客户端 ip 地址
	remoteIP := net.ParseIP(ip)
	for _, mask := range plugin.BlacklistMask {
		if mask.Contains(remoteIP) {
			return conn, false
		}
	}

	// 放行
	return conn, true
}
