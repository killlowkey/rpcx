package client

import (
	"context"
	"net"

	"github.com/smallnest/rpcx/protocol"
)

// pluginContainer implements PluginContainer interface.
// 作为全局插件的入口点，来去调用容器中所有插件，需要重点关注插件生命周期
type pluginContainer struct {
	plugins []Plugin
}

func NewPluginContainer() PluginContainer {
	return &pluginContainer{}
}

// Plugin is the client plugin interface.
type Plugin interface{}

// Add adds a plugin.
func (p *pluginContainer) Add(plugin Plugin) {
	p.plugins = append(p.plugins, plugin)
}

// Remove removes a plugin by its name.
func (p *pluginContainer) Remove(plugin Plugin) {
	if p.plugins == nil {
		return
	}

	var plugins []Plugin
	for _, pp := range p.plugins {
		if pp != plugin {
			plugins = append(plugins, pp)
		}
	}

	p.plugins = plugins
}

// All returns all plugins
func (p *pluginContainer) All() []Plugin {
	return p.plugins
}

// DoPreCall executes before call
func (p *pluginContainer) DoPreCall(ctx context.Context, servicePath, serviceMethod string, args interface{}) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PreCallPlugin); ok {
			err := plugin.PreCall(ctx, servicePath, serviceMethod, args)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// DoPostCall executes after call
func (p *pluginContainer) DoPostCall(ctx context.Context, servicePath, serviceMethod string, args interface{}, reply interface{}, err error) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PostCallPlugin); ok {
			err = plugin.PostCall(ctx, servicePath, serviceMethod, args, reply, err)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// DoConnCreated is called in case of client connection created.
func (p *pluginContainer) DoConnCreated(conn net.Conn) (net.Conn, error) {
	var err error
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(ConnCreatedPlugin); ok {
			conn, err = plugin.ConnCreated(conn)
			if err != nil {
				return conn, err
			}
		}
	}
	return conn, nil
}

// DoConnCreated is called in case of client connection created.
func (p *pluginContainer) DoConnCreateFailed(network, address string) {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(ConnCreateFailedPlugin); ok {
			plugin.ConnCreateFailed(network, address)
		}
	}
}

// DoClientConnected is called in case of connected.
func (p *pluginContainer) DoClientConnected(conn net.Conn) (net.Conn, error) {
	var err error
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(ClientConnectedPlugin); ok {
			conn, err = plugin.ClientConnected(conn)
			if err != nil {
				return conn, err
			}
		}
	}
	return conn, nil
}

// DoClientConnected is called in case of connected.
func (p *pluginContainer) DoClientConnectionClose(conn net.Conn) error {
	var err error
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(ClientConnectionClosePlugin); ok {
			err = plugin.ClientConnectionClose(conn)
			if err != nil {
				return err
			}
		}
	}
	return err
}

// DoClientBeforeEncode is called when requests are encoded and sent.
func (p *pluginContainer) DoClientBeforeEncode(req *protocol.Message) error {
	var err error
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(ClientBeforeEncodePlugin); ok {
			err = plugin.ClientBeforeEncode(req)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// DoClientBeforeEncode is called when requests are encoded and sent.
func (p *pluginContainer) DoClientAfterDecode(req *protocol.Message) error {
	var err error
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(ClientAfterDecodePlugin); ok {
			err = plugin.ClientAfterDecode(req)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// DoWrapSelect is called when select a node.
func (p *pluginContainer) DoWrapSelect(fn SelectFunc) SelectFunc {
	rt := fn
	for i := range p.plugins {
		if pn, ok := p.plugins[i].(SelectNodePlugin); ok {
			rt = pn.WrapSelect(rt)
		}
	}

	return rt
}

// Plugin 行为进行拆分，只需要实现特定方法就行，无需实现整体方法
type (
	// PreCallPlugin is invoked before the client calls a server.
	// 客户端调用服务之前被调用
	PreCallPlugin interface {
		PreCall(ctx context.Context, servicePath, serviceMethod string, args interface{}) error
	}

	// PostCallPlugin is invoked after the client calls a server.
	// 客户端调用服务之后被调用
	PostCallPlugin interface {
		PostCall(ctx context.Context, servicePath, serviceMethod string, args interface{}, reply interface{}, err error) error
	}

	// ConnCreatedPlugin is invoked when the client connection has created.
	// 当连接被创建时被调用
	ConnCreatedPlugin interface {
		ConnCreated(net.Conn) (net.Conn, error)
	}

	// ConnCreateFailedPlugin is invoked when the client connection has failed
	// 当连接创建失败时被调用
	ConnCreateFailedPlugin interface {
		ConnCreateFailed(network, address string)
	}

	// ClientConnectedPlugin is invoked when the client has connected the server.
	// 当客户端连接到服务端被调用
	ClientConnectedPlugin interface {
		ClientConnected(net.Conn) (net.Conn, error)
	}

	// ClientConnectionClosePlugin is invoked when the connection is closing.
	// 客户端处于正在关闭状态被调用
	ClientConnectionClosePlugin interface {
		ClientConnectionClose(net.Conn) error
	}

	// ClientBeforeEncodePlugin is invoked when the message is encoded and sent.
	// 客户端消息处于编码之前被调用
	ClientBeforeEncodePlugin interface {
		ClientBeforeEncode(*protocol.Message) error
	}

	// ClientAfterDecodePlugin is invoked when the message is decoded.
	// 客户端消息处于编码之后被调用
	ClientAfterDecodePlugin interface {
		ClientAfterDecode(*protocol.Message) error
	}

	// SelectNodePlugin can interrupt selecting of xclient and add customized logics such as skipping some nodes.
	// 进行负载均衡时，选择节点时被调用
	SelectNodePlugin interface {
		WrapSelect(SelectFunc) SelectFunc
	}

	// PluginContainer represents a plugin container that defines all methods to manage plugins.
	// And it also defines all extension points.
	// 分为两大块：
	// 1. 插件的添加与删除
	// 2. 插件生命周期的调用
	PluginContainer interface {
		Add(plugin Plugin)
		Remove(plugin Plugin)
		All() []Plugin

		DoConnCreated(net.Conn) (net.Conn, error)
		DoConnCreateFailed(network, address string)
		DoClientConnected(net.Conn) (net.Conn, error)
		DoClientConnectionClose(net.Conn) error

		DoPreCall(ctx context.Context, servicePath, serviceMethod string, args interface{}) error
		DoPostCall(ctx context.Context, servicePath, serviceMethod string, args interface{}, reply interface{}, err error) error

		DoClientBeforeEncode(*protocol.Message) error
		DoClientAfterDecode(*protocol.Message) error

		DoWrapSelect(SelectFunc) SelectFunc
	}
)
