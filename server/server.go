package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smallnest/rpcx/log"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/share"
	"github.com/soheilhy/cmux"
	"golang.org/x/net/websocket"
)

// ErrServerClosed is returned by the Server's Serve, ListenAndServe after a call to Shutdown or Close.
var (
	ErrServerClosed  = errors.New("http: Server closed")
	ErrReqReachLimit = errors.New("request reached rate limit")
)

const (
	// ReaderBuffsize is used for bufio reader.
	ReaderBuffsize = 1024
	// WriterBuffsize is used for bufio writer.
	WriterBuffsize = 1024

	// // WriteChanSize is used for response.
	// WriteChanSize = 1024 * 1024
)

// contextKey is a value for use with context.WithValue. It's used as
// a pointer so it fits in an interface{} without allocation.
type contextKey struct {
	name string
}

func (k *contextKey) String() string { return "rpcx context value " + k.name }

var (
	// RemoteConnContextKey is a context key. It can be used in
	// services with context.WithValue to access the connection arrived on.
	// The associated value will be of type net.Conn.
	RemoteConnContextKey = &contextKey{"remote-conn"}
	// StartRequestContextKey records the start time
	StartRequestContextKey = &contextKey{"start-parse-request"}
	// StartSendRequestContextKey records the start time
	StartSendRequestContextKey = &contextKey{"start-send-request"}
	// TagContextKey is used to record extra info in handling services. Its value is a map[string]interface{}
	TagContextKey = &contextKey{"service-tag"}
	// HttpConnContextKey is used to store http connection.
	HttpConnContextKey = &contextKey{"http-conn"}
)

type Handler func(ctx *Context) error

type WorkerPool interface {
	Submit(task func())
	StopAndWaitFor(deadline time.Duration)
	Stop()
	StopAndWait()
}

// Server is rpcx server that use TCP or UDP.
type Server struct {
	ln                 net.Listener  // 连接监听
	readTimeout        time.Duration // 读超时
	writeTimeout       time.Duration // 写超时
	gatewayHTTPServer  *http.Server  // 网关
	jsonrpcHTTPServer  *http.Server
	DisableHTTPGateway bool       // disable http invoke or not.
	DisableJSONRPC     bool       // disable json rpc or not.
	AsyncWrite         bool       // set true if your server only serves few clients
	pool               WorkerPool // 池化 goroutine 用于处理请求

	serviceMapMu sync.RWMutex
	serviceMap   map[string]*service // 服务注册

	router map[string]Handler // 系统内部路由处理

	mu         sync.RWMutex          // 读写锁
	activeConn map[net.Conn]struct{} // 当前持有的连接
	doneChan   chan struct{}         // 完成 channel
	seq        uint64                // 当前序列号

	inShutdown int32             // 是否为关闭状态
	onShutdown []func(s *Server) // 关闭时回调
	onRestart  []func(s *Server) // 重启时回调

	// TLSConfig for creating tls tcp connection.
	// tls 配置，用于配置加密的 connection
	tlsConfig *tls.Config
	// BlockCrypt for kcp.BlockCrypt
	options map[string]interface{}

	// CORS options
	corsOptions *CORSOptions // 跨越选项

	Plugins PluginContainer // 插件容器

	// AuthFunc can be used to auth.
	// 授权函数
	AuthFunc func(ctx context.Context, req *protocol.Message, token string) error

	handlerMsgNum int32 // 当前处理消息数量

	// HandleServiceError is used to get all service errors. You can use it write logs or others.
	// 获取所有服务端错误，可以写入到日志或者其它地方
	HandleServiceError func(error)

	// ServerErrorFunc is a customized error handlers, and you can use it to return customized error strings to clients.
	// If not set, it use err.Error()
	// 自定义的错误处理器，用于返回自定义错误内容给客户端，如果并未设置，将使用 err.Error()
	ServerErrorFunc func(res *protocol.Message, err error) string
}

// NewServer returns a server.
func NewServer(options ...OptionFn) *Server {
	s := &Server{
		Plugins:    &pluginContainer{},
		options:    make(map[string]interface{}),
		activeConn: make(map[net.Conn]struct{}),
		doneChan:   make(chan struct{}),
		serviceMap: make(map[string]*service),
		router:     make(map[string]Handler),
		AsyncWrite: false, // 除非你想做进一步的优化测试，否则建议你设置为false
	}

	// 遍历 Options 进行配置修改
	for _, op := range options {
		op(s)
	}

	if s.options["TCPKeepAlivePeriod"] == nil {
		s.options["TCPKeepAlivePeriod"] = 3 * time.Minute
	}
	return s
}

// Address returns listened address.
// 返回监听的地址
func (s *Server) Address() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// AddHandler 添加处理器，类似于路由机制
func (s *Server) AddHandler(servicePath, serviceMethod string, handler func(*Context) error) {
	s.router[servicePath+"."+serviceMethod] = handler
}

// ActiveClientConn returns active connections.
// 返回当前持有的 connections
func (s *Server) ActiveClientConn() []net.Conn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]net.Conn, 0, len(s.activeConn))
	for clientConn := range s.activeConn {
		result = append(result, clientConn)
	}
	return result
}

// SendMessage a request to the specified client.
// The client is designated by the conn.
// conn can be gotten from context in services:
//
//	ctx.Value(RemoteConnContextKey)
//
// servicePath, serviceMethod, metadata can be set to zero values.
func (s *Server) SendMessage(conn net.Conn, servicePath, serviceMethod string, metadata map[string]string, data []byte) error {
	ctx := share.WithValue(context.Background(), StartSendRequestContextKey, time.Now().UnixNano())
	s.Plugins.DoPreWriteRequest(ctx)

	req := protocol.NewMessage()
	req.SetMessageType(protocol.Request)

	seq := atomic.AddUint64(&s.seq, 1)
	req.SetSeq(seq)
	req.SetOneway(true)
	req.SetSerializeType(protocol.SerializeNone)
	req.ServicePath = servicePath
	req.ServiceMethod = serviceMethod
	req.Metadata = metadata
	req.Payload = data

	b := req.EncodeSlicePointer()
	_, err := conn.Write(*b)
	protocol.PutData(b)

	s.Plugins.DoPostWriteRequest(ctx, req, err)

	return err
}

func (s *Server) getDoneChan() <-chan struct{} {
	return s.doneChan
}

// Serve starts and listens RPC requests.
// It is blocked until receiving connections from clients.
// 启动和监听 RPC 请求，阻塞等待接收客户端的连接
func (s *Server) Serve(network, address string) (err error) {
	var ln net.Listener
	ln, err = s.makeListener(network, address)
	if err != nil {
		return err
	}

	defer s.UnregisterAll()

	if network == "http" {
		s.serveByHTTP(ln, "")
		return nil
	}

	if network == "ws" || network == "wss" {
		s.serveByWS(ln, "")
		return nil
	}

	// try to start gateway
	// 去启动 gateway
	ln = s.startGateway(network, ln)

	return s.serveListener(ln)
}

// ServeListener listens RPC requests.
// It is blocked until receiving connections from clients.
// 监听 RPC 请求，阻塞等待接收客户端的连接
func (s *Server) ServeListener(network string, ln net.Listener) (err error) {
	defer s.UnregisterAll()

	if network == "http" {
		s.serveByHTTP(ln, "")
		return nil
	}

	// try to start gateway
	ln = s.startGateway(network, ln)

	return s.serveListener(ln)
}

// serveListener accepts incoming connections on the Listener ln,
// creating a new service goroutine for each.
// The service goroutines read requests and then call services to reply to them.
func (s *Server) serveListener(ln net.Listener) error {
	var tempDelay time.Duration

	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()

	for {
		// 接收客户端连接
		conn, e := ln.Accept()
		// 发生错误
		if e != nil {
			// 服务已经关闭，就没必要处理了
			if s.isShutdown() {
				<-s.doneChan
				return ErrServerClosed
			}

			// 发生错误
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}

				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}

				log.Errorf("rpcx: Accept error: %v; retrying in %v", e, tempDelay)
				time.Sleep(tempDelay)
				continue
			}

			if errors.Is(e, cmux.ErrListenerClosed) {
				return ErrServerClosed
			}
			return e
		}
		tempDelay = 0

		// 设置 TCP 一些配置
		if tc, ok := conn.(*net.TCPConn); ok {
			period := s.options["TCPKeepAlivePeriod"]
			if period != nil {
				tc.SetKeepAlive(true)
				tc.SetKeepAlivePeriod(period.(time.Duration))
				tc.SetLinger(10)
			}
		}

		conn, ok := s.Plugins.DoPostConnAccept(conn)
		if !ok {
			conn.Close()
			continue
		}

		s.mu.Lock()
		// 存储该 connection
		s.activeConn[conn] = struct{}{}
		s.mu.Unlock()

		if share.Trace {
			log.Debugf("server accepted an conn: %v", conn.RemoteAddr().String())
		}

		// 处理 connection
		go s.serveConn(conn)
	}
}

// serveByHTTP serves by HTTP.
// if rpcPath is an empty string, use share.DefaultRPCPath.
func (s *Server) serveByHTTP(ln net.Listener, rpcPath string) {
	s.ln = ln

	if rpcPath == "" {
		rpcPath = share.DefaultRPCPath
	}
	mux := http.NewServeMux()
	mux.Handle(rpcPath, s)
	srv := &http.Server{Handler: mux}

	srv.Serve(ln)
}

// serveByWS 服务 Websocket 请求
func (s *Server) serveByWS(ln net.Listener, rpcPath string) {
	s.ln = ln

	if rpcPath == "" {
		rpcPath = share.DefaultRPCPath
	}
	mux := http.NewServeMux()
	mux.Handle(rpcPath, websocket.Handler(s.ServeWS))
	srv := &http.Server{Handler: mux}

	srv.Serve(ln)
}

// sendResponse 返回响应，写响应要么池化 Goroutine、开启新的 Goroutine、同步写（当前 Goroutine 写数据）
func (s *Server) sendResponse(ctx *share.Context, conn net.Conn, err error, req, res *protocol.Message) {
	// payload 大于 1024 并且开启了压缩，则设置压缩类型
	if len(res.Payload) > 1024 && req.CompressType() != protocol.None {
		res.SetCompressType(req.CompressType())
	}

	// 调用插件写响应之前的回调
	s.Plugins.DoPreWriteResponse(ctx, req, res, err)

	// 对数据进行编码，得到二进制数据
	data := res.EncodeSlicePointer()

	// 抽出一个函数实现写
	writeData := func() {
		if s.writeTimeout != 0 {
			conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		}
		conn.Write(*data)
		protocol.PutData(data)
	}

	// 异步写，就提交给 goroutine（池化/新建） 写
	if s.AsyncWrite {
		// 当前没有池化
		if s.pool != nil {
			s.pool.Submit(writeData)
		} else {
			go writeData()
		}
	} else {
		// 同步写
		writeData()
	}

	// 调用插件写响应之后的回调
	s.Plugins.DoPostWriteResponse(ctx, req, res, err)
}

// serveConn 处理 connection，重点关注，服务端核心设计
// 每个链接对应一个 Goroutine，而链接中每个请求交由池化 Goroutine 处理或者开启新的 Goroutine
// 1. 关注序列化与反序列化
// 2. 关注本地 RPC 方法调用
// 3. 因为奔溃引发的问题处理
// 4. 内部状态处理
func (s *Server) serveConn(conn net.Conn) {
	// 服务处于关闭状态，直接关闭连接
	if s.isShutdown() {
		s.closeConn(conn)
		return
	}

	defer func() {
		// 获取调用栈
		if err := recover(); err != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			ss := runtime.Stack(buf, false)
			if ss > size {
				ss = size
			}
			buf = buf[:ss]
			log.Errorf("serving %s panic error: %s, stack:\n %s", conn.RemoteAddr(), err, buf)
		}

		if share.Trace {
			log.Debugf("server closed conn: %v", conn.RemoteAddr().String())
		}

		// make sure all inflight requests are handled and all drained
		// 确保当前的请求都被处理
		if s.isShutdown() {
			<-s.doneChan
		}

		// 关闭 connection
		s.closeConn(conn)
	}()

	// tls connection 设置一些配置
	if tlsConn, ok := conn.(*tls.Conn); ok {
		if d := s.readTimeout; d != 0 {
			conn.SetReadDeadline(time.Now().Add(d))
		}
		if d := s.writeTimeout; d != 0 {
			conn.SetWriteDeadline(time.Now().Add(d))
		}
		if err := tlsConn.Handshake(); err != nil {
			log.Errorf("rpcx: TLS handshake error from %s: %v", conn.RemoteAddr(), err)
			return
		}
	}

	// 创建缓冲区，以后该连接的读都是采用该缓存区
	r := bufio.NewReaderSize(conn, ReaderBuffsize)

	// read requests and handle it
	// 读取请求并处理
	for {
		// 服务关闭了，就无须处理
		if s.isShutdown() {
			return
		}

		// 读取截止时间，当前时间加上读超时
		// 在截止时间后没有读到数据，就报错
		t0 := time.Now()
		if s.readTimeout != 0 {
			conn.SetReadDeadline(t0.Add(s.readTimeout))
		}

		// create a rpcx Context
		ctx := share.WithValue(context.Background(), RemoteConnContextKey, conn)

		// read a request from the underlying connection
		// 从底层的连接中读取请求
		req, err := s.readRequest(ctx, r)
		// 读取时发生错误：EOF、链接关闭、请求限流
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Infof("client has closed this connection: %s", conn.RemoteAddr().String())
			} else if errors.Is(err, net.ErrClosed) {
				log.Infof("rpcx: connection %s is closed", conn.RemoteAddr().String())
			} else if errors.Is(err, ErrReqReachLimit) {
				if !req.IsOneway() { // return a error response
					res := req.Clone()
					res.SetMessageType(protocol.Response)

					s.handleError(res, err)
					s.sendResponse(ctx, conn, err, req, res)
				} else { // Oneway and only call the plugins
					s.Plugins.DoPreWriteResponse(ctx, req, nil, err)
				}
				// 无需处理该请求，跳出来重新读取请求
				continue
			} else { // wrong data
				log.Warnf("rpcx: failed to read request: %v", err)
			}

			if s.HandleServiceError != nil {
				s.HandleServiceError(err)
			}

			return
		}

		if share.Trace {
			log.Debugf("server received an request %+v from conn: %v", req, conn.RemoteAddr().String())
		}

		// 封装成 context（rpcx context）
		ctx = share.WithLocalValue(ctx, StartRequestContextKey, time.Now().UnixNano())
		closeConn := false
		// 非健康检查请求，需要授权
		// 元数据需要携带授权数据，似乎每个请求都需要授权？
		if !req.IsHeartbeat() {
			err = s.auth(ctx, req)
			closeConn = err != nil
		}

		// 处理发生错误
		if err != nil {
			if !req.IsOneway() { // return a error response
				res := req.Clone()
				res.SetMessageType(protocol.Response)
				s.handleError(res, err)
				s.sendResponse(ctx, conn, err, req, res)
			} else {
				s.Plugins.DoPreWriteResponse(ctx, req, nil, err)
			}

			if s.HandleServiceError != nil {
				s.HandleServiceError(err)
			}

			// auth failed, closed the connection
			// 授权失败，关闭该连接
			if closeConn {
				log.Infof("auth failed for conn %s: %v", conn.RemoteAddr().String(), err)
				return
			}
			continue
		}

		// 池化（对于 goroutine）处理
		// 对 Goroutine 进行池化，是为了复用 Goroutine，从而减少 Goroutine 创建和销毁的开销
		// 池化思想在中间件设计中特别常见，可以对线程、协程、内存、对象等进行池化
		if s.pool != nil {
			s.pool.Submit(func() {
				s.processOneRequest(ctx, req, conn)
			})
		} else {
			// 创建一个新的 goroutine 处理
			go s.processOneRequest(ctx, req, conn)
		}
	}
}

// processOneRequest 处理请求，重点关注
func (s *Server) processOneRequest(ctx *share.Context, req *protocol.Message, conn net.Conn) {
	// 奔溃，打印调用栈
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 1024)
			buf = buf[:runtime.Stack(buf, true)]

			log.Errorf("failed to handle the request: %v， stacks: %s", r, buf)
		}
	}()

	// 记录当前处理请求数量
	atomic.AddInt32(&s.handlerMsgNum, 1)
	defer atomic.AddInt32(&s.handlerMsgNum, -1)

	// 心跳请求，直接处理返回
	if req.IsHeartbeat() {
		s.Plugins.DoHeartbeatRequest(ctx, req)
		req.SetMessageType(protocol.Response)
		data := req.EncodeSlicePointer()

		if s.writeTimeout != 0 {
			conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		}
		conn.Write(*data)

		protocol.PutData(data)

		return
	}

	// 解析超时，整个函数跑完后，取消 context
	cancelFunc := parseServerTimeout(ctx, req)
	if cancelFunc != nil {
		defer cancelFunc()
	}

	// 响应元数据
	resMetadata := make(map[string]string)
	if req.Metadata == nil {
		req.Metadata = make(map[string]string)
	}
	ctx = share.WithLocalValue(share.WithLocalValue(ctx, share.ReqMetaDataKey, req.Metadata),
		share.ResMetaDataKey, resMetadata)

	s.Plugins.DoPreHandleRequest(ctx, req)

	if share.Trace {
		log.Debugf("server handle request %+v from conn: %v", req, conn.RemoteAddr().String())
	}

	// use handlers first
	// 首先使用 handlers，自定义的路由
	if handler, ok := s.router[req.ServicePath+"."+req.ServiceMethod]; ok {
		sctx := NewContext(ctx, conn, req, s.AsyncWrite)
		err := handler(sctx)
		if err != nil {
			log.Errorf("[handler internal error]: servicepath: %s, servicemethod, err: %v", req.ServicePath, req.ServiceMethod, err)
		}

		return
	}

	// 处理请求，得到响应
	res, err := s.handleRequest(ctx, req)
	if err != nil {
		if s.HandleServiceError != nil {
			s.HandleServiceError(err)
		} else {
			log.Warnf("rpcx: failed to handle request: %v", err)
		}
	}

	// 非单向请求，需要处理响应元数据
	if !req.IsOneway() {
		// copy meta in context to responses
		// 拷贝 context 中的元数据到响应中
		if len(resMetadata) > 0 {
			// 响应数据库
			meta := res.Metadata
			// 没有直接赋值
			if meta == nil {
				res.Metadata = resMetadata
			} else {
				// 补充响应元数据不存在 key
				for k, v := range resMetadata {
					if meta[k] == "" {
						meta[k] = v
					}
				}
			}
		}

		// 返回响应
		s.sendResponse(ctx, conn, err, req, res)
	}

	if share.Trace {
		log.Debugf("server write response %+v for an request %+v from conn: %v", res, req, conn.RemoteAddr().String())
	}
}

// parseServerTimeout 解析服务端超时时间，封装成 context.Context
func parseServerTimeout(ctx *share.Context, req *protocol.Message) context.CancelFunc {
	if req == nil || req.Metadata == nil {
		return nil
	}

	// 从元数据中读取超时时间文本
	st := req.Metadata[share.ServerTimeout]
	if st == "" {
		return nil
	}

	// 解析超时
	timeout, err := strconv.ParseInt(st, 10, 64)
	if err != nil {
		return nil
	}

	// 创建 context
	newCtx, cancel := context.WithTimeout(ctx.Context, time.Duration(timeout)*time.Millisecond)
	ctx.Context = newCtx
	return cancel
}

func (s *Server) isShutdown() bool {
	return atomic.LoadInt32(&s.inShutdown) == 1
}

// closeConn 关闭连接
func (s *Server) closeConn(conn net.Conn) {
	s.mu.Lock()
	// remove this connection from activeConn
	delete(s.activeConn, conn)
	s.mu.Unlock()

	// close connection
	conn.Close()

	// invoke plugin's DoPostConnClose method
	s.Plugins.DoPostConnClose(conn)
}

// readRequest 从 connection 读取请求
func (s *Server) readRequest(ctx context.Context, r io.Reader) (req *protocol.Message, err error) {
	err = s.Plugins.DoPreReadRequest(ctx)
	if err != nil {
		return nil, err
	}
	// pool req? 是否需要池化 request
	req = protocol.NewMessage()
	err = req.Decode(r)
	if err == io.EOF {
		return req, err
	}
	perr := s.Plugins.DoPostReadRequest(ctx, req, err)
	if err == nil {
		err = perr
	}
	return req, err
}

// auth 授权操作，从元数据提取出 token，然后调用自定义的 AuthFunc
func (s *Server) auth(ctx context.Context, req *protocol.Message) error {
	// 如果不存在 AuthFunc，那么就无需授权
	if s.AuthFunc != nil {
		token := req.Metadata[share.AuthKey]
		return s.AuthFunc(ctx, req, token)
	}

	return nil
}

// handleRequest 处理请求
// 1. 从请求克隆响应
// 2. 解码方法参数，查找对应的服务进行调用
// 3. 归还 RPC 方法参数
// 4. 调用错误，需要进行处理
// 5. 返回响应
func (s *Server) handleRequest(ctx context.Context, req *protocol.Message) (res *protocol.Message, err error) {
	// RPC 方法基本信息
	serviceName := req.ServicePath
	methodName := req.ServiceMethod

	// 从请求克隆得到响应
	res = req.Clone()

	// 修改消息类型为响应
	res.SetMessageType(protocol.Response)
	s.serviceMapMu.RLock()
	// 根据服务名称获取到服务
	service := s.serviceMap[serviceName]

	if share.Trace {
		log.Debugf("server get service %+v for an request %+v", service, req)
	}

	s.serviceMapMu.RUnlock()
	// 未找到服务
	if service == nil {
		err = errors.New("rpcx: can't find service " + serviceName)
		return s.handleError(res, err)
	}
	// 从服务里面获取对应的 RPC 方法
	mtype := service.method[methodName]
	if mtype == nil {
		if service.function[methodName] != nil { // check raw functions
			return s.handleRequestForFunction(ctx, req)
		}
		err = errors.New("rpcx: can't find method " + methodName)
		return s.handleError(res, err)
	}

	// get a argv object from object pool
	// 获取该 RPC 方法的参数
	argv := reflectTypePools.Get(mtype.ArgType)

	// 根据序列化类型拿到编解码器
	codec := share.Codecs[req.SerializeType()]
	if codec == nil {
		err = fmt.Errorf("can not find codec for %d", req.SerializeType())
		return s.handleError(res, err)
	}

	// 解码请求参数
	err = codec.Decode(req.Payload, argv)
	if err != nil {
		return s.handleError(res, err)
	}

	// and get a reply object from object pool
	replyv := reflectTypePools.Get(mtype.ReplyType)

	argv, err = s.Plugins.DoPreCall(ctx, serviceName, methodName, argv)
	if err != nil {
		// return reply to object pool
		reflectTypePools.Put(mtype.ReplyType, replyv)
		return s.handleError(res, err)
	}

	// 本地调用 RPC 方法
	if mtype.ArgType.Kind() != reflect.Ptr { // 非指针
		err = service.call(ctx, mtype, reflect.ValueOf(argv).Elem(), reflect.ValueOf(replyv))
	} else {
		err = service.call(ctx, mtype, reflect.ValueOf(argv), reflect.ValueOf(replyv))
	}

	if err == nil {
		replyv, err = s.Plugins.DoPostCall(ctx, serviceName, methodName, argv, replyv)
	}

	// return argc to object pool
	// 归还 argc 给对象池
	reflectTypePools.Put(mtype.ArgType, argv)

	// 出错，处理错误
	if err != nil {
		if replyv != nil {
			data, err := codec.Encode(replyv)
			// return reply to object pool
			reflectTypePools.Put(mtype.ReplyType, replyv)
			if err != nil {
				return s.handleError(res, err)
			}
			res.Payload = data
		}
		return s.handleError(res, err)
	}

	// 非单向，需要返回响应
	if !req.IsOneway() {
		// 编码方法返回值
		data, err := codec.Encode(replyv)
		// return reply to object pool
		reflectTypePools.Put(mtype.ReplyType, replyv)
		if err != nil {
			return s.handleError(res, err)
		}
		res.Payload = data
	} else if replyv != nil {
		reflectTypePools.Put(mtype.ReplyType, replyv)
	}

	if share.Trace {
		log.Debugf("server called service %+v for an request %+v", service, req)
	}

	return res, nil
}

func (s *Server) handleRequestForFunction(ctx context.Context, req *protocol.Message) (res *protocol.Message, err error) {
	res = req.Clone()

	res.SetMessageType(protocol.Response)

	serviceName := req.ServicePath
	methodName := req.ServiceMethod
	s.serviceMapMu.RLock()
	service := s.serviceMap[serviceName]
	s.serviceMapMu.RUnlock()
	if service == nil {
		err = errors.New("rpcx: can't find service  for func raw function")
		return s.handleError(res, err)
	}
	mtype := service.function[methodName]
	if mtype == nil {
		err = errors.New("rpcx: can't find method " + methodName)
		return s.handleError(res, err)
	}

	argv := reflectTypePools.Get(mtype.ArgType)

	codec := share.Codecs[req.SerializeType()]
	if codec == nil {
		err = fmt.Errorf("can not find codec for %d", req.SerializeType())
		return s.handleError(res, err)
	}

	err = codec.Decode(req.Payload, argv)
	if err != nil {
		return s.handleError(res, err)
	}

	replyv := reflectTypePools.Get(mtype.ReplyType)

	if mtype.ArgType.Kind() != reflect.Ptr {
		err = service.callForFunction(ctx, mtype, reflect.ValueOf(argv).Elem(), reflect.ValueOf(replyv))
	} else {
		err = service.callForFunction(ctx, mtype, reflect.ValueOf(argv), reflect.ValueOf(replyv))
	}

	reflectTypePools.Put(mtype.ArgType, argv)

	if err != nil {
		reflectTypePools.Put(mtype.ReplyType, replyv)
		return s.handleError(res, err)
	}

	if !req.IsOneway() {
		data, err := codec.Encode(replyv)
		reflectTypePools.Put(mtype.ReplyType, replyv)
		if err != nil {
			return s.handleError(res, err)
		}
		res.Payload = data
	} else if replyv != nil {
		reflectTypePools.Put(mtype.ReplyType, replyv)
	}

	return res, nil
}

// handleError 处理错误
func (s *Server) handleError(res *protocol.Message, err error) (*protocol.Message, error) {
	res.SetMessageStatusType(protocol.Error)
	if res.Metadata == nil {
		res.Metadata = make(map[string]string)
	}

	if s.ServerErrorFunc != nil {
		res.Metadata[protocol.ServiceError] = s.ServerErrorFunc(res, err)
	} else {
		res.Metadata[protocol.ServiceError] = err.Error()
	}

	return res, err
}

// Can connect to RPC service using HTTP CONNECT to rpcPath.
var connected = "200 Connected to rpcx"

// ServeHTTP implements a http.Handler that answers RPC requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodConnect {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Info("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")

	s.mu.Lock()
	s.activeConn[conn] = struct{}{}
	s.mu.Unlock()

	s.serveConn(conn)
}

// ServeWS 处理 web socket 连接
func (s *Server) ServeWS(conn *websocket.Conn) {
	s.mu.Lock()
	// 保存该连接
	s.activeConn[conn] = struct{}{}
	s.mu.Unlock()

	// 二进制类型数据
	conn.PayloadType = websocket.BinaryFrame
	// 调用统一的处理链接方法
	s.serveConn(conn)
}

// Close immediately closes all active net.Listeners.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 关闭监听器
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	// 遍历 connection 进行关闭
	for c := range s.activeConn {
		c.Close()
		delete(s.activeConn, c)
		s.Plugins.DoPostConnClose(c)
	}
	s.closeDoneChanLocked()

	// 等待 10s 关闭 goroutine 池
	if s.pool != nil {
		s.pool.StopAndWaitFor(10 * time.Second)
	}

	return err
}

// RegisterOnShutdown registers a function to call on Shutdown.
// This can be used to gracefully shutdown connections.
// 注册一个函数在 Shutdown 时来调用，可以用来实现优雅关闭连接
func (s *Server) RegisterOnShutdown(f func(s *Server)) {
	s.mu.Lock()
	s.onShutdown = append(s.onShutdown, f)
	s.mu.Unlock()
}

// RegisterOnRestart registers a function to call on Restart.
// 注册一个函数在重启时来调用
func (s *Server) RegisterOnRestart(f func(s *Server)) {
	s.mu.Lock()
	s.onRestart = append(s.onRestart, f)
	s.mu.Unlock()
}

var shutdownPollInterval = 1000 * time.Millisecond

// Shutdown gracefully shuts down the server without interrupting any
// active connections. Shutdown works by first closing the
// listener, then closing all idle connections, and then waiting
// indefinitely for connections to return to idle and then shut down.
// If the provided context expires before the shutdown is complete,
// Shutdown returns the context's error, otherwise it returns any
// error returned from closing the Server's underlying Listener.
func (s *Server) Shutdown(ctx context.Context) error {
	var err error
	if atomic.CompareAndSwapInt32(&s.inShutdown, 0, 1) {
		log.Info("shutdown begin")

		s.mu.Lock()

		// 主动注销注册的服务
		if s.Plugins != nil {
			for name := range s.serviceMap {
				s.Plugins.DoUnregister(name)
			}
		}
		if s.ln != nil {
			s.ln.Close()
		}
		for conn := range s.activeConn {
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				tcpConn.CloseRead()
			}
		}
		s.mu.Unlock()

		// wait all in-processing requests finish.
		ticker := time.NewTicker(shutdownPollInterval)
		defer ticker.Stop()
	outer:
		for {
			if s.checkProcessMsg() {
				break
			}
			select {
			case <-ctx.Done():
				err = ctx.Err()
				break outer
			case <-ticker.C:
			}
		}

		if s.gatewayHTTPServer != nil {
			if err := s.closeHTTP1APIGateway(ctx); err != nil {
				log.Warnf("failed to close gateway: %v", err)
			} else {
				log.Info("closed gateway")
			}
		}

		if s.jsonrpcHTTPServer != nil {
			if err := s.closeJSONRPC2(ctx); err != nil {
				log.Warnf("failed to close JSONRPC: %v", err)
			} else {
				log.Info("closed JSONRPC")
			}
		}

		s.mu.Lock()
		for conn := range s.activeConn {
			conn.Close()
			delete(s.activeConn, conn)
			s.Plugins.DoPostConnClose(conn)
		}
		s.closeDoneChanLocked()

		s.mu.Unlock()

		log.Info("shutdown end")

	}
	return err
}

// Restart restarts this server gracefully.
// It starts a new rpcx server with the same port with SO_REUSEPORT socket option,
// and shutdown this rpcx server gracefully.
// 优雅重启服务，使用相同的端口和 SO_REUSEPORT socket 选项启动一个新的 rpcx 服务，并且优雅的关闭当前的服务
func (s *Server) Restart(ctx context.Context) error {
	pid, err := s.startProcess()
	if err != nil {
		return err
	}
	log.Infof("restart a new rpcx server: %d", pid)

	// TODO: is it necessary?
	time.Sleep(3 * time.Second)
	return s.Shutdown(ctx)
}

// startProcess 启动进程，返回进程号
func (s *Server) startProcess() (int, error) {
	argv0, err := exec.LookPath(os.Args[0])
	if err != nil {
		return 0, err
	}

	// Pass on the environment and replace the old count key with the new one.
	// 传递 environment，并替换旧的 count key
	var env []string
	env = append(env, os.Environ()...)

	originalWD, _ := os.Getwd()
	allFiles := []*os.File{os.Stdin, os.Stdout, os.Stderr}
	process, err := os.StartProcess(argv0, os.Args, &os.ProcAttr{
		Dir:   originalWD,
		Env:   env,
		Files: allFiles,
	})
	if err != nil {
		return 0, err
	}
	return process.Pid, nil
}

func (s *Server) checkProcessMsg() bool {
	size := atomic.LoadInt32(&s.handlerMsgNum)
	log.Info("need handle in-processing msg size:", size)
	return size == 0
}

func (s *Server) closeDoneChanLocked() {
	select {
	case <-s.doneChan:
		// Already closed. Don't close again.
	default:
		// Safe to close here. We're the only closer, guarded
		// by s.mu.RegisterName
		close(s.doneChan)
	}
}

var ip4Reg = regexp.MustCompile(`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$`)

func validIP4(ipAddress string) bool {
	ipAddress = strings.Trim(ipAddress, " ")
	i := strings.LastIndex(ipAddress, ":")
	ipAddress = ipAddress[:i] // remove port

	return ip4Reg.MatchString(ipAddress)
}

func validIP6(ipAddress string) bool {
	ipAddress = strings.Trim(ipAddress, " ")
	i := strings.LastIndex(ipAddress, ":")
	ipAddress = ipAddress[:i] // remove port
	ipAddress = strings.TrimPrefix(ipAddress, "[")
	ipAddress = strings.TrimSuffix(ipAddress, "]")
	ip := net.ParseIP(ipAddress)
	if ip != nil && ip.To4() == nil {
		return true
	} else {
		return false
	}
}
