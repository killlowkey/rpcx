package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
	"github.com/smallnest/rpcx/log"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/share"
	"github.com/soheilhy/cmux"
)

// startGateway 启动网关
// 1. 兼容 JSON RPC
// 2. 兼容 HTTP 1.0
func (s *Server) startGateway(network string, ln net.Listener) net.Listener {
	if network != "tcp" && network != "tcp4" && network != "tcp6" && network != "reuseport" {
		// log.Infof("network is not tcp/tcp4/tcp6 so can not start gateway")
		return ln
	}

	m := cmux.New(ln)

	rpcxLn := m.Match(rpcxPrefixByteMatcher())

	// mux Plugins
	if s.Plugins != nil {
		s.Plugins.MuxMatch(m)
	}

	if !s.DisableJSONRPC {
		jsonrpc2Ln := m.Match(cmux.HTTP1HeaderField("X-JSONRPC-2.0", "true"))
		go s.startJSONRPC2(jsonrpc2Ln)
	}

	if !s.DisableHTTPGateway {
		httpLn := m.Match(cmux.HTTP1Fast())
		go s.startHTTP1APIGateway(httpLn)
	}

	go m.Serve()

	return rpcxLn
}

func rpcxPrefixByteMatcher() cmux.Matcher {
	magic := protocol.MagicNumber()
	return func(r io.Reader) bool {
		buf := make([]byte, 1)
		n, _ := r.Read(buf)
		return n == 1 && buf[0] == magic
	}
}

// startHTTP1APIGateway 处理 HTTP 请求
// 将 HTTP 封装成 message，进行内部调用
func (s *Server) startHTTP1APIGateway(ln net.Listener) {
	router := httprouter.New()
	router.POST("/*servicePath", s.handleGatewayRequest)
	router.GET("/*servicePath", s.handleGatewayRequest)
	router.PUT("/*servicePath", s.handleGatewayRequest)

	if s.corsOptions != nil {
		opt := cors.Options(*s.corsOptions)
		c := cors.New(opt)
		mux := c.Handler(router)
		s.mu.Lock()
		s.gatewayHTTPServer = &http.Server{Handler: mux}
		s.mu.Unlock()
	} else {
		s.mu.Lock()
		s.gatewayHTTPServer = &http.Server{Handler: router}
		s.mu.Unlock()
	}

	// 启动 HTTP 服务
	if err := s.gatewayHTTPServer.Serve(ln); err != nil {
		if errors.Is(err, ErrServerClosed) || errors.Is(err, cmux.ErrListenerClosed) || errors.Is(err, cmux.ErrServerClosed) {
			log.Info("gateway server closed")
		} else {
			log.Warnf("error in gateway Serve: %T %s", err, err)
		}
	}
}

func (s *Server) closeHTTP1APIGateway(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gatewayHTTPServer != nil {
		return s.gatewayHTTPServer.Shutdown(ctx)
	}

	return nil
}

// handleGatewayRequest 处理 HTTP 请求
// 1. 从请求头中读取元数据：ServicePath、Version、ID
// 2. HTTP 请求封装成 RPC 请求进行处理
// 3. 处理后得到 RPC 响应，转换成 HTTP 响应返回
func (s *Server) handleGatewayRequest(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	ctx := share.WithValue(r.Context(), RemoteConnContextKey, r.RemoteAddr) // notice: It is a string, different with TCP (net.Conn)
	err := s.Plugins.DoPreReadRequest(ctx)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if r.Header.Get(XServicePath) == "" {
		servicePath := params.ByName("servicePath")
		servicePath = strings.TrimPrefix(servicePath, "/")
		r.Header.Set(XServicePath, servicePath)
	}
	servicePath := r.Header.Get(XServicePath)
	wh := w.Header()
	// HTTP 请求转 RPC 请求
	req, err := HTTPRequest2RpcxRequest(r)

	// set headers
	wh.Set(XVersion, r.Header.Get(XVersion))
	wh.Set(XMessageID, r.Header.Get(XMessageID))

	if err == nil && servicePath == "" {
		err = errors.New("empty servicepath")
	} else {
		wh.Set(XServicePath, servicePath)
	}

	if err == nil && r.Header.Get(XServiceMethod) == "" {
		err = errors.New("empty servicemethod")
	} else {
		wh.Set(XServiceMethod, r.Header.Get(XServiceMethod))
	}

	if err == nil && r.Header.Get(XSerializeType) == "" {
		err = errors.New("empty serialized type")
	} else {
		wh.Set(XSerializeType, r.Header.Get(XSerializeType))
	}

	if err != nil {
		rh := r.Header
		for k, v := range rh {
			if strings.HasPrefix(k, "X-RPCX-") && len(v) > 0 {
				wh.Set(k, v[0])
			}
		}

		wh.Set(XMessageStatusType, "Error")
		wh.Set(XErrorMessage, err.Error())
		return
	}
	err = s.Plugins.DoPostReadRequest(ctx, req, nil)
	if err != nil {
		s.Plugins.DoPreWriteResponse(ctx, req, nil, err)
		http.Error(w, err.Error(), 500)
		s.Plugins.DoPostWriteResponse(ctx, req, req.Clone(), err)
		return
	}

	ctx.SetValue(StartRequestContextKey, time.Now().UnixNano())
	// 授权操作
	err = s.auth(ctx, req)
	if err != nil {
		s.Plugins.DoPreWriteResponse(ctx, req, nil, err)
		wh.Set(XMessageStatusType, "Error")
		wh.Set(XErrorMessage, err.Error())
		w.WriteHeader(401)
		s.Plugins.DoPostWriteResponse(ctx, req, req.Clone(), err)
		return
	}

	resMetadata := make(map[string]string)
	newCtx := share.WithLocalValue(share.WithLocalValue(ctx, share.ReqMetaDataKey, req.Metadata),
		share.ResMetaDataKey, resMetadata)

	// RPC 响应，后续需要封装成 HTTP 响应
	res, err := s.handleRequest(newCtx, req)

	if err != nil {
		// call DoPreWriteResponse
		s.Plugins.DoPreWriteResponse(ctx, req, nil, err)
		if s.HandleServiceError != nil {
			s.HandleServiceError(err)
		} else {
			log.Warnf("rpcx:  gateway request: %v", err)
		}
		wh.Set(XMessageStatusType, "Error")
		wh.Set(XErrorMessage, err.Error())
		w.WriteHeader(500)
		// call DoPostWriteResponse
		s.Plugins.DoPostWriteResponse(ctx, req, req.Clone(), err)
		return
	}

	// will set res to call
	s.Plugins.DoPreWriteResponse(newCtx, req, res, nil)
	if len(resMetadata) > 0 { // copy meta in context to request
		meta := res.Metadata
		if meta == nil {
			res.Metadata = resMetadata
		} else {
			for k, v := range resMetadata {
				meta[k] = v
			}
		}
	}

	meta := url.Values{}
	for k, v := range res.Metadata {
		meta.Add(k, v)
	}
	// 元数据写回响应头中，进行 URL 编码
	wh.Set(XMeta, meta.Encode())
	w.Write(res.Payload)
	s.Plugins.DoPostWriteResponse(newCtx, req, res, err)
}
