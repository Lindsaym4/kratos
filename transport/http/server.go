package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/internal/endpoint"
	"github.com/go-kratos/kratos/v2/internal/matcher"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/gorilla/mux"
)

// SupportPackageIsVersion1 These constants play a role in template generation.
const SupportPackageIsVersion1 = true

// DecodeRequestFunc is decode request func.
type DecodeRequestFunc func(*http.Request, interface{}) error

// EncodeResponseFunc is encode response func.
type EncodeResponseFunc func(http.ResponseWriter, *http.Request, interface{}) error

// EncodeErrorFunc is encode error func.
type EncodeErrorFunc func(http.ResponseWriter, *http.Request, error)

// HandlerFunc defines a function to serve HTTP requests.
type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

// ServerOption is an HTTP server option.
type ServerOption func(*Server)

// Network with server network.
func Network(network string) ServerOption {
	return func(s *Server) {
		s.network = network
	}
}

// Address with server address.
func Address(addr string) ServerOption {
	return func(s *Server) {
		s.address = addr
	}
}

// Timeout with server timeout.
func Timeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.timeout = timeout
	}
}

// Logger with server logger.
// Deprecated: use global logger instead.
func Logger(logger interface{}) ServerOption {
	return func(s *Server) {
	}
}

// Middleware with service middleware option.
func Middleware(m ...middleware.Middleware) ServerOption {
	return func(s *Server) {
		s.ms = m
	}
}

// Filter with HTTP middleware option.
func Filter(filters ...FilterFunc) ServerOption {
	return func(s *Server) {
		s.filters = filters
	}
}

// RequestDecoder with request decoder.
func RequestDecoder(dec DecodeRequestFunc) ServerOption {
	return func(s *Server) {
		s.dec = dec
	}
}

// ResponseEncoder with response encoder.
func ResponseEncoder(enc EncodeResponseFunc) ServerOption {
	return func(s *Server) {
		s.enc = enc
	}
}

// ErrorEncoder with error encoder.
func ErrorEncoder(ene EncodeErrorFunc) ServerOption {
	return func(s *Server) {
		s.ene = ene
	}
}

// TLSConfig with TLS config.
func TLSConfig(c *tls.Config) ServerOption {
	return func(s *Server) {
		s.tlsConf = c
	}
}

// StrictSlash with mux's StrictSlash.
// If true, when the path pattern is "/path/", accessing "/path" will
// redirect to the former and vice versa.
func StrictSlash(strictSlash bool) ServerOption {
	return func(s *Server) {
		s.strictSlash = strictSlash
	}
}

// Server is an HTTP server wrapper.
type Server struct {
	*http.Server
	lis         net.Listener
	tlsConf     *tls.Config
	endpoint    *url.URL
	err         error
	network     string
	address     string
	timeout     time.Duration
	filters     []FilterFunc
	ms          []middleware.Middleware
	dec         DecodeRequestFunc
	enc         EncodeResponseFunc
	ene         EncodeErrorFunc
	strictSlash bool
	router      *mux.Router
}

// NewServer creates an HTTP server by options.
func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		network: "tcp",
		address: ":0",
		timeout: 1 * time.Second,
		dec:     DefaultRequestDecoder,
		enc:     DefaultResponseEncoder,
		ene:     DefaultErrorEncoder,
		router:  mux.NewRouter(),
	}
	for _, o := range opts {
		o(srv)
	}
	srv.router.StrictSlash(srv.strictSlash)
	srv.Server = &http.Server{
		Handler:   srv,
		TLSConfig: srv.tlsConf,
	}
	return srv
}

// Use uses middleware.
func (s *Server) Use(m ...middleware.Middleware) {
	s.ms = append(s.ms, m...)
}

// Handle registers a new route with a matcher for the URL path.
func (s *Server) Handle(path string, handler http.Handler) {
	s.router.Handle(path, handler)
}

// HandlePrefix registers a new route with a matcher for the URL path prefix.
func (s *Server) HandlePrefix(prefix string, handler http.Handler) {
	s.router.PathPrefix(prefix).Handler(handler)
}

// HandleFunc registers a new route with a matcher for the URL path.
func (s *Server) HandleFunc(path string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(path, handler)
}

// HandleHeader registers a new route with a matcher for the header.
func (s *Server) HandleHeader(key, val string, handler http.Handler) {
	s.router.Headers(key, val).Handler(handler)
}

// Route registers a new route with a matcher for the URL path.
func (s *Server) Route(path string) *Route {
	return &Route{
		s:      s,
		router: s.router,
		path:   path,
	}
}

// ServeHTTP should be writeable to match the http.Handler interface
func (s *Server) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	if s.endpoint != nil {
		ctx = transport.NewServerContext(ctx, &Transport{
			endpoint:     s.endpoint.String(),
			reqHeader:    req.Header,
			replyHeader:  res.Header(),
			request:      req,
			pathTemplate: req.URL.Path,
		})
	} else {
		ctx = transport.NewServerContext(ctx, &Transport{
			reqHeader:   req.Header,
			replyHeader: res.Header(),
			request:     req,
		})
	}

	if s.timeout > 0 {
		ctx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()

		done := make(chan struct{})
		panicChan := make(chan interface{}, 1)
		tw := &timeoutWriter{
			w: res,
			h: make(http.Header),
		}

		go func() {
			defer func() {
				if p := recover(); p != nil {
					panicChan <- p
				} else {
					close(done)
				}
			}()
			s.router.ServeHTTP(tw, req.WithContext(ctx))
		}()

		select {
		case <-done:
			tw.mu.Lock()
			defer tw.mu.Unlock()
			dst := res.Header()
			for k, vv := range tw.h {
				dst[k] = vv
			}
			if !tw.wroteHeader {
				tw.code = http.StatusOK
			}
			res.WriteHeader(tw.code)
			res.Write(tw.wbuf.Bytes())
		case p := <-panicChan:
			panic(p)
		case <-ctx.Done():
			tw.mu.Lock()
			defer tw.mu.Unlock()
			tw.timedOut = true
			s.ene(res, req, ctx.Err())
		}
	} else {
		s.router.ServeHTTP(res, req.WithContext(ctx))
	}
}

type timeoutWriter struct {
	w           http.ResponseWriter
	h           http.Header
	wbuf        bytes.Buffer
	mu          sync.Mutex
	timedOut    bool
	wroteHeader bool
	code        int
}

func (tw *timeoutWriter) Header() http.Header {
	return tw.h
}

func (tw *timeoutWriter) Write(b []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut {
		return 0, http.ErrHandlerTimeout
	}
	if !tw.wroteHeader {
		tw.wroteHeader = true
		tw.code = http.StatusOK
	}
	return tw.wbuf.Write(b)
}

func (tw *timeoutWriter) WriteHeader(code int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut || tw.wroteHeader {
		return
	}
	tw.wroteHeader = true
	tw.code = code
}

// Endpoint return a real address to registry or discovery.
// hot fix: if the address is in the form of client side, we will return it directly.
func (s *Server) Endpoint() (*url.URL, error) {
	if err := s.listenAndAutoAddr(); err != nil {
		return nil, err
	}
	if s.endpoint != nil {
		return s.endpoint, nil
	}
	addr, err := endpoint.ParseEndpoint(s.address, s.lis)
	if err != nil {
		return nil, err
	}
	return url.Parse(addr)
}

// Start start the HTTP server.
func (s *Server) Start(ctx context.Context) error {
	if err := s.listenAndAutoAddr(); err != nil {
		return err
	}
	s.BaseContext = func(net.Listener) context.Context {
		return ctx
	}
	s.err = nil
	var err error
	if s.tlsConf != nil {
		err = s.ServeTLS(s.lis, "", "")
	} else {
		err = s.Serve(s.lis)
	}
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Stop stop the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	return s.Shutdown(ctx)
}

func (s *Server) listenAndAutoAddr() error {
	if s.lis == nil {
		lis, err := net.Listen(s.network, s.address)
		if err != nil {
			return err
		}
		s.lis = lis
		s.address = lis.Addr().String()
	}
	return nil
}
