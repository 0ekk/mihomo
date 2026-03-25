package xhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/metacubex/http"
	"github.com/metacubex/mihomo/adapter/inbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/transport/socks5"
	"github.com/metacubex/quic-go"
	http3 "github.com/metacubex/quic-go/http3"
	"github.com/metacubex/tls"
)

type requestHandler struct {
	config               *Config
	sessions             sync.Map
	tunnel               C.Tunnel
	additions            []inbound.Addition
	idleTimeout          time.Duration
	connectedIdleTimeout time.Duration
}

func deriveConnectedIdleTimeout(idleTimeout time.Duration) time.Duration {
	if idleTimeout <= 0 {
		return DefaultConnectedSessionIdleTimeout
	}
	connectedTimeout := idleTimeout * 3
	if connectedTimeout < DefaultConnectedSessionIdleTimeout {
		connectedTimeout = DefaultConnectedSessionIdleTimeout
	}
	return connectedTimeout
}

type streamUploadConn struct {
	io.ReadCloser
	writer http.ResponseWriter
	done   chan struct{}
	once   sync.Once
}

func newStreamUploadConn(body io.ReadCloser, writer http.ResponseWriter) *streamUploadConn {
	return &streamUploadConn{
		ReadCloser: body,
		writer:     writer,
		done:       make(chan struct{}),
	}
}

func (s *streamUploadConn) Close() error {
	err := s.ReadCloser.Close()
	s.once.Do(func() {
		close(s.done)
	})
	return err
}

func (s *streamUploadConn) Wait() <-chan struct{} {
	return s.done
}

func (s *streamUploadConn) Write(p []byte) (int, error) {
	n, err := s.writer.Write(p)
	if err == nil {
		if f, ok := s.writer.(http.Flusher); ok {
			f.Flush()
		}
	}
	return n, err
}

func (h *requestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h.validateRequest(r); err != nil {
		log.Debugln("xhttp: validation failed: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodGet {
		sessionID, err := h.parseSessionID(r.URL.Path)
		if err != nil {
			log.Debugln("xhttp: invalid session ID: %v", err)
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		h.handleDownload(w, r, sessionID)
		return
	}

	if r.Method == http.MethodPost {
		if h.isBasePath(r.URL.Path) {
			sessionID := uuid.Must(uuid.NewV4()).String()
			h.handleStreamUpload(w, r, sessionID, true)
			return
		}

		sessionID, err := h.parseSessionID(r.URL.Path)
		if err != nil {
			log.Debugln("xhttp: invalid session ID: %v", err)
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		if seq, err := h.parseSeq(r.URL.Path); err == nil {
			h.handlePacketUpload(w, r, sessionID, seq)
		} else {
			h.handleStreamUpload(w, r, sessionID, false)
		}
		return
	}

	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}

func (h *requestHandler) validateRequest(r *http.Request) error {
	if h.config.Host != "" && r.Host != h.config.Host && r.Host != "" {
		return fmt.Errorf("host mismatch: expected %s, got %s", h.config.Host, r.Host)
	}

	if !strings.HasPrefix(r.URL.Path, h.config.Path) {
		return fmt.Errorf("path mismatch: expected prefix %s", h.config.Path)
	}

	return nil
}

func isValidSessionID(id string) bool {
	_, err := uuid.FromString(id)
	return err == nil
}

func (h *requestHandler) parseSessionID(path string) (string, error) {
	if !strings.HasPrefix(path, h.config.Path) {
		return "", errors.New("invalid path prefix")
	}

	remainder := strings.TrimPrefix(path, h.config.Path)
	parts := strings.Split(strings.Trim(remainder, "/"), "/")

	if len(parts) < 1 || parts[0] == "" {
		return "", errors.New("missing session ID")
	}

	sessionID := parts[0]
	if !isValidSessionID(sessionID) {
		return "", errors.New("invalid session ID format")
	}

	return sessionID, nil
}

func (h *requestHandler) parseSeq(path string) (uint64, error) {
	remainder := strings.TrimPrefix(path, h.config.Path)
	parts := strings.Split(strings.Trim(remainder, "/"), "/")

	if len(parts) < 2 {
		return 0, errors.New("missing sequence number")
	}

	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid sequence number: %w", err)
	}

	return seq, nil
}

func (h *requestHandler) isBasePath(path string) bool {
	trimmedPath := strings.TrimRight(path, "/")
	trimmedBase := strings.TrimRight(h.config.Path, "/")
	return trimmedPath == trimmedBase
}

func (h *requestHandler) getOrCreateSession(sessionId string) (*httpSession, error) {
	if val, ok := h.sessions.Load(sessionId); ok {
		session := val.(*httpSession)
		session.setIdleTimeouts(h.idleTimeout, h.connectedIdleTimeout)
		session.touch(h.idleTimeout)
		return session, nil
	}

	maxPackets := DefaultMaxPackets
	if !h.config.ScMaxBufferedPosts.IsZero() {
		maxPackets = int(h.config.ScMaxBufferedPosts.Random())
	}

	session := newHTTPSession(sessionId, maxPackets)
	session.setIdleTimeouts(h.idleTimeout, h.connectedIdleTimeout)
	session.touch(h.idleTimeout)
	actual, _ := h.sessions.LoadOrStore(sessionId, session)
	loaded := actual.(*httpSession)
	loaded.setIdleTimeouts(h.idleTimeout, h.connectedIdleTimeout)
	loaded.touch(h.idleTimeout)
	return loaded, nil
}

func (h *requestHandler) closeAndDeleteSession(sessionID string, session *httpSession) {
	if session != nil {
		session.close()
	}
	h.sessions.Delete(sessionID)
}

func (h *requestHandler) cleanupExpiredSessions(now time.Time) {
	h.sessions.Range(func(key, value any) bool {
		sessionID, ok := key.(string)
		if !ok {
			return true
		}
		session, ok := value.(*httpSession)
		if !ok {
			return true
		}
		if session.closed.Load() || session.isExpired(now) {
			h.closeAndDeleteSession(sessionID, session)
		}
		return true
	})
}

func (h *requestHandler) closeAllSessions() {
	h.sessions.Range(func(key, value any) bool {
		sessionID, ok := key.(string)
		if !ok {
			return true
		}
		session, ok := value.(*httpSession)
		if !ok {
			return true
		}
		h.closeAndDeleteSession(sessionID, session)
		return true
	})
}

func (h *requestHandler) runSessionJanitor(ctx context.Context) {
	if h.idleTimeout <= 0 {
		return
	}

	cleanupInterval := DefaultSessionCleanupInterval
	if h.idleTimeout < cleanupInterval {
		cleanupInterval = h.idleTimeout / 2
		if cleanupInterval < time.Second {
			cleanupInterval = time.Second
		}
	}

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.closeAllSessions()
			return
		case now := <-ticker.C:
			h.cleanupExpiredSessions(now)
		}
	}
}

func (h *requestHandler) applyResponseHeaders(w http.ResponseWriter) {
	if !h.config.NoGRPCHeader {
		w.Header().Set("Content-Type", "application/grpc")
	} else if !h.config.NoSSEHeader {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	for k, v := range h.config.Headers {
		w.Header().Set(k, v)
	}
}

func (h *requestHandler) handleDownload(w http.ResponseWriter, r *http.Request, sessionID string) {
	session, err := h.getOrCreateSession(sessionID)
	if err != nil {
		log.Warnln("xhttp: failed to get session: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	session.markFullyConnected()
	session.touch(h.idleTimeout)

	h.applyResponseHeaders(w)
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	var keepAliveTicker *time.Ticker
	var keepAliveInterval time.Duration
	if !h.config.ScStreamUpServerSecs.IsZero() {
		secs := h.config.ScStreamUpServerSecs.Random()
		if secs > 0 {
			keepAliveInterval = time.Duration(secs) * time.Second
			keepAliveTicker = time.NewTicker(keepAliveInterval)
			defer keepAliveTicker.Stop()
		}
	}

	pollTicker := time.NewTicker(DefaultPollInterval)
	defer pollTicker.Stop()

	keepAliveByte := []byte{0x00}
	lastActivity := time.Now()

	for {
		select {
		case data, ok := <-session.downloadQueue:
			if !ok {
				h.closeAndDeleteSession(sessionID, session)
				return
			}
			session.touch(h.idleTimeout)
			lastActivity = time.Now()
			if _, writeErr := w.Write(data); writeErr != nil {
				log.Debugln("xhttp: download write error: %v", writeErr)
				h.closeAndDeleteSession(sessionID, session)
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}

		case <-r.Context().Done():
			log.Debugln("xhttp: client disconnected, closing session %s", sessionID)
			h.closeAndDeleteSession(sessionID, session)
			return

		case <-pollTicker.C:
			if keepAliveTicker != nil && time.Since(lastActivity) >= keepAliveInterval {
				if _, writeErr := w.Write(keepAliveByte); writeErr != nil {
					log.Debugln("xhttp: keep-alive write error: %v", writeErr)
					h.closeAndDeleteSession(sessionID, session)
					return
				}
				session.touch(h.idleTimeout)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				lastActivity = time.Now()
			}
		}
	}
}

func (h *requestHandler) handleStreamUpload(w http.ResponseWriter, r *http.Request, sessionID string, closeWhenDone bool) {
	session, err := h.getOrCreateSession(sessionID)
	if err != nil {
		log.Warnln("xhttp: failed to get session: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	if closeWhenDone {
		defer h.closeAndDeleteSession(sessionID, session)
	}
	session.touch(h.idleTimeout)

	httpSC := newStreamUploadConn(r.Body, w)
	defer httpSC.Close()

	packet := Packet{
		Reader: httpSC,
		Seq:    0,
	}

	if err := session.uploadQueue.Push(packet); err != nil {
		log.Warnln("xhttp: failed to push stream packet: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	session.touch(h.idleTimeout)

	remoteAddr, _ := net.ResolveTCPAddr("tcp", r.RemoteAddr)
	localAddr, _ := net.ResolveTCPAddr("tcp", r.Host)
	if remoteAddr == nil {
		remoteAddr = &net.TCPAddr{IP: net.IPv4zero, Port: 0}
	}
	if localAddr == nil {
		localAddr = &net.TCPAddr{IP: net.IPv4zero, Port: 0}
	}

	if h.tunnel != nil {
		session.startTunnel(remoteAddr, localAddr, func(conn net.Conn) {
			h.tunnel.HandleTCPConn(inbound.NewSocket(socks5.ParseAddr("0.0.0.0:0"), conn, C.HTTPS, h.additions...))
		})
	}

	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	// In tests or misconfiguration where no tunnel is attached, return immediately.
	// Otherwise this handler can block forever waiting for stream closure signals.
	if h.tunnel == nil {
		return
	}

	if referrer := r.Header.Get("Referer"); referrer != "" && !h.config.ScStreamUpServerSecs.IsZero() {
		if secs := h.config.ScStreamUpServerSecs.Random(); secs > 0 {
			go func(interval time.Duration) {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-httpSC.Wait():
						return
					case <-r.Context().Done():
						return
					case <-ticker.C:
						_, writeErr := httpSC.Write([]byte{'X'})
						if writeErr != nil {
							return
						}
					}
				}
			}(time.Duration(secs) * time.Second)
		}
	}

	select {
	case <-r.Context().Done():
	case <-httpSC.Wait():
	}
	session.touch(h.idleTimeout)
}

func (h *requestHandler) handlePacketUpload(w http.ResponseWriter, r *http.Request, sessionID string, seq uint64) {
	session, err := h.getOrCreateSession(sessionID)
	if err != nil {
		log.Warnln("xhttp: failed to get session: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	session.touch(h.idleTimeout)

	maxBytes := int(h.config.ScMaxEachPostBytes.Random())
	if maxBytes <= 0 {
		maxBytes = 1024 * 1024
	}

	payload := make([]byte, maxBytes)
	n, err := io.ReadFull(r.Body, payload)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		log.Warnln("xhttp: failed to read packet: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	packet := Packet{
		Payload: payload[:n],
		Seq:     seq,
	}

	if err := session.uploadQueue.Push(packet); err != nil {
		log.Warnln("xhttp: failed to push packet: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	session.touch(h.idleTimeout)

	remoteAddr, _ := net.ResolveTCPAddr("tcp", r.RemoteAddr)
	localAddr, _ := net.ResolveTCPAddr("tcp", r.Host)
	if remoteAddr == nil {
		remoteAddr = &net.TCPAddr{IP: net.IPv4zero, Port: 0}
	}
	if localAddr == nil {
		localAddr = &net.TCPAddr{IP: net.IPv4zero, Port: 0}
	}

	if h.tunnel != nil {
		session.startTunnel(remoteAddr, localAddr, func(conn net.Conn) {
			h.tunnel.HandleTCPConn(inbound.NewSocket(socks5.ParseAddr("0.0.0.0:0"), conn, C.HTTPS, h.additions...))
		})
	}

	w.WriteHeader(http.StatusOK)
}

func NewHTTP1Server(config *Config, tunnel C.Tunnel, additions []inbound.Addition) (*http.Server, error) {
	if config == nil {
		return nil, errors.New("xhttp: config is required")
	}
	handler := &requestHandler{
		config:               config,
		tunnel:               tunnel,
		additions:            additions,
		idleTimeout:          DefaultSessionIdleTimeout,
		connectedIdleTimeout: deriveConnectedIdleTimeout(DefaultSessionIdleTimeout),
	}
	return &http.Server{
		Handler: handler,
	}, nil
}

func NewHTTP2Server(config *Config, tunnel C.Tunnel, additions []inbound.Addition, tlsCfg *tls.Config) (*http.Server, error) {
	if config == nil {
		return nil, errors.New("xhttp: config is required")
	}
	if tlsCfg == nil {
		tlsCfg = &tls.Config{}
	}
	if len(tlsCfg.NextProtos) == 0 {
		tlsCfg.NextProtos = []string{"h2", "http/1.1"}
	}
	handler := &requestHandler{
		config:               config,
		tunnel:               tunnel,
		additions:            additions,
		idleTimeout:          DefaultSessionIdleTimeout,
		connectedIdleTimeout: deriveConnectedIdleTimeout(DefaultSessionIdleTimeout),
	}
	srv := &http.Server{
		Handler:   handler,
		TLSConfig: tlsCfg,
	}
	if err := http.Http2ConfigureServer(srv, &http.Http2Server{}); err != nil {
		return nil, fmt.Errorf("xhttp: failed to configure HTTP/2: %w", err)
	}
	return srv, nil
}

func NewHTTP3Server(config *Config, tunnel C.Tunnel, additions []inbound.Addition, tlsCfg *tls.Config) (*http3.Server, error) {
	if config == nil {
		return nil, errors.New("xhttp: config is required")
	}
	if tlsCfg == nil {
		tlsCfg = &tls.Config{
			MinVersion: tls.VersionTLS13,
		}
	}
	if tlsCfg.MinVersion < tls.VersionTLS13 {
		tlsCfg.MinVersion = tls.VersionTLS13
	}
	if len(tlsCfg.NextProtos) == 0 {
		tlsCfg.NextProtos = []string{"h3"}
	}
	handler := &requestHandler{
		config:               config,
		tunnel:               tunnel,
		additions:            additions,
		idleTimeout:          DefaultSessionIdleTimeout,
		connectedIdleTimeout: deriveConnectedIdleTimeout(DefaultSessionIdleTimeout),
	}
	quicCfg := &quic.Config{
		MaxIdleTimeout: 60 * 1000000000,
	}
	return &http3.Server{
		Handler:         handler,
		TLSConfig:       tlsCfg,
		QUICConfig:      quicCfg,
		Addr:            "",
		EnableDatagrams: false,
	}, nil
}

func NewServer(ctx context.Context, config *Config, tunnel C.Tunnel, additions []inbound.Addition, listener net.Listener, tlsCfg interface{}) error {
	if config == nil {
		return errors.New("xhttp: config is required")
	}
	if listener == nil {
		return errors.New("xhttp: listener is required")
	}

	config.normalize()
	httpVersion := config.httpVersion(tlsCfg != nil)
	handler := &requestHandler{
		config:               config,
		tunnel:               tunnel,
		additions:            additions,
		idleTimeout:          DefaultSessionIdleTimeout,
		connectedIdleTimeout: deriveConnectedIdleTimeout(DefaultSessionIdleTimeout),
	}

	switch httpVersion {
	case "3":
		utlsCfg, ok := tlsCfg.(*tls.Config)
		if !ok && tlsCfg != nil {
			return errors.New("xhttp: HTTP/3 requires *tls.Config")
		}
		srv, err := NewHTTP3Server(config, tunnel, additions, utlsCfg)
		if err != nil {
			return err
		}
		srv.Handler = handler
		packetConn, ok := listener.(net.PacketConn)
		if !ok {
			return errors.New("xhttp: HTTP/3 requires PacketConn listener")
		}
		go func() {
			if err := srv.Serve(packetConn); err != nil && err != http.ErrServerClosed {
				log.Errorln("xhttp: HTTP/3 server error: %v", err)
			}
		}()
		go func() {
			<-ctx.Done()
			srv.Close()
		}()
		go handler.runSessionJanitor(ctx)
		return nil

	case "2":
		tlsCfg2, ok := tlsCfg.(*tls.Config)
		if !ok && tlsCfg != nil {
			return errors.New("xhttp: HTTP/2 requires *tls.Config")
		}
		srv, err := NewHTTP2Server(config, tunnel, additions, tlsCfg2)
		if err != nil {
			return err
		}
		srv.Handler = handler
		go func() {
			if err := srv.ServeTLS(listener, "", ""); err != nil && err != http.ErrServerClosed {
				log.Errorln("xhttp: HTTP/2 server error: %v", err)
			}
		}()
		go func() {
			<-ctx.Done()
			srv.Close()
		}()
		go handler.runSessionJanitor(ctx)
		return nil

	default:
		srv, err := NewHTTP1Server(config, tunnel, additions)
		if err != nil {
			return err
		}
		srv.Handler = handler
		go func() {
			if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
				log.Errorln("xhttp: HTTP/1.1 server error: %v", err)
			}
		}()
		go func() {
			<-ctx.Done()
			srv.Close()
		}()
		go handler.runSessionJanitor(ctx)
		return nil
	}
}
