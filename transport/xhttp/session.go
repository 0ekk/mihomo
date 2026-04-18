package xhttp

import (
	"container/heap"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/log"
)

var bufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 32*1024)
		return &buf
	},
}

type Packet struct {
	Payload []byte
	Reader  io.ReadCloser
	Seq     uint64
}

type uploadHeap []Packet

func (h uploadHeap) Len() int           { return len(h) }
func (h uploadHeap) Less(i, j int) bool { return h[i].Seq < h[j].Seq }
func (h uploadHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *uploadHeap) Push(x any) {
	*h = append(*h, x.(Packet))
}

func (h *uploadHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

type uploadQueue struct {
	pushedPackets chan Packet
	heap          *uploadHeap
	nextSeq       uint64
	maxPackets    int
	reader        io.ReadCloser
	nomore        bool
	closed        atomic.Bool
	writeCloseMu  sync.Mutex
	mu            sync.Mutex
}

var errPacketQueueTooLarge = errors.New("xhttp: packet queue is too large")

func newUploadQueue(maxPackets int) *uploadQueue {
	if maxPackets <= 0 {
		maxPackets = DefaultMaxPackets
	}
	h := &uploadHeap{}
	heap.Init(h)
	return &uploadQueue{
		pushedPackets: make(chan Packet, maxPackets),
		heap:          h,
		nextSeq:       0,
		maxPackets:    maxPackets,
	}
}

func (uq *uploadQueue) Push(packet Packet) error {
	uq.writeCloseMu.Lock()
	defer uq.writeCloseMu.Unlock()

	if uq.closed.Load() {
		return io.ErrClosedPipe
	}
	if uq.nomore {
		return io.ErrClosedPipe
	}
	if packet.Reader != nil {
		uq.nomore = true
	}

	uq.pushedPackets <- packet
	return nil
}

func (uq *uploadQueue) Read(p []byte) (n int, err error) {
	uq.mu.Lock()
	defer uq.mu.Unlock()

	for {
		if uq.reader != nil {
			return uq.reader.Read(p)
		}

		if uq.heap.Len() == 0 {
			if uq.closed.Load() {
				return 0, io.EOF
			}

			uq.mu.Unlock()
			packet, ok := <-uq.pushedPackets
			uq.mu.Lock()
			if !ok {
				if uq.reader != nil {
					return uq.reader.Read(p)
				}
				return 0, io.EOF
			}

			if packet.Reader != nil {
				uq.reader = packet.Reader
				continue
			}
			heap.Push(uq.heap, packet)
		}

		for uq.heap.Len() > 0 {
			packet := heap.Pop(uq.heap).(Packet)

			if packet.Seq < uq.nextSeq {
				continue
			}

			if packet.Seq == uq.nextSeq {
				if packet.Reader != nil {
					uq.reader = packet.Reader
					return uq.reader.Read(p)
				}

				n = copy(p, packet.Payload)
				if n < len(packet.Payload) {
					packet.Payload = packet.Payload[n:]
					heap.Push(uq.heap, packet)
				} else {
					uq.nextSeq = packet.Seq + 1
				}
				return n, nil
			}

			if uq.heap.Len() > uq.maxPackets {
				return 0, errPacketQueueTooLarge
			}

			heap.Push(uq.heap, packet)

			uq.mu.Unlock()
			nextPacket, ok := <-uq.pushedPackets
			uq.mu.Lock()
			if !ok {
				if uq.reader != nil {
					return uq.reader.Read(p)
				}
				if uq.heap.Len() == 0 {
					return 0, io.EOF
				}
				return 0, io.ErrUnexpectedEOF
			}

			if nextPacket.Reader != nil {
				if uq.reader == nil {
					uq.reader = nextPacket.Reader
				} else {
					_ = nextPacket.Reader.Close()
				}
				continue
			}

			heap.Push(uq.heap, nextPacket)
		}
	}
}

func (uq *uploadQueue) Close() error {
	uq.writeCloseMu.Lock()
	defer uq.writeCloseMu.Unlock()

	if !uq.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(uq.pushedPackets)

	uq.mu.Lock()
	defer uq.mu.Unlock()

	for packet := range uq.pushedPackets {
		if packet.Reader != nil {
			if uq.reader == nil {
				uq.reader = packet.Reader
			} else {
				_ = packet.Reader.Close()
			}
			continue
		}
		heap.Push(uq.heap, packet)
	}

	for uq.heap.Len() > 0 {
		packet := heap.Pop(uq.heap).(Packet)
		if packet.Reader != nil {
			_ = packet.Reader.Close()
		}
	}

	if uq.reader != nil {
		return uq.reader.Close()
	}

	return nil
}

type httpSession struct {
	sessionId     string
	uploadQueue   *uploadQueue
	downloadQueue chan []byte
	proxyConn     net.Conn
	mode          string
	created       time.Time
	expiry        time.Time
	idleTimeout   time.Duration
	connectedIdleTimeout time.Duration
	closed        atomic.Bool
	isFullyConnected atomic.Bool
	tunnelStarted atomic.Bool
	mu            sync.Mutex
}

func newHTTPSession(sessionId string, maxPackets int) *httpSession {
	return &httpSession{
		sessionId:     sessionId,
		uploadQueue:   newUploadQueue(maxPackets),
		downloadQueue: make(chan []byte, DefaultPacketChannelSize),
		created:       time.Now(),
		idleTimeout:   DefaultSessionIdleTimeout,
		connectedIdleTimeout: DefaultConnectedSessionIdleTimeout,
	}
}

func (s *httpSession) setIdleTimeouts(timeout, connectedTimeout time.Duration) {
	if timeout <= 0 || connectedTimeout <= 0 {
		return
	}
	s.mu.Lock()
	s.idleTimeout = timeout
	s.connectedIdleTimeout = connectedTimeout
	s.mu.Unlock()
}

func (s *httpSession) markFullyConnected() {
	s.isFullyConnected.Store(true)
	s.touch(0)
}

func (s *httpSession) touch(timeout time.Duration) {
	if timeout <= 0 {
		if s.isFullyConnected.Load() {
			timeout = s.connectedIdleTimeout
		} else {
			timeout = s.idleTimeout
		}
	}
	if timeout <= 0 {
		return
	}
	s.mu.Lock()
	if !s.closed.Load() {
		s.expiry = time.Now().Add(timeout)
	}
	s.mu.Unlock()
}

func (s *httpSession) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() {
		return nil
	}
	s.closed.Store(true)

	if s.uploadQueue != nil {
		s.uploadQueue.Close()
	}

	if s.downloadQueue != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Warnln("xhttp: panic closing downloadQueue: %v", r)
				}
			}()
			close(s.downloadQueue)
		}()
	}
	s.expiry = time.Now()

	if s.proxyConn != nil {
		s.proxyConn.Close()
	}

	log.Debugln("xhttp: session %s closed", s.sessionId)
	return nil
}

func (s *httpSession) isExpired(now time.Time) bool {
	return !s.expiry.IsZero() && now.After(s.expiry)
}

func (s *httpSession) startTunnel(remoteAddr, localAddr net.Addr, handleFunc func(net.Conn)) bool {
	if !s.tunnelStarted.CompareAndSwap(false, true) {
		return false
	}

	conn := newXHTTPConn(s, remoteAddr, localAddr)
	s.mu.Lock()
	s.proxyConn = conn
	s.mu.Unlock()

	go handleFunc(conn)
	return true
}

type xhttpConn struct {
	session    *httpSession
	remoteAddr net.Addr
	localAddr  net.Addr
	readBuf    []byte
}

func newXHTTPConn(session *httpSession, remoteAddr, localAddr net.Addr) *xhttpConn {
	return &xhttpConn{
		session:    session,
		remoteAddr: remoteAddr,
		localAddr:  localAddr,
	}
}

func (c *xhttpConn) Read(b []byte) (n int, err error) {
	if len(c.readBuf) > 0 {
		n = copy(b, c.readBuf)
		c.readBuf = c.readBuf[n:]
		return n, nil
	}

	bufPtr := bufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer bufferPool.Put(bufPtr)

	n, err = c.session.uploadQueue.Read(buf)
	if n > 0 {
		c.session.touch(0)
		copied := copy(b, buf[:n])
		if copied < n {
			c.readBuf = make([]byte, n-copied)
			copy(c.readBuf, buf[copied:n])
		}
		return copied, nil
	}
	return 0, err
}

func (c *xhttpConn) Write(b []byte) (n int, err error) {
	if c.session.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	if len(b) > DefaultMaxWriteSize {
		return 0, io.ErrShortWrite
	}

	data := make([]byte, len(b))
	copy(data, b)

	if sendOnDownloadQueue(c.session.downloadQueue, data) {
		return 0, io.ErrClosedPipe
	}

	c.session.touch(0)
	return len(b), nil
}

func sendOnDownloadQueue(ch chan []byte, payload []byte) (closed bool) {
	defer func() {
		if recover() != nil {
			closed = true
		}
	}()

	ch <- payload
	return false
}

func (c *xhttpConn) Close() error {
	return c.session.close()
}

func (c *xhttpConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *xhttpConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *xhttpConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *xhttpConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *xhttpConn) SetWriteDeadline(t time.Time) error {
	return nil
}
