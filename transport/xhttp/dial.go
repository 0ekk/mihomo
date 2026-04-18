package xhttp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/metacubex/http"
	"github.com/metacubex/http/httptrace"
	"github.com/metacubex/mihomo/log"
	tuicCommon "github.com/metacubex/mihomo/transport/tuic/common"
	"github.com/metacubex/quic-go"
	http3 "github.com/metacubex/quic-go/http3"
	"github.com/metacubex/tls"
)

type withoutCancelCtx struct {
	context.Context
}

func (withoutCancelCtx) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (withoutCancelCtx) Done() <-chan struct{} {
	return nil
}

func (withoutCancelCtx) Err() error {
	return nil
}

// This matches the behavior of context.WithoutCancel from Go 1.21+.
func withoutCancel(parent context.Context) context.Context {
	if parent == nil {
		panic("cannot create context from nil parent")
	}
	return withoutCancelCtx{parent}
}

// DialFunc dials the remote endpoint using the provided network (e.g. "tcp" or "udp").
type DialFunc func(ctx context.Context, network string) (net.Conn, error)

// Options configures the Dial call.
type Options struct {
	Dial         DialFunc
	Config       *Config
	Scheme       string
	HostHeader   string
	Address      string
	HTTPVersion  string
	PreferStream bool
	Tag          string
}

type endpoint struct {
	cfg         *Config
	url         *url.URL
	client      *http.Client
	httpVersion string
	releaseOnce sync.Once
	releaseFunc func()
}

func (e *endpoint) release() {
	if e == nil || e.releaseFunc == nil {
		return
	}
	e.releaseOnce.Do(func() {
		e.releaseFunc()
	})
}

// Dial establishes an XHTTP (SplitHTTP) connection and returns a net.Conn compatible stream.
func Dial(ctx context.Context, opts Options) (net.Conn, error) {
	if opts.Dial == nil {
		return nil, errors.New("xhttp: DialFunc is required")
	}

	cfg := opts.Config.clone()
	cfg.normalize()

	var downloadCfg *Config
	if cfg.Download != nil {
		downloadCfg = cfg.Download.clone()
		downloadCfg.normalize()
	} else {
		downloadCfg = cfg
	}

	mode := resolveMode(cfg.Mode, opts.PreferStream, downloadCfg != cfg)
	streamOneMode := mode == "stream-one"

	sessionID := ""
	if !streamOneMode {
		sessionID = uuid.Must(uuid.NewV4()).String()
	}

	uploadEP, err := prepareEndpoint(cfg, opts, sessionID, false)
	if err != nil {
		return nil, err
	}

	downloadEP := uploadEP
	if downloadCfg != cfg {
		downloadEP, err = prepareEndpoint(downloadCfg, opts, sessionID, true)
		if err != nil {
			uploadEP.release()
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(ctx)

	var conn net.Conn
	switch mode {
	case "stream-one":
		conn, err = dialStreamOne(ctx, cancel, uploadEP)
	case "stream-up":
		conn, err = dialStreamUp(ctx, cancel, uploadEP, downloadEP)
	default:
		conn, err = dialPacketUp(ctx, cancel, uploadEP, downloadEP)
	}
	if err != nil {
		cancel()
		uploadEP.release()
		if downloadEP != uploadEP {
			downloadEP.release()
		}
		return nil, err
	}

	return conn, nil
}

func resolveMode(mode string, preferStream bool, hasDownload bool) string {
	switch mode {
	case "stream-one", "stream-up", "packet-up":
		return mode
	}
	if preferStream {
		if hasDownload {
			return "stream-up"
		}
		return "stream-one"
	}
	return "packet-up"
}

func prepareEndpoint(cfg *Config, opts Options, sessionID string, isDownload bool) (*endpoint, error) {
	scheme := opts.Scheme
	if scheme == "" {
		scheme = "http"
	}
	host := firstNonEmpty(cfg.Host, opts.HostHeader, extractHost(opts.Address))
	if host == "" {
		host = "127.0.0.1"
	}

	httpVersion := opts.HTTPVersion
	if httpVersion == "" {
		httpVersion = "1.1"
	}
	if httpVersion == "3" {
		scheme = "https"
	}

	baseURL := buildBaseURL(cfg, scheme, host, sessionID)

	xmuxCfg := cfg.normalizedXmux()
	key := fmt.Sprintf("%s|%s|%s|%s|%t", opts.Address, host, cfg.Path, httpVersion, isDownload)

	slot, err := acquireClient(key, xmuxCfg, func() (*clientSlot, error) {
		client, transport, err := newHTTPClient(httpVersion, cfg, func(ctx context.Context, network string) (net.Conn, error) {
			target := "tcp"
			if network != "" {
				target = network
			}
			if httpVersion == "3" {
				target = "udp"
			}
			return opts.Dial(ctx, target)
		}, xmuxCfg.keepAlive, cfg.internalTLSConfig(), host)
		if err != nil {
			return nil, err
		}
		return &clientSlot{
			client:    client,
			transport: transport,
			cfg:       xmuxCfg,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, errors.New("xhttp: unable to acquire client")
	}

	return &endpoint{
		cfg:         cfg,
		url:         baseURL,
		client:      slot.client,
		httpVersion: httpVersion,
		releaseFunc: slot.release,
	}, nil
}

func dialStreamOne(ctx context.Context, cancel context.CancelFunc, ep *endpoint) (net.Conn, error) {
	// Isolate POST request from parent context cancellation using withoutCancel,
	// so long-lived stream-one upload is not interrupted by parent timeout/cancel.
	postCtx, cancelPost := context.WithCancel(withoutCancel(ctx))
	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(postCtx, http.MethodPost, ep.url.String(), pr)
	if err != nil {
		cancelPost()
		return nil, err
	}
	applyHeaders(req, ep.cfg, ep.url)
	if ep.cfg.NoGRPCHeader {
		req.Header.Del("Content-Type")
	}

	resp, remoteAddr, localAddr, err := doRequest(ep.client, req)
	if err != nil {
		cancelPost()
		return nil, err
	}

	conn := &splitConn{
		reader: resp.Body,
		writer: &pipeWriter{PipeWriter: pw},
		remote: remoteAddr,
		local:  localAddr,
		onClose: func() {
			cancel()
			cancelPost()
			_ = pw.Close()
			_ = resp.Body.Close()
			ep.release()
		},
	}
	return conn, nil
}

func dialStreamUp(ctx context.Context, cancel context.CancelFunc, uploadEP, downloadEP *endpoint) (net.Conn, error) {
	downloadCtx, cancelDownload := context.WithCancel(withoutCancel(ctx))
	downloadReq, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, downloadEP.url.String(), nil)
	if err != nil {
		cancelDownload()
		return nil, err
	}
	applyHeaders(downloadReq, downloadEP.cfg, downloadEP.url)
	if !downloadEP.cfg.NoSSEHeader {
		downloadReq.Header.Set("Content-Type", "text/event-stream")
	}

	downloadResp, remoteAddr, localAddr, err := doRequest(downloadEP.client, downloadReq)
	if err != nil {
		cancelDownload()
		return nil, err
	}

	// For upload to ensure it can complete even if the parent
	// context is cancelled (e.g. during graceful shutdown)
	uploadCtx, cancelUpload := context.WithCancel(withoutCancel(ctx))
	pr, pw := io.Pipe()
	uploadReq, err := http.NewRequestWithContext(uploadCtx, http.MethodPost, uploadEP.url.String(), pr)
	if err != nil {
		cancelUpload()
		cancelDownload()
		_ = downloadResp.Body.Close()
		return nil, err
	}
	applyHeaders(uploadReq, uploadEP.cfg, uploadEP.url)
	if uploadEP.cfg.NoGRPCHeader {
		uploadReq.Header.Del("Content-Type")
	}

	uploadDone := make(chan error, 1)

	go func() {
		defer cancelUpload()
		resp, err := uploadEP.client.Do(uploadReq)
		if err != nil {
			_ = downloadResp.Body.Close()
			pr.CloseWithError(err)
			uploadDone <- err
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		_ = downloadResp.Body.Close()
		uploadDone <- nil
	}()

	conn := &splitConn{
		reader: downloadResp.Body,
		writer: &pipeWriter{PipeWriter: pw},
		remote: remoteAddr,
		local:  localAddr,
		onClose: func() {
			cancel()
			cancelUpload()
			cancelDownload()
			_ = pw.Close()
			_ = downloadResp.Body.Close()
			select {
			case <-uploadDone:
			case <-time.After(5 * time.Second):
			}
			uploadEP.release()
			downloadEP.release()
		},
	}
	return conn, nil
}

func dialPacketUp(ctx context.Context, cancel context.CancelFunc, uploadEP, downloadEP *endpoint) (net.Conn, error) {
	downloadCtx := withoutCancel(ctx)
	downloadReq, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, downloadEP.url.String(), nil)
	if err != nil {
		return nil, err
	}
	applyHeaders(downloadReq, downloadEP.cfg, downloadEP.url)
	if !downloadEP.cfg.NoSSEHeader {
		downloadReq.Header.Set("Content-Type", "text/event-stream")
	}

	downloadResp, remoteAddr, localAddr, err := doRequest(downloadEP.client, downloadReq)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	maxPostBytes := normalizePacketUpMaxPostBytes(int(uploadEP.cfg.ScMaxEachPostBytes.Random()), uploadEP.httpVersion, uploadEP.cfg.H3WeakNetwork)
	flushEvery := 2 * time.Millisecond
	if cfgInterval := time.Duration(uploadEP.cfg.ScMinPostsIntervalMs.Random()) * time.Millisecond; cfgInterval > flushEvery {
		flushEvery = cfgInterval
	}
	flushEvery = normalizePacketUpFlushEvery(flushEvery, uploadEP.httpVersion, uploadEP.cfg.H3WeakNetwork)
	writer := newPacketBatchWriter(pw, maxPostBytes, flushEvery)

	uploadDone := make(chan error, 1)

	conn := &splitConn{
		reader: downloadResp.Body,
		writer: writer,
		remote: remoteAddr,
		local:  localAddr,
		onClose: func() {
			cancel()
			_ = writer.Close()
			_ = downloadResp.Body.Close()
			select {
			case <-uploadDone:
			case <-time.After(5 * time.Second):
			}
			uploadEP.release()
			downloadEP.release()
		},
	}

	go func() {
		err := handleUploads(ctx, uploadEP, pr, downloadResp.Body)
		uploadDone <- err
	}()

	return conn, nil
}

func handleUploads(ctx context.Context, ep *endpoint, reader *io.PipeReader, downloadBody io.ReadCloser) error {
	defer reader.Close()
	startedAt := time.Now()
	weakNetwork := ep.cfg != nil && ep.cfg.H3WeakNetwork
	maxPostBytes := normalizePacketUpMaxPostBytes(int(ep.cfg.ScMaxEachPostBytes.Random()), ep.httpVersion, weakNetwork)
	buf := make([]byte, maxPostBytes)
	seq := int64(0)
	interval := normalizePacketUpInterval(time.Duration(ep.cfg.ScMinPostsIntervalMs.Random())*time.Millisecond, ep.httpVersion, weakNetwork)
	lastPostStart := time.Time{}
	basePath := strings.TrimSuffix(ep.url.Path, "/")
	maxInFlight := normalizePacketUpMaxInFlight(int(ep.cfg.ScMaxBufferedPosts.Random()), ep.httpVersion, weakNetwork)
	inFlight := make(chan struct{}, maxInFlight)
	errCh := make(chan error, 1)
	maxUploadAttempts, retryBackoff := uploadRetryProfile(ep.httpVersion, weakNetwork)
	const progressLogEvery = int64(256)
	queuedPackets := int64(0)
	queuedBytes := int64(0)
	var donePackets atomic.Int64
	var doneBytes atomic.Int64
	var failedPackets atomic.Int64
	var postWG sync.WaitGroup
	reportErr := func(e error) {
		if e == nil {
			return
		}
		select {
		case errCh <- e:
		default:
		}
	}
	emitSummary := func(stage string, err error) {
		completed := donePackets.Load()
		completedBytes := doneBytes.Load()
		failed := failedPackets.Load()
		d := time.Since(startedAt)
		if err != nil {
			log.Warnln("xhttp: packet-up upload summary stage=%s path=%s queued=%d queued_bytes=%d completed=%d completed_bytes=%d failed=%d inflight=%d duration=%s err=%v",
				stage, basePath, queuedPackets, queuedBytes, completed, completedBytes, failed, len(inFlight), d, err)
			return
		}
		log.Infoln("xhttp: packet-up upload summary stage=%s path=%s queued=%d queued_bytes=%d completed=%d completed_bytes=%d failed=%d duration=%s",
			stage, basePath, queuedPackets, queuedBytes, completed, completedBytes, failed, d)
	}

	postOnce := func(payload []byte, postSeq int64) {
		postWG.Add(1)
		inFlight <- struct{}{}
		go func() {
			defer postWG.Done()
			defer func() { <-inFlight }()
			var finalErr error

			for attempt := 1; attempt <= maxUploadAttempts; attempt++ {
				urlCopy := *ep.url
				urlCopy.Path = fmt.Sprintf("%s/%d", basePath, postSeq)
				req, reqErr := http.NewRequestWithContext(withoutCancel(ctx), http.MethodPost, urlCopy.String(), bytes.NewReader(payload))
				if reqErr != nil {
					finalErr = reqErr
					break
				}
				applyHeaders(req, ep.cfg, ep.url)
				if ep.cfg.NoGRPCHeader {
					req.Header.Del("Content-Type")
				}

				resp, reqErr := ep.client.Do(req)
				if reqErr != nil {
					finalErr = reqErr
					if attempt < maxUploadAttempts {
						time.Sleep(retryBackoff)
						continue
					}
					break
				}

				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				if resp.StatusCode == http.StatusOK {
					finalErr = nil
					break
				}

				statusErr := fmt.Errorf("xhttp: unexpected upload status %d", resp.StatusCode)
				finalErr = statusErr
				if resp.StatusCode >= 500 && attempt < maxUploadAttempts {
					time.Sleep(retryBackoff)
					continue
				}
				break
			}

			if finalErr != nil {
				failIdx := failedPackets.Add(1)
				if failIdx <= 5 || failIdx%50 == 0 {
					if strings.Contains(finalErr.Error(), "unexpected upload status") {
						log.Warnln("xhttp: packet-up upload bad status path=%s seq=%d bytes=%d inflight=%d err=%v", basePath, postSeq, len(payload), len(inFlight), finalErr)
					} else {
						log.Warnln("xhttp: packet-up upload request failed path=%s seq=%d bytes=%d inflight=%d err=%v", basePath, postSeq, len(payload), len(inFlight), finalErr)
					}
				}
				reportErr(finalErr)
				_ = downloadBody.Close()
				reader.CloseWithError(finalErr)
				return
			}
			donePackets.Add(1)
			doneBytes.Add(int64(len(payload)))
		}()
	}

	for {
		select {
		case uploadErr := <-errCh:
			postWG.Wait()
			emitSummary("error-early", uploadErr)
			return uploadErr
		default:
		}

		n, err := reader.Read(buf)
		if n > 0 {
			if wait := minPostIntervalWait(interval, lastPostStart, time.Now()); wait > 0 {
				time.Sleep(wait)
			}
			lastPostStart = time.Now()

			payload := make([]byte, n)
			copy(payload, buf[:n])
			queuedPackets++
			queuedBytes += int64(n)
			if queuedPackets%progressLogEvery == 0 {
				log.Infoln("xhttp: packet-up upload progress path=%s queued=%d completed=%d failed=%d inflight=%d queued_bytes=%d interval_ms=%d max_post_bytes=%d max_inflight=%d",
					basePath,
					queuedPackets,
					donePackets.Load(),
					failedPackets.Load(),
					len(inFlight),
					queuedBytes,
					interval/time.Millisecond,
					maxPostBytes,
					maxInFlight,
				)
			}
			postOnce(payload, seq)
			seq++
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				reader.CloseWithError(err)
				postWG.Wait()
				emitSummary("read-error", err)
				return err
			}
			postWG.Wait()
			select {
			case uploadErr := <-errCh:
				emitSummary("error-on-eof", uploadErr)
				return uploadErr
			default:
				emitSummary("eof", nil)
				return nil
			}
		}
	}
}

func minPostIntervalWait(interval time.Duration, lastPostStart, now time.Time) time.Duration {
	if interval <= 0 || lastPostStart.IsZero() {
		return 0
	}
	elapsed := now.Sub(lastPostStart)
	if elapsed >= interval {
		return 0
	}
	return interval - elapsed
}

func normalizePacketUpMaxPostBytes(maxPostBytes int, httpVersion string, weakNetwork bool) int {
	if maxPostBytes <= 0 {
		maxPostBytes = 1024 * 1024
	}
	if httpVersion != "3" {
		return maxPostBytes
	}
	capBytes := DefaultH3MaxPostBytes
	if weakNetwork {
		capBytes = DefaultH3WeakMaxPostBytes
	}
	if maxPostBytes > capBytes {
		maxPostBytes = capBytes
	}
	if maxPostBytes < 8*1024 {
		maxPostBytes = 8 * 1024
	}
	return maxPostBytes
}

func normalizePacketUpFlushEvery(flushEvery time.Duration, httpVersion string, weakNetwork bool) time.Duration {
	if flushEvery <= 0 {
		flushEvery = 2 * time.Millisecond
	}
	if httpVersion != "3" {
		if flushEvery > 15*time.Millisecond {
			return 15 * time.Millisecond
		}
		return flushEvery
	}
	if weakNetwork {
		if flushEvery < 2*time.Millisecond {
			return 2 * time.Millisecond
		}
		if flushEvery > DefaultH3WeakPacketFlushInterval {
			return DefaultH3WeakPacketFlushInterval
		}
		return flushEvery
	}
	if flushEvery < time.Millisecond {
		return time.Millisecond
	}
	if flushEvery > DefaultH3PacketFlushInterval {
		return DefaultH3PacketFlushInterval
	}
	return flushEvery
}

func normalizePacketUpInterval(interval time.Duration, httpVersion string, weakNetwork bool) time.Duration {
	if httpVersion != "3" {
		return interval
	}
	if interval <= 0 {
		if weakNetwork {
			return DefaultH3WeakMinPostInterval
		}
		return DefaultH3MinPostInterval
	}
	if weakNetwork {
		if interval < DefaultH3WeakMinPostInterval {
			return DefaultH3WeakMinPostInterval
		}
		if interval > 20*time.Millisecond {
			return 20 * time.Millisecond
		}
		return interval
	}
	if interval < 2*time.Millisecond {
		return 2 * time.Millisecond
	}
	if interval > 12*time.Millisecond {
		return 12 * time.Millisecond
	}
	return interval
}

func normalizePacketUpMaxInFlight(maxInFlight int, httpVersion string, weakNetwork bool) int {
	if maxInFlight <= 0 {
		maxInFlight = DefaultMaxPackets
	}
	if httpVersion != "3" {
		return maxInFlight
	}
	minInFlight := DefaultH3MinInFlightPosts
	if weakNetwork {
		minInFlight = DefaultH3WeakMinInFlightPosts
	}
	if maxInFlight < minInFlight {
		maxInFlight = minInFlight
	}
	if maxInFlight > 256 {
		maxInFlight = 256
	}
	return maxInFlight
}

func uploadRetryProfile(httpVersion string, weakNetwork bool) (int, time.Duration) {
	if httpVersion != "3" {
		return 2, 60 * time.Millisecond
	}
	if weakNetwork {
		return DefaultH3UploadRetryAttempts + 1, DefaultH3UploadRetryBackoff + 40*time.Millisecond
	}
	return DefaultH3UploadRetryAttempts, DefaultH3UploadRetryBackoff
}

func resolveH3KeepAlive(keepAlive time.Duration, weakNetwork bool) time.Duration {
	period := DefaultH3KeepAlivePeriod
	if keepAlive > 0 && keepAlive < period {
		period = keepAlive
	}
	if weakNetwork && period > 8*time.Second {
		period = 8 * time.Second
	}
	if period <= 0 {
		period = DefaultH3KeepAlivePeriod
	}
	return period
}

func resolveH3MaxIdleTimeout(keepAlive time.Duration, weakNetwork bool) time.Duration {
	idle := DefaultH3MaxIdleTimeout
	if keepAlive > 0 && keepAlive*2 > idle {
		idle = keepAlive * 2
	}
	if weakNetwork && idle < time.Minute {
		idle = time.Minute
	}
	return idle
}

func applyH3CongestionController(quicConn *quic.Conn, cfg *Config, weakNetwork bool) {
	cc, cwnd := (&Config{}).resolvedH3Congestion()
	if cfg != nil {
		cc, cwnd = cfg.resolvedH3Congestion()
	}

	switch cc {
	case "adaptive":
		if setH3BrutalCongestion(quicConn, cfg, weakNetwork) {
			return
		}
		log.Warnln("xhttp: h3 adaptive brutal unavailable, fallback to bbr")
		tuicCommon.SetCongestionController(quicConn, "bbr", cwnd)
		log.Debugln("xhttp: h3 congestion=bbr source=adaptive-fallback cwnd=%d", cwnd)
	case "brutal":
		if setH3BrutalCongestion(quicConn, cfg, weakNetwork) {
			return
		}
		log.Warnln("xhttp: h3 brutal unavailable, fallback to bbr")
		tuicCommon.SetCongestionController(quicConn, "bbr", cwnd)
		log.Debugln("xhttp: h3 congestion=bbr source=brutal-fallback cwnd=%d", cwnd)
	default:
		tuicCommon.SetCongestionController(quicConn, cc, cwnd)
		log.Debugln("xhttp: h3 congestion=%s cwnd=%d", cc, cwnd)
	}
}

func setH3BrutalCongestion(quicConn *quic.Conn, cfg *Config, weakNetwork bool) bool {
	sendBPS := resolveH3BrutalSendBPS(cfg, weakNetwork)
	if sendBPS == 0 {
		return false
	}
	if !tuicCommon.SetBrutalCongestionController(quicConn, sendBPS) {
		return false
	}
	log.Debugln("xhttp: h3 congestion=brutal send_bps=%d", sendBPS)
	return true
}

func resolveH3BrutalSendBPS(cfg *Config, weakNetwork bool) uint64 {
	maxPostBytes := DefaultH3MaxPostBytes
	interval := DefaultH3MinPostInterval
	if weakNetwork {
		maxPostBytes = DefaultH3WeakMaxPostBytes
		interval = DefaultH3WeakMinPostInterval
	}

	if cfg != nil {
		postRange := cfg.ScMaxEachPostBytes.WithDefault(1_000_000, 1_000_000)
		intervalRange := cfg.ScMinPostsIntervalMs.WithDefault(30, 30)

		rawPost := int((postRange.From + postRange.To) / 2)
		rawInterval := time.Duration((intervalRange.From+intervalRange.To)/2) * time.Millisecond

		maxPostBytes = normalizePacketUpMaxPostBytes(rawPost, "3", weakNetwork)
		interval = normalizePacketUpInterval(rawInterval, "3", weakNetwork)
	}

	if interval <= 0 {
		if weakNetwork {
			interval = DefaultH3WeakMinPostInterval
		} else {
			interval = DefaultH3MinPostInterval
		}
	}

	sendBPS := uint64(maxPostBytes) * uint64(time.Second) / uint64(interval)
	if sendBPS == 0 {
		if weakNetwork {
			sendBPS = DefaultH3WeakBrutalFallbackSendBPS
		} else {
			sendBPS = DefaultH3BrutalFallbackSendBPS
		}
	}

	maxBPS := uint64(DefaultH3BrutalMaxSendBPS)
	if weakNetwork {
		maxBPS = uint64(DefaultH3WeakBrutalMaxSendBPS)
	}
	if sendBPS < uint64(DefaultH3BrutalMinSendBPS) {
		sendBPS = uint64(DefaultH3BrutalMinSendBPS)
	}
	if sendBPS > maxBPS {
		sendBPS = maxBPS
	}
	return sendBPS
}

type packetBatchWriter struct {
	base       *io.PipeWriter
	buffered   *bufio.Writer
	flushEvery time.Duration
	closed     atomic.Bool
	flushStop  chan struct{}
	flushDone  chan struct{}
	mu         sync.Mutex
}

func newPacketBatchWriter(base *io.PipeWriter, maxChunk int, flushEvery time.Duration) *packetBatchWriter {
	if maxChunk <= 0 {
		maxChunk = 1024 * 1024
	}
	if flushEvery <= 0 {
		flushEvery = 2 * time.Millisecond
	}
	w := &packetBatchWriter{
		base:       base,
		buffered:   bufio.NewWriterSize(base, maxChunk),
		flushEvery: flushEvery,
		flushStop:  make(chan struct{}),
		flushDone:  make(chan struct{}),
	}
	go w.flushLoop()
	return w
}

func (w *packetBatchWriter) flushLoop() {
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()
	defer close(w.flushDone)

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			_ = w.buffered.Flush()
			w.mu.Unlock()
		case <-w.flushStop:
			return
		}
	}
}

func (w *packetBatchWriter) Write(b []byte) (int, error) {
	if w.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	w.mu.Lock()
	n, err := w.buffered.Write(b)
	if err == nil {
		if w.buffered.Available() == 0 {
			err = w.buffered.Flush()
		}
	}
	w.mu.Unlock()
	return n, err
}

func (w *packetBatchWriter) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(w.flushStop)
	<-w.flushDone

	w.mu.Lock()
	flushErr := w.buffered.Flush()
	closeErr := w.base.Close()
	w.mu.Unlock()

	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

func applyHeaders(req *http.Request, cfg *Config, baseURL *url.URL) {
	for k, v := range cfg.Headers {
		if strings.EqualFold(k, "Host") {
			continue
		}
		req.Header.Set(k, v)
	}
	req.Host = baseURL.Host
	cfg.ApplyXPaddingToRequest(req, cfg.buildRequestXPaddingConfig(baseURL.String()))

	q := req.URL.Query()
	if !cfg.ScStreamUpServerSecs.IsZero() {
		q.Set("sc_stream_up_server_secs", fmt.Sprintf("%d", cfg.ScStreamUpServerSecs.Random()))
	}
	req.URL.RawQuery = q.Encode()

	if req.Method == http.MethodPost && !cfg.NoGRPCHeader {
		req.Header.Set("Content-Type", "application/grpc")
	}
}

func doRequest(client *http.Client, req *http.Request) (*http.Response, net.Addr, net.Addr, error) {
	var remoteAddr, localAddr net.Addr
	gotConn := make(chan struct{}, 1)
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			remoteAddr = info.Conn.RemoteAddr()
			localAddr = info.Conn.LocalAddr()
			select {
			case gotConn <- struct{}{}:
			default:
			}
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, nil, err
	}
	select {
	case <-gotConn:
	default:
	}
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return nil, nil, nil, fmt.Errorf("xhttp: unexpected status %d", resp.StatusCode)
	}
	return resp, remoteAddr, localAddr, nil
}

func newHTTPClient(httpVersion string, cfg *Config, dial DialFunc, keepAlive time.Duration, tlsCfg *tls.Config, host string) (*http.Client, http.RoundTripper, error) {
	switch httpVersion {
	case "3":
		if tlsCfg == nil {
			tlsCfg = &tls.Config{
				MinVersion: tls.VersionTLS13,
				ServerName: host,
			}
		}
		if tlsCfg.ServerName == "" {
			tlsCfg.ServerName = host
		}
		if len(tlsCfg.NextProtos) == 0 {
			tlsCfg.NextProtos = []string{"h3"}
		}
		weakNetwork := cfg != nil && cfg.H3WeakNetwork
		quicCfg := &quic.Config{
			InitialStreamReceiveWindow:     DefaultH3InitStreamReceiveWindow,
			MaxStreamReceiveWindow:         DefaultH3MaxStreamReceiveWindow,
			InitialConnectionReceiveWindow: DefaultH3InitConnReceiveWindow,
			MaxConnectionReceiveWindow:     DefaultH3MaxConnReceiveWindow,
			KeepAlivePeriod:                resolveH3KeepAlive(keepAlive, weakNetwork),
			MaxIdleTimeout:                 resolveH3MaxIdleTimeout(keepAlive, weakNetwork),
			DisablePathMTUDiscovery:        weakNetwork,
		}
		transport := &http3.Transport{
			TLSClientConfig: tlsCfg,
			QUICConfig:      quicCfg,
			Dial: func(ctx context.Context, addr string, tlsCfg *tls.Config, quicCfg *quic.Config) (*quic.Conn, error) {
				conn, err := dial(ctx, "udp")
				if err != nil {
					return nil, err
				}
				packetConn, ok := conn.(net.PacketConn)
				if !ok {
					_ = conn.Close()
					return nil, fmt.Errorf("dial requires net.PacketConn, got %T", conn)
				}

				var udpAddr *net.UDPAddr
				if remoteAddr := conn.RemoteAddr(); remoteAddr != nil {
					if resolvedRemote, ok := remoteAddr.(*net.UDPAddr); ok {
						udpAddr = resolvedRemote
					}
				}
				if udpAddr == nil {
					udpAddr, err = net.ResolveUDPAddr("udp", addr)
					if err != nil {
						_ = conn.Close()
						return nil, err
					}
				}
				quicConn, err := quic.DialEarly(ctx, packetConn, udpAddr, tlsCfg, quicCfg)
				if err != nil {
					_ = conn.Close()
					return nil, err
				}
				applyH3CongestionController(quicConn, cfg, weakNetwork)
				return quicConn, nil
			},
		}
		return &http.Client{Transport: transport}, transport, nil
	case "2":
		transport := &http.Http2Transport{
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return dial(ctx, "tcp")
			},
			TLSClientConfig:    tlsCfg,
			AllowHTTP:          false,
			DisableCompression: true,
			ReadIdleTimeout:    keepAlive,
			PingTimeout:        0,
		}
		return &http.Client{Transport: transport}, transport, nil
	default:
		transport := &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dial(ctx, "tcp")
			},
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dial(ctx, "tcp")
			},
			IdleConnTimeout:     keepAlive,
			MaxIdleConnsPerHost: 4,
		}
		return &http.Client{Transport: transport}, transport, nil
	}
}

type pipeWriter struct {
	*io.PipeWriter
	closed atomic.Bool
}

func (w *pipeWriter) Close() error {
	if w.closed.CompareAndSwap(false, true) {
		return w.PipeWriter.Close()
	}
	return nil
}

func (w *pipeWriter) Write(b []byte) (int, error) {
	return w.PipeWriter.Write(b)
}

func buildBaseURL(cfg *Config, scheme, host, sessionID string) *url.URL {
	path := cfg.Path
	if sessionID != "" {
		path += sessionID
	}
	u := &url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   path,
	}
	return u
}

func extractHost(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err == nil {
		return host
	}
	return address
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
