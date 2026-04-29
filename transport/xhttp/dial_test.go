package xhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"testing"
	"time"

	mhttp "github.com/metacubex/http"
)

type streamUpCancelTransport struct{}

func (t *streamUpCancelTransport) RoundTrip(req *mhttp.Request) (*mhttp.Response, error) {
	switch req.Method {
	case mhttp.MethodGet:
		return &mhttp.Response{
			StatusCode: mhttp.StatusOK,
			Body:       io.NopCloser(&blockingReader{}),
			Request:    req,
		}, nil
	case mhttp.MethodPost:
		<-req.Context().Done()
		return nil, req.Context().Err()
	default:
		return nil, errors.New("unexpected method")
	}
}

type blockingReader struct{}

func (*blockingReader) Read([]byte) (int, error) {
	select {}
}

type closeAwareBlockingReader struct {
	done chan struct{}
	once sync.Once
}

func newCloseAwareBlockingReader() *closeAwareBlockingReader {
	return &closeAwareBlockingReader{done: make(chan struct{})}
}

func (r *closeAwareBlockingReader) Read([]byte) (int, error) {
	<-r.done
	return 0, io.EOF
}

func (r *closeAwareBlockingReader) Close() error {
	r.once.Do(func() {
		close(r.done)
	})
	return nil
}

type streamOneCtxTransport struct {
	mu       sync.Mutex
	postCtx  context.Context
	postSeen chan struct{}
}

func newStreamOneCtxTransport() *streamOneCtxTransport {
	return &streamOneCtxTransport{postSeen: make(chan struct{}, 1)}
}

func (t *streamOneCtxTransport) RoundTrip(req *mhttp.Request) (*mhttp.Response, error) {
	if req.Method != mhttp.MethodPost {
		return nil, fmt.Errorf("unexpected method: %s", req.Method)
	}

	t.mu.Lock()
	t.postCtx = req.Context()
	t.mu.Unlock()

	select {
	case t.postSeen <- struct{}{}:
	default:
	}

	return &mhttp.Response{
		StatusCode: mhttp.StatusOK,
		Body:       newCloseAwareBlockingReader(),
		Request:    req,
	}, nil
}

func (t *streamOneCtxTransport) waitPostCtx(timeout time.Duration) (context.Context, error) {
	select {
	case <-t.postSeen:
	case <-time.After(timeout):
		return nil, errors.New("timeout waiting for POST request")
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.postCtx == nil {
		return nil, errors.New("post context not captured")
	}
	return t.postCtx, nil
}

func TestDialStreamUpCloseCancelsUploadPromptly(t *testing.T) {
	transport := &streamUpCancelTransport{}
	client := &mhttp.Client{Transport: transport}

	ep := &endpoint{
		cfg:    &Config{},
		url:    &url.URL{Scheme: "http", Host: "example.com", Path: "/xhttp/session"},
		client: client,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := dialStreamUp(ctx, cancel, ep, ep)
	if err != nil {
		t.Fatalf("dialStreamUp() error = %v", err)
	}

	start := time.Now()
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatalf("conn.Close() error = %v", closeErr)
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("conn.Close() took too long: %v", elapsed)
	}
}

func TestDialStreamOneParentCancelDoesNotCancelPostRequest(t *testing.T) {
	transport := newStreamOneCtxTransport()
	client := &mhttp.Client{Transport: transport}

	ep := &endpoint{
		cfg:    &Config{},
		url:    &url.URL{Scheme: "http", Host: "example.com", Path: "/xhttp"},
		client: client,
	}

	ctx, cancel := context.WithCancel(context.Background())
	conn, err := dialStreamOne(ctx, cancel, ep)
	if err != nil {
		t.Fatalf("dialStreamOne() error = %v", err)
	}
	defer conn.Close()

	postCtx, err := transport.waitPostCtx(time.Second)
	if err != nil {
		t.Fatalf("failed to capture POST context: %v", err)
	}

	// Parent cancellation should not directly cancel stream-one POST request context.
	cancel()

	select {
	case <-postCtx.Done():
		t.Fatal("POST request context canceled by parent context")
	case <-time.After(120 * time.Millisecond):
	}
}

func TestDialStreamOneCloseCancelsPostRequestPromptly(t *testing.T) {
	transport := newStreamOneCtxTransport()
	client := &mhttp.Client{Transport: transport}

	ep := &endpoint{
		cfg:    &Config{},
		url:    &url.URL{Scheme: "http", Host: "example.com", Path: "/xhttp"},
		client: client,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := dialStreamOne(ctx, cancel, ep)
	if err != nil {
		t.Fatalf("dialStreamOne() error = %v", err)
	}

	postCtx, err := transport.waitPostCtx(time.Second)
	if err != nil {
		t.Fatalf("failed to capture POST context: %v", err)
	}

	start := time.Now()
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatalf("conn.Close() error = %v", closeErr)
	}

	select {
	case <-postCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("POST request context not canceled after conn.Close()")
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("conn.Close() took too long: %v", elapsed)
	}
}
