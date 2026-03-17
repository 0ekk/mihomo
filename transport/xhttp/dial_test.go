package xhttp

import (
	"context"
	"errors"
	"io"
	"net/url"
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

func TestDialStreamUpCloseCancelsUploadPromptly(t *testing.T) {
	transport := &streamUpCancelTransport{}
	client := &mhttp.Client{Transport: transport}

	ep := &endpoint{
		cfg: &Config{},
		url: &url.URL{Scheme: "http", Host: "example.com", Path: "/xhttp/session"},
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
