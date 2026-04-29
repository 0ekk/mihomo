package xhttp

import (
	"bytes"
	"context"
	http "github.com/metacubex/http"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPacketBatchWriterFlushOnClose(t *testing.T) {
	pr, pw := io.Pipe()
	writer := newPacketBatchWriter(pw, 64, 2*time.Millisecond)
	target := bytes.Repeat([]byte("a"), 256)
	done := make(chan []byte, 1)

	go func() {
		all, _ := io.ReadAll(pr)
		done <- all
	}()

	if _, err := writer.Write(target[:100]); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if _, err := writer.Write(target[100:]); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	select {
	case got := <-done:
		if !bytes.Equal(got, target) {
			t.Fatalf("unexpected payload length=%d, want=%d", len(got), len(target))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for batched payload")
	}
}

func TestMinPostIntervalWait(t *testing.T) {
	now := time.Now()
	interval := 30 * time.Millisecond

	if got := minPostIntervalWait(interval, time.Time{}, now); got != 0 {
		t.Fatalf("first post should not wait, got %v", got)
	}

	if got := minPostIntervalWait(interval, now.Add(-50*time.Millisecond), now); got != 0 {
		t.Fatalf("elapsed >= interval should not wait, got %v", got)
	}

	got := minPostIntervalWait(interval, now.Add(-10*time.Millisecond), now)
	if got <= 0 || got > 21*time.Millisecond {
		t.Fatalf("expected wait around 20ms, got %v", got)
	}
}

func TestHandleUploadsNon200ReturnsError(t *testing.T) {
	u, err := url.Parse("https://example.com/xhttp/session")
	if err != nil {
		t.Fatalf("parse url failed: %v", err)
	}

	ep := &endpoint{
		cfg: &Config{
			ScMaxEachPostBytes:   Range{From: 1_000_000, To: 1_000_000},
			ScMinPostsIntervalMs: Range{From: 0, To: 0},
		},
		url: u,
		client: &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader("error")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		})},
	}

	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("payload"))
		_ = pw.Close()
	}()

	err = handleUploads(context.Background(), ep, pr, io.NopCloser(strings.NewReader("")))
	if err == nil {
		t.Fatal("expected non-200 upload to return error")
	}
	if !strings.Contains(err.Error(), "unexpected upload status") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type roundTripperFunc func(req *http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
