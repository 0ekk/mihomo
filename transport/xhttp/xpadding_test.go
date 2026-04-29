package xhttp

import (
	"strings"
	"testing"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

func TestApplyXPaddingToRequestDefaultReferer(t *testing.T) {
	cfg := &Config{}
	cfg.normalize()

	req := httptest.NewRequest(http.MethodGet, "http://example.com/xhttp/", nil)
	cfg.ApplyXPaddingToRequest(req, cfg.buildRequestXPaddingConfig("http://example.com/xhttp/"))

	ref := req.Header.Get("Referer")
	if ref == "" {
		t.Fatal("expected Referer to be set")
	}
	if !strings.Contains(ref, "x_padding=") {
		t.Fatalf("expected Referer x_padding query, got %q", ref)
	}
}

func TestApplyXPaddingToRequestObfsHeader(t *testing.T) {
	cfg := &Config{
		XPaddingObfsMode:  true,
		XPaddingPlacement: PlacementHeader,
		XPaddingHeader:    "X-Test-Padding",
		XPaddingKey:       "x_pad",
		XPaddingMethod:    string(PaddingMethodRepeatX),
		XPaddingBytes:     Range{From: 128, To: 128},
	}
	cfg.normalize()

	req := httptest.NewRequest(http.MethodGet, "http://example.com/xhttp/", nil)
	cfg.ApplyXPaddingToRequest(req, cfg.buildRequestXPaddingConfig("http://example.com/xhttp/"))

	pad := req.Header.Get("X-Test-Padding")
	if len(pad) != 128 {
		t.Fatalf("expected X-Test-Padding len 128, got %d", len(pad))
	}
}

func TestExtractXPaddingFromRequestObfsCookie(t *testing.T) {
	cfg := &Config{
		XPaddingObfsMode:  true,
		XPaddingPlacement: PlacementCookie,
		XPaddingKey:       "x_pad",
		XPaddingHeader:    "X-Test-Padding",
		XPaddingMethod:    string(PaddingMethodRepeatX),
		XPaddingBytes:     Range{From: 64, To: 64},
	}
	cfg.normalize()

	req := httptest.NewRequest(http.MethodGet, "http://example.com/xhttp/", nil)
	req.AddCookie(&http.Cookie{Name: "x_pad", Value: strings.Repeat("X", 64)})

	pad, placement := cfg.ExtractXPaddingFromRequest(req, true)
	if len(pad) != 64 {
		t.Fatalf("expected extracted cookie padding len 64, got %d", len(pad))
	}
	if !strings.Contains(placement, PlacementCookie) {
		t.Fatalf("expected cookie placement, got %q", placement)
	}
}

func TestIsPaddingValidTokenish(t *testing.T) {
	cfg := &Config{}
	cfg.normalize()

	pad := GeneratePadding(PaddingMethodTokenish, 160)
	if pad == "" {
		t.Fatal("expected non-empty tokenish padding")
	}
	if !cfg.IsPaddingValid(pad, 120, 220, PaddingMethodTokenish) {
		t.Fatal("expected tokenish padding to be valid in configured range")
	}
	if cfg.IsPaddingValid(pad, 1, 10, PaddingMethodTokenish) {
		t.Fatal("expected tokenish padding to be invalid in very narrow low range")
	}
}

func TestBuildResponseXPaddingConfigObfsHeader(t *testing.T) {
	cfg := &Config{
		XPaddingObfsMode:  true,
		XPaddingPlacement: PlacementHeader,
		XPaddingHeader:    "X-Test-Padding",
		XPaddingKey:       "x_pad",
		XPaddingMethod:    string(PaddingMethodRepeatX),
		XPaddingBytes:     Range{From: 77, To: 77},
	}
	cfg.normalize()

	w := httptest.NewRecorder()
	cfg.ApplyXPaddingToHeader(w.Header(), cfg.buildResponseXPaddingConfig())

	if got := w.Header().Get("X-Test-Padding"); len(got) != 77 {
		t.Fatalf("expected obfs response padding len 77, got %d", len(got))
	}
	if got := w.Header().Get("X-Padding"); got != "" {
		t.Fatalf("expected X-Padding to be empty in obfs header mode, got %q", got)
	}
}
