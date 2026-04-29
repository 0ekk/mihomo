package xhttp

import "testing"

func TestResolveH3BrutalSendBPSDefaults(t *testing.T) {
	normal := resolveH3BrutalSendBPS(nil, false)
	weak := resolveH3BrutalSendBPS(nil, true)

	if normal == 0 || weak == 0 {
		t.Fatalf("expected non-zero sendBPS, got normal=%d weak=%d", normal, weak)
	}
	if normal <= weak {
		t.Fatalf("expected normal sendBPS > weak sendBPS, got normal=%d weak=%d", normal, weak)
	}
	if normal != 65536000 {
		t.Fatalf("unexpected normal default sendBPS: got=%d want=65536000", normal)
	}
	if weak != 12288000 {
		t.Fatalf("unexpected weak default sendBPS: got=%d want=12288000", weak)
	}
}

func TestResolveH3BrutalSendBPSAppliesMinimumClamp(t *testing.T) {
	cfg := &Config{
		ScMaxEachPostBytes:   Range{From: 1, To: 1},
		ScMinPostsIntervalMs: Range{From: 10_000, To: 10_000},
	}
	cfg.normalize()

	sendBPS := resolveH3BrutalSendBPS(cfg, false)
	if sendBPS != uint64(DefaultH3BrutalMinSendBPS) {
		t.Fatalf("expected minimum clamp %d, got %d", DefaultH3BrutalMinSendBPS, sendBPS)
	}
}
