package xhttp

import "testing"

func TestConfigNormalizeDefaultScStreamUpServerSecs(t *testing.T) {
	cfg := &Config{}
	cfg.normalize()

	if cfg.ScStreamUpServerSecs.IsZero() {
		t.Fatal("ScStreamUpServerSecs should not be zero after normalize")
	}
	if cfg.ScStreamUpServerSecs.From != 20 || cfg.ScStreamUpServerSecs.To != 80 {
		t.Fatalf("unexpected ScStreamUpServerSecs default: got %d-%d, want 20-80", cfg.ScStreamUpServerSecs.From, cfg.ScStreamUpServerSecs.To)
	}
}

func TestConfigNormalizeKeepsExplicitScStreamUpServerSecs(t *testing.T) {
	cfg := &Config{
		ScStreamUpServerSecs: Range{From: 7, To: 9},
	}
	cfg.normalize()

	if cfg.ScStreamUpServerSecs.From != 7 || cfg.ScStreamUpServerSecs.To != 9 {
		t.Fatalf("explicit ScStreamUpServerSecs should be kept, got %d-%d", cfg.ScStreamUpServerSecs.From, cfg.ScStreamUpServerSecs.To)
	}
}

func TestConfigNormalizeInvalidH3CongestionController(t *testing.T) {
	cfg := &Config{H3CongestionController: "INVALID"}
	cfg.normalize()

	if cfg.H3CongestionController != "" {
		t.Fatalf("invalid H3 congestion should be reset, got %q", cfg.H3CongestionController)
	}
}

func TestConfigNormalizeAcceptsAdaptiveAndBrutal(t *testing.T) {
	tests := []string{"adaptive", "brutal", "bbr"}
	for _, tc := range tests {
		cfg := &Config{H3CongestionController: tc}
		cfg.normalize()
		if cfg.H3CongestionController != tc {
			t.Fatalf("unexpected normalized controller for %q: got %q", tc, cfg.H3CongestionController)
		}
	}
}

func TestConfigResolvedH3CongestionDefaultsAndOverride(t *testing.T) {
	cc, cwnd := (&Config{}).resolvedH3Congestion()
	if cc != DefaultH3CongestionController || cwnd != DefaultH3CongestionCWND {
		t.Fatalf("unexpected defaults cc=%q cwnd=%d", cc, cwnd)
	}

	cfg := &Config{H3CongestionController: "cubic", H3CWND: 96}
	cfg.normalize()
	cc, cwnd = cfg.resolvedH3Congestion()
	if cc != "cubic" || cwnd != 96 {
		t.Fatalf("unexpected override cc=%q cwnd=%d", cc, cwnd)
	}
}
