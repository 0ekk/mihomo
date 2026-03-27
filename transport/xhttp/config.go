package xhttp

import (
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/metacubex/tls"
)

// Config holds SplitHTTP client settings.
type Config struct {
	Host                 string            `proxy:"host,omitempty" json:"host"`
	Path                 string            `proxy:"path,omitempty" json:"path"`
	HTTPVersion          string            `proxy:"http-version,omitempty" json:"http-version"`
	Mode                 string            `proxy:"mode,omitempty" json:"mode"`
	Headers              map[string]string `proxy:"headers,omitempty" json:"headers"`
	NoGRPCHeader         bool              `proxy:"no-grpc-header,omitempty" json:"no-grpc-header"`
	NoSSEHeader          bool              `proxy:"no-sse-header,omitempty" json:"no-sse-header"`
	XPaddingBytes        Range             `proxy:"x-padding-bytes,omitempty" json:"x-padding-bytes"`
	XPaddingObfsMode     bool              `proxy:"x-padding-obfs-mode,omitempty" json:"x-padding-obfs-mode"`
	XPaddingKey          string            `proxy:"x-padding-key,omitempty" json:"x-padding-key"`
	XPaddingHeader       string            `proxy:"x-padding-header,omitempty" json:"x-padding-header"`
	XPaddingPlacement    string            `proxy:"x-padding-placement,omitempty" json:"x-padding-placement"`
	XPaddingMethod       string            `proxy:"x-padding-method,omitempty" json:"x-padding-method"`
	ScMaxEachPostBytes   Range             `proxy:"sc-max-each-post-bytes,omitempty" json:"sc-max-each-post-bytes"`
	ScMinPostsIntervalMs Range             `proxy:"sc-min-posts-interval-ms,omitempty" json:"sc-min-posts-interval-ms"`
	ScMaxBufferedPosts   Range             `proxy:"sc-max-buffered-posts,omitempty" json:"sc-max-buffered-posts"`
	ScStreamUpServerSecs Range             `proxy:"sc-stream-up-server-secs,omitempty" json:"sc-stream-up-server-secs"`
	Xmux                 *XmuxConfig       `proxy:"xmux,omitempty" json:"xmux"`
	Download             *Config           `proxy:"download-settings,omitempty" json:"download-settings"`
	ClientFingerprint    string            `proxy:"client-fingerprint,omitempty" json:"client-fingerprint"`

	internalTLS *tls.Config `proxy:"-" json:"-"`
}

func (c *Config) EnsureHTTP3TLS(fallbackHost string, skipVerify bool, httpVersion string) {
	if c == nil {
		return
	}
	if httpVersion == "3" {
		host := c.Host
		if host == "" {
			host = fallbackHost
		}
		tlsCfg := &tls.Config{
			ServerName:         host,
			InsecureSkipVerify: skipVerify,
			MinVersion:         tls.VersionTLS13,
			NextProtos:         []string{"h3"},
		}
		c.internalTLS = tlsCfg
	}
	if c.Download != nil {
		c.Download.EnsureHTTP3TLS(fallbackHost, skipVerify, httpVersion)
	}
}

func (c *Config) clone() *Config {
	if c == nil {
		return defaultConfig()
	}
	cp := *c
	if len(c.Headers) != 0 {
		cp.Headers = make(map[string]string, len(c.Headers))
		for k, v := range c.Headers {
			cp.Headers[k] = v
		}
	}
	if c.Download != nil {
		cp.Download = c.Download.clone()
	}
	if c.Xmux != nil {
		cp.Xmux = c.Xmux.clone()
	}
	return &cp
}

func defaultConfig() *Config {
	return &Config{
		Path:                 "/",
		XPaddingBytes:        Range{From: 100, To: 1000},
		ScMaxEachPostBytes:   Range{From: 1_000_000, To: 1_000_000},
		ScMinPostsIntervalMs: Range{From: 30, To: 30},
		ScMaxBufferedPosts:   Range{From: 30, To: 30},
		ScStreamUpServerSecs: Range{From: 20, To: 80},
	}
}

func (c *Config) normalize() {
	if c == nil {
		return
	}
	c.Path = normalizePath(c.Path)
	c.XPaddingBytes = c.XPaddingBytes.WithDefault(100, 1000)
	c.ScMaxEachPostBytes = c.ScMaxEachPostBytes.WithDefault(1_000_000, 1_000_000)
	c.ScMinPostsIntervalMs = c.ScMinPostsIntervalMs.WithDefault(30, 30)
	c.ScMaxBufferedPosts = c.ScMaxBufferedPosts.WithDefault(30, 30)
	c.ScStreamUpServerSecs = c.ScStreamUpServerSecs.WithDefault(20, 80)
	c.normalizeXPadding()
	switch c.Mode {
	case "", "auto", "packet-up", "stream-up", "stream-one":
	default:
		c.Mode = "packet-up"
	}
	if c.Xmux == nil {
		c.Xmux = &XmuxConfig{}
	}
	c.Xmux.normalize()
}

func (c *Config) normalizeXPadding() {
	if c == nil || !c.XPaddingObfsMode {
		return
	}
	if c.XPaddingKey == "" {
		c.XPaddingKey = "x_padding"
	}
	if c.XPaddingHeader == "" {
		c.XPaddingHeader = "Referer"
	}
	switch c.XPaddingPlacement {
	case PlacementQueryInHeader, PlacementCookie, PlacementHeader, PlacementQuery:
	default:
		c.XPaddingPlacement = PlacementQueryInHeader
	}
	switch PaddingMethod(c.XPaddingMethod) {
	case PaddingMethodRepeatX, PaddingMethodTokenish:
	default:
		c.XPaddingMethod = string(PaddingMethodRepeatX)
	}
}

func normalizePath(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return path.Clean(p) + "/"
}

func withPadding(rawURL string, padding int) string {
	if padding <= 0 {
		padding = 1
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	query := u.Query()
	query.Set("x_padding", strings.Repeat("X", padding))
	u.RawQuery = query.Encode()
	return u.String()
}

func (c *Config) validate() error {
	if c == nil {
		return fmt.Errorf("xhttp config is nil")
	}
	if c.Host != "" && strings.Contains(c.Host, "://") {
		return fmt.Errorf("xhttp host should not include scheme")
	}
	return nil
}

func (c *Config) internalTLSConfig() *tls.Config {
	return c.internalTLS
}

// httpVersion resolves the HTTP version to use for server based on the
// configured HTTPVersion and whether TLS is present.
func (c *Config) httpVersion(hasTLS bool) string {
	if c == nil {
		if hasTLS {
			return "2"
		}
		return "1.1"
	}
	v := strings.TrimSpace(strings.ToLower(c.HTTPVersion))
	if v == "" || v == "auto" {
		if hasTLS {
			return "2"
		}
		return "1.1"
	}
	switch v {
	case "3", "h3":
		return "3"
	case "2", "h2":
		return "2"
	default:
		return "1.1"
	}
}

func (c *Config) Clone() *Config {
	return c.clone()
}

func (c *Config) normalizedXmux() normalizedXmux {
	if c == nil || c.Xmux == nil {
		return defaultXmux()
	}
	return c.Xmux.normalized()
}

type XmuxConfig struct {
	MaxConcurrency   Range `proxy:"max-concurrency,omitempty" json:"max-concurrency"`
	MaxConnections   Range `proxy:"max-connections,omitempty" json:"max-connections"`
	CMaxReuseTimes   Range `proxy:"c-max-reuse-times,omitempty" json:"c-max-reuse-times"`
	HMaxRequestTimes Range `proxy:"h-max-request-times,omitempty" json:"h-max-request-times"`
	HMaxReusableSecs Range `proxy:"h-max-reusable-secs,omitempty" json:"h-max-reusable-secs"`
	HKeepAlivePeriod int64 `proxy:"h-keep-alive-period,omitempty" json:"h-keep-alive-period"`
}

func (x *XmuxConfig) clone() *XmuxConfig {
	if x == nil {
		return nil
	}
	cp := *x
	return &cp
}

func (x *XmuxConfig) normalize() {
	if x == nil {
		return
	}
}

type normalizedXmux struct {
	maxConcurrency int32
	maxConnections int
	reuseRange     Range
	requestRange   Range
	reusableRange  Range
	keepAlive      time.Duration
}

func defaultXmux() normalizedXmux {
	return normalizedXmux{
		maxConcurrency: 1,
		maxConnections: 0,
		reuseRange:     Range{},
		requestRange:   Range{From: 600, To: 900},
		reusableRange:  Range{From: 1800, To: 3000},
		keepAlive:      30 * time.Second,
	}
}

func (x *XmuxConfig) normalized() normalizedXmux {
	if x == nil {
		return defaultXmux()
	}
	n := defaultXmux()
	if v := x.MaxConcurrency.WithDefault(n.maxConcurrency, n.maxConcurrency); v.To >= v.From {
		n.maxConcurrency = v.Random()
	}
	if v := x.MaxConnections.WithDefault(0, 0); v.To >= v.From {
		n.maxConnections = int(v.Random())
	}
	if !x.CMaxReuseTimes.IsZero() {
		n.reuseRange = x.CMaxReuseTimes
	}
	if !x.HMaxRequestTimes.IsZero() {
		n.requestRange = x.HMaxRequestTimes
	}
	if !x.HMaxReusableSecs.IsZero() {
		n.reusableRange = x.HMaxReusableSecs
	}
	if x.HKeepAlivePeriod != 0 {
		if x.HKeepAlivePeriod < 0 {
			n.keepAlive = 0
		} else {
			n.keepAlive = time.Duration(x.HKeepAlivePeriod) * time.Second
		}
	}
	return n
}

func (n normalizedXmux) newSlotLimits() (int32, int32, time.Time) {
	var uses int32
	if !n.reuseRange.IsZero() {
		uses = n.reuseRange.Random()
	}
	var requests int32
	if !n.requestRange.IsZero() {
		requests = n.requestRange.Random()
		if requests <= 0 {
			requests = 1
		}
	}
	expiry := time.Time{}
	if !n.reusableRange.IsZero() {
		secs := n.reusableRange.Random()
		if secs > 0 {
			expiry = time.Now().Add(time.Duration(secs) * time.Second)
		}
	}
	return uses, requests, expiry
}
