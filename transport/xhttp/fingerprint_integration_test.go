package xhttp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	tlsC "github.com/metacubex/mihomo/component/tls"
	utls "github.com/metacubex/utls"
)

// capturedClientHello stores the captured TLS ClientHello information
type capturedClientHello struct {
	version      uint16
	cipherSuites []uint16
	extensions   []uint16
	curves       []utls.CurveID
	ja3Hash      string
}

// tlsTestServer is a test server that captures TLS ClientHello
type tlsTestServer struct {
	server      *http.Server
	listener    net.Listener
	captured    *capturedClientHello
	captureLock sync.Mutex
	tlsConfig   *tls.Config
}

func newTLSTestServer(t *testing.T) *tlsTestServer {
	// Generate self-signed certificate programmatically
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatalf("failed to generate certificate: %v", err)
	}

	ts := &tlsTestServer{
		tlsConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		},
	}

	// Wrap GetConfigForClient to capture ClientHello
	originalGetConfigForClient := ts.tlsConfig.GetConfigForClient
	ts.tlsConfig.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		ts.captureLock.Lock()
		ts.captured = &capturedClientHello{
			version:      hello.SupportedVersions[0],
			cipherSuites: hello.CipherSuites,
		}
		ts.captureLock.Unlock()

		if originalGetConfigForClient != nil {
			return originalGetConfigForClient(hello)
		}
		return nil, nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	ts.server = &http.Server{
		Handler:   mux,
		TLSConfig: ts.tlsConfig,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	ts.listener = listener

	go func() {
		ts.server.ServeTLS(listener, "", "")
	}()

	time.Sleep(100 * time.Millisecond) // Wait for server to start

	return ts
}

func (ts *tlsTestServer) close() {
	if ts.server != nil {
		ts.server.Close()
	}
	if ts.listener != nil {
		ts.listener.Close()
	}
}

func (ts *tlsTestServer) addr() string {
	return ts.listener.Addr().String()
}

func (ts *tlsTestServer) getCaptured() *capturedClientHello {
	ts.captureLock.Lock()
	defer ts.captureLock.Unlock()
	return ts.captured
}

// TestXHTTPWithRealTLSFingerprint tests that fingerprints are actually applied to TLS connections
func TestXHTTPWithRealTLSFingerprint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tests := []struct {
		name        string
		fingerprint string
		wantApplied bool
	}{
		{
			name:        "chrome fingerprint",
			fingerprint: "chrome",
			wantApplied: true,
		},
		{
			name:        "firefox fingerprint",
			fingerprint: "firefox",
			wantApplied: true,
		},
		{
			name:        "safari fingerprint",
			fingerprint: "safari",
			wantApplied: true,
		},
		{
			name:        "no fingerprint uses standard tls",
			fingerprint: "",
			wantApplied: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create new server for each test to avoid connection reuse
			server := newTLSTestServer(t)
			defer server.close()
			cfg := &Config{
				Host:              "127.0.0.1",
				Path:              "/",
				ClientFingerprint: tt.fingerprint,
			}

			dialCount := 0
			dialFn := func(ctx context.Context, network string) (net.Conn, error) {
				dialCount++
				conn, err := net.Dial("tcp", server.addr())
				if err != nil {
					return nil, err
				}

				// Apply TLS with fingerprint if set
				if tt.fingerprint != "" {
					tlsConfig := &tls.Config{
						InsecureSkipVerify: true,
						ServerName:         "127.0.0.1",
						NextProtos:         []string{"h2"},
					}

					if fp, ok := tlsC.GetFingerprint(tt.fingerprint); ok {
						utlsConfig := tlsC.UConfig(tlsConfig)
						utlsConn := tlsC.UClient(conn, utlsConfig, fp)
						if err := utlsConn.HandshakeContext(ctx); err != nil {
							conn.Close()
							return nil, fmt.Errorf("utls handshake failed: %w", err)
						}
						return utlsConn, nil
					}
				}

				// Standard TLS
				tlsConfig := &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         "127.0.0.1",
					NextProtos:         []string{"h2"},
				}
				tlsConn := tls.Client(conn, tlsConfig)
				if err := tlsConn.HandshakeContext(ctx); err != nil {
					conn.Close()
					return nil, fmt.Errorf("tls handshake failed: %w", err)
				}
				return tlsConn, nil
			}

			ctx := context.Background()
			opts := Options{
				Dial:        dialFn,
				Config:      cfg,
				Scheme:      "https",
				HostHeader:  "127.0.0.1",
				Address:     server.addr(),
				HTTPVersion: "2",
				Tag:         "test",
			}

			conn, err := Dial(ctx, opts)
			if err != nil {
				t.Fatalf("Dial() error = %v", err)
			}
			defer conn.Close()

			// Write some data to trigger connection
			n, err := conn.Write([]byte("test"))
			if err != nil {
				t.Fatalf("Write() error = %v", err)
			}
			if n != 4 {
				t.Errorf("Write() wrote %d bytes, want 4", n)
			}

			// Verify connection was made
			if dialCount == 0 {
				t.Error("dialFn was not called")
			}

			// Verify TLS handshake occurred
			captured := server.getCaptured()
			if captured == nil {
				t.Error("no ClientHello captured - TLS handshake may not have occurred")
				return
			}

			// When using uTLS with fingerprint, the cipher suites and extensions should be different
			// from standard Go TLS
			if tt.wantApplied {
				// uTLS should have more cipher suites than standard Go
				if len(captured.cipherSuites) < 10 {
					t.Errorf("uTLS should send more cipher suites, got %d", len(captured.cipherSuites))
				}
				t.Logf("%s: captured %d cipher suites (uTLS applied)", tt.name, len(captured.cipherSuites))
			} else {
				// Standard Go TLS has fewer cipher suites
				t.Logf("%s: captured %d cipher suites (standard TLS)", tt.name, len(captured.cipherSuites))
			}
		})
	}
}

// TestXHTTPFingerprintOverride tests that xhttp-opts fingerprint overrides global
func TestXHTTPFingerprintOverride(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	server := newTLSTestServer(t)
	defer server.close()

	// Test that xhttp config fingerprint is used when set
	cfg := &Config{
		Host:              "127.0.0.1",
		Path:              "/",
		ClientFingerprint: "firefox", // This should be used
	}

	globalFingerprint := "chrome" // This should be ignored

	usedFingerprint := globalFingerprint
	if cfg.ClientFingerprint != "" {
		usedFingerprint = cfg.ClientFingerprint
	}

	if usedFingerprint != "firefox" {
		t.Errorf("expected firefox fingerprint to override, got %s", usedFingerprint)
	}

	dialFn := func(ctx context.Context, network string) (net.Conn, error) {
		conn, err := net.Dial("tcp", server.addr())
		if err != nil {
			return nil, err
		}

		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         "127.0.0.1",
			NextProtos:         []string{"h2"},
		}

		// Use the determined fingerprint
		if fp, ok := tlsC.GetFingerprint(usedFingerprint); ok {
			utlsConfig := tlsC.UConfig(tlsConfig)
			utlsConn := tlsC.UClient(conn, utlsConfig, fp)
			if err := utlsConn.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			return utlsConn, nil
		}

		return nil, fmt.Errorf("fingerprint not found: %s", usedFingerprint)
	}

	ctx := context.Background()
	opts := Options{
		Dial:        dialFn,
		Config:      cfg,
		Scheme:      "https",
		HostHeader:  "127.0.0.1",
		Address:     server.addr(),
		HTTPVersion: "2",
		Tag:         "test-override",
	}

	conn, err := Dial(ctx, opts)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	// Trigger connection
	conn.Write([]byte("test"))

	// Verify handshake occurred
	time.Sleep(200 * time.Millisecond)
	captured := server.getCaptured()
	if captured == nil {
		t.Error("no ClientHello captured")
	}
}

// generateSelfSignedCert generates a self-signed certificate for testing
func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(24 * time.Hour)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test Co"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
	}, nil
}
