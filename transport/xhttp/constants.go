package xhttp

import "time"

// Buffer and channel size constants
const (
	DefaultPacketChannelSize = 64
	DefaultReadBufferSize    = 32 * 1024
	DefaultMaxWriteSize      = 1024 * 1024
)

// HTTP/3 tuning defaults inspired by Hysteria2's QUIC profile.
const (
	DefaultH3CongestionController    = "adaptive"
	DefaultH3CongestionCWND          = 48
	DefaultH3KeepAlivePeriod         = 10 * time.Second
	DefaultH3MaxIdleTimeout          = 45 * time.Second
	DefaultH3InitStreamReceiveWindow = 8 * 1024 * 1024
	DefaultH3MaxStreamReceiveWindow  = 8 * 1024 * 1024
	DefaultH3InitConnReceiveWindow   = 20 * 1024 * 1024
	DefaultH3MaxConnReceiveWindow    = 20 * 1024 * 1024

	DefaultH3MaxPostBytes     = 256 * 1024
	DefaultH3WeakMaxPostBytes = 96 * 1024

	DefaultH3PacketFlushInterval     = 4 * time.Millisecond
	DefaultH3WeakPacketFlushInterval = 8 * time.Millisecond
	DefaultH3MinPostInterval         = 4 * time.Millisecond
	DefaultH3WeakMinPostInterval     = 8 * time.Millisecond

	DefaultH3MinInFlightPosts     = 48
	DefaultH3WeakMinInFlightPosts = 64

	DefaultH3UploadRetryAttempts = 3
	DefaultH3UploadRetryBackoff  = 80 * time.Millisecond

	DefaultH3BrutalFallbackSendBPS     = 80 * 1000 * 1000 / 8
	DefaultH3WeakBrutalFallbackSendBPS = 24 * 1000 * 1000 / 8
	DefaultH3BrutalMinSendBPS          = 10 * 1000 * 1000 / 8
	DefaultH3BrutalMaxSendBPS          = 1200 * 1000 * 1000 / 8
	DefaultH3WeakBrutalMaxSendBPS      = 220 * 1000 * 1000 / 8
)

// Timeout and interval constants
const (
	DefaultPollInterval                = 100 * time.Millisecond
	DefaultMaxPackets                  = 30
	DefaultSessionIdleTimeout          = 10 * time.Minute
	DefaultConnectedSessionIdleTimeout = 30 * time.Minute
	DefaultSessionCleanupInterval      = 30 * time.Second
	DefaultEnqueueTimeout              = 500 * time.Millisecond
)
