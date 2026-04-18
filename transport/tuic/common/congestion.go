package common

import (
	hyCongestion "github.com/metacubex/mihomo/transport/hysteria/congestion"
	"github.com/metacubex/mihomo/transport/tuic/congestion"
	congestionv2 "github.com/metacubex/mihomo/transport/tuic/congestion_v2"

	"github.com/metacubex/quic-go"
	c "github.com/metacubex/quic-go/congestion"
)

const (
	DefaultStreamReceiveWindow     = 15728640 // 15 MB/s
	DefaultConnectionReceiveWindow = 67108864 // 64 MB/s
)

func SetCongestionController(quicConn *quic.Conn, cc string, cwnd int) {
	if cwnd == 0 {
		cwnd = 32
	}
	switch cc {
	case "cubic":
		quicConn.SetCongestionControl(
			congestion.NewCubicSender(
				congestion.GetInitialPacketSize(quicConn),
				false,
			),
		)
	case "new_reno":
		quicConn.SetCongestionControl(
			congestion.NewCubicSender(
				congestion.GetInitialPacketSize(quicConn),
				true,
			),
		)
	case "bbr_meta_v1":
		quicConn.SetCongestionControl(
			congestion.NewBBRSender(
				congestion.GetInitialPacketSize(quicConn),
				c.ByteCount(cwnd)*congestion.InitialMaxDatagramSize,
				congestion.DefaultBBRMaxCongestionWindow*congestion.InitialMaxDatagramSize,
			),
		)
	case "bbr_meta_v2":
		fallthrough
	case "bbr":
		quicConn.SetCongestionControl(
			congestionv2.NewBbrSender(
				congestionv2.GetInitialPacketSize(quicConn),
				c.ByteCount(cwnd),
			),
		)
	}
}

func SetBrutalCongestionController(quicConn *quic.Conn, sendBPS uint64) bool {
	if quicConn == nil || sendBPS == 0 {
		return false
	}
	quicConn.SetCongestionControl(hyCongestion.NewBrutalSender(c.ByteCount(sendBPS)))
	return true
}
