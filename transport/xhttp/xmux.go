package xhttp

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/http"
	"github.com/metacubex/mihomo/transport/gun"
	http3 "github.com/metacubex/quic-go/http3"
)

type clientSlot struct {
	client          *http.Client
	transport       http.RoundTripper
	manager         *xmuxManager
	cfg             normalizedXmux
	openUsage       atomic.Int32
	lastTrafficUnix atomic.Int64
	draining        atomic.Bool
	closeOnce       sync.Once
}

func (s *clientSlot) shouldDrop(now time.Time) bool {
	last := s.lastTrafficUnix.Load()
	if last == 0 {
		return false
	}
	idleTimeout := resolveClientSlotIdleTimeout(s.cfg)
	if idleTimeout <= 0 {
		return false
	}
	if now.Sub(time.Unix(0, last)) >= idleTimeout {
		return true
	}
	return false
}

func (s *clientSlot) shouldHardDrop(now time.Time) bool {
	last := s.lastTrafficUnix.Load()
	if last == 0 {
		return false
	}
	hardTimeout := resolveClientSlotHardIdleTimeout(s.cfg)
	if hardTimeout <= 0 {
		return false
	}
	return now.Sub(time.Unix(0, last)) >= hardTimeout
}

func (s *clientSlot) markTraffic() {
	if s != nil {
		s.lastTrafficUnix.Store(time.Now().UnixNano())
		s.draining.Store(false)
	}
}

func (s *clientSlot) release() {
	if s == nil {
		return
	}
	if s.manager != nil {
		s.manager.release(s)
		return
	}
	s.openUsage.Add(-1)
}

func (s *clientSlot) close() {
	s.closeOnce.Do(func() {
		if s.client != nil {
			s.client.CloseIdleConnections()
		}
		forceCloseRoundTripper(s.transport)
	})
}

func forceCloseRoundTripper(transport http.RoundTripper) {
	if transport == nil {
		return
	}
	if closer, ok := transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
	switch tr := transport.(type) {
	case *http.Http2Transport:
		gun.CloseTransport(tr)
		return
	case *http3.Transport:
		_ = tr.Close()
		return
	}
	if closer, ok := transport.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

func resolveClientSlotIdleTimeout(cfg normalizedXmux) time.Duration {
	if cfg.keepAlive > 0 {
		idle := cfg.keepAlive * 4
		if idle < 30*time.Second {
			idle = 30 * time.Second
		}
		return idle
	}
	return DefaultSessionIdleTimeout
}

func resolveClientSlotHardIdleTimeout(cfg normalizedXmux) time.Duration {
	soft := resolveClientSlotIdleTimeout(cfg)
	if soft <= 0 {
		return 0
	}
	hard := soft * 3
	if hard < 2*time.Minute {
		hard = 2 * time.Minute
	}
	return hard
}

func resolveMinWarmSlots(cfg normalizedXmux) int {
	if cfg.maxConnections == 1 {
		return 0
	}
	if cfg.maxConnections > 0 && cfg.maxConnections < 3 {
		return 0
	}
	return 1
}

const xmuxJanitorInterval = 20 * time.Second

var xmuxJanitorOnce sync.Once

func startXmuxJanitor() {
	xmuxJanitorOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(xmuxJanitorInterval)
			defer ticker.Stop()
			for now := range ticker.C {
				xmuxRegistry.Range(func(_, value any) bool {
					manager, ok := value.(*xmuxManager)
					if !ok || manager == nil {
						return true
					}
					manager.mu.Lock()
					manager.compactLocked(now)
					manager.pruneFromRegistryIfEmptyLocked()
					manager.mu.Unlock()
					return true
				})
			}
		}()
	})
}

type xmuxManager struct {
	key       string
	cfg       normalizedXmux
	newClient func() (*clientSlot, error)

	mu      sync.Mutex
	clients []*clientSlot
}

func (m *xmuxManager) pruneFromRegistryIfEmptyLocked() {
	if len(m.clients) == 0 {
		xmuxRegistry.Delete(m.key)
	}
}

func (m *xmuxManager) compactLocked(now time.Time) {
	if len(m.clients) == 0 {
		return
	}

	minWarm := resolveMinWarmSlots(m.cfg)
	warmCount := 0
	for _, slot := range m.clients {
		if !slot.draining.Load() {
			warmCount++
		}
	}

	j := 0
	for _, slot := range m.clients {
		usage := slot.openUsage.Load()

		if slot.shouldHardDrop(now) && usage == 0 {
			if !slot.draining.Load() && warmCount > 0 {
				warmCount--
			}
			slot.close()
			continue
		}

		if slot.shouldDrop(now) && !slot.draining.Load() && warmCount > minWarm {
			slot.draining.Store(true)
			warmCount--
		}

		if slot.draining.Load() && usage == 0 {
			slot.close()
			continue
		}

		m.clients[j] = slot
		j++
	}
	m.clients = m.clients[:j]
}

func (m *xmuxManager) acquire() (*clientSlot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.compactLocked(now)

	var candidate *clientSlot
	warmEligible := make([]*clientSlot, 0, len(m.clients))
	drainingEligible := make([]*clientSlot, 0, len(m.clients))
	for _, slot := range m.clients {
		if m.cfg.maxConcurrency > 0 && slot.openUsage.Load() >= m.cfg.maxConcurrency {
			continue
		}
		if slot.draining.Load() {
			drainingEligible = append(drainingEligible, slot)
			continue
		}
		warmEligible = append(warmEligible, slot)
	}

	if len(warmEligible) > 0 {
		candidate = warmEligible[rand.Intn(len(warmEligible))]
	} else if len(drainingEligible) > 0 {
		candidate = drainingEligible[rand.Intn(len(drainingEligible))]
		candidate.draining.Store(false)
	}

	if candidate == nil {
		if m.cfg.maxConnections == 0 || len(m.clients) < m.cfg.maxConnections {
			slot, err := m.newClient()
			if err != nil {
				return nil, err
			}
			slot.manager = m
			m.clients = append(m.clients, slot)
			candidate = slot
		} else if len(m.clients) > 0 {
			candidate = m.clients[0]
		}
	}

	if candidate == nil {
		return nil, nil
	}

	candidate.markTraffic()
	candidate.openUsage.Add(1)
	return candidate, nil
}

func (m *xmuxManager) release(slot *clientSlot) {
	if slot == nil {
		return
	}

	remaining := slot.openUsage.Add(-1)
	if remaining != 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.compactLocked(time.Now())
	m.pruneFromRegistryIfEmptyLocked()
}

var xmuxRegistry sync.Map

func acquireClient(key string, cfg normalizedXmux, factory func() (*clientSlot, error)) (*clientSlot, error) {
	startXmuxJanitor()

	managerAny, ok := xmuxRegistry.Load(key)
	if !ok {
		manager := &xmuxManager{
			key:       key,
			cfg:       cfg,
			newClient: factory,
		}
		actual, _ := xmuxRegistry.LoadOrStore(key, manager)
		managerAny = actual
	}

	manager := managerAny.(*xmuxManager)
	slot, err := manager.acquire()
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, nil
	}
	return slot, nil
}
