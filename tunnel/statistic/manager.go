package statistic

import (
	"os"
	"strings"
	"sync"
	stdatomic "sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/xsync"
	"github.com/metacubex/mihomo/component/memory"
)

var DefaultManager *Manager

const maxClosedConnections = 5000

func init() {
	DefaultManager = &Manager{
		uploadTemp:    atomic.NewInt64(0),
		downloadTemp:  atomic.NewInt64(0),
		uploadBlip:    atomic.NewInt64(0),
		downloadBlip:  atomic.NewInt64(0),
		uploadTotal:   atomic.NewInt64(0),
		downloadTotal: atomic.NewInt64(0),
		pid:           int32(os.Getpid()),
	}

	go DefaultManager.handle()
}

type Manager struct {
	connections    xsync.Map[string, Tracker]
	processTraffic xsync.Map[string, *ProcessTraffic]
	historyMux     sync.Mutex
	historyEnabled stdatomic.Bool
	closedMux      sync.Mutex
	closed         []*TrackerInfo
	failedMux      sync.Mutex
	failed         []*FailedConnectionInfo
	uploadTemp     atomic.Int64
	downloadTemp   atomic.Int64
	uploadBlip     atomic.Int64
	downloadBlip   atomic.Int64
	uploadTotal    atomic.Int64
	downloadTotal  atomic.Int64
	pid            int32
	memory         uint64
}

func (m *Manager) Join(c Tracker) {
	m.connections.Store(c.ID(), c)
}

func (m *Manager) Leave(c Tracker) {
	if m.historyEnabled.Load() {
		m.recordClosedConnection(c.Info())
	}
	m.connections.Delete(c.ID())
}

func (m *Manager) Get(id string) (c Tracker) {
	if value, ok := m.connections.Load(id); ok {
		c = value
	}
	return
}

func (m *Manager) Range(f func(c Tracker) bool) {
	m.connections.Range(func(key string, value Tracker) bool {
		return f(value)
	})
}

func (m *Manager) PushUploaded(size int64) {
	m.uploadTemp.Add(size)
	m.uploadTotal.Add(size)
}

func (m *Manager) PushDownloaded(size int64) {
	m.downloadTemp.Add(size)
	m.downloadTotal.Add(size)
}

func (m *Manager) PushUploadedForProcess(process string, size int64) {
	m.PushUploaded(size)
	if !m.historyEnabled.Load() {
		return
	}
	m.processTrafficFor(process).UploadTotal.Add(size)
}

func (m *Manager) PushDownloadedForProcess(process string, size int64) {
	m.PushDownloaded(size)
	if !m.historyEnabled.Load() {
		return
	}
	m.processTrafficFor(process).DownloadTotal.Add(size)
}

func (m *Manager) Now() (up int64, down int64) {
	return m.uploadBlip.Load(), m.downloadBlip.Load()
}

func (m *Manager) Total() (up, down int64) {
	return m.uploadTotal.Load(), m.downloadTotal.Load()
}

func (m *Manager) Memory() uint64 {
	m.updateMemory()
	return m.memory
}

func (m *Manager) Snapshot() *Snapshot {
	var connections []*TrackerInfo
	m.Range(func(c Tracker) bool {
		connections = append(connections, c.Info())
		return true
	})
	processTraffic := make(map[string]*ProcessTraffic)
	if m.historyEnabled.Load() {
		m.processTraffic.Range(func(key string, value *ProcessTraffic) bool {
			processTraffic[key] = value
			return true
		})
	}
	return &Snapshot{
		UploadTotal:       m.uploadTotal.Load(),
		DownloadTotal:     m.downloadTotal.Load(),
		Connections:       connections,
		ClosedConnections: m.ClosedConnections(),
		FailedConnections: m.FailedConnections(),
		ProcessTraffic:    processTraffic,
		Memory:            m.memory,
	}
}

func (m *Manager) SetHistoryEnabled(enabled bool) {
	m.historyMux.Lock()
	defer m.historyMux.Unlock()

	if m.historyEnabled.Load() == enabled {
		return
	}
	if enabled {
		m.ClearHistory()
		m.historyEnabled.Store(true)
	} else {
		m.historyEnabled.Store(false)
		m.ClearHistory()
	}
}

func (m *Manager) HistoryEnabled() bool {
	return m.historyEnabled.Load()
}

func (m *Manager) ClearHistory() {
	m.processTraffic.Range(func(key string, value *ProcessTraffic) bool {
		m.processTraffic.Delete(key)
		return true
	})

	m.closedMux.Lock()
	m.closed = nil
	m.closedMux.Unlock()

	m.failedMux.Lock()
	m.failed = nil
	m.failedMux.Unlock()
}

func (m *Manager) recordClosedConnection(info *TrackerInfo) {
	if info == nil {
		return
	}

	m.closedMux.Lock()
	defer m.closedMux.Unlock()

	m.closed = append(m.closed, info)
	if len(m.closed) > maxClosedConnections {
		copy(m.closed, m.closed[len(m.closed)-maxClosedConnections:])
		m.closed = m.closed[:maxClosedConnections]
	}
}

func (m *Manager) ClosedConnections() []*TrackerInfo {
	if !m.historyEnabled.Load() {
		return nil
	}

	m.closedMux.Lock()
	defer m.closedMux.Unlock()

	closed := make([]*TrackerInfo, len(m.closed))
	copy(closed, m.closed)
	return closed
}

func (m *Manager) FailedConnections() []*FailedConnectionInfo {
	if !m.historyEnabled.Load() {
		return nil
	}

	m.failedMux.Lock()
	defer m.failedMux.Unlock()

	failed := make([]*FailedConnectionInfo, len(m.failed))
	copy(failed, m.failed)
	return failed
}

func (m *Manager) updateMemory() {
	stat, err := memory.GetMemoryInfo(m.pid)
	if err != nil {
		return
	}
	m.memory = stat.RSS
}

func (m *Manager) ResetStatistic() {
	m.uploadTemp.Store(0)
	m.uploadBlip.Store(0)
	m.uploadTotal.Store(0)
	m.downloadTemp.Store(0)
	m.downloadBlip.Store(0)
	m.downloadTotal.Store(0)
	m.ClearHistory()
}

func (m *Manager) handle() {
	ticker := time.NewTicker(time.Second)

	for range ticker.C {
		m.uploadBlip.Store(m.uploadTemp.Swap(0))
		m.downloadBlip.Store(m.downloadTemp.Swap(0))
	}
}

type Snapshot struct {
	DownloadTotal     int64                      `json:"downloadTotal"`
	UploadTotal       int64                      `json:"uploadTotal"`
	Connections       []*TrackerInfo             `json:"connections"`
	ClosedConnections []*TrackerInfo             `json:"closedConnections"`
	FailedConnections []*FailedConnectionInfo    `json:"failedConnections"`
	ProcessTraffic    map[string]*ProcessTraffic `json:"processTraffic"`
	Memory            uint64                     `json:"memory"`
}

type ProcessTraffic struct {
	DownloadTotal atomic.Int64 `json:"download"`
	UploadTotal   atomic.Int64 `json:"upload"`
}

func (m *Manager) processTrafficFor(process string) *ProcessTraffic {
	key := normalizeProcessName(process)
	if value, ok := m.processTraffic.Load(key); ok {
		return value
	}

	traffic := &ProcessTraffic{
		UploadTotal:   atomic.NewInt64(0),
		DownloadTotal: atomic.NewInt64(0),
	}
	actual, _ := m.processTraffic.LoadOrStore(key, traffic)
	return actual
}

func normalizeProcessName(process string) string {
	base, _, _ := strings.Cut(process, ":")
	if base == "" {
		return "Unknown"
	}
	return base
}
