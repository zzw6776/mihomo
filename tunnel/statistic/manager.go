package statistic

import (
	"os"
	"strings"
	"sync"
	stdatomic "sync/atomic"
	"time"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/xsync"
	"github.com/metacubex/mihomo/component/memory"
)

var DefaultManager *Manager

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
	connections         xsync.Map[string, Tracker]
	processTraffic      xsync.Map[string, *ProcessTraffic]
	historyMux          sync.Mutex
	historyEnabled      stdatomic.Bool
	eventsMux           sync.Mutex
	eventsSequence      uint64
	eventsDB            *bbolt.DB
	eventsJournalDirty  bool
	eventsSession       string
	eventsJournalFailed bool
	eventsNeedsClear    bool
	eventsFallback      []historyEvent
	eventsDropped       uint64
	eventsAckToken      uint64
	eventsPendingAck    *historyPendingAck
	uploadTemp          atomic.Int64
	downloadTemp        atomic.Int64
	uploadBlip          atomic.Int64
	downloadBlip        atomic.Int64
	uploadTotal         atomic.Int64
	downloadTotal       atomic.Int64
	pid                 int32
	memory              uint64
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

func (m *Manager) SetHistoryEnabled(enabled bool, session string) {
	m.historyMux.Lock()
	defer m.historyMux.Unlock()
	m.eventsMux.Lock()
	defer m.eventsMux.Unlock()

	if !enabled {
		m.historyEnabled.Store(false)
		m.clearProcessTraffic()
		m.setHistorySessionLocked(session)
		_ = m.clearJournalLocked()
		return
	}

	wasEnabled := m.historyEnabled.Load()
	sameSession := session == "" || m.eventsSession == session
	if wasEnabled && sameSession && !m.eventsJournalFailed && !m.eventsNeedsClear {
		return
	}
	if !wasEnabled || !sameSession {
		m.clearProcessTraffic()
	}
	m.setHistorySessionLocked(session)
	m.historyEnabled.Store(true)
}

func (m *Manager) HistoryEnabled() bool {
	return m.historyEnabled.Load()
}

func (m *Manager) ClearHistory() {
	m.historyMux.Lock()
	defer m.historyMux.Unlock()

	m.clearProcessTraffic()

	m.eventsMux.Lock()
	_ = m.clearJournalLocked()
	m.eventsMux.Unlock()
}

func (m *Manager) clearProcessTraffic() {
	m.processTraffic.Range(func(key string, value *ProcessTraffic) bool {
		m.processTraffic.Delete(key)
		return true
	})
}

func (m *Manager) recordClosedConnection(info *TrackerInfo) {
	if info == nil {
		return
	}

	m.appendHistoryEvent(historyEvent{closed: info})
}

func (m *Manager) ClosedConnections() []*TrackerInfo {
	if !m.historyEnabled.Load() {
		return nil
	}

	events := m.peekHistoryEventRecords(5000)
	closed := make([]*TrackerInfo, 0, len(events))
	for _, event := range events {
		if event.closed != nil {
			closed = append(closed, event.closed)
		}
	}
	return closed
}

func (m *Manager) FailedConnections() []*FailedConnectionInfo {
	if !m.historyEnabled.Load() {
		return nil
	}

	events := m.peekHistoryEventRecords(5000)
	failed := make([]*FailedConnectionInfo, 0, len(events))
	for _, event := range events {
		if event.failed != nil {
			failed = append(failed, event.failed)
		}
	}
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
	m.clearProcessTraffic()
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

type HistoryEvents struct {
	AckToken          uint64                  `json:"ackToken"`
	AckSequence       uint64                  `json:"ackSequence"`
	ClosedConnections []*TrackerInfo          `json:"closedConnections"`
	ClosedAt          map[string]int64        `json:"closedAt"`
	FailedConnections []*FailedConnectionInfo `json:"failedConnections"`
	FailedAt          map[string]int64        `json:"failedAt"`
}

type historyEvent struct {
	sequence   uint64
	occurredAt int64
	closed     *TrackerInfo
	failed     *FailedConnectionInfo
}

type historyEventSource uint8

const (
	historyEventSourceNone historyEventSource = iota
	historyEventSourceJournal
	historyEventSourceFallback
)

type historyPendingAck struct {
	token    uint64
	sequence uint64
	source   historyEventSource
	session  string
}

func (m *Manager) appendHistoryEvent(event historyEvent) {
	m.historyMux.Lock()
	defer m.historyMux.Unlock()
	if !m.historyEnabled.Load() {
		return
	}

	m.eventsMux.Lock()
	defer m.eventsMux.Unlock()

	if event.occurredAt == 0 {
		event.occurredAt = time.Now().UnixMilli()
	}
	if err := m.appendJournalEventLocked(event); err != nil {
		if !m.eventsJournalFailed {
			m.markHistoryJournalFailedLocked("Unable to append connection history event", err)
		}
		m.appendFallbackEventLocked(event)
	}
}

func (m *Manager) PeekHistoryEvents(limit int) *HistoryEvents {
	if !m.historyEnabled.Load() {
		return newHistoryEvents()
	}
	if limit <= 0 {
		return newHistoryEvents()
	}

	m.eventsMux.Lock()
	defer m.eventsMux.Unlock()

	records, source := m.peekHistoryEventRecordsLocked(limit)
	if source == historyEventSourceJournal {
		if err := m.syncHistoryJournalLocked(); err != nil && !m.eventsJournalFailed {
			m.markHistoryJournalFailedLocked("Unable to sync connection history journal", err)
		}
	}
	events := newHistoryEvents()
	for _, event := range records {
		events.AckSequence = event.sequence
		if event.closed != nil {
			events.ClosedConnections = append(events.ClosedConnections, event.closed)
			events.ClosedAt[event.closed.UUID.String()] = event.occurredAt
		}
		if event.failed != nil {
			events.FailedConnections = append(events.FailedConnections, event.failed)
			events.FailedAt[event.failed.UUID.String()] = event.occurredAt
		}
	}
	if events.AckSequence > 0 {
		m.eventsAckToken++
		if m.eventsAckToken == 0 {
			m.eventsAckToken++
		}
		events.AckToken = m.eventsAckToken
		m.eventsPendingAck = &historyPendingAck{
			token:    events.AckToken,
			sequence: events.AckSequence,
			source:   source,
			session:  m.eventsSession,
		}
	}
	return events
}

func newHistoryEvents() *HistoryEvents {
	return &HistoryEvents{
		ClosedConnections: make([]*TrackerInfo, 0),
		ClosedAt:          make(map[string]int64),
		FailedConnections: make([]*FailedConnectionInfo, 0),
		FailedAt:          make(map[string]int64),
	}
}

func (m *Manager) peekHistoryEventRecords(limit int) []historyEvent {
	m.eventsMux.Lock()
	defer m.eventsMux.Unlock()
	records, _ := m.peekHistoryEventRecordsLocked(limit)
	return records
}

func (m *Manager) peekHistoryEventRecordsLocked(limit int) ([]historyEvent, historyEventSource) {
	records, err := m.peekJournalEventsLocked(limit)
	if err == nil {
		return records, historyEventSourceJournal
	}
	if !m.eventsJournalFailed {
		m.markHistoryJournalFailedLocked("Unable to read connection history journal", err)
	}
	count := limit
	if count > len(m.eventsFallback) {
		count = len(m.eventsFallback)
	}
	records = make([]historyEvent, count)
	copy(records, m.eventsFallback[:count])
	return records, historyEventSourceFallback
}

func (m *Manager) AckHistoryEvents(token, sequence uint64) {
	if token == 0 || sequence == 0 {
		return
	}

	m.eventsMux.Lock()
	defer m.eventsMux.Unlock()

	pending := m.eventsPendingAck
	if pending == nil || pending.token != token || pending.sequence != sequence || pending.session != m.eventsSession {
		return
	}
	m.eventsPendingAck = nil

	if pending.source == historyEventSourceJournal {
		if err := m.ackJournalEventsLocked(sequence); err != nil && !m.eventsJournalFailed {
			m.markHistoryJournalFailedLocked("Unable to acknowledge connection history journal", err)
		}
		return
	}
	if pending.source != historyEventSourceFallback {
		return
	}

	firstPending := 0
	for firstPending < len(m.eventsFallback) && m.eventsFallback[firstPending].sequence <= sequence {
		firstPending++
	}
	copy(m.eventsFallback, m.eventsFallback[firstPending:])
	m.eventsFallback = m.eventsFallback[:len(m.eventsFallback)-firstPending]
	if len(m.eventsFallback) == 0 && m.eventsJournalFailed && !m.eventsNeedsClear {
		m.setHistorySessionLocked(m.eventsSession)
	}
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
