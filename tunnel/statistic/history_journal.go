package statistic

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/metacubex/bbolt"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

const (
	historyJournalFile       = "connection-history-events.db"
	historyFallbackMaxEvents = 5000
)

var (
	historyEventsBucket = []byte("events")
	historyMetaBucket   = []byte("meta")
	historySessionKey   = []byte("session")
)

type persistedHistoryEvent struct {
	Sequence   uint64                `json:"sequence"`
	OccurredAt int64                 `json:"occurredAt"`
	Closed     *TrackerInfo          `json:"closed,omitempty"`
	RawClosed  *TrackerInfo          `json:"rawClosed,omitempty"`
	Failed     *FailedConnectionInfo `json:"failed,omitempty"`
}

func (m *Manager) setHistorySessionLocked(session string) {
	if session == "" {
		return
	}
	if m.eventsSession != session {
		m.invalidatePendingHistoryAckLocked()
		m.eventsSession = session
		m.eventsSequence = 0
		m.eventsFallback = nil
		m.eventsDropped = 0
	} else if m.eventsJournalFailed && !m.eventsNeedsClear && m.eventsPendingAck != nil &&
		m.eventsPendingAck.source == historyEventSourceFallback {
		return
	}
	if err := m.ensureHistoryJournalLocked(); err != nil {
		m.markHistoryJournalFailedLocked("Unable to open connection history journal", err)
		return
	}

	err := m.eventsDB.Update(func(tx *bbolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(historyMetaBucket)
		if err != nil {
			return err
		}
		events, err := tx.CreateBucketIfNotExists(historyEventsBucket)
		if err != nil {
			return err
		}
		storedSession := string(meta.Get(historySessionKey))
		if m.eventsNeedsClear || (storedSession != "" && storedSession != session) {
			if err = tx.DeleteBucket(historyEventsBucket); err != nil {
				return err
			}
			if events, err = tx.CreateBucket(historyEventsBucket); err != nil {
				return err
			}
		}
		if err = meta.Put(historySessionKey, []byte(session)); err != nil {
			return err
		}
		m.eventsSequence = events.Sequence()
		return nil
	})
	if err != nil {
		m.markHistoryJournalFailedLocked("Unable to initialize connection history journal", err)
		return
	}
	m.eventsJournalDirty = true
	if err = m.syncHistoryJournalLocked(); err != nil {
		m.markHistoryJournalFailedLocked("Unable to sync connection history journal session", err)
		return
	}

	m.eventsJournalFailed = false
	m.eventsNeedsClear = false
	if len(m.eventsFallback) > 0 {
		pending := m.eventsFallback
		m.eventsFallback = nil
		for index, event := range pending {
			if err = m.appendJournalEventLocked(event); err != nil {
				m.eventsFallback = append(m.eventsFallback, pending[index:]...)
				m.markHistoryJournalFailedLocked("Unable to migrate in-memory connection history to journal", err)
				break
			}
		}
	}
}

func (m *Manager) ensureHistoryJournalLocked() error {
	if m.eventsDB != nil {
		return nil
	}
	home := C.Path.HomeDir()
	if home == "" {
		return errors.New("home directory is not initialized")
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return err
	}
	db, err := bbolt.Open(
		filepath.Join(home, historyJournalFile),
		0o600,
		&bbolt.Options{Timeout: time.Second, NoSync: true},
	)
	if err != nil {
		return err
	}
	m.eventsDB = db
	return nil
}

func (m *Manager) markHistoryJournalFailedLocked(message string, err error) {
	m.eventsJournalFailed = true
	if m.eventsDB != nil {
		_ = m.eventsDB.Close()
		m.eventsDB = nil
	}
	m.eventsJournalDirty = false
	log.Warnln("%s: %v", message, err)
}

func (m *Manager) syncHistoryJournalLocked() error {
	if m.eventsDB == nil || !m.eventsJournalDirty {
		return nil
	}
	if err := m.eventsDB.Sync(); err != nil {
		return err
	}
	m.eventsJournalDirty = false
	return nil
}

func (m *Manager) invalidatePendingHistoryAckLocked() {
	m.eventsPendingAck = nil
}

func (m *Manager) appendJournalEventLocked(event historyEvent) error {
	if m.eventsDB == nil || m.eventsSession == "" || m.eventsJournalFailed {
		return errors.New("history journal is unavailable")
	}
	err := m.eventsDB.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(historyEventsBucket)
		if bucket == nil {
			return errors.New("history events bucket is missing")
		}
		sequence, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		event.sequence = sequence
		value, err := json.Marshal(persistedHistoryEvent{
			Sequence:   sequence,
			OccurredAt: event.occurredAt,
			Closed:     event.closed,
			RawClosed:  event.rawClosed,
			Failed:     event.failed,
		})
		if err != nil {
			return err
		}
		m.eventsSequence = sequence
		return bucket.Put(historySequenceKey(sequence), value)
	})
	if err == nil {
		m.eventsJournalDirty = true
	}
	return err
}

func (m *Manager) appendFallbackEventLocked(event historyEvent) {
	m.eventsSequence++
	event.sequence = m.eventsSequence
	if len(m.eventsFallback) >= historyFallbackMaxEvents {
		copy(m.eventsFallback, m.eventsFallback[1:])
		m.eventsFallback = m.eventsFallback[:historyFallbackMaxEvents-1]
		m.eventsDropped++
		if m.eventsDropped == 1 {
			log.Errorln("Connection history journal unavailable; dropping oldest events after %d entries", historyFallbackMaxEvents)
		}
	}
	m.eventsFallback = append(m.eventsFallback, event)
}

func (m *Manager) peekJournalEventsLocked(limit int) ([]historyEvent, error) {
	if m.eventsDB == nil || m.eventsSession == "" || m.eventsJournalFailed {
		return nil, errors.New("history journal is unavailable")
	}
	events := make([]historyEvent, 0, limit)
	err := m.eventsDB.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(historyEventsBucket)
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil && len(events) < limit; key, value = cursor.Next() {
			if len(key) != 8 {
				return errors.New("invalid history event sequence key")
			}
			sequence := binary.BigEndian.Uint64(key)
			var persisted persistedHistoryEvent
			if err := json.Unmarshal(value, &persisted); err != nil {
				log.Warnln("Skipping corrupted connection history event %d: %v", sequence, err)
				events = append(events, historyEvent{sequence: sequence})
				continue
			}
			events = append(events, historyEvent{
				sequence:   sequence,
				occurredAt: persisted.OccurredAt,
				closed:     persisted.Closed,
				rawClosed:  persisted.RawClosed,
				failed:     persisted.Failed,
			})
		}
		return nil
	})
	return events, err
}

func (m *Manager) ackJournalEventsLocked(sequence uint64) error {
	if m.eventsDB == nil || m.eventsSession == "" || m.eventsJournalFailed {
		return errors.New("history journal is unavailable")
	}
	err := m.eventsDB.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(historyEventsBucket)
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			if binary.BigEndian.Uint64(key) > sequence {
				break
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		m.eventsJournalDirty = true
	}
	return err
}

func (m *Manager) clearJournalLocked() error {
	m.invalidatePendingHistoryAckLocked()
	m.eventsFallback = nil
	m.eventsDropped = 0

	if err := m.ensureHistoryJournalLocked(); err != nil {
		m.eventsNeedsClear = true
		m.markHistoryJournalFailedLocked("Unable to open connection history journal for clearing", err)
		return err
	}
	err := m.eventsDB.Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(historyEventsBucket) != nil {
			if err := tx.DeleteBucket(historyEventsBucket); err != nil {
				return err
			}
		}
		_, err := tx.CreateBucket(historyEventsBucket)
		return err
	})
	if err != nil {
		m.eventsNeedsClear = true
		m.markHistoryJournalFailedLocked("Unable to clear connection history journal", err)
		return err
	}
	m.eventsJournalDirty = true
	if err = m.syncHistoryJournalLocked(); err != nil {
		m.eventsNeedsClear = true
		m.markHistoryJournalFailedLocked("Unable to sync cleared connection history journal", err)
		return err
	}

	m.eventsSequence = 0
	m.eventsJournalFailed = false
	m.eventsNeedsClear = false
	return nil
}

func historySequenceKey(sequence uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, sequence)
	return key
}
