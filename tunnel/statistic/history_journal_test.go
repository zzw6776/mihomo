package statistic

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

func TestHistoryJournalPeekAckAndRecover(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")

	closed := &TrackerInfo{UUID: utils.NewUUIDV4()}
	failed := &FailedConnectionInfo{UUID: utils.NewUUIDV4()}
	manager.appendHistoryEvent(historyEvent{closed: closed, occurredAt: 1234})
	manager.appendHistoryEvent(historyEvent{failed: failed, occurredAt: 2345})

	first := manager.PeekHistoryEvents(1)
	if len(first.ClosedConnections) != 1 || len(first.FailedConnections) != 0 {
		t.Fatalf("unexpected first batch: %+v", first)
	}
	if first.ClosedAt[closed.UUID.String()] != 1234 {
		t.Fatalf("unexpected close time: %+v", first.ClosedAt)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	var shape map[string]any
	if err = json.Unmarshal(encoded, &shape); err != nil {
		t.Fatal(err)
	}
	if shape["closedConnections"] == nil || shape["failedConnections"] == nil || shape["failedAt"] == nil {
		t.Fatalf("history arrays must not be null: %s", encoded)
	}

	manager.AckHistoryEvents(first.AckToken, first.AckSequence)
	manager.eventsMux.Lock()
	if manager.eventsDB != nil {
		_ = manager.eventsDB.Close()
		manager.eventsDB = nil
	}
	manager.eventsMux.Unlock()

	recovered := &Manager{}
	recovered.SetHistoryEnabled(true, "session-a")
	t.Cleanup(func() {
		recovered.eventsMux.Lock()
		defer recovered.eventsMux.Unlock()
		if recovered.eventsDB != nil {
			_ = recovered.eventsDB.Close()
		}
	})
	remaining := recovered.PeekHistoryEvents(10)
	if len(remaining.ClosedConnections) != 0 || len(remaining.FailedConnections) != 1 {
		t.Fatalf("unexpected recovered batch: %+v", remaining)
	}
	if remaining.FailedAt[failed.UUID.String()] != 2345 {
		t.Fatalf("unexpected failure time: %+v", remaining.FailedAt)
	}

	recovered.SetHistoryEnabled(true, "session-b")
	if batch := recovered.PeekHistoryEvents(10); batch.AckSequence != 0 {
		t.Fatalf("new session retained old events: %+v", batch)
	}
	recovered.appendHistoryEvent(historyEvent{failed: &FailedConnectionInfo{UUID: utils.NewUUIDV4()}})
	recovered.eventsMux.Lock()
	_ = recovered.eventsDB.Close()
	recovered.eventsDB = nil
	recovered.eventsMux.Unlock()

	clearer := &Manager{}
	clearer.SetHistoryEnabled(false, "session-b")
	clearer.eventsMux.Lock()
	_ = clearer.eventsDB.Close()
	clearer.eventsDB = nil
	clearer.eventsMux.Unlock()

	verified := &Manager{}
	verified.SetHistoryEnabled(true, "session-b")
	t.Cleanup(func() {
		verified.eventsMux.Lock()
		defer verified.eventsMux.Unlock()
		if verified.eventsDB != nil {
			_ = verified.eventsDB.Close()
		}
	})
	if batch := verified.PeekHistoryEvents(10); batch.AckSequence != 0 {
		t.Fatalf("disabled history recovered old events: %+v", batch)
	}
}

func TestJournalSyncIsDeferredUntilPeek(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")
	t.Cleanup(func() { closeHistoryJournal(manager) })

	manager.appendHistoryEvent(historyEvent{closed: &TrackerInfo{UUID: utils.NewUUIDV4()}})
	if !manager.eventsJournalDirty {
		t.Fatal("journal append was synchronized on the connection path")
	}

	batch := manager.PeekHistoryEvents(1)
	if len(batch.ClosedConnections) != 1 {
		t.Fatalf("unexpected batch: %+v", batch)
	}
	if manager.eventsJournalDirty {
		t.Fatal("journal was not synchronized before delivery")
	}
}

func TestHistoryFallbackIsBounded(t *testing.T) {
	manager := &Manager{
		eventsSession:       "session-a",
		eventsJournalFailed: true,
	}
	manager.historyEnabled.Store(true)

	for index := 0; index < historyFallbackMaxEvents+10; index++ {
		manager.appendHistoryEvent(historyEvent{
			failed: &FailedConnectionInfo{UUID: utils.NewUUIDV4()},
		})
	}

	if len(manager.eventsFallback) != historyFallbackMaxEvents {
		t.Fatalf("fallback size = %d", len(manager.eventsFallback))
	}
	if manager.eventsDropped != 10 {
		t.Fatalf("dropped events = %d", manager.eventsDropped)
	}
}

func TestDisabledHistoryDoesNotAppendAndReturnsEmptyCollections(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")
	manager.SetHistoryEnabled(false, "session-a")
	t.Cleanup(func() { closeHistoryJournal(manager) })

	manager.appendHistoryEvent(historyEvent{
		failed: &FailedConnectionInfo{UUID: utils.NewUUIDV4()},
	})
	if len(manager.eventsFallback) != 0 {
		t.Fatalf("disabled history appended %d events", len(manager.eventsFallback))
	}

	encoded, err := json.Marshal(manager.PeekHistoryEvents(10))
	if err != nil {
		t.Fatal(err)
	}
	var shape map[string]any
	if err = json.Unmarshal(encoded, &shape); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"closedConnections", "closedAt", "failedConnections", "failedAt"} {
		if shape[key] == nil {
			t.Fatalf("%s must not be null: %s", key, encoded)
		}
	}

	manager.SetHistoryEnabled(true, "session-a")
	if batch := manager.PeekHistoryEvents(10); batch.AckSequence != 0 {
		t.Fatalf("re-enabled history retained events recorded while disabled: %+v", batch)
	}
}

func TestJournalRecoveryDoesNotResetProcessTraffic(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")
	t.Cleanup(func() { closeHistoryJournal(manager) })

	traffic := &ProcessTraffic{}
	traffic.UploadTotal.Store(123)
	traffic.DownloadTotal.Store(456)
	manager.processTraffic.Store("example", traffic)
	manager.eventsMux.Lock()
	manager.eventsJournalFailed = true
	manager.eventsMux.Unlock()

	manager.SetHistoryEnabled(true, "session-a")
	stored, ok := manager.processTraffic.Load("example")
	if !ok || stored.UploadTotal.Load() != 123 || stored.DownloadTotal.Load() != 456 {
		t.Fatalf("process traffic was reset during journal recovery: %+v", stored)
	}
}

func TestSessionSwitchDropsFallbackEvents(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{
		eventsSession:       "session-a",
		eventsJournalFailed: true,
	}
	manager.historyEnabled.Store(true)
	manager.appendHistoryEvent(historyEvent{
		failed: &FailedConnectionInfo{UUID: utils.NewUUIDV4()},
	})
	if len(manager.eventsFallback) != 1 {
		t.Fatalf("fallback size = %d", len(manager.eventsFallback))
	}

	manager.SetHistoryEnabled(true, "session-b")
	t.Cleanup(func() { closeHistoryJournal(manager) })
	if batch := manager.PeekHistoryEvents(10); batch.AckSequence != 0 {
		t.Fatalf("new session retained fallback events: %+v", batch)
	}
}

func TestFailedClearIsRetriedBeforeRecovery(t *testing.T) {
	home := t.TempDir()
	C.SetHomeDir(home)
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")
	manager.appendHistoryEvent(historyEvent{
		failed: &FailedConnectionInfo{UUID: utils.NewUUIDV4()},
	})
	closeHistoryJournal(manager)

	badHome := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(badHome, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	C.SetHomeDir(badHome)
	manager.SetHistoryEnabled(false, "session-a")
	if !manager.eventsJournalFailed || !manager.eventsNeedsClear {
		t.Fatalf("failed clear state was lost: failed=%v needsClear=%v", manager.eventsJournalFailed, manager.eventsNeedsClear)
	}

	C.SetHomeDir(home)
	manager.SetHistoryEnabled(true, "session-a")
	t.Cleanup(func() { closeHistoryJournal(manager) })
	if manager.eventsJournalFailed || manager.eventsNeedsClear {
		t.Fatalf("journal did not recover: failed=%v needsClear=%v", manager.eventsJournalFailed, manager.eventsNeedsClear)
	}
	if batch := manager.PeekHistoryEvents(10); batch.AckSequence != 0 {
		t.Fatalf("failed clear recovered stale events: %+v", batch)
	}
}

func TestJournalReadFailureTriggersRecovery(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")
	manager.appendHistoryEvent(historyEvent{
		failed: &FailedConnectionInfo{UUID: utils.NewUUIDV4()},
	})
	closeHistoryJournal(manager)

	if batch := manager.PeekHistoryEvents(10); batch.AckSequence != 0 {
		t.Fatalf("unavailable journal returned events: %+v", batch)
	}
	if !manager.eventsJournalFailed {
		t.Fatal("journal read failure did not mark the journal failed")
	}

	manager.SetHistoryEnabled(true, "session-a")
	t.Cleanup(func() { closeHistoryJournal(manager) })
	if batch := manager.PeekHistoryEvents(10); len(batch.FailedConnections) != 1 {
		t.Fatalf("journal did not recover pending events: %+v", batch)
	}
}

func TestJournalAckFailureTriggersRecovery(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")
	manager.appendHistoryEvent(historyEvent{
		failed: &FailedConnectionInfo{UUID: utils.NewUUIDV4()},
	})
	batch := manager.PeekHistoryEvents(10)
	if batch.AckSequence == 0 {
		t.Fatalf("missing pending event: %+v", batch)
	}
	closeHistoryJournal(manager)

	manager.AckHistoryEvents(batch.AckToken, batch.AckSequence)
	if !manager.eventsJournalFailed {
		t.Fatal("journal ack failure did not mark the journal failed")
	}

	manager.SetHistoryEnabled(true, "session-a")
	t.Cleanup(func() { closeHistoryJournal(manager) })
	replayed := manager.PeekHistoryEvents(10)
	if len(replayed.FailedConnections) != 1 {
		t.Fatalf("journal did not replay the unacknowledged event: %+v", replayed)
	}
	manager.AckHistoryEvents(replayed.AckToken, replayed.AckSequence)
	if remaining := manager.PeekHistoryEvents(10); remaining.AckSequence != 0 {
		t.Fatalf("acknowledged event remained in journal: %+v", remaining)
	}
}

func TestCorruptedJournalEventCanBeAcknowledged(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")
	t.Cleanup(func() { closeHistoryJournal(manager) })

	manager.eventsMux.Lock()
	err := manager.eventsDB.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(historyEventsBucket)
		sequence, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		manager.eventsSequence = sequence
		return bucket.Put(historySequenceKey(sequence), []byte("not-json"))
	})
	manager.eventsMux.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	batch := manager.PeekHistoryEvents(10)
	if batch.AckSequence == 0 || len(batch.ClosedConnections) != 0 || len(batch.FailedConnections) != 0 {
		t.Fatalf("unexpected corrupted event batch: %+v", batch)
	}
	if manager.eventsJournalFailed {
		t.Fatal("a corrupted event incorrectly disabled the journal")
	}
	manager.AckHistoryEvents(batch.AckToken, batch.AckSequence)
	if remaining := manager.PeekHistoryEvents(10); remaining.AckSequence != 0 {
		t.Fatalf("corrupted event was not removed: %+v", remaining)
	}
}

func TestCorruptedJournalKeyDoesNotPanicDuringAck(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")
	manager.appendHistoryEvent(historyEvent{
		failed: &FailedConnectionInfo{UUID: utils.NewUUIDV4()},
	})
	batch := manager.PeekHistoryEvents(10)
	if batch.AckSequence == 0 {
		t.Fatalf("missing pending event: %+v", batch)
	}

	manager.eventsMux.Lock()
	err := manager.eventsDB.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(historyEventsBucket).Put([]byte{0}, []byte("invalid-key"))
	})
	manager.eventsMux.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	manager.AckHistoryEvents(batch.AckToken, batch.AckSequence)
	if !manager.eventsJournalFailed {
		t.Fatal("invalid journal key did not mark the journal unavailable")
	}
}

func TestFallbackBatchBlocksRecoveryUntilAcknowledged(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")
	journalEvent := &FailedConnectionInfo{UUID: utils.NewUUIDV4()}
	manager.appendHistoryEvent(historyEvent{failed: journalEvent})
	closeHistoryJournal(manager)

	fallbackEvent := &FailedConnectionInfo{UUID: utils.NewUUIDV4()}
	manager.appendHistoryEvent(historyEvent{failed: fallbackEvent})
	fallbackBatch := manager.PeekHistoryEvents(10)
	if len(fallbackBatch.FailedConnections) != 1 || fallbackBatch.FailedConnections[0].UUID != fallbackEvent.UUID {
		t.Fatalf("unexpected fallback batch: %+v", fallbackBatch)
	}

	manager.SetHistoryEnabled(true, "session-a")
	if !manager.eventsJournalFailed || manager.eventsDB != nil {
		t.Fatal("journal recovered before the fallback batch was acknowledged")
	}
	manager.AckHistoryEvents(fallbackBatch.AckToken, fallbackBatch.AckSequence)
	t.Cleanup(func() { closeHistoryJournal(manager) })

	journalBatch := manager.PeekHistoryEvents(10)
	if len(journalBatch.FailedConnections) != 1 || journalBatch.FailedConnections[0].UUID != journalEvent.UUID {
		t.Fatalf("unacknowledged journal event was lost: %+v", journalBatch)
	}
}

func TestAckTokenIsInvalidatedBySessionSwitch(t *testing.T) {
	C.SetHomeDir(t.TempDir())
	manager := &Manager{}
	manager.SetHistoryEnabled(true, "session-a")
	manager.appendHistoryEvent(historyEvent{
		failed: &FailedConnectionInfo{UUID: utils.NewUUIDV4()},
	})
	oldBatch := manager.PeekHistoryEvents(10)

	manager.SetHistoryEnabled(true, "session-b")
	currentEvent := &FailedConnectionInfo{UUID: utils.NewUUIDV4()}
	manager.appendHistoryEvent(historyEvent{failed: currentEvent})
	manager.AckHistoryEvents(oldBatch.AckToken, oldBatch.AckSequence)
	t.Cleanup(func() { closeHistoryJournal(manager) })

	currentBatch := manager.PeekHistoryEvents(10)
	if len(currentBatch.FailedConnections) != 1 || currentBatch.FailedConnections[0].UUID != currentEvent.UUID {
		t.Fatalf("stale ack removed the current session event: %+v", currentBatch)
	}
}

func closeHistoryJournal(manager *Manager) {
	manager.eventsMux.Lock()
	defer manager.eventsMux.Unlock()
	if manager.eventsDB != nil {
		_ = manager.eventsDB.Close()
		manager.eventsDB = nil
	}
}
