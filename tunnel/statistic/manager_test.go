package statistic

import (
	"testing"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/utils"
)

func TestConnectionInfoForCurrentHistorySubtractsBaseline(t *testing.T) {
	manager := &Manager{}
	info := &TrackerInfo{
		UUID:          utils.NewUUIDV4(),
		UploadTotal:   atomic.NewInt64(120),
		DownloadTotal: atomic.NewInt64(340),
	}
	manager.connectionTrafficBaselines.Store(info.UUID.String(), connectionTrafficBaseline{
		upload:   100,
		download: 300,
	})

	normalized := manager.connectionInfoForCurrentHistory(info)
	if normalized == info {
		t.Fatal("baseline connection info must be copied before normalization")
	}
	if normalized.UploadTotal.Load() != 20 || normalized.DownloadTotal.Load() != 40 {
		t.Fatalf("normalized traffic = (%d, %d), want (20, 40)", normalized.UploadTotal.Load(), normalized.DownloadTotal.Load())
	}
	if info.UploadTotal.Load() != 120 || info.DownloadTotal.Load() != 340 {
		t.Fatalf("source traffic was modified: (%d, %d)", info.UploadTotal.Load(), info.DownloadTotal.Load())
	}
}

func TestConnectionInfoForSnapshotPreservesPublicTraffic(t *testing.T) {
	manager := &Manager{}
	info := &TrackerInfo{
		UUID:          utils.NewUUIDV4(),
		UploadTotal:   atomic.NewInt64(120),
		DownloadTotal: atomic.NewInt64(340),
	}
	manager.connectionTrafficBaselines.Store(info.UUID.String(), connectionTrafficBaseline{
		upload:   100,
		download: 300,
	})

	publicInfo := manager.connectionInfoForSnapshot(info, false)
	if publicInfo.UploadTotal.Load() != 120 || publicInfo.DownloadTotal.Load() != 340 {
		t.Fatalf("public traffic = (%d, %d), want (120, 340)", publicInfo.UploadTotal.Load(), publicInfo.DownloadTotal.Load())
	}
	historyInfo := manager.connectionInfoForSnapshot(info, true)
	if historyInfo.UploadTotal.Load() != 20 || historyInfo.DownloadTotal.Load() != 40 {
		t.Fatalf("history traffic = (%d, %d), want (20, 40)", historyInfo.UploadTotal.Load(), historyInfo.DownloadTotal.Load())
	}
}

func TestSnapshotUsesEmptyHistoryLists(t *testing.T) {
	snapshot := (&Manager{}).Snapshot()
	if snapshot.Connections == nil || snapshot.ClosedConnections == nil || snapshot.FailedConnections == nil {
		t.Fatalf("snapshot must use empty lists instead of null: %+v", snapshot)
	}
}
