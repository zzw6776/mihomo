package statistic

import (
	"io"
	"net"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/mmdb"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/gofrs/uuid/v5"
)

const enableRuleTrace = false
const maxFailedConnections = 1000

type Tracker interface {
	ID() string
	Close() error
	Info() *TrackerInfo
	C.Connection
}

type TrackerInfo struct {
	UUID          uuid.UUID    `json:"id"`
	Metadata      *C.Metadata  `json:"metadata"`
	UploadTotal   atomic.Int64 `json:"upload"`
	DownloadTotal atomic.Int64 `json:"download"`
	Start         time.Time    `json:"start"`
	Chain         C.Chain      `json:"chains"`
	ProviderChain C.Chain      `json:"providerChains"`
	Rule          string       `json:"rule"`
	RulePayload   string       `json:"rulePayload"`
	DNSServer     string       `json:"dnsServer,omitempty"`
}

type FailedConnectionInfo struct {
	UUID          uuid.UUID   `json:"id"`
	Metadata      *C.Metadata `json:"metadata"`
	FailedAt      time.Time   `json:"failedAt"`
	Chain         C.Chain     `json:"chains"`
	ProviderChain C.Chain     `json:"providerChains"`
	Rule          string      `json:"rule"`
	RulePayload   string      `json:"rulePayload"`
	Proxy         string      `json:"proxy"`
	Error         string      `json:"error"`
}

func (m *Manager) RecordFailedConnection(metadata *C.Metadata, rule C.Rule, proxy C.ProxyAdapter, err error) {
	if !m.HistoryEnabled() {
		return
	}
	if metadata == nil || err == nil {
		return
	}

	proxyName := ""
	chain := C.Chain{}
	if proxy != nil {
		proxyName = proxy.Name()
		chain = C.Chain{proxyName}
	}

	info := &FailedConnectionInfo{
		UUID:     utils.NewUUIDV4(),
		Metadata: metadata.Clone(),
		FailedAt: time.Now(),
		Chain:    chain,
		Proxy:    proxyName,
		Error:    err.Error(),
	}
	if rule != nil {
		info.Rule = rule.RuleType().String()
		info.RulePayload = rule.Payload()
	}

	m.failedMux.Lock()
	defer m.failedMux.Unlock()

	m.failed = append(m.failed, info)
	if len(m.failed) > maxFailedConnections {
		copy(m.failed, m.failed[len(m.failed)-maxFailedConnections:])
		m.failed = m.failed[:maxFailedConnections]
	}
}

type tcpTracker struct {
	C.Conn `json:"-"`
	*TrackerInfo
	manager *Manager

	pushToManager bool `json:"-"`
}

func (tt *tcpTracker) ID() string {
	return tt.UUID.String()
}

func (tt *tcpTracker) Info() *TrackerInfo {
	if tt.TrackerInfo.Metadata.DstIP.IsValid() {
		if server, ok := C.ResolvedIPToDNS.Get(tt.TrackerInfo.Metadata.DstIP.String()); ok {
			tt.TrackerInfo.DNSServer = server
		}
		if len(tt.TrackerInfo.Metadata.DstGeoIP) == 0 {
			tt.TrackerInfo.Metadata.DstGeoIP = mmdb.IPInstance().LookupCode(tt.TrackerInfo.Metadata.DstIP.AsSlice())
		}
	}
	return tt.TrackerInfo
}

func (tt *tcpTracker) Read(b []byte) (int, error) {
	n, err := tt.Conn.Read(b)
	download := int64(n)
	if tt.pushToManager {
		tt.manager.PushDownloadedForProcess(tt.Metadata.Process, download)
	}
	tt.DownloadTotal.Add(download)
	return n, err
}

func (tt *tcpTracker) ReadBuffer(buffer *buf.Buffer) (err error) {
	err = tt.Conn.ReadBuffer(buffer)
	download := int64(buffer.Len())
	if tt.pushToManager {
		tt.manager.PushDownloadedForProcess(tt.Metadata.Process, download)
	}
	tt.DownloadTotal.Add(download)
	return
}

func (tt *tcpTracker) UnwrapReader() (io.Reader, []N.CountFunc) {
	return tt.Conn, []N.CountFunc{func(download int64) {
		if tt.pushToManager {
			tt.manager.PushDownloadedForProcess(tt.Metadata.Process, download)
		}
		tt.DownloadTotal.Add(download)
	}}
}

func (tt *tcpTracker) Write(b []byte) (int, error) {
	n, err := tt.Conn.Write(b)
	upload := int64(n)
	if tt.pushToManager {
		tt.manager.PushUploadedForProcess(tt.Metadata.Process, upload)
	}
	tt.UploadTotal.Add(upload)
	return n, err
}

func (tt *tcpTracker) WriteBuffer(buffer *buf.Buffer) (err error) {
	upload := int64(buffer.Len())
	err = tt.Conn.WriteBuffer(buffer)
	if tt.pushToManager {
		tt.manager.PushUploadedForProcess(tt.Metadata.Process, upload)
	}
	tt.UploadTotal.Add(upload)
	return
}

func (tt *tcpTracker) UnwrapWriter() (io.Writer, []N.CountFunc) {
	return tt.Conn, []N.CountFunc{func(upload int64) {
		if tt.pushToManager {
			tt.manager.PushUploadedForProcess(tt.Metadata.Process, upload)
		}
		tt.UploadTotal.Add(upload)
	}}
}

func (tt *tcpTracker) Close() error {
	tt.manager.Leave(tt)
	return tt.Conn.Close()
}

func (tt *tcpTracker) Upstream() any {
	return tt.Conn
}

func NewTCPTracker(conn C.Conn, manager *Manager, metadata *C.Metadata, rule C.Rule, uploadTotal int64, downloadTotal int64, pushToManager bool) *tcpTracker {
	metadata.RemoteDst = conn.RemoteDestination()

	t := &tcpTracker{
		Conn:    conn,
		manager: manager,
		TrackerInfo: &TrackerInfo{
			UUID:          utils.NewUUIDV4(),
			Start:         time.Now(),
			Metadata:      metadata,
			Chain:         conn.Chains(),
			ProviderChain: conn.ProviderChains(),
			Rule:          "",
			UploadTotal:   atomic.NewInt64(uploadTotal),
			DownloadTotal: atomic.NewInt64(downloadTotal),
		},
		pushToManager: pushToManager,
	}

	if pushToManager {
		if uploadTotal > 0 {
			manager.PushUploadedForProcess(metadata.Process, uploadTotal)
		}
		if downloadTotal > 0 {
			manager.PushDownloadedForProcess(metadata.Process, downloadTotal)
		}
	}

	if rule != nil {
		t.TrackerInfo.Rule = rule.RuleType().String()
		t.TrackerInfo.RulePayload = rule.Payload()
	}

	if enableRuleTrace {
		log.Debugln("[RuleTrace] tracker tcp id=%s host=%q dstIP=%s chain=%s rule=%q payload=%q specialProxy=%q specialRules=%q type=%s push=%t",
			t.ID(), metadata.Host, metadata.DstIP.String(), t.TrackerInfo.Chain.String(), t.TrackerInfo.Rule,
			t.TrackerInfo.RulePayload, metadata.SpecialProxy, metadata.SpecialRules, metadata.Type.String(), pushToManager)
	}

	manager.Join(t)
	return t
}

type udpTracker struct {
	C.PacketConn `json:"-"`
	*TrackerInfo
	manager *Manager

	pushToManager bool `json:"-"`
}

func (ut *udpTracker) ID() string {
	return ut.UUID.String()
}

func (ut *udpTracker) Info() *TrackerInfo {
	if ut.TrackerInfo.Metadata.DstIP.IsValid() {
		if server, ok := C.ResolvedIPToDNS.Get(ut.TrackerInfo.Metadata.DstIP.String()); ok {
			ut.TrackerInfo.DNSServer = server
		}
		if len(ut.TrackerInfo.Metadata.DstGeoIP) == 0 {
			ut.TrackerInfo.Metadata.DstGeoIP = mmdb.IPInstance().LookupCode(ut.TrackerInfo.Metadata.DstIP.AsSlice())
		}
	}
	return ut.TrackerInfo
}

func (ut *udpTracker) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := ut.PacketConn.ReadFrom(b)
	download := int64(n)
	if ut.pushToManager {
		ut.manager.PushDownloadedForProcess(ut.Metadata.Process, download)
	}
	ut.DownloadTotal.Add(download)
	return n, addr, err
}

func (ut *udpTracker) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	data, put, addr, err = ut.PacketConn.WaitReadFrom()
	download := int64(len(data))
	if ut.pushToManager {
		ut.manager.PushDownloadedForProcess(ut.Metadata.Process, download)
	}
	ut.DownloadTotal.Add(download)
	return
}

func (ut *udpTracker) WriteTo(b []byte, addr net.Addr) (int, error) {
	n, err := ut.PacketConn.WriteTo(b, addr)
	upload := int64(n)
	if ut.pushToManager {
		ut.manager.PushUploadedForProcess(ut.Metadata.Process, upload)
	}
	ut.UploadTotal.Add(upload)
	return n, err
}

func (ut *udpTracker) Close() error {
	ut.manager.Leave(ut)
	return ut.PacketConn.Close()
}

func (ut *udpTracker) Upstream() any {
	return ut.PacketConn
}

func NewUDPTracker(conn C.PacketConn, manager *Manager, metadata *C.Metadata, rule C.Rule, uploadTotal int64, downloadTotal int64, pushToManager bool) *udpTracker {
	metadata.RemoteDst = conn.RemoteDestination()

	ut := &udpTracker{
		PacketConn: conn,
		manager:    manager,
		TrackerInfo: &TrackerInfo{
			UUID:          utils.NewUUIDV4(),
			Start:         time.Now(),
			Metadata:      metadata,
			Chain:         conn.Chains(),
			ProviderChain: conn.ProviderChains(),
			Rule:          "",
			UploadTotal:   atomic.NewInt64(uploadTotal),
			DownloadTotal: atomic.NewInt64(downloadTotal),
		},
		pushToManager: pushToManager,
	}

	if pushToManager {
		if uploadTotal > 0 {
			manager.PushUploadedForProcess(metadata.Process, uploadTotal)
		}
		if downloadTotal > 0 {
			manager.PushDownloadedForProcess(metadata.Process, downloadTotal)
		}
	}

	if rule != nil {
		ut.TrackerInfo.Rule = rule.RuleType().String()
		ut.TrackerInfo.RulePayload = rule.Payload()
	}

	if enableRuleTrace {
		log.Debugln("[RuleTrace] tracker udp id=%s host=%q dstIP=%s chain=%s rule=%q payload=%q specialProxy=%q specialRules=%q type=%s push=%t",
			ut.ID(), metadata.Host, metadata.DstIP.String(), ut.TrackerInfo.Chain.String(), ut.TrackerInfo.Rule,
			ut.TrackerInfo.RulePayload, metadata.SpecialProxy, metadata.SpecialRules, metadata.Type.String(), pushToManager)
	}

	manager.Join(ut)
	return ut
}
