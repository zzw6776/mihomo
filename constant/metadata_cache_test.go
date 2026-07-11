package constant

import "testing"

func TestResolvedDNSIsScopedByDomainAndIP(t *testing.T) {
	ip := "203.0.113.10"
	RecordResolvedDNS("first.example", ip, "dns-one")
	RecordResolvedDNS("second.example", ip, "dns-two")

	if server, ok := LookupResolvedDNS("FIRST.EXAMPLE.", ip); !ok || server != "dns-one" {
		t.Fatalf("first domain lookup = %q, %v", server, ok)
	}
	if server, ok := LookupResolvedDNS("second.example", ip); !ok || server != "dns-two" {
		t.Fatalf("second domain lookup = %q, %v", server, ok)
	}
	if _, ok := LookupResolvedDNS("", ip); ok {
		t.Fatal("an IP-only lookup must not claim a DNS server")
	}
}
