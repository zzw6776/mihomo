package constant

import (
	"strings"

	"github.com/metacubex/mihomo/common/lru"
)

// resolvedDomainIPToDNS tracks the DNS server by both queried domain and answer
// IP. An IP-only key is ambiguous when multiple domains resolve to the same IP.
var resolvedDomainIPToDNS = lru.New[string, string](
	lru.WithSize[string, string](1024),
	lru.WithAge[string, string](1800), // 30 minutes expiration
)

func RecordResolvedDNS(domain, ip, server string) {
	key := resolvedDNSKey(domain, ip)
	if key == "" || server == "" {
		return
	}
	resolvedDomainIPToDNS.Set(key, server)
}

func LookupResolvedDNS(domain, ip string) (string, bool) {
	key := resolvedDNSKey(domain, ip)
	if key == "" {
		return "", false
	}
	return resolvedDomainIPToDNS.Get(key)
}

func resolvedDNSKey(domain, ip string) string {
	domain = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(domain), "."))
	ip = strings.TrimSpace(ip)
	if domain == "" || ip == "" {
		return ""
	}
	return domain + "\x00" + ip
}
