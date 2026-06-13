package constant

import (
	"github.com/metacubex/mihomo/common/lru"
)

// ResolvedIPToDNS maps resolved IP string to the DNS Server string that resolved it.
// This is used to track which DNS server was responsible for an IP connection.
var ResolvedIPToDNS = lru.New[string, string](
	lru.WithSize[string, string](1024),
	lru.WithAge[string, string](1800), // 30 minutes expiration
)
