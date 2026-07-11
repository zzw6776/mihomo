package resource

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
)

func TestHTTPVehicleRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("oversized"))
	}))
	defer server.Close()

	vehicle := NewHTTPVehicle(server.URL, "", "", nil, time.Second, 4)
	content, _, err := vehicle.Read(context.Background(), utils.HashType{})
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("oversized response result = %q, %v", content, err)
	}
}
