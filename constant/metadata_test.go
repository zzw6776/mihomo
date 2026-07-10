package constant

import (
	"encoding/json"
	"testing"
)

func TestMetadataEnumJSONRoundTrip(t *testing.T) {
	var metadata Metadata
	if err := json.Unmarshal([]byte(`{"network":"tcp","type":"Tun"}`), &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.NetWork != TCP || metadata.Type != TUN {
		t.Fatalf("metadata enums = (%s, %s), want (tcp, Tun)", metadata.NetWork, metadata.Type)
	}
}

func TestInvalidNetworkJSONRoundTrip(t *testing.T) {
	encoded, err := json.Marshal(NetWork(InvalidNet))
	if err != nil {
		t.Fatal(err)
	}
	var network NetWork
	if err = json.Unmarshal(encoded, &network); err != nil {
		t.Fatal(err)
	}
	if network != InvalidNet {
		t.Fatalf("network = %s, want invalid", network)
	}
}
