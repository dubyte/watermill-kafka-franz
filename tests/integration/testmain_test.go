//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	redpandaAddr    = "127.0.0.1:9092"
	redpandaInternal = "redpanda:9092" // Redpanda's address as seen from inside Docker
	toxiproxyAPI    = "http://127.0.0.1:8474"
	toxiproxyKafka  = "127.0.0.1:19092" // proxy port exposed by Toxiproxy for kafka
)

func TestMain(m *testing.M) {
	if err := waitForBroker(redpandaAddr, 30*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "Redpanda not ready at %s: %v\n", redpandaAddr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func waitForBroker(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

// toxiproxyAvailable returns true if the Toxiproxy control API is reachable.
func toxiproxyAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, toxiproxyAPI+"/proxies", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// createToxiproxy creates or resets a named proxy pointing at the Kafka broker.
// Returns the local listen address (host:port).
func createToxiproxy(t *testing.T, name, listen, upstream string) {
	t.Helper()
	// Delete if exists
	req, _ := http.NewRequest(http.MethodDelete, toxiproxyAPI+"/proxies/"+name, nil)
	resp, _ := http.DefaultClient.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	// Create
	body := fmt.Sprintf(`{"name":%q,"listen":%q,"upstream":%q,"enabled":true}`, name, listen, upstream)
	req, _ = http.NewRequest(http.MethodPost, toxiproxyAPI+"/proxies", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("createToxiproxy %s: %v", name, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("createToxiproxy %s: status %d", name, resp.StatusCode)
	}
	t.Cleanup(func() {
		req, _ := http.NewRequest(http.MethodDelete, toxiproxyAPI+"/proxies/"+name, nil)
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
	})
}

// addToxic adds a toxic to a named proxy. toxicType e.g. "timeout", "latency", "reset_peer".
func addToxic(t *testing.T, proxyName, toxicName, toxicType, stream string, attribs map[string]interface{}) {
	t.Helper()
	attrJSON := "{"
	first := true
	for k, v := range attribs {
		if !first {
			attrJSON += ","
		}
		switch val := v.(type) {
		case int:
			attrJSON += fmt.Sprintf("%q:%d", k, val)
		case float64:
			attrJSON += fmt.Sprintf("%q:%f", k, val)
		default:
			attrJSON += fmt.Sprintf("%q:%v", k, val)
		}
		first = false
	}
	attrJSON += "}"
	body := fmt.Sprintf(`{"name":%q,"type":%q,"stream":%q,"toxicity":1.0,"attributes":%s}`,
		toxicName, toxicType, stream, attrJSON)
	req, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/proxies/%s/toxics", toxiproxyAPI, proxyName),
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("addToxic %s/%s: %v", proxyName, toxicName, err)
	}
	resp.Body.Close()
}

// removeToxic removes a toxic from a named proxy.
func removeToxic(t *testing.T, proxyName, toxicName string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/proxies/%s/toxics/%s", toxiproxyAPI, proxyName, toxicName),
		nil)
	resp, _ := http.DefaultClient.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
}
