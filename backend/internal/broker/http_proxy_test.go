package broker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPProxyPublish(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/topics/recap.generation-commands.v1" {
			t.Fatalf("unexpected path %s", request.URL.Path)
		}
		if request.Header.Get("Content-Type") != kafkaJSONContentType {
			t.Fatalf("unexpected content type %q", request.Header.Get("Content-Type"))
		}
		data, _ := io.ReadAll(request.Body)
		if !strings.Contains(string(data), `"key":"request-1"`) {
			t.Fatalf("unexpected payload %s", data)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"offsets":[{"partition":0,"offset":7}]}`))
	}))
	defer server.Close()

	proxy := NewHTTPProxy(server.URL, time.Second)
	if err := proxy.Publish(context.Background(), "recap.generation-commands.v1", "request-1", map[string]string{"id": "request-1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func TestHTTPConsumerLifecycleAndCommitNextOffset(t *testing.T) {
	var commitBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/consumers/workers":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"instance_id":"worker-1","base_uri":"` + "http://" + request.Host + `/consumers/workers/instances/worker-1"}`))
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/subscription"):
			writer.WriteHeader(http.StatusNoContent)
		case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/records"):
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`[{"topic":"commands","key":"request-1","value":{"recap_id":"request-1"},"partition":2,"offset":9}]`))
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/offsets"):
			commitBody, _ = io.ReadAll(request.Body)
			writer.WriteHeader(http.StatusNoContent)
		case request.Method == http.MethodDelete:
			writer.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	proxy := NewHTTPProxy(server.URL, time.Second)
	consumer, err := proxy.NewConsumer(context.Background(), ConsumerConfig{
		Group: "workers", Name: "worker-1", Topics: []string{"commands"},
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	records, err := consumer.Poll(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(records) != 1 || records[0].Offset != 9 {
		t.Fatalf("unexpected records: %+v", records)
	}
	if err := consumer.Commit(context.Background(), records); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var payload struct {
		Partitions []commitPartition `json:"partitions"`
	}
	if err := json.Unmarshal(commitBody, &payload); err != nil {
		t.Fatalf("decode commit: %v", err)
	}
	if len(payload.Partitions) != 1 || payload.Partitions[0].Offset != 10 {
		t.Fatalf("commit must store the next offset: %+v", payload.Partitions)
	}
	if err := consumer.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestHTTPProxyPublishAcceptsZeroBrokerErrorCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Content-Type"); got != kafkaJSONContentType {
			t.Fatalf("unexpected content type %q", got)
		}
		if got := request.Header.Get("Accept"); got != kafkaV2ContentType {
			t.Fatalf("unexpected accept header %q", got)
		}
		writer.Header().Set("Content-Type", kafkaV2ContentType)
		_, _ = writer.Write([]byte(`{"offsets":[{"partition":0,"offset":11,"error_code":0}]}`))
	}))
	defer server.Close()

	proxy := NewHTTPProxy(server.URL, time.Second)
	if err := proxy.Publish(context.Background(), "commands", "request-1", map[string]string{"id": "request-1"}); err != nil {
		t.Fatalf("publish with error_code=0 must succeed: %v", err)
	}
}

func TestHTTPProxyPublishReturnsReadableBrokerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", kafkaV2ContentType)
		_, _ = writer.Write([]byte(`{"offsets":[{"partition":0,"offset":-1,"error_code":3,"error":"unknown topic"}]}`))
	}))
	defer server.Close()

	proxy := NewHTTPProxy(server.URL, time.Second)
	err := proxy.Publish(context.Background(), "commands", "request-1", map[string]string{"id": "request-1"})
	if err == nil {
		t.Fatal("expected broker error")
	}
	if !strings.Contains(err.Error(), "code=3") || !strings.Contains(err.Error(), "unknown topic") {
		t.Fatalf("unexpected broker error: %v", err)
	}
}
