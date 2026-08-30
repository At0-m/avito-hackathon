package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	kafkaJSONContentType = "application/vnd.kafka.json.v2+json"
	kafkaV2ContentType   = "application/vnd.kafka.v2+json"
)

type HTTPProxy struct {
	BaseURL string
	Client  *http.Client
}

func NewHTTPProxy(baseURL string, timeout time.Duration) *HTTPProxy {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &HTTPProxy{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Client:  &http.Client{Timeout: timeout},
	}
}

func (p *HTTPProxy) Health(ctx context.Context) error {
	if strings.TrimSpace(p.BaseURL) == "" {
		return errors.New("redpanda HTTP proxy URL is empty")
	}
	var response struct {
		Brokers []int `json:"brokers"`
	}
	if err := p.doJSON(ctx, http.MethodGet, p.BaseURL+"/brokers", "", nil, &response); err != nil {
		return err
	}
	if len(response.Brokers) == 0 {
		return errors.New("redpanda HTTP proxy returned no brokers")
	}
	return nil
}

type produceRequest struct {
	Records []produceRecord `json:"records"`
}

type produceRecord struct {
	Key   string      `json:"key,omitempty"`
	Value interface{} `json:"value"`
}

type produceResponse struct {
	Offsets []struct {
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
		ErrorCode *int   `json:"error_code,omitempty"`
		Error     string `json:"error,omitempty"`
	} `json:"offsets"`
}

func (p *HTTPProxy) Publish(ctx context.Context, topic, key string, value interface{}) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("broker topic is empty")
	}
	endpoint := p.BaseURL + "/topics/" + url.PathEscape(topic)
	payload := produceRequest{Records: []produceRecord{{Key: key, Value: value}}}
	var response produceResponse
	if err := p.doJSONWithAccept(ctx, http.MethodPost, endpoint, kafkaJSONContentType, kafkaV2ContentType, payload, &response); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	if len(response.Offsets) != 1 {
		return fmt.Errorf("publish to %s: expected one offset, got %d", topic, len(response.Offsets))
	}
	if item := response.Offsets[0]; (item.ErrorCode != nil && *item.ErrorCode != 0) || item.Error != "" {
		code := 0
		if item.ErrorCode != nil {
			code = *item.ErrorCode
		}
		if item.Error != "" {
			return fmt.Errorf("publish to %s: broker error code=%d: %s", topic, code, item.Error)
		}
		return fmt.Errorf("publish to %s: broker error code=%d", topic, code)
	}
	return nil
}

type ConsumerConfig struct {
	Group             string
	Name              string
	Topics            []string
	AutoOffsetReset   string
	RequestTimeout    time.Duration
	FetchMinimumBytes int
}

type HTTPConsumer struct {
	proxy    *HTTPProxy
	group    string
	name     string
	baseURI  string
	closed   bool
	maxBytes int
}

type consumerCreateRequest struct {
	Name                   string `json:"name"`
	Format                 string `json:"format"`
	AutoOffsetReset        string `json:"auto.offset.reset"`
	AutoCommitEnable       string `json:"auto.commit.enable"`
	FetchMinimumBytes      string `json:"fetch.min.bytes"`
	ConsumerRequestTimeout string `json:"consumer.request.timeout.ms"`
}

type consumerCreateResponse struct {
	InstanceID string `json:"instance_id"`
	BaseURI    string `json:"base_uri"`
}

func (p *HTTPProxy) NewConsumer(ctx context.Context, config ConsumerConfig) (*HTTPConsumer, error) {
	if strings.TrimSpace(config.Group) == "" || strings.TrimSpace(config.Name) == "" {
		return nil, errors.New("consumer group and name are required")
	}
	if len(config.Topics) == 0 {
		return nil, errors.New("at least one consumer topic is required")
	}
	if config.AutoOffsetReset == "" {
		config.AutoOffsetReset = "earliest"
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 10 * time.Second
	}
	if config.FetchMinimumBytes <= 0 {
		config.FetchMinimumBytes = 1
	}

	create := consumerCreateRequest{
		Name:                   config.Name,
		Format:                 "json",
		AutoOffsetReset:        config.AutoOffsetReset,
		AutoCommitEnable:       "false",
		FetchMinimumBytes:      strconv.Itoa(config.FetchMinimumBytes),
		ConsumerRequestTimeout: strconv.FormatInt(config.RequestTimeout.Milliseconds(), 10),
	}
	var response consumerCreateResponse
	endpoint := p.BaseURL + "/consumers/" + url.PathEscape(config.Group)
	if err := p.doJSON(ctx, http.MethodPost, endpoint, kafkaV2ContentType, create, &response); err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	instanceID := response.InstanceID
	if instanceID == "" {
		instanceID = config.Name
	}

	baseURI := endpoint + "/instances/" + url.PathEscape(instanceID)

	consumer := &HTTPConsumer{
		proxy:    p,
		group:    config.Group,
		name:     instanceID,
		baseURI:  strings.TrimRight(baseURI, "/"),
		maxBytes: 1 << 20,
	}
	if err := consumer.subscribe(ctx, config.Topics); err != nil {
		_ = consumer.Close(context.Background())
		return nil, err
	}
	return consumer, nil
}

func (c *HTTPConsumer) subscribe(ctx context.Context, topics []string) error {
	payload := struct {
		Topics []string `json:"topics"`
	}{Topics: topics}
	if err := c.proxy.doJSON(ctx, http.MethodPost, c.baseURI+"/subscription", kafkaV2ContentType, payload, nil); err != nil {
		return fmt.Errorf("subscribe consumer: %w", err)
	}
	return nil
}

type Record struct {
	Topic     string          `json:"topic"`
	Key       json.RawMessage `json:"key"`
	Value     json.RawMessage `json:"value"`
	Partition int             `json:"partition"`
	Offset    int64           `json:"offset"`
}

func (c *HTTPConsumer) Poll(ctx context.Context, timeout time.Duration) ([]Record, error) {
	if c.closed {
		return nil, errors.New("consumer is closed")
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	endpoint := c.baseURI + "/records?timeout=" + strconv.FormatInt(timeout.Milliseconds(), 10) +
		"&max_bytes=" + strconv.Itoa(c.maxBytes)
	var records []Record
	if err := c.proxy.doJSON(ctx, http.MethodGet, endpoint, kafkaJSONContentType, nil, &records); err != nil {
		return nil, fmt.Errorf("poll consumer: %w", err)
	}
	return records, nil
}

type commitPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

func (c *HTTPConsumer) Commit(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	type key struct {
		topic     string
		partition int
	}
	latest := make(map[key]int64)
	for _, record := range records {
		item := key{topic: record.Topic, partition: record.Partition}
		next := record.Offset + 1
		if current, ok := latest[item]; !ok || next > current {
			latest[item] = next
		}
	}
	partitions := make([]commitPartition, 0, len(latest))
	for item, offset := range latest {
		partitions = append(partitions, commitPartition{
			Topic: item.topic, Partition: item.partition, Offset: offset,
		})
	}
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic == partitions[j].Topic {
			return partitions[i].Partition < partitions[j].Partition
		}
		return partitions[i].Topic < partitions[j].Topic
	})
	payload := struct {
		Partitions []commitPartition `json:"partitions"`
	}{Partitions: partitions}
	if err := c.proxy.doJSON(ctx, http.MethodPost, c.baseURI+"/offsets", kafkaV2ContentType, payload, nil); err != nil {
		return fmt.Errorf("commit consumer offsets: %w", err)
	}
	return nil
}

func (c *HTTPConsumer) Close(ctx context.Context) error {
	if c.closed {
		return nil
	}
	c.closed = true
	if err := c.proxy.doJSON(ctx, http.MethodDelete, c.baseURI, kafkaV2ContentType, nil, nil); err != nil {
		return fmt.Errorf("delete consumer: %w", err)
	}
	return nil
}

func (p *HTTPProxy) doJSON(
	ctx context.Context,
	method, endpoint, contentType string,
	payload interface{},
	target interface{},
) error {
	return p.doJSONWithAccept(ctx, method, endpoint, contentType, contentType, payload, target)
}

func (p *HTTPProxy) doJSONWithAccept(
	ctx context.Context,
	method, endpoint, contentType, acceptType string,
	payload interface{},
	target interface{},
) error {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if acceptType != "" {
		request.Header.Set("Accept", acceptType)
	}
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("perform request: %w", err)
	}
	defer response.Body.Close()

	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(data)))
	}
	if target == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
