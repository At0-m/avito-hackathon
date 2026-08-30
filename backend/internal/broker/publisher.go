package broker

import (
	"context"

	"recap-personalization/internal/eventing"
)

type EventPublisher struct {
	Proxy          *HTTPProxy
	LifecycleTopic string
	DLQTopic       string
}

func (p EventPublisher) PublishLifecycle(ctx context.Context, value eventing.RecapLifecycleV1) error {
	topic := p.LifecycleTopic
	if topic == "" {
		topic = eventing.LifecycleTopic
	}
	return p.Proxy.Publish(ctx, topic, value.RecapID.String(), value)
}

func (p EventPublisher) PublishFailedCommand(ctx context.Context, value eventing.FailedCommandV1) error {
	topic := p.DLQTopic
	if topic == "" {
		topic = eventing.GenerationDLQTopic
	}
	return p.Proxy.Publish(ctx, topic, value.RecapID.String(), value)
}
