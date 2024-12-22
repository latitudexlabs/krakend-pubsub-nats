package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var errNoBackendHostDefined = fmt.Errorf("no host backend defined")

const (
	publisherNamespace = "github_com/anshulgoel27/krakend-nats-publisher"
)

func NewBackendFactory(ctx context.Context, logger logging.Logger, bf proxy.BackendFactory) *BackendFactory {
	return &BackendFactory{
		logger: logger,
		bf:     bf,
		ctx:    ctx,
	}
}

type BackendFactory struct {
	ctx    context.Context
	logger logging.Logger
	bf     proxy.BackendFactory
}

func (f *BackendFactory) New(remote *config.Backend) proxy.Proxy {
	if prxy, err := f.initPublisher(f.ctx, remote); err == nil {
		return prxy
	}

	return f.bf(remote)
}

func (f *BackendFactory) initPublisher(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	if len(remote.Host) < 1 {
		return proxy.NoopProxy, errNoBackendHostDefined
	}

	cfg := &publisherCfg{}

	if err := getConfig(remote, publisherNamespace, cfg); err != nil {
		if _, ok := err.(*NamespaceNotFoundErr); !ok {
			f.logger.Error(fmt.Sprintf("[BACKEND][NATS-PubSub] Error initializing publisher: %s", err.Error()))
		}
		return proxy.NoopProxy, err
	}

	if cfg.TopicQueryKey == "" {
		err := fmt.Errorf("topic_query_key not provided")
		f.logger.Error(fmt.Sprintf("[BACKEND][NATS-PubSub] Error initializing publisher: %s", err.Error()))
		return proxy.NoopProxy, err
	}

	logPrefix := "[BACKEND][NATS-PubSub]"
	url := os.Getenv("NATS_SERVER_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	// Connect to NATS server
	nc, err := nats.Connect(url)
	if err != nil {
		f.logger.Error(fmt.Sprintf("%s Error connecting to NATS: %s", logPrefix, err.Error()))
		return proxy.NoopProxy, err
	}

	// Create a JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		f.logger.Error(fmt.Sprintf("%s Error initializing JetStream: %s", logPrefix, err.Error()))
		return proxy.NoopProxy, err
	}

	f.logger.Debug(fmt.Sprintf("%s Publisher initialized successfully", logPrefix))

	// Handle graceful shutdown
	go func() {
		<-ctx.Done()
		nc.Drain()
	}()

	return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}

		topic := r.URL.Query().Get(cfg.TopicQueryKey)
		if topic == "" {
			err := fmt.Errorf("invalid topic %s", topic)
			f.logger.Error(fmt.Sprintf("%s Error publishing message: %s", logPrefix, err.Error()))
			return nil, err
		}

		// Publish the message to the topic
		msg := &nats.Msg{
			Subject: topic,
			Data:    body,
		}

		if _, err := js.PublishMsgAsync(msg); err != nil {
			f.logger.Error(fmt.Sprintf("%s Error publishing message: %s", logPrefix, err.Error()))
			return nil, err
		}

		return &proxy.Response{IsComplete: true}, nil
	}, nil
}

type publisherCfg struct {
	TopicQueryKey string `json:"topic_query_key"`
}

func getConfig(remote *config.Backend, namespace string, v interface{}) error {
	cfg, ok := remote.ExtraConfig[namespace]
	if !ok {
		return &NamespaceNotFoundErr{
			Namespace: namespace,
		}
	}

	b, err := json.Marshal(&cfg)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &v)
}

type NamespaceNotFoundErr struct {
	Namespace string
}

func (n *NamespaceNotFoundErr) Error() string {
	return n.Namespace + " not found in the extra config"
}
