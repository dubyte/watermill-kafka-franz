package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDefaultPublisherConfig(t *testing.T) {
	config := DefaultPublisherConfig()

	if config.MaxBufferedRecords != 10000 {
		t.Errorf("expected MaxBufferedRecords to be 10000, got %d", config.MaxBufferedRecords)
	}

	if config.ProduceRequestTimeout != 10*time.Second {
		t.Errorf("expected ProduceRequestTimeout to be 10s, got %v", config.ProduceRequestTimeout)
	}

	if config.BatchMaxBytes != 1<<20 {
		t.Errorf("expected BatchMaxBytes to be 1MB, got %d", config.BatchMaxBytes)
	}

	if len(config.Compression) != 2 {
		t.Errorf("expected 2 compression codecs, got %d", len(config.Compression))
	}

	if config.ClientID != "watermill" {
		t.Errorf("expected ClientID to be 'watermill', got %s", config.ClientID)
	}
}

func TestDefaultSubscriberConfig(t *testing.T) {
	config := DefaultSubscriberConfig()

	if config.AutoOffsetReset != "latest" {
		t.Errorf("expected AutoOffsetReset to be 'latest', got %s", config.AutoOffsetReset)
	}

	if config.HeartbeatInterval != 3*time.Second {
		t.Errorf("expected HeartbeatInterval to be 3s, got %v", config.HeartbeatInterval)
	}

	if config.SessionTimeout != 45*time.Second {
		t.Errorf("expected SessionTimeout to be 45s, got %v", config.SessionTimeout)
	}

	if config.RebalanceTimeout != 60*time.Second {
		t.Errorf("expected RebalanceTimeout to be 60s, got %v", config.RebalanceTimeout)
	}

	if config.AutoCommitInterval != 5*time.Second {
		t.Errorf("expected AutoCommitInterval to be 5s, got %v", config.AutoCommitInterval)
	}

	if config.FetchMinBytes != 1 {
		t.Errorf("expected FetchMinBytes to be 1, got %d", config.FetchMinBytes)
	}

	if config.FetchMaxBytes != 50<<20 {
		t.Errorf("expected FetchMaxBytes to be 50MB, got %d", config.FetchMaxBytes)
	}

	if config.FetchMaxPartitionBytes != 1<<20 {
		t.Errorf("expected FetchMaxPartitionBytes to be 1MB, got %d", config.FetchMaxPartitionBytes)
	}

	if config.FetchMaxWait != 5*time.Second {
		t.Errorf("expected FetchMaxWait to be 5s, got %v", config.FetchMaxWait)
	}

	if config.NackResendSleep != 100*time.Millisecond {
		t.Errorf("expected NackResendSleep to be 100ms, got %v", config.NackResendSleep)
	}

	if config.ClientID != "watermill" {
		t.Errorf("expected ClientID to be 'watermill', got %s", config.ClientID)
	}
}

func TestPublisherConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		config := DefaultPublisherConfig()
		config.Brokers = []string{"localhost:9092"}
		config.Marshaler = DefaultMarshaler{}
		if err := config.Validate(); err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("empty brokers", func(t *testing.T) {
		config := DefaultPublisherConfig()
		config.Marshaler = DefaultMarshaler{}
		err := config.Validate()
		if err == nil {
			t.Error("expected error for empty brokers")
		}
	})

	t.Run("nil marshaler", func(t *testing.T) {
		config := PublisherConfig{
			Brokers:   []string{"localhost:9092"},
			Marshaler: nil,
		}
		err := config.Validate()
		if err == nil {
			t.Error("expected error for nil marshaler")
		}
	})
}

func TestSubscriberConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		config := DefaultSubscriberConfig()
		config.Brokers = []string{"localhost:9092"}
		config.Unmarshaler = DefaultMarshaler{}
		if err := config.Validate(); err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("empty brokers", func(t *testing.T) {
		config := DefaultSubscriberConfig()
		config.Unmarshaler = DefaultMarshaler{}
		err := config.Validate()
		if err == nil {
			t.Error("expected error for empty brokers")
		}
	})

	t.Run("nil unmarshaler", func(t *testing.T) {
		config := SubscriberConfig{
			Brokers:     []string{"localhost:9092"},
			Unmarshaler: nil,
		}
		err := config.Validate()
		if err == nil {
			t.Error("expected error for nil unmarshaler")
		}
	})
}

func TestConfigWithOverwriteKgoOpts(t *testing.T) {
	// Test PublisherConfig with custom kgo options
	pubConfig := DefaultPublisherConfig()
	pubConfig.OverwriteKgoOpts = []kgo.Opt{
		kgo.SeedBrokers("127.0.0.1:9092"),
	}

	if len(pubConfig.OverwriteKgoOpts) != 1 {
		t.Errorf("expected 1 overwrite option, got %d", len(pubConfig.OverwriteKgoOpts))
	}

	// Test SubscriberConfig with custom kgo options
	subConfig := DefaultSubscriberConfig()
	subConfig.OverwriteKgoOpts = []kgo.Opt{
		kgo.SeedBrokers("127.0.0.1:9092"),
		kgo.ConsumerGroup("test-group"),
	}

	if len(subConfig.OverwriteKgoOpts) != 2 {
		t.Errorf("expected 2 overwrite options, got %d", len(subConfig.OverwriteKgoOpts))
	}
}
