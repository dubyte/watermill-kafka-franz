package kafka

import (
	"context"
	"time"
)

// contextKey is a type for context keys.
type contextKey string

const (
	// partitionKey is the context key for partition.
	partitionKey contextKey = "partition"
	// offsetKey is the context key for offset.
	offsetKey contextKey = "offset"
	// timestampKey is the context key for message timestamp.
	timestampKey contextKey = "timestamp"
	// messageKey is the context key for message key.
	messageKey contextKey = "message_key"
)

// setPartitionToCtx adds partition info to context (internal helper).
func setPartitionToCtx(ctx context.Context, partition int32) context.Context {
	return context.WithValue(ctx, partitionKey, partition)
}

// setPartitionOffsetToCtx adds offset info to context (internal helper).
func setPartitionOffsetToCtx(ctx context.Context, offset int64) context.Context {
	return context.WithValue(ctx, offsetKey, offset)
}

// setMessageTimestampToCtx adds timestamp info to context (internal helper).
func setMessageTimestampToCtx(ctx context.Context, timestamp time.Time) context.Context {
	return context.WithValue(ctx, timestampKey, timestamp)
}

// setMessageKeyToCtx adds message key info to context (internal helper).
func setMessageKeyToCtx(ctx context.Context, key []byte) context.Context {
	return context.WithValue(ctx, messageKey, key)
}

// ContextWithPartition adds partition info to context.
func ContextWithPartition(ctx context.Context, partition int32) context.Context {
	return context.WithValue(ctx, partitionKey, partition)
}

// PartitionFromContext extracts partition from context.
func PartitionFromContext(ctx context.Context) (int32, bool) {
	partition, ok := ctx.Value(partitionKey).(int32)
	return partition, ok
}

// ContextWithOffset adds offset info to context.
func ContextWithOffset(ctx context.Context, offset int64) context.Context {
	return context.WithValue(ctx, offsetKey, offset)
}

// OffsetFromContext extracts offset from context.
func OffsetFromContext(ctx context.Context) (int64, bool) {
	offset, ok := ctx.Value(offsetKey).(int64)
	return offset, ok
}

// MessageTimestampFromContext extracts message timestamp from context.
func MessageTimestampFromContext(ctx context.Context) (time.Time, bool) {
	timestamp, ok := ctx.Value(timestampKey).(time.Time)
	return timestamp, ok
}

// MessageKeyFromContext extracts message key from context.
func MessageKeyFromContext(ctx context.Context) ([]byte, bool) {
	key, ok := ctx.Value(messageKey).([]byte)
	return key, ok
}
