package dkafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/streamingfast/bstream/forkable"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewRequest(filter string, startBlock int64, stopBlock uint64, cursor string, irreversibleOnly bool) *pbbstream.BlocksRequestV2 {
	req := pbbstream.BlocksRequestV2{
		IncludeFilterExpr: filter,
		StartBlockNum:     startBlock,
		StopBlockNum:      stopBlock,
		StartCursor:       cursor,
	}
	if irreversibleOnly {
		zlog.Debug("Request only irreversible blocks")
		req.ForkSteps = []pbbstream.ForkStep{pbbstream.ForkStep_STEP_IRREVERSIBLE}
	}
	return &req
}

func findCursor(headers []kafka.Header) (cursor string) {
	for _, header := range headers {
		zlog.Debug("check heard", zap.String("key", header.Key), zap.ByteString("value", header.Value))
		if header.Key == CursorHeaderKey {
			zlog.Debug("find cursor", zap.String("key", header.Key), zap.ByteString("value", header.Value))
			return string(header.Value)
		}
	}
	return
}

// LoadCursor load the latest cursor stored in the given topic and update the request accordingly
func LoadCursor(config kafka.ConfigMap, topic string) (opaqueCursor string, err error) {
	config["group.id"] = "cursor-loader"
	config["enable.auto.commit"] = false

	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		return opaqueCursor, fmt.Errorf("creating consumer to load cursor: %w", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			zlog.Error("error closing consumer after loading cursor", zap.Error(err))
		}
	}()

	consumer.Subscribe(topic, nil)

	md, err := consumer.GetMetadata(&topic, false, 500)
	if err != nil {
		return opaqueCursor, fmt.Errorf("getting metadata for loading cursor from topic: %s,error: %w", topic, err)
	}
	parts := md.Topics[topic].Partitions
	if len(parts) == 0 {
		zlog.Info("topic does not exist no cursor to load", zap.String("topic", topic))
		return opaqueCursor, nil
	}
	var cursor *forkable.Cursor
	for _, partition := range parts {
		c, oc, err := getHeadCursorFromPartion(consumer, topic, partition)
		if err != nil {
			return opaqueCursor, err
		}
		if cursor == nil || cursor.Block.Num() < c.Block.Num() {
			zlog.Debug("found max cursor", zap.Int32("partition", partition.ID), zap.Uint64("block_num", c.Block.Num()))
			cursor = c
			opaqueCursor = oc
		}
	}
	return opaqueCursor, nil
}

func getHeadCursorFromPartion(consumer *kafka.Consumer, topic string, partition kafka.PartitionMetadata) (cursor *forkable.Cursor, opaqueCursor string, err error) {
	low, high, err := consumer.QueryWatermarkOffsets(topic, partition.ID, 500)
	if err != nil {
		return cursor, opaqueCursor, fmt.Errorf("getting low/high watermarque for topic: %s, partition: %d, error: %w", topic, partition.ID, err)
	}

	for i := kafka.Offset(high) - 1; i >= kafka.Offset(low); i-- {
		zlog.Debug("retrive cursor header from kafka message", zap.String("topic", topic), zap.Int32("partition", partition.ID), zap.Int64("offset", int64(i)))
		err = consumer.Assign([]kafka.TopicPartition{
			{
				Topic:     &topic,
				Partition: partition.ID,
				Offset:    i,
			}})

		if err != nil {
			return
		}

		ev := consumer.Poll(1000)
		switch event := ev.(type) {
		case kafka.Error:
			return cursor, opaqueCursor, event
		case *kafka.Message:
			zlog.Debug("look for cursor header", zap.Int("nb_headers", len(event.Headers)))
			opaqueCursor = findCursor(event.Headers)
			if opaqueCursor == "" {
				zlog.Debug("no cursor found in headers")
				continue
			}
			zlog.Debug("read opaque cursor")
			cursor, err = forkable.CursorFromOpaque(opaqueCursor)
			return
		default:
			zlog.Debug("un-handled kafka.Event type", zap.Any("event", event))
		}
	}
	return
}

type kHeaders []kafka.Header

func (hs kHeaders) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, h := range hs {
		enc.AppendString(fmt.Sprintf("key:%s, value:%s", h.Key, h.Value))
	}
	return nil
}
