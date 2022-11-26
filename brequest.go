package dkafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/streamingfast/bstream/forkable"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type position struct {
	cursor               *forkable.Cursor
	opaqueCursor         string
	previousCursor       *forkable.Cursor
	previousOpaqueCursor string
}

func (p position) gt(that position) bool {
	if p.previousCursor != nil && that.previousCursor == nil {
		return true
	}
	if p.previousCursor == nil && that.previousCursor != nil {
		return false
	}
	if p.previousCursor != nil && that.previousCursor != nil {
		return that.previousCursor.Block.Num() < p.previousCursor.Block.Num()
	}
	return that.cursor.Block.Num() < p.cursor.Block.Num()
}

func (p position) opaque() string {
	if len(p.previousOpaqueCursor) > 0 {
		return p.previousOpaqueCursor
	} else {
		return p.opaqueCursor
	}
}

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

func findPosition(headers []kafka.Header) (position position) {
	for _, header := range headers {
		zlog.Debug("check heard", zap.String("key", header.Key), zap.ByteString("value", header.Value))
		if header.Key == CursorHeaderKey {
			zlog.Debug("find cursor", zap.String("key", header.Key), zap.ByteString("value", header.Value))
			position.opaqueCursor = string(header.Value)
		}
		if header.Key == PreviousCursorHeaderKey {
			zlog.Debug("find cursor", zap.String("key", header.Key), zap.ByteString("value", header.Value))
			position.previousOpaqueCursor = string(header.Value)
		}
	}
	return
}

// LoadCursor load the latest cursor stored in the given topic and update the request accordingly
func LoadCursor(config kafka.ConfigMap, topic string) (string, error) {
	config["group.id"] = "cursor-loader"
	config["enable.auto.commit"] = false

	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		return "", fmt.Errorf("creating consumer to load cursor: %w", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			zlog.Error("error closing consumer after loading cursor", zap.Error(err))
		}
	}()

	consumer.Subscribe(topic, nil)

	md, err := consumer.GetMetadata(&topic, false, 500)
	if err != nil {
		return "", fmt.Errorf("getting metadata for loading cursor from topic: %s,error: %w", topic, err)
	}
	parts := md.Topics[topic].Partitions
	if len(parts) == 0 {
		zlog.Info("topic does not exist no cursor to load", zap.String("topic", topic))
		return "", nil
	}
	var latest position = position{}
	for _, partition := range parts {
		position, err := getHeadCursorFromPartition(consumer, topic, partition)
		if err != nil {
			return "", err
		}
		if position.cursor == nil { // happen if strange messages in the topic or old dkafka messages
			continue
		}
		if position.gt(latest) {
			zlog.Debug("found max cursor", zap.Int32("partition", partition.ID), zap.Uint64("block_num", position.cursor.Block.Num()))
			latest = position
		}
	}
	return latest.opaque(), nil
}

func getHeadCursorFromPartition(consumer *kafka.Consumer, topic string, partition kafka.PartitionMetadata) (position position, err error) {
	low, high, err := consumer.QueryWatermarkOffsets(topic, partition.ID, 500)
	if err != nil {
		return position, fmt.Errorf("getting low/high watermark for topic: %s, partition: %d, error: %w", topic, partition.ID, err)
	}

	for i := kafka.Offset(high) - 1; i >= kafka.Offset(low); i-- {
		zlog.Debug("retrieve cursor header from kafka message", zap.String("topic", topic), zap.Int32("partition", partition.ID), zap.Int64("offset", int64(i)))
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
			return position, event
		case *kafka.Message:
			zlog.Debug("look for cursor header", zap.Int("nb_headers", len(event.Headers)))
			position = findPosition(event.Headers)
			if position.opaqueCursor == "" {
				// should not happen but if the producer in this topic are not only dkafka instances...
				// or if the message where produce by a very old version of dkafka
				// which was not using cursor headers
				zlog.Debug("no cursor found in headers")
				continue
			}
			zlog.Debug("read opaque cursor")
			if position.cursor, err = forkable.CursorFromOpaque(position.opaqueCursor); err != nil {
				return
			}
			position.previousCursor, _ = forkable.CursorFromOpaque(position.previousOpaqueCursor)
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
