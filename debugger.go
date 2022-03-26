package dkafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/streamingfast/bstream/forkable"
	"go.uber.org/zap"
)

type Debugger struct {
	config *Config
}

func NewDebugger(config *Config) *Debugger {
	return &Debugger{
		config: config,
	}
}

func (d *Debugger) ReadCursor() error {
	conf := createKafkaConfig(d.config)

	producer, err := getKafkaProducer(conf, "")
	if err != nil {
		return fmt.Errorf("getting kafka producer: %w", err)
	}

	cp := newKafkaCheckpointer(conf, d.config.KafkaCursorTopic, d.config.KafkaCursorPartition, d.config.KafkaTopic, d.config.KafkaCursorConsumerGroupID, producer)

	cursor, err := cp.Load()
	if err != nil {
		return err
	}
	fmt.Println("cursor is", cursor)
	if cursor != "" {
		c, err := forkable.CursorFromOpaque(cursor)
		if err != nil {
			return err
		}
		zlog.Info("running in live mode, found cursor",
			zap.String("cursor", cursor),
			zap.Stringer("plain_cursor", c),
			zap.Stringer("cursor_block", c.Block),
			zap.Stringer("cursor_head_block", c.HeadBlock),
			zap.Stringer("cursor_LIB", c.LIB),
		)
		fmt.Printf("%+v\n", c)
	}
	return nil
}

func (d *Debugger) WriteCursor(cursor string) error {
	if cursor == "" {
		return d.DeleteCursor()
	}

	conf := createKafkaConfig(d.config)

	producer, err := getKafkaProducer(conf, "")
	if err != nil {
		return fmt.Errorf("getting kafka producer: %w", err)
	}

	if c, err := forkable.CursorFromString(cursor); err == nil {
		cursor = c.ToOpaque() // we always write opaque version
	}
	if _, err = forkable.CursorFromOpaque(cursor); err != nil {
		return fmt.Errorf("invalid cursor: %s", cursor)
	}

	cp := newKafkaCheckpointer(conf, d.config.KafkaCursorTopic, d.config.KafkaCursorPartition, d.config.KafkaTopic, d.config.KafkaCursorConsumerGroupID, producer)

	err = cp.Save(cursor)
	if err != nil {
		return err
	}
	fmt.Println("successfully set cursor to", cursor)
	producer.Close()
	return nil
}

func (d *Debugger) DeleteCursor() error {
	conf := createKafkaConfig(d.config)

	producer, err := getKafkaProducer(conf, "")
	if err != nil {
		return fmt.Errorf("getting kafka producer: %w", err)
	}

	cp := newKafkaCheckpointer(conf, d.config.KafkaCursorTopic, d.config.KafkaCursorPartition, d.config.KafkaTopic, d.config.KafkaCursorConsumerGroupID, producer)

	err = cp.Save("")
	if err != nil {
		return err
	}
	fmt.Println("successfully set empty cursor")
	producer.Close()
	return nil
}

func (d *Debugger) Write(key, val string) error {
	conf := createKafkaConfig(d.config)
	producer, err := getKafkaProducer(conf, d.config.KafkaTransactionID)
	if err != nil {
		return fmt.Errorf("getting kafka producer: %w", err)
	}

	ctx := context.Background()
	s, err := NewSender(ctx, producer, &nilCheckpointer{}, false)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(val),
		TopicPartition: kafka.TopicPartition{
			Topic:     &d.config.KafkaTopic,
			Partition: kafka.PartitionAny,
		},
	}
	fmt.Printf("sending message: %s:%s to topic %s\n", key, val, d.config.KafkaTopic)
	if err := s.Send(ctx, []*kafka.Message{&msg}, ""); err != nil {
		return fmt.Errorf("sending message: %w", err)
	}
	producer.Close()
	return nil
}

func (d *Debugger) Read(groupID string, numValues int, startOffset int) error {
	conf := createKafkaConfig(d.config)

	conf["group.id"] = groupID
	//conf["auto.offset.reset"] = "smallest"
	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		return fmt.Errorf("creating consumer: %w", err)
	}

	defer func() {
		if err := consumer.Unsubscribe(); err != nil {
			zlog.Error("error unsubscribing consumer", zap.Error(err))
		}
		if err := consumer.Close(); err != nil {
			zlog.Error("error closing consumer", zap.Error(err))
		}
	}()

	consumer.Subscribe(d.config.KafkaTopic, nil)

	if startOffset >= 0 {
		zlog.Debug("setting offset to..", zap.Int("start_offset", startOffset))
		err = consumer.Assign([]kafka.TopicPartition{
			{
				Topic:  &d.config.KafkaTopic,
				Offset: kafka.Offset(startOffset),
			}})
		if err != nil {
			return fmt.Errorf("assigning topic and offset: %w", err)
		}
	}

	for i := 0; numValues <= 0 || i < numValues; i++ {
		ev := consumer.Poll(1000)
		switch event := ev.(type) {
		case kafka.Error:
			fmt.Printf("got error: %s\n", event)
			return event
		case *kafka.Message:
			fmt.Printf("got event: %+v, key:%s, val:%s (partition: %d)\n", event, string(event.Key), string(event.Value), event.TopicPartition.Partition)
		default:
			if ev == nil {
				fmt.Println("got nil value")
			} else {
				fmt.Println("got unexpected value", ev)
			}
		}
	}

	return nil
}
