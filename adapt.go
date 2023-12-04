package dkafka

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/pinax-network/firehose-antelope/types/pb/sf/antelope/type/v1"
	pbbstream "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"
)

type Adapter interface {
	Adapt(blkStep BlockStep) ([]*kafka.Message, error)
}

type BlockStep struct {
	blk            *pbcodec.Block
	step           pbbstream.ForkStep
	cursor         string
	previousCursor string
}

func (bs BlockStep) time() time.Time {
	return bs.blk.Time().UTC()
}

func (bs BlockStep) timeString() string {
	blkTime := bs.time()
	return blkTime.Format(time.RFC3339)
}

func (bs BlockStep) timeHeader() kafka.Header {
	return kafka.Header{
		Key:   "ce_time",
		Value: []byte(bs.timeString()),
	}
}

func (bs BlockStep) opaqueCursor() string {
	return bs.cursor
}

func (bs BlockStep) previousOpaqueCursor() string {
	return bs.previousCursor
}
func (bs BlockStep) blockId() string {
	return bs.blk.Id
}
func (bs BlockStep) blockNum() uint32 {
	return bs.blk.Number
}

type CdCAdapter struct {
	topic     string
	saveBlock SaveBlock
	generator GeneratorAtTransactionLevel
	headers   []kafka.Header
	abiCodec  ABICodec
}

// orderSliceOnBlockStep reverse the slice order is the block step is UNDO
func orderSliceOnBlockStep[T any](input []T, step pbbstream.ForkStep) []T {
	output := input
	if step == pbbstream.ForkStep_STEP_UNDO {
		output = Reverse(input)
	}
	return output
}

func (m *CdCAdapter) Adapt(blkStep BlockStep) ([]*kafka.Message, error) {
	blk := blkStep.blk
	step := sanitizeStep(blkStep.step.String())

	m.saveBlock(blk)
	if blk.Number%100 == 0 {
		zlog.Info("incoming block 1/100", zap.Uint32("block_num", blk.Number), zap.String("step", step), zap.Int("length_filtered_trx_traces", len(blk.FilteredTransactionTraces)))
	}
	if blk.Number%10 == 0 {
		zlog.Debug("incoming block 1/10", zap.Uint32("block_num", blk.Number), zap.String("step", step), zap.Int("length_filtered_trx_traces", len(blk.FilteredTransactionTraces)))
	}
	msgs := make([]*kafka.Message, 0)
	trxs := blk.TransactionTraces()
	zlog.Debug("adapt block", zap.Uint32("num", blk.Number), zap.Int("nb_trx", len(trxs)))
	for _, trx := range orderSliceOnBlockStep(trxs, blkStep.step) {
		transactionTracesReceived.Inc()
		trxCtx := TransactionContext{
			block:       blk,
			stepName:    step,
			transaction: trx,
			cursor:      blkStep.cursor,
			step:        blkStep.step,
			blockStep:   blkStep,
		}
		msgs1, err := m.generator.Apply(trxCtx)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msgs1...)
	}
	zlog.Debug("produced kafka messages", zap.Uint32("block_num", blk.Number), zap.String("step", step), zap.Int("nb_messages", len(msgs)))
	return msgs, nil
}
