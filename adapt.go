package dkafka

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
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
	return bs.blk.MustTime().UTC()
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
	generator Generator2
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
		// manage correlation
		correlation, err := getCorrelation(trx.ActionTraces)
		if err != nil {
			return nil, err
		}
		acts := trx.ActionTraces
		zlog.Debug("adapt transaction", zap.Uint32("block_num", blk.Number), zap.Int("trx_index", int(trx.Index)), zap.Int("nb_acts", len(acts)))
		for _, act := range orderSliceOnBlockStep(acts, blkStep.step) {
			if !act.FilteringMatched {
				continue
			}
			if act.Action.Name == "setabi" {
				zlog.Info("new abi published defer clear ABI cache at end of this block parsing", zap.Uint32("block_num", blk.Number), zap.Int("trx_index", int(trx.Index)), zap.String("trx_id", trx.Id))
				m.abiCodec.UpdateABI(blk.Number, blkStep.step, trx.Id, act)
				continue
			}
			actionTracesReceived.Inc()
			// generation
			generations, err := m.generator.Apply(GenContext{
				block:       blk,
				stepName:    step,
				transaction: trx,
				actionTrace: act,
				correlation: correlation,
				cursor:      blkStep.cursor,
			})

			if err != nil {
				zlog.Debug("fail fast on generator.Apply()", zap.Error(err))
				return nil, err
			}

			for _, generation := range generations {
				headers := append(m.headers,
					kafka.Header{
						Key:   "ce_id",
						Value: generation.CeId,
					},
					kafka.Header{
						Key:   "ce_type",
						Value: []byte(generation.CeType),
					},
					blkStep.timeHeader(),
					kafka.Header{
						Key:   "ce_blkstep",
						Value: []byte(step),
					},
				)
				headers = append(headers, generation.Headers...)
				if correlation != nil {
					headers = append(headers,
						kafka.Header{
							Key:   "ce_parentid",
							Value: []byte(correlation.Id),
						},
					)
				}
				msg := &kafka.Message{
					Key:     []byte(generation.Key),
					Headers: headers,
					Value:   generation.Value,
					TopicPartition: kafka.TopicPartition{
						Topic:     &m.topic,
						Partition: kafka.PartitionAny,
					},
				}
				msgs = append(msgs, msg)
			}
		}
	}
	zlog.Debug("produced kafka messages", zap.Uint32("block_num", blk.Number), zap.String("step", step), zap.Int("nb_messages", len(msgs)))
	return msgs, nil
}
