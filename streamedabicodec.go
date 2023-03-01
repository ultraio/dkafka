package dkafka

import (
	"fmt"

	pbabicodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/abicodec/v1"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/eoscanada/eos-go"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
)

type ABIBootstrapper struct {
	overrides   map[string]*ABI
	abiCodecCli pbabicodec.DecoderClient
}

func (b *ABIBootstrapper) IsNOOP() bool {
	return b.overrides == nil && b.abiCodecCli == nil
}

type AbiItem struct {
	abi          *eos.ABI
	blockNum     uint32
	irreversible bool
}

type StreamedABICodec struct {
	bootstrapper *ABIBootstrapper
	latestABI    *AbiItem
	abiHistory   []*AbiItem
}

func (s *StreamedABICodec) IsNOOP() bool {
	return s.bootstrapper.IsNOOP()
}

func (s *StreamedABICodec) UpdateABI(blockNum uint32, step pbbstream.ForkStep, trxID string, actionTrace *pbcodec.ActionTrace) error {
	zlog.Info("update abi", zap.Uint32("block_num", blockNum), zap.String("transaction_id", trxID))
	abi, err := DecodeABIAtBlock(trxID, actionTrace)
	if err != nil {
		return fmt.Errorf("fail to decode abi error: %w", err)
	}
	s.doUpdateABI(abi, blockNum, step)
	return err
}

func (s *StreamedABICodec) doUpdateABI(abi *eos.ABI, blockNum uint32, step pbbstream.ForkStep) {
	if step == pbbstream.ForkStep_STEP_UNKNOWN {
		zlog.Warn("skip ABI update on unknown step", zap.Uint32("block_num", blockNum), zap.Int32("step", int32(step)))
		return
	}
	if step == pbbstream.ForkStep_STEP_UNDO {
		if s.latestABI == nil { // invalid state maybe should fail here
			zlog.Warn("undo skipped no latest abi", zap.Uint32("undo_block_num", blockNum))
			return
		}
		if blockNum > s.latestABI.blockNum { // must have been replaced by a irreversible compaction
			zlog.Info("undo skipped as undo block > latest abi block", zap.Uint32("undo_block_num", blockNum), zap.Uint32("latest_block_num", s.latestABI.blockNum))
			return
		}
		zlog.Info("undo actual/latest ABI", zap.Uint32("undo_block_num", blockNum), zap.Uint32("latest_block_num", s.latestABI.blockNum))
		s.latestABI = nil
		if len(s.abiHistory) > 0 {
			// pop from history
			previousAbi := s.abiHistory[len(s.abiHistory)-1]
			zlog.Info("pop previous ABI from history on undo", zap.Uint32("undo_block_num", blockNum), zap.Uint32("previous_block_num", previousAbi.blockNum))
			s.latestABI = previousAbi
			s.abiHistory = s.abiHistory[:len(s.abiHistory)-1]
			if len(s.abiHistory) == 0 { // fix a testing compare issue
				s.abiHistory = nil
			}
		} else {
			zlog.Info("nothing pop from history (empty) on undo", zap.Uint32("undo_block_num", blockNum))
		}
	} else {
		newAbiItem := &AbiItem{abi, blockNum, step == pbbstream.ForkStep_STEP_IRREVERSIBLE}
		abiHistory := s.abiHistory
		if s.latestABI != nil {
			if len(abiHistory) > 0 && abiHistory[len(abiHistory)-1].blockNum == s.latestABI.blockNum {
				s.abiHistory[len(abiHistory)-1] = s.latestABI
			} else {
				s.abiHistory = append(s.abiHistory, s.latestABI)
			}

		}
		s.latestABI = newAbiItem
	}
}

func DecodeABIAtBlock(trxID string, actionTrace *pbcodec.ActionTrace) (*eos.ABI, error) {
	account := actionTrace.GetData("account").String()
	hexABI := actionTrace.GetData("abi")
	if !hexABI.Exists() {
		zlog.Error("'setabi' action data payload not present", zap.String("account", account), zap.String("transaction_id", trxID))
		return nil, fmt.Errorf("'setabi' action data payload not present. account: %s, transaction_id: %s ", account, trxID)
	}

	hexData := hexABI.String()
	return DecodeABI(trxID, account, hexData)
}
