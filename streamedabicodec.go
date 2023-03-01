package dkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	pbabicodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/abicodec/v1"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/eoscanada/eos-go"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
)

type AbiRepository interface {
	GetAbi(contract string, blockNum uint32) (*ABI, error)
	IsNOOP() bool
}

type DfuseAbiRepository struct {
	overrides   map[string]*ABI
	abiCodecCli pbabicodec.DecoderClient
	context     context.Context
}

func (a *DfuseAbiRepository) GetAbi(contract string, blockNum uint32) (*ABI, error) {
	if a.overrides != nil {
		if abi, ok := a.overrides[contract]; ok {
			return abi, nil
		}
	}
	if a.abiCodecCli == nil {
		return nil, fmt.Errorf("unable to get abi for contract %q, no client, no overrides you need at least one of them", contract)
	}
	resp, err := a.abiCodecCli.GetAbi(a.context, &pbabicodec.GetAbiRequest{
		Account:    contract,
		AtBlockNum: blockNum,
	})
	if err != nil {
		return nil, fmt.Errorf("fail to call dfuse abi server for contract %q: %w", contract, err)
	}

	var eosAbi *eos.ABI
	err = json.Unmarshal([]byte(resp.JsonPayload), &eosAbi)
	if err != nil {
		return nil, fmt.Errorf("unable to decode abi for contract %q: %w", contract, err)
	}
	var abi = ABI{eosAbi, resp.AbiBlockNum}
	zlog.Info("new ABI loaded", zap.String("contract", contract), zap.Uint32("block_num", blockNum), zap.Uint32("abi_block_num", abi.AbiBlockNum))
	return &abi, nil
}

func (b *DfuseAbiRepository) IsNOOP() bool {
	return b.overrides == nil && b.abiCodecCli == nil
}

// TODO converge AbiItem with dkafka.ABI
type AbiItem struct {
	abi          *eos.ABI
	blockNum     uint32
	irreversible bool
}

type StreamedAbiCodec struct {
	bootstrapper         AbiRepository
	latestABI            *AbiItem
	abiHistory           []*AbiItem
	getSchema            MessageSchemaSupplier
	schemaRegistryClient srclient.ISchemaRegistryClient
	account              string
	codecCache           map[string]Codec
	schemaRegistryURL    string
}

func NewStreamedAbiCodec(
	bootstrapper AbiRepository,
	getSchema MessageSchemaSupplier,
	schemaRegistryClient srclient.ISchemaRegistryClient,
	account string,
	schemaRegistryURL string,
) ABICodec {
	codec := &StreamedAbiCodec{
		bootstrapper:         bootstrapper,
		getSchema:            getSchema,
		schemaRegistryClient: schemaRegistryClient,
		account:              account,
		schemaRegistryURL:    schemaRegistryURL,
	}
	codec.resetCodecs()
	return codec
}

func (s *StreamedAbiCodec) IsNOOP() bool {
	return s.bootstrapper.IsNOOP()
}

func (s *StreamedAbiCodec) GetCodec(name string, blockNum uint32) (Codec, error) {
	if codec, found := s.codecCache[name]; found {
		return codec, nil
	}
	abi, err := s.getLatestAbi(blockNum)
	if err != nil {
		return nil, fmt.Errorf("cannot get ABI for codec: %s, error: %w", name, err)
	}
	zlog.Debug("create schema from abi", zap.Uint32("block_num", blockNum), zap.Uint32("abi_block_num", abi.blockNum), zap.String("entry", name))
	messageSchema, err := s.getSchema(name, &ABI{abi.abi, abi.blockNum})
	if err != nil {
		return nil, fmt.Errorf("fail to generate schema: %s, from ABI at block: %d, abi_block: %d, error: %w", name, blockNum, abi.blockNum, err)
	}
	codec, err := s.newCodec(messageSchema)
	if err != nil {
		return nil, fmt.Errorf("KafkaAvroABICodec.GetCodec fail to create codec for schema %s, error: %w", messageSchema.Name, err)
	}
	zlog.Debug("register codec into cache", zap.String("name", name))
	s.codecCache[name] = codec
	return codec, nil
}

func (s *StreamedAbiCodec) getLatestAbi(blockNum uint32) (*AbiItem, error) {
	if s.latestABI == nil {
		if bootstrapAbi, err := s.bootstrapper.GetAbi(s.account, blockNum); err != nil {
			return nil, fmt.Errorf("fail to bootstrap ABI at block: %d, error: %w", blockNum, err)
		} else {
			s.latestABI = &AbiItem{
				abi:          bootstrapAbi.ABI,
				blockNum:     bootstrapAbi.AbiBlockNum,
				irreversible: true,
			}
		}
	}
	return s.latestABI, nil
}

func (s *StreamedAbiCodec) DecodeDBOp(in *pbcodec.DBOp, blockNum uint32) (decoded *decodedDBOp, err error) {
	decoded = &decodedDBOp{DBOp: in}
	err = s.decodeDBOp(decoded, blockNum)
	return
}

func (s *StreamedAbiCodec) decodeDBOp(op *decodedDBOp, blockNum uint32) error {
	latestAbi, err := s.getLatestAbi(blockNum)
	if err != nil {
		return fmt.Errorf("fail to get ABI for decoding dbop in block: %d, error: %w", blockNum, err)
	}
	abi := latestAbi.abi
	tableDef := abi.TableForName(eos.TableName(op.TableName))
	if tableDef == nil {
		return fmt.Errorf("table %s not present in ABI for contract %s at block: %d", op.TableName, op.Code, blockNum)
	}

	if len(op.NewData) > 0 {
		asMap, err := abi.DecodeTableRowTypedNative(tableDef.Type, op.NewData)
		if err != nil {
			return fmt.Errorf("fail to decode new row at block: %d, error: %w", blockNum, err)
		}
		op.NewJSON = asMap
	}
	if len(op.OldData) > 0 {
		asMap, err := abi.DecodeTableRowTypedNative(tableDef.Type, op.OldData)
		if err != nil {
			return fmt.Errorf("fail to decode old row at block: %d, error: %w", blockNum, err)
		}
		op.OldJSON = asMap
	}
	return nil
}

func (s *StreamedAbiCodec) UpdateABI(blockNum uint32, step pbbstream.ForkStep, trxID string, actionTrace *pbcodec.ActionTrace) error {
	zlog.Info("update abi", zap.Uint32("block_num", blockNum), zap.String("transaction_id", trxID))
	abi, err := decodeABIAtBlock(trxID, actionTrace)
	if err != nil {
		return fmt.Errorf("fail to decode abi error: %w", err)
	}
	s.resetCodecs()
	s.doUpdateABI(abi, blockNum, step)
	return err
}

func (s *StreamedAbiCodec) doUpdateABI(abi *eos.ABI, blockNum uint32, step pbbstream.ForkStep) {
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

func (s *StreamedAbiCodec) resetCodecs() {
	zlog.Info("reset schema cache on reload static schema")
	s.codecCache = s.initStaticSchema(make(map[string]Codec))
}

func (s *StreamedAbiCodec) initStaticSchema(cache map[string]Codec) map[string]Codec {
	codec, err := s.newCodec(CheckpointMessageSchema)
	if err != nil {
		zlog.Error("initStaticSchema fail to create codec", zap.String("schema", dkafkaCheckpoint), zap.Error(err))
		panic("initStaticSchema fail to create codec DKafkaCheckpoint")
	}
	cache[dkafkaCheckpoint] = codec
	return cache
}

func (s *StreamedAbiCodec) setSubjectCompatibilityToForward(subject string) error {
	zlog.Debug("set subject compatibility to FORWARD", zap.String("subject", subject), zap.String("compatibility", string(srclient.Forward)))
	_, err := s.schemaRegistryClient.ChangeSubjectCompatibilityLevel(subject, srclient.Forward)
	if err != nil {
		if strings.HasPrefix(err.Error(), "mock") {
			return nil
		}
		return fmt.Errorf("cannot change compatibility level of subject: '%s', error: %w", subject, err)
	}
	return err
}

func (c *StreamedAbiCodec) newCodec(messageSchema MessageSchema) (Codec, error) {
	subject := fmt.Sprintf("%s.%s", messageSchema.Namespace, messageSchema.Name)
	jsonSchema, err := json.Marshal(messageSchema)
	if err != nil {
		return nil, err
	}
	zlog.Debug("register schema", zap.String("subject", subject), zap.ByteString("schema", jsonSchema))
	zlog.Debug("get compatibility level of subject's schema", zap.String("subject", subject))
	actualCompatibilityLevel, err := c.schemaRegistryClient.GetCompatibilityLevel(subject, true)
	unknownSubject := false
	if err != nil {
		unknownSubject = true
	} else if *actualCompatibilityLevel != srclient.Forward {
		err = c.setSubjectCompatibilityToForward(subject)
		if err != nil {
			return nil, err
		}
	}
	schema, err := c.schemaRegistryClient.CreateSchema(subject, string(jsonSchema), srclient.Avro)
	if err != nil {
		return nil, fmt.Errorf("CreateSchema on subject: '%s', schema:\n%s error: %w", subject, string(jsonSchema), err)
	}
	if unknownSubject {
		err = c.setSubjectCompatibilityToForward(subject)
		if err != nil {
			return nil, err
		}
	}

	zlog.Debug("create kafka avro codec", zap.String("subject", subject), zap.Int("ID", schema.ID()))
	ac, err := goavro.NewCodecWithConverters(schema.Schema(), schemaTypeConverters)
	if err != nil {
		return nil, fmt.Errorf("goavro.NewCodecWithConverters error: %w, with schema %s", err, string(jsonSchema))
	}
	codec := NewKafkaAvroCodec(c.schemaRegistryURL, schema, ac)
	return codec, nil
}

func decodeABIAtBlock(trxID string, actionTrace *pbcodec.ActionTrace) (*eos.ABI, error) {
	account := actionTrace.GetData("account").String()
	hexABI := actionTrace.GetData("abi")
	if !hexABI.Exists() {
		zlog.Error("'setabi' action data payload not present", zap.String("account", account), zap.String("transaction_id", trxID))
		return nil, fmt.Errorf("'setabi' action data payload not present. account: %s, transaction_id: %s ", account, trxID)
	}

	hexData := hexABI.String()
	return DecodeABI(trxID, account, hexData)
}
