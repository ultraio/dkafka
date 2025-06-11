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

const (
	// CodecIdPrefix is the prefix used for codec IDs in the StreamedAbiCodec.
	DefaultCompatibility = srclient.Forward
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
	var abi = ABI{eosAbi, resp.AbiBlockNum, contract, true}
	zlog.Info("new ABI loaded", zap.String("contract", contract), zap.Uint32("block_num", blockNum), zap.Uint32("abi_block_num", abi.AbiBlockNum))
	return &abi, nil
}

func (a *DfuseAbiRepository) IsNOOP() bool {
	return a.overrides == nil && a.abiCodecCli == nil
}

func (a CodecId) String() string {
	return fmt.Sprintf("%v::%v", a.Account, a.Name)
}

type StreamedAbiCodec struct {
	bootstrapper         AbiRepository
	latestABIs           map[string]*ABI
	abiHistories         map[string][]*ABI
	getSchema            MessageSchemaSupplier
	schemaRegistryClient srclient.ISchemaRegistryClient
	account              string
	codecCache           map[CodecId]Codec
	schemaRegistryURL    string
	staticSchemas        []MessageSchema
	compatibility        srclient.CompatibilityLevel
}

type StreamAbiCodecConstructor = func(AbiRepository,
	MessageSchemaSupplier,
	srclient.ISchemaRegistryClient,
	string,
	string,
	srclient.CompatibilityLevel,
) ABICodec

func NewStreamedAbiCodec(
	bootstrapper AbiRepository,
	getSchema MessageSchemaSupplier,
	schemaRegistryClient srclient.ISchemaRegistryClient,
	account string,
	schemaRegistryURL string,
	compatibility srclient.CompatibilityLevel,
) ABICodec {
	return newStreamedAbiCodec(
		bootstrapper,
		getSchema,
		schemaRegistryClient,
		account,
		schemaRegistryURL,
		[]MessageSchema{CheckpointMessageSchema},
		compatibility,
	)
}

func NewStreamedAbiCodecWithTransaction(
	bootstrapper AbiRepository,
	getSchema MessageSchemaSupplier,
	schemaRegistryClient srclient.ISchemaRegistryClient,
	account string,
	schemaRegistryURL string,
	compatibility srclient.CompatibilityLevel,
) ABICodec {
	return newStreamedAbiCodec(
		bootstrapper,
		getSchema,
		schemaRegistryClient,
		account,
		schemaRegistryURL,
		[]MessageSchema{CheckpointMessageSchema, TransactionMessageSchema},
		compatibility,
	)
}

func newStreamedAbiCodec(
	bootstrapper AbiRepository,
	getSchema MessageSchemaSupplier,
	schemaRegistryClient srclient.ISchemaRegistryClient,
	account string,
	schemaRegistryURL string,
	staticSchemas []MessageSchema,
	compatibility srclient.CompatibilityLevel,
) ABICodec {
	codec := &StreamedAbiCodec{
		bootstrapper:         bootstrapper,
		getSchema:            getSchema,
		schemaRegistryClient: schemaRegistryClient,
		account:              account,
		schemaRegistryURL:    schemaRegistryURL,
		staticSchemas:        staticSchemas,
		latestABIs:           make(map[string]*ABI),
		abiHistories:         make(map[string][]*ABI),
		codecCache:           make(map[CodecId]Codec),
		compatibility:        compatibility,
	}
	codec.resetCodecs()
	return codec
}

func (s *StreamedAbiCodec) IsNOOP() bool {
	return s.bootstrapper.IsNOOP()
}

func (s *StreamedAbiCodec) GetCodec(codecId CodecId, blockNum uint32) (Codec, error) {

	if codec, found := s.codecCache[codecId]; found {
		return codec, nil
	}
	abi, err := s.getLatestAbi(codecId.Account, blockNum)
	if err != nil {
		return nil, fmt.Errorf("cannot get ABI for codec: %s, error: %w", codecId, err)
	}
	zlog.Debug("create schema from abi", zap.Uint32("block_num", blockNum), zap.Uint32("abi_block_num", abi.AbiBlockNum), zap.Stringer("entry", codecId))
	messageSchema, err := s.getSchema(codecId.Name, abi)
	if err != nil {
		return nil, fmt.Errorf("fail to generate schema: %s, from ABI at block: %d, abi_block: %d, error: %w", codecId, blockNum, abi.AbiBlockNum, err)
	}
	codec, err := s.newCodec(messageSchema)
	if err != nil {
		return nil, fmt.Errorf("StreamedAbiCodec.newCodec fail to create codec for schema %s, error: %w", messageSchema.Name, err)
	}
	zlog.Debug("register codec into cache", zap.Stringer("name", codecId))
	s.codecCache[codecId] = codec
	return codec, nil
}

func (s *StreamedAbiCodec) getLatestAbi(account string, blockNum uint32) (latestAbi *ABI, err error) {
	var found = false
	if latestAbi, found = s.latestABIs[account]; !found {
		if bootstrapAbi, er := s.bootstrapper.GetAbi(account, blockNum); er != nil {
			err = fmt.Errorf("fail to bootstrap ABI at block: %d, error: %w", blockNum, err)
		} else {
			latestAbi = bootstrapAbi
			s.latestABIs[account] = latestAbi
		}
	}
	return
}

func (s *StreamedAbiCodec) DecodeDBOp(in *pbcodec.DBOp, blockNum uint32) (decoded *decodedDBOp, err error) {
	decoded = &decodedDBOp{DBOp: in}
	err = s.decodeDBOp(decoded, blockNum)
	return
}

func (s *StreamedAbiCodec) decodeDBOp(op *decodedDBOp, blockNum uint32) error {
	latestAbi, err := s.getLatestAbi(op.Code, blockNum)
	if err != nil {
		return fmt.Errorf("fail to get ABI for decoding dbop in block: %d, error: %w", blockNum, err)
	}
	abi := latestAbi
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

	s.doUpdateABI(ABI{
		ABI:          abi,
		AbiBlockNum:  blockNum,
		Account:      actionTrace.GetData("account").String(),
		Irreversible: (step == pbbstream.ForkStep_STEP_UNDO),
	}, blockNum, step)
	return err
}

func (s *StreamedAbiCodec) doUpdateABI(abi ABI, blockNum uint32, step pbbstream.ForkStep) {
	if step == pbbstream.ForkStep_STEP_UNKNOWN {
		zlog.Warn("skip ABI update on unknown step", zap.Uint32("block_num", blockNum), zap.Int32("step", int32(step)))
		return
	}
	if step == pbbstream.ForkStep_STEP_UNDO {
		if latestAbi, found := s.latestABIs[abi.Account]; found {
			if blockNum > latestAbi.AbiBlockNum { // must have been replaced by a irreversible compaction
				zlog.Info("undo skipped as undo block > latest abi block", zap.Uint32("undo_block_num", blockNum), zap.Uint32("latest_block_num", latestAbi.AbiBlockNum))
				return
			}
			zlog.Info("undo actual/latest ABI", zap.Uint32("undo_block_num", blockNum), zap.Uint32("latest_block_num", latestAbi.AbiBlockNum))
			delete(s.latestABIs, abi.Account)
			if abiHistory, found := s.abiHistories[abi.Account]; found {
				if len(abiHistory) > 0 {
					// pop from history
					previousAbi := abiHistory[len(abiHistory)-1]
					zlog.Info("pop previous ABI from history on undo", zap.Uint32("undo_block_num", blockNum), zap.Uint32("previous_block_num", previousAbi.AbiBlockNum))
					s.latestABIs[abi.Account] = previousAbi
					s.abiHistories[abi.Account] = abiHistory[:len(abiHistory)-1]
					if len(s.abiHistories[abi.Account]) == 0 { // fix a testing compare issue
						delete(s.abiHistories, abi.Account)
					}
				}
			} else {
				zlog.Info("nothing pop from history (empty) on undo", zap.Uint32("undo_block_num", blockNum))
			}
		} else {
			zlog.Warn("undo skipped no latest abi", zap.Uint32("undo_block_num", blockNum))
			return
		}
	} else {
		newAbiItem := &abi
		abiHistory := s.abiHistories[abi.Account]
		latestABI := s.latestABIs[abi.Account]
		if latestABI != nil {
			if len(abiHistory) > 0 && abiHistory[len(abiHistory)-1].AbiBlockNum == latestABI.AbiBlockNum {
				abiHistory[len(abiHistory)-1] = latestABI
			} else {
				abiHistory = append(abiHistory, latestABI)
			}
			s.abiHistories[abi.Account] = abiHistory
		}
		s.latestABIs[abi.Account] = newAbiItem
	}
}

func (s *StreamedAbiCodec) resetCodecs() {
	zlog.Info("reset schema cache on reload static schema")
	s.codecCache = s.initStaticSchemas(make(map[CodecId]Codec))
}

func (s *StreamedAbiCodec) initStaticSchemas(cache map[CodecId]Codec) map[CodecId]Codec {
	for i, schema := range s.staticSchemas {
		zlog.Info("register static schema", zap.Int("index", i), zap.String("name", schema.Name))
		s.registerStaticSchema(cache, schema)
	}
	return cache
}

func (s *StreamedAbiCodec) registerStaticSchema(cache map[CodecId]Codec, schema MessageSchema) {
	codec, err := s.newCodec(schema)
	if err != nil {
		zlog.Error("initStaticSchemas fail to create codec", zap.String("schema", schema.Name), zap.Error(err))
		panic(fmt.Sprintf("initStaticSchemas fail to create codec %s", schema.Name))
	}
	cache[schema.AsCodecId()] = codec
}

func (s *StreamedAbiCodec) setSubjectCompatibilityToForward(subject string) error {
	compatibility := s.getCompatibility()

	zlog.Debug("set subject compatibility", zap.String("subject", subject), zap.String("compatibility", compatibility.String()))
	_, err := s.schemaRegistryClient.ChangeSubjectCompatibilityLevel(subject, compatibility)
	if err != nil {
		if strings.HasPrefix(err.Error(), "mock") {
			return nil
		}
		return fmt.Errorf("cannot change compatibility level of subject: '%s', error: %w", subject, err)
	}
	return err
}

func (s *StreamedAbiCodec) getCompatibility() srclient.CompatibilityLevel {
	var compatibility = s.compatibility
	if compatibility == "" {
		compatibility = DefaultCompatibility
	}
	return compatibility
}

func (s *StreamedAbiCodec) newCodec(messageSchema MessageSchema) (Codec, error) {
	messageSchema.Meta.Compatibility = s.getCompatibility().String()
	subject := fmt.Sprintf("%s.%s", messageSchema.Namespace, messageSchema.Name)
	jsonSchema, err := json.Marshal(messageSchema)
	if err != nil {
		return nil, err
	}
	zlog.Debug("register schema", zap.String("subject", subject), zap.ByteString("schema", jsonSchema))
	zlog.Debug("get compatibility level of subject's schema", zap.String("subject", subject))
	actualCompatibilityLevel, err := s.schemaRegistryClient.GetCompatibilityLevel(subject, true)
	unknownSubject := false
	if err != nil {
		unknownSubject = true
	} else if *actualCompatibilityLevel != s.getCompatibility() {
		err = s.setSubjectCompatibilityToForward(subject)
		if err != nil {
			return nil, err
		}
	}
	schema, err := s.schemaRegistryClient.CreateSchema(subject, string(jsonSchema), srclient.Avro)
	if err != nil {
		return nil, fmt.Errorf("CreateSchema on subject: '%s', schema:\n%s error: %w", subject, string(jsonSchema), err)
	}
	if unknownSubject {
		err = s.setSubjectCompatibilityToForward(subject)
		if err != nil {
			return nil, err
		}
	}

	zlog.Debug("create kafka avro codec", zap.String("subject", subject), zap.Int("ID", schema.ID()))
	ac, err := goavro.NewCodecWithConverters(schema.Schema(), schemaTypeConverters)
	if err != nil {
		return nil, fmt.Errorf("goavro.NewCodecWithConverters error: %w, with schema %s", err, string(jsonSchema))
	}
	codec := NewKafkaAvroCodec(s.schemaRegistryURL, schema, ac)
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
