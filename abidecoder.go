package dkafka

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	pbabicodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/abicodec/v1"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/eoscanada/eos-go"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"go.uber.org/zap"
)

type ABI struct {
	*eos.ABI
	AbiBlockNum uint32
}

type ABICodec interface {
	IsNOOP() bool
	DecodeDBOp(in *pbcodec.DBOp, blockNum uint32) (*decodedDBOp, error)
	GetCodec(name string, blockNum uint32) (Codec, error)
	Refresh(blockNum uint32) error
	Reset()
}

type JsonABICodec struct {
	*ABIDecoder
	codec   Codec
	account string
}

func (c *JsonABICodec) GetCodec(name string, blockNum uint32) (Codec, error) {
	return c.codec, nil
}

func (c *JsonABICodec) Refresh(blockNum uint32) error {
	return nil
}

func (c *JsonABICodec) Reset() {
	// nothing to do
}

func NewJsonABICodec(
	decoder *ABIDecoder,
	account string,
) ABICodec {
	return &JsonABICodec{
		decoder,
		NewJSONCodec(),
		account,
	}
}

// MessageSchemaSupplier is a function that return the specific message schema
// of a given entity (i.e. table or action)
type MessageSchemaSupplier = func(string, *ABI) (MessageSchema, error)

type KafkaAvroABICodec struct {
	*ABIDecoder
	getSchema            MessageSchemaSupplier
	schemaRegistryClient srclient.ISchemaRegistryClient
	account              string
	codecCache           map[string]Codec
	schemaRegistryURL    string
}

func (c *KafkaAvroABICodec) GetCodec(name string, blockNum uint32) (Codec, error) {
	if codec, found := c.codecCache[name]; found {
		return codec, nil
	}

	abi, err := c.abi(c.account, blockNum, false)
	if err != nil {
		return nil, err
	}
	zlog.Debug("create schema from abi", zap.String("entry", name))
	messageSchema, err := c.getSchema(name, abi)
	if err != nil {
		return nil, err
	}
	codec, err := c.newCodec(messageSchema)
	if err != nil {
		return nil, fmt.Errorf("KafkaAvroABICodec.GetCodec fail to create codec for schema %s, error: %w", messageSchema.Name, err)
	}
	zlog.Debug("register codec into cache", zap.String("name", name))
	c.codecCache[name] = codec
	return codec, nil
}

func (c *KafkaAvroABICodec) setSubjectCompatibilityToForward(subject string) error {
	zlog.Debug("Set subject compatibility to FORWARD", zap.String("subject", subject), zap.String("compatibility", string(srclient.Forward)))
	_, err := c.schemaRegistryClient.ChangeSubjectCompatibilityLevel(subject, srclient.Forward)
	if err != nil {
		if strings.HasPrefix(err.Error(), "mock") {
			return nil
		}
		return fmt.Errorf("cannot change compatibility level of subject: '%s', error: %w", subject, err)
	}
	return err
}

func (c *KafkaAvroABICodec) newCodec(messageSchema MessageSchema) (Codec, error) {
	subject := fmt.Sprintf("%s.%s", messageSchema.Namespace, messageSchema.Name)
	jsonSchema, err := json.Marshal(messageSchema)
	if err != nil {
		return nil, err
	}
	if traceEnabled {
		zlog.Debug("register schema", zap.String("subject", subject), zap.ByteString("schema", jsonSchema))
	}
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
	zlog.Debug("register schema", zap.String("subject", subject))
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

	zlog.Debug("create kafka avro codec", zap.Int("ID", schema.ID()))
	ac, err := goavro.NewCodecWithConverters(schema.Schema(), schemaTypeConverters)
	if err != nil {
		return nil, fmt.Errorf("goavro.NewCodecWithConverters error: %w, with schema %s", err, string(jsonSchema))
	}
	codec := NewKafkaAvroCodec(c.schemaRegistryURL, schema, ac)
	return codec, nil
}

func (c *KafkaAvroABICodec) Refresh(blockNum uint32) error {
	_, err := c.abi(c.account, blockNum, true)
	return err
}

func (c *KafkaAvroABICodec) Reset() {
	c.onReload()
	c.abisCache = make(map[string]*ABI)
}

func (c *KafkaAvroABICodec) onReload() {
	zlog.Info("clear schema cache on reload static schema")
	c.codecCache = c.initStaticSchema(make(map[string]Codec))
}

func (c *KafkaAvroABICodec) initStaticSchema(cache map[string]Codec) map[string]Codec {
	codec, err := c.newCodec(CheckpointMessageSchema)
	if err != nil {
		zlog.Error("initStaticSchema fail to create codec", zap.String("schema", dkafkaCheckpoint), zap.Error(err))
		panic(1)
	}
	cache[dkafkaCheckpoint] = codec
	return cache
}

func NewKafkaAvroABICodec(
	decoder *ABIDecoder,
	getSchema MessageSchemaSupplier,
	schemaRegistryClient srclient.ISchemaRegistryClient,
	account string,
	schemaRegistryURL string,
) ABICodec {
	codec := &KafkaAvroABICodec{
		decoder,
		getSchema,
		schemaRegistryClient,
		account,
		make(map[string]Codec, 5),
		schemaRegistryURL,
	}
	decoder.onReload = codec.onReload
	zlog.Info("NewKafkaAvroABICodec() => call onReload()", zap.String("account", account))
	codec.onReload()
	return codec
}

// ABIDecoder legacy abi codec does not support schema registry
type ABIDecoder struct {
	overrides   map[string]*ABI
	abiCodecCli pbabicodec.DecoderClient
	abisCache   map[string]*ABI
	onReload    func()
	context     context.Context
}

func (a *ABIDecoder) IsNOOP() bool {
	return a.overrides == nil && a.abiCodecCli == nil
}

func ParseABIFileSpecs(specs []string) (abiFileSpecs map[string]string, err error) {
	abiFileSpecs = make(map[string]string)
	for _, ext := range specs {
		account, abiPath, err := ParseABIFileSpec(ext)
		if err != nil {
			break
		}
		abiFileSpecs[account] = abiPath
	}
	return
}

func ParseABIFileSpec(spec string) (account string, abiPath string, err error) {
	kv := strings.SplitN(spec, ":", 2)
	if len(kv) != 2 {
		err = fmt.Errorf("invalid value for local ABI file: %s", spec)
	} else {
		account = kv[0]
		abiPath = kv[1]
	}
	return
}

// LoadABIFiles will load ABIs for different accounts from JSON files
func LoadABIFiles(abiFiles map[string]string) (map[string]*ABI, error) {
	out := make(map[string]*ABI)
	for contract, abiFile := range abiFiles {
		abi, err := LoadABIFile(abiFile)
		if err != nil {
			return nil, fmt.Errorf("reading abi file %s: %w", abiFile, err)
		}
		out[contract] = abi
	}
	return out, nil
}

func LoadABIFile(abiFile string) (*ABI, error) {
	kv := strings.SplitN(abiFile, ":", 2) //[abiFilePath] - [abiFilePath, abiNumber]
	var abiPath = abiFile
	var abiBlockNum = uint64(0)
	if len(kv) == 2 {
		abiPath = kv[0]
		var err error
		abiBlockNum, err = strconv.ParseUint(kv[1], 10, 32)
		if err != nil {
			return nil, err
		}
	}
	f, err := os.Open(abiPath)
	if err == nil {
		defer f.Close()
		eosAbi, err := eos.NewABI(f)
		abi := &ABI{eosAbi, uint32(abiBlockNum)}
		return abi, err
	}
	return nil, err
}

func NewABIDecoder(
	overrides map[string]*ABI,
	abiCodecCli pbabicodec.DecoderClient,
	context context.Context,
) *ABIDecoder {
	return &ABIDecoder{
		overrides:   overrides,
		abiCodecCli: abiCodecCli,
		abisCache:   make(map[string]*ABI),
		onReload:    func() {},
		context:     context,
	}
}

type decodedDBOp struct {
	*pbcodec.DBOp
	NewJSON map[string]interface{} `json:"new_json,omitempty"`
	OldJSON map[string]interface{} `json:"old_json,omitempty"`
}

func (dbOp *decodedDBOp) asMap(dbOpRecordName string, dbOpIndex int) map[string]interface{} {
	asMap := newDBOpBasic(dbOp.DBOp, dbOpIndex)
	addOptional(&asMap, "old_json", dbOp.OldJSON)
	addOptional(&asMap, "new_json", dbOp.NewJSON)
	// addOptionalRecord(&asMap, "old_json", dbOpRecordName, dbOp.OldJSON)
	// addOptionalRecord(&asMap, "new_json", dbOpRecordName, dbOp.NewJSON)
	return asMap
}

func addOptional(m *map[string]interface{}, key string, value map[string]interface{}) {
	if len(value) > 0 {
		(*m)[key] = value
	}
}

// func addOptionalRecord(m *map[string]interface{}, key string, rType string, value map[string]interface{}) {
// 	if len(value) > 0 {
// 		addOptional(m, key, map[string]interface{}{rType: value})
// 	}
// }

func (a *ABIDecoder) abi(contract string, blockNum uint32, forceRefresh bool) (*ABI, error) {
	if a.overrides != nil {
		if abi, ok := a.overrides[contract]; ok {
			return abi, nil
		}
	}

	if a.abiCodecCli == nil {
		return nil, fmt.Errorf("unable to get abi for contract %q", contract)
	}

	if !forceRefresh {
		if abiObj, ok := a.abisCache[contract]; ok {
			if abiObj.AbiBlockNum < blockNum {
				return abiObj, nil
			}
		}
	}
	zlog.Info("ABIDecoder.abi(...) => call onReload()", zap.String("contract", contract), zap.Uint32("block_num", blockNum), zap.Bool("force_refresh", forceRefresh))
	a.onReload()
	resp, err := a.abiCodecCli.GetAbi(a.context, &pbabicodec.GetAbiRequest{
		Account:    contract,
		AtBlockNum: blockNum,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get abi for contract %q: %w", contract, err)
	}

	var eosAbi *eos.ABI
	err = json.Unmarshal([]byte(resp.JsonPayload), &eosAbi)
	if err != nil {
		return nil, fmt.Errorf("unable to decode abi for contract %q: %w", contract, err)
	}
	var abi = ABI{eosAbi, resp.AbiBlockNum}
	zlog.Info("new ABI loaded", zap.String("contract", contract), zap.Uint32("block_num", blockNum), zap.Uint32("abi_block_num", abi.AbiBlockNum))
	// store abi in cache for late uses
	a.abisCache[contract] = &abi
	return &abi, nil
}

func (a *ABIDecoder) decodeDBOp(op *decodedDBOp, blockNum uint32, forceRefresh bool) error {
	abi, err := a.abi(op.Code, blockNum, forceRefresh)
	if err != nil {
		return fmt.Errorf("decoding dbop in block %d: %w", blockNum, err)
	}
	tableDef := abi.TableForName(eos.TableName(op.TableName))
	if tableDef == nil {
		return fmt.Errorf("table %s not present in ABI for contract %s", op.TableName, op.Code)
	}

	if len(op.NewData) > 0 {
		asMap, err := abi.DecodeTableRowTypedNative(tableDef.Type, op.NewData)
		if err != nil {
			return fmt.Errorf("decode row: %w", err)
		}
		op.NewJSON = asMap
	}
	if len(op.OldData) > 0 {
		asMap, err := abi.DecodeTableRowTypedNative(tableDef.Type, op.OldData)
		if err != nil {
			return fmt.Errorf("decode row: %w", err)
		}
		op.OldJSON = asMap
	}
	return nil
}

func (a *ABIDecoder) DecodeDBOp(in *pbcodec.DBOp, blockNum uint32) (decoded *decodedDBOp, err error) {
	decoded = &decodedDBOp{DBOp: in}
	err = a.decodeDBOp(decoded, blockNum, false)
	if err != nil {
		err = a.decodeDBOp(decoded, blockNum, true) //force refreshing ABI from cache
	}
	return
}

func (a *ABIDecoder) DecodeDBOps(in []*pbcodec.DBOp, blockNum uint32) (decodedDBOps []*decodedDBOp, err error) {
	for _, op := range in {
		decoded := &decodedDBOp{DBOp: op}
		decodedDBOps = append(decodedDBOps, decoded)
	}

	if a.IsNOOP() {
		return
	}

	var errors []error
	for _, op := range decodedDBOps {
		err := a.decodeDBOp(op, blockNum, false)
		if err != nil {
			err = a.decodeDBOp(op, blockNum, true) //force refreshing ABI from cache
		}
		if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		errorStr := ""
		for _, e := range errors {
			errorStr = fmt.Sprintf("%s; %s", errorStr, e.Error())
		}
		err = fmt.Errorf(errorStr)
	}
	return
}

// type abiItem struct {
// 	abi      *eos.ABI
// 	blockNum uint32
// }

func DecodeABIAtBlock(trxID string, actionTrace *pbcodec.ActionTrace) (*eos.ABI, error) {
	account := actionTrace.GetData("account").String()
	hexABI := actionTrace.GetData("abi")
	if !hexABI.Exists() {
		zlog.Warn("'setabi' action data payload not present", zap.String("account", account), zap.String("transaction_id", trxID))
		return nil, fmt.Errorf("setabi' action data payload not present. account: %s, transaction_id: %s ", account, trxID)
	}
	hexData := hexABI.String()
	return DecodeABI(trxID, account, hexData)
}

func DecodeABI(trxID string, account string, hexData string) (abi *eos.ABI, err error) {
	if hexData == "" {
		zlog.Warn("empty ABI in 'setabi' action", zap.String("account", account), zap.String("transaction_id", trxID))
		return
	}
	abiData, err := hex.DecodeString(hexData)
	if err != nil {
		zlog.Error("failed to hex decode abi string", zap.String("account", account), zap.String("transaction_id", trxID), zap.Error(err))
		return // do not return the error. Worker will retry otherwise
	}
	err = eos.UnmarshalBinary(abiData, &abi)
	if err != nil {
		zlog.Error("failed to hex decode abi string", zap.String("account", account), zap.String("transaction_id", trxID), zap.Error(err))
		return // do not return the error. Worker will retry otherwise
	}
	return
}
