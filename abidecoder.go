package dkafka

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	pbabicodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/abicodec/v1"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/eoscanada/eos-go"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
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
	UpdateABI(blockNum uint32, step pbbstream.ForkStep, trxID string, actionTrace *pbcodec.ActionTrace) error
}

type JsonABICodec struct {
	*ABIDecoder
	codec   Codec
	account string
}

func (c *JsonABICodec) GetCodec(name string, blockNum uint32) (Codec, error) {
	return c.codec, nil
}

func (c *JsonABICodec) UpdateABI(_ uint32, _ pbbstream.ForkStep, _ string, _ *pbcodec.ActionTrace) error {
	return nil
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

// ABIDecoder legacy abi codec does not support schema registry
type ABIDecoder struct {
	overrides   map[string]*ABI
	abiCodecCli pbabicodec.DecoderClient
	abisCache   map[string]*ABI
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

func DecodeABI(trxID string, account string, hexData string) (abi *eos.ABI, err error) {
	if hexData == "" {
		zlog.Warn("empty ABI in 'setabi' action", zap.String("account", account), zap.String("transaction_id", trxID))
		return nil, fmt.Errorf("empty ABI in 'setabi' action, account: %s, trx: %s", account, trxID)
	}
	abiData, err := hex.DecodeString(hexData)
	if err != nil {
		zlog.Error("failed to hex decode abi string", zap.String("account", account), zap.String("transaction_id", trxID), zap.Error(err))
		return nil, fmt.Errorf("failed to hex decode abi string, account: %s, trx: %s, error: %w", account, trxID, err)
	}
	err = eos.UnmarshalBinary(abiData, &abi)
	if err != nil {
		abiHexCutAt := math.Min(50, float64(len(hexData)))

		zlog.Error("failed to unmarshal abi from binary",
			zap.String("account", account),
			zap.String("transaction_id", trxID),
			zap.String("abi_hex_prefix", hexData[0:int(abiHexCutAt)]),
			zap.Error(err),
		)

		return nil, fmt.Errorf("failed to unmarshal abi from binary, account: %s, trx: %s, error: %w", account, trxID, err)
	}
	zlog.Debug("setting new abi", zap.String("account", account), zap.String("transaction_id", trxID))
	return
}
