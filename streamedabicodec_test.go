package dkafka

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	pbabicodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/abicodec/v1"
	"github.com/eoscanada/eos-go"
	"github.com/riferrei/srclient"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"google.golang.org/grpc"
)

type DecoderClientStub struct {
	err      error
	response *pbabicodec.Response
}

func (d *DecoderClientStub) DecodeTable(ctx context.Context, in *pbabicodec.DecodeTableRequest, opts ...grpc.CallOption) (r *pbabicodec.Response, e error) {
	return
}
func (d *DecoderClientStub) DecodeAction(ctx context.Context, in *pbabicodec.DecodeActionRequest, opts ...grpc.CallOption) (r *pbabicodec.Response, e error) {
	return
}
func (d *DecoderClientStub) GetAbi(ctx context.Context, in *pbabicodec.GetAbiRequest, opts ...grpc.CallOption) (*pbabicodec.Response, error) {
	return d.response, d.err
}

func TestDfuseAbiRepository_GetAbi(t *testing.T) {
	type args struct {
		contract string
		blockNum uint32
	}
	tests := []struct {
		name    string
		sut     *DfuseAbiRepository
		args    args
		want    *ABI
		wantErr bool
	}{
		{
			name: "overrides",
			sut: &DfuseAbiRepository{
				overrides: map[string]*ABI{"test": {
					ABI:         &eos.ABI{Version: "1.2.3"},
					AbiBlockNum: 42,
				}},
				abiCodecCli: nil,
				context:     nil,
			},
			args: args{
				contract: "test",
				blockNum: 101,
			},
			want: &ABI{
				ABI:         &eos.ABI{Version: "1.2.3"},
				AbiBlockNum: 42,
			},
			wantErr: false,
		},
		{
			name: "error-missing-client",
			sut: &DfuseAbiRepository{
				overrides:   nil,
				abiCodecCli: nil,
				context:     nil,
			},
			args: args{
				contract: "test",
				blockNum: 101,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error-dfuse-call",
			sut: &DfuseAbiRepository{
				overrides: nil,
				abiCodecCli: &DecoderClientStub{
					err: fmt.Errorf("test"),
				},
				context: nil,
			},
			args: args{
				contract: "test",
				blockNum: 101,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error-dfuse-response-decode",
			sut: &DfuseAbiRepository{
				overrides: nil,
				abiCodecCli: &DecoderClientStub{
					err: nil,
					response: &pbabicodec.Response{
						AbiBlockNum: 42,
						JsonPayload: "invalid json",
					},
				},
				context: nil,
			},
			args: args{
				contract: "test",
				blockNum: 101,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "valid-dfuse-response",
			sut: &DfuseAbiRepository{
				overrides: nil,
				abiCodecCli: &DecoderClientStub{
					err: nil,
					response: &pbabicodec.Response{
						AbiBlockNum: 42,
						JsonPayload: "{\"version\":\"1.2.3\"}",
					},
				},
				context: nil,
			},
			args: args{
				contract: "test",
				blockNum: 101,
			},
			want: &ABI{
				ABI:         &eos.ABI{Version: "1.2.3"},
				AbiBlockNum: 42,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := tt.sut
			got, err := a.GetAbi(tt.args.contract, tt.args.blockNum)
			if (err != nil) != tt.wantErr {
				t.Errorf("DfuseAbiRepository.GetAbi() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DfuseAbiRepository.GetAbi() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDfuseAbiRepository_IsNOOP(t *testing.T) {
	tests := []struct {
		name string
		sut  *DfuseAbiRepository
		want bool
	}{
		{
			name: "noop",
			sut:  &DfuseAbiRepository{},
			want: true,
		},
		{
			name: "noop",
			sut: &DfuseAbiRepository{
				overrides: make(map[string]*ABI),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.sut
			if got := b.IsNOOP(); got != tt.want {
				t.Errorf("DfuseAbiRepository.IsNOOP() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStreamedABICodec_doUpdateABI(t *testing.T) {
	type args struct {
		abi      *eos.ABI
		blockNum uint32
		step     pbbstream.ForkStep
	}
	tests := []struct {
		name string
		sut  *StreamedAbiCodec
		args args
		want *StreamedAbiCodec
	}{
		{
			name: "empty-irreversible",
			sut:  &StreamedAbiCodec{},
			args: args{
				abi:      &eos.ABI{Version: "123"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_IRREVERSIBLE,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "123"},
					blockNum:     42,
					irreversible: true,
				},
			},
		},
		{
			name: "empty-new",
			sut:  &StreamedAbiCodec{},
			args: args{
				abi:      &eos.ABI{Version: "123"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_NEW,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "123"},
					blockNum:     42,
					irreversible: false,
				},
			},
		},
		{
			name: "not-empty-irreversible",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: false,
				},
				abiHistory: []*AbiItem{{abi: &eos.ABI{Version: "123"},
					blockNum:     1,
					irreversible: true,
				}},
			},
			args: args{
				abi:      &eos.ABI{Version: "456"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_IRREVERSIBLE,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
					{
						abi:          &eos.ABI{Version: "456"},
						blockNum:     42,
						irreversible: false,
					},
				},
			},
		},
		{
			name: "irreversible-compaction",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
					{
						abi:          &eos.ABI{Version: "456"},
						blockNum:     42,
						irreversible: false,
					},
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "789"},
				blockNum: 64,
				step:     pbbstream.ForkStep_STEP_NEW,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "789"},
					blockNum:     64,
					irreversible: false,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
					{
						abi:          &eos.ABI{Version: "456"},
						blockNum:     42,
						irreversible: true,
					},
				},
			},
		},
		{
			name: "irreversible-only",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "789"},
				blockNum: 64,
				step:     pbbstream.ForkStep_STEP_IRREVERSIBLE,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "789"},
					blockNum:     64,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
					{
						abi:          &eos.ABI{Version: "456"},
						blockNum:     42,
						irreversible: true,
					},
				},
			},
		},
		{
			name: "undo",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: false,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "456"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "123"},
					blockNum:     1,
					irreversible: true,
				},
			},
		},
		{
			name: "unknown",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: false,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "456"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNKNOWN,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: false,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
				},
			},
		},
		{
			name: "undo-empty",
			sut:  &StreamedAbiCodec{},
			args: args{
				abi:      &eos.ABI{Version: "456"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{},
		},
		{
			name: "undo-gt-latest",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "123"},
					blockNum:     24,
					irreversible: true,
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "456"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "123"},
					blockNum:     24,
					irreversible: true,
				},
			},
		},
		{
			name: "undo-long-history",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "789"},
					blockNum:     42,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
					{
						abi:          &eos.ABI{Version: "456"},
						blockNum:     24,
						irreversible: true,
					},
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "789"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     24,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.sut.doUpdateABI(tt.args.abi, tt.args.blockNum, tt.args.step)
			assertEqual(t, tt.sut, tt.want)
		})
	}
}

func assertEqual(t *testing.T, actual, expected *StreamedAbiCodec) {
	if !reflect.DeepEqual(actual, expected) {
		equal := assertFieldsEqual(t, "abiHistory", actual.abiHistory, expected.abiHistory) &&
			assertFieldsEqual(t, "latestABI.abi", actual.latestABI.abi, expected.latestABI.abi) &&
			assertFieldsEqual(t, "latestABI.blockNum", actual.latestABI.blockNum, expected.latestABI.blockNum) &&
			assertFieldsEqual(t, "latestABI.irreversible", actual.latestABI.irreversible, expected.latestABI.irreversible)
		if equal {
			// the test above did detect the diff use default solution
			t.Errorf("doUpdateABI() = %v, want %v", actual, expected)
		}
	}
}

func assertFieldsEqual(t *testing.T, path string, actual, expected interface{}) bool {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("%s:\nactual = %v\nwant   = %v", path, actual, expected)
		return false
	}
	return true
}

type AbiRepositoryStub struct {
	noop bool
	abi  *ABI
	err  error
}

func (s *AbiRepositoryStub) IsNOOP() bool {
	return s.noop
}

func (s *AbiRepositoryStub) GetAbi(contract string, blockNum uint32) (*ABI, error) {
	return s.abi, s.err
}

func TestStreamedAbiCodec_IsNOOP(t *testing.T) {

	tests := []struct {
		name string
		sut  *StreamedAbiCodec
		want bool
	}{
		{
			name: "noop",
			sut: &StreamedAbiCodec{
				bootstrapper: &AbiRepositoryStub{
					noop: false,
				},
			},
			want: false,
		},
		{
			name: "operational",
			sut: &StreamedAbiCodec{
				bootstrapper: &AbiRepositoryStub{
					noop: true,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.sut
			if got := s.IsNOOP(); got != tt.want {
				t.Errorf("StreamedAbiCodec.IsNOOP() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStreamedAbiCodec_GetCodec(t *testing.T) {
	var localABIFiles = map[string]string{
		"eosio.nft.ft": "testdata/eosio.nft.ft.abi:1",
	}
	abiFiles, _ := LoadABIFiles(localABIFiles)

	dummyCodec := NewJSONCodec()
	msg := MessageSchemaGenerator{
		Namespace: "test",
		Version:   "",
		Account:   "eosio.nft.ft",
		Source:    "test",
	}
	type args struct {
		name     string
		blockNum uint32
	}
	tests := []struct {
		name    string
		sut     ABICodec
		args    args
		want    Codec
		wantErr bool
	}{
		{
			name: "cached-codec",
			sut: &StreamedAbiCodec{
				bootstrapper: nil,
				latestABI:    nil,
				abiHistory:   nil,
				getSchema: func(string, *ABI) (MessageSchema, error) {
					return MessageSchema{}, nil
				},
				schemaRegistryClient: nil,
				account:              "test",
				codecCache:           map[string]Codec{"TestTable": dummyCodec},
				schemaRegistryURL:    "http://localhost:8083",
			},
			args: args{
				name:     "TestTable",
				blockNum: 42,
			},
			want:    dummyCodec,
			wantErr: false,
		},
		{
			name: "bootstrap-abi-error",
			sut: &StreamedAbiCodec{
				bootstrapper: &AbiRepositoryStub{
					abi: nil,
					err: fmt.Errorf("bootstrap-abi-error"),
				},
				latestABI:  nil,
				abiHistory: nil,
				getSchema: func(string, *ABI) (MessageSchema, error) {
					return MessageSchema{}, nil
				},
				schemaRegistryClient: nil,
				account:              "test",
				codecCache:           map[string]Codec{},
				schemaRegistryURL:    "http://localhost:8083",
			},
			args: args{
				name:     "TestTable",
				blockNum: 42,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "dkafka-checkpoint-codec",
			sut: NewStreamedAbiCodec(
				&AbiRepositoryStub{
					abi: nil,
					err: nil,
				},
				msg.getTableSchema,
				srclient.CreateMockSchemaRegistryClient("mock://TestKafkaAvroABICodec_GetCodec"),
				"eosio.nft.ft",
				"mock://TestKafkaAvroABICodec_GetCodec",
			),
			args: args{
				name:     dkafkaCheckpoint,
				blockNum: 42,
			},
			want: &KafkaAvroCodec{
				schemaURLTemplate: "mock://TestKafkaAvroABICodec_GetCodec/schemas/ids/%d",
				schema: RegisteredSchema{
					id:      1,
					schema:  "{\"type\":\"record\",\"name\":\"DKafkaCheckpoint\",\"namespace\":\"io.dkafka\",\"doc\":\"Periodically emitted checkpoint used to save the current position\",\"fields\":[{\"name\":\"step\",\"doc\":\"Step of the current block value can be: 1(New),2(Undo),3(Redo),4(Handoff),5(Irreversible),6(Stalled)\\n - 1(New): First time we're seeing this block\\n - 2(Undo): We are undoing this block (it was done previously)\\n - 4(Redo): We are redoing this block (it was done previously)\\n - 8(Handoff): The block passed a handoff from one producer to another\\n - 16(Irreversible): This block passed the LIB barrier and is in chain\\n - 32(Stalled): This block passed the LIB and is definitely forked out\\n\",\"type\":\"int\"},{\"name\":\"block\",\"type\":{\"type\":\"record\",\"name\":\"BlockRef\",\"namespace\":\"io.dkafka\",\"doc\":\"BlockRef represents a reference to a block and is mainly define as the pair \\u003cBlockID, BlockNum\\u003e\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"num\",\"type\":\"long\"}]}},{\"name\":\"headBlock\",\"type\":\"BlockRef\"},{\"name\":\"lastIrreversibleBlock\",\"type\":\"BlockRef\"},{\"name\":\"time\",\"type\":{\"logicalType\":\"timestamp-millis\",\"type\":\"long\"}}],\"meta\":{\"compatibility\":\"FORWARD\",\"type\":\"notification\",\"version\":\"1.0.0\",\"source\":\"dkafka-cli\",\"domain\":\"dkafka\"}}",
					version: 1,
				},
			},
			wantErr: false,
		},
		{
			name: "factory.a-codec",
			sut: NewStreamedAbiCodec(
				&DfuseAbiRepository{
					overrides:   abiFiles,
					abiCodecCli: nil,
					context:     nil,
				},
				msg.getTableSchema,
				srclient.CreateMockSchemaRegistryClient("mock://TestKafkaAvroABICodec_GetCodec"),
				"eosio.nft.ft",
				"mock://TestKafkaAvroABICodec_GetCodec",
			),
			args: args{
				name:     "factory.a",
				blockNum: 42,
			},
			want: &KafkaAvroCodec{
				schemaURLTemplate: "mock://TestKafkaAvroABICodec_GetCodec/schemas/ids/%d",
				schema: RegisteredSchema{
					id:      2,
					schema:  "{\"type\":\"record\",\"name\":\"FactoryATableNotification\",\"namespace\":\"test\",\"fields\":[{\"name\":\"context\",\"type\":{\"type\":\"record\",\"name\":\"NotificationContext\",\"namespace\":\"io.dkafka\",\"fields\":[{\"name\":\"block_num\",\"type\":\"long\"},{\"name\":\"block_id\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"executed\",\"type\":\"boolean\"},{\"name\":\"block_step\",\"type\":\"string\"},{\"name\":\"correlation\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Correlation\",\"namespace\":\"io.dkafka\",\"fields\":[{\"name\":\"payer\",\"type\":\"string\"},{\"name\":\"id\",\"type\":\"string\"}]}],\"default\":null},{\"name\":\"trx_id\",\"type\":\"string\"},{\"name\":\"time\",\"type\":{\"logicalType\":\"timestamp-millis\",\"type\":\"long\"}},{\"name\":\"cursor\",\"type\":\"string\"}]}},{\"name\":\"action\",\"type\":{\"type\":\"record\",\"name\":\"ActionInfoBasic\",\"namespace\":\"io.dkafka\",\"fields\":[{\"name\":\"account\",\"type\":\"string\"},{\"name\":\"receiver\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"global_seq\",\"type\":\"long\"},{\"name\":\"authorizations\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}},{\"name\":\"db_op\",\"type\":{\"type\":\"record\",\"name\":\"FactoryATableOpInfo\",\"fields\":[{\"name\":\"operation\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"action_index\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"index\",\"type\":\"int\"},{\"name\":\"code\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"scope\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"primary_key\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"old_payer\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"new_payer\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"old_data\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"new_data\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"old_json\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"FactoryATableOp\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"asset_manager\",\"type\":\"string\"},{\"name\":\"asset_creator\",\"type\":\"string\"},{\"name\":\"conversion_rate_oracle_contract\",\"type\":\"string\"},{\"name\":\"chosen_rate\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Asset\",\"namespace\":\"eosio\",\"convert\":\"eosio.Asset\",\"fields\":[{\"name\":\"amount\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":32,\"scale\":8}},{\"name\":\"symbol\",\"type\":\"string\"},{\"name\":\"precision\",\"type\":\"int\"}]}}},{\"name\":\"minimum_resell_price\",\"type\":\"eosio.Asset\"},{\"name\":\"resale_shares\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ResaleShare\",\"fields\":[{\"name\":\"receiver\",\"type\":\"string\"},{\"name\":\"basis_point\",\"type\":\"int\"}]}}},{\"name\":\"mintable_window_start\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"mintable_window_end\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"trading_window_start\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"trading_window_end\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"recall_window_start\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"recall_window_end\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"lockup_time\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"conditionless_receivers\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"stat\",\"type\":\"int\"},{\"name\":\"meta_uris\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"meta_hash\",\"type\":\"bytes\"},{\"name\":\"max_mintable_tokens\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"minted_tokens_no\",\"type\":\"long\"},{\"name\":\"existing_tokens_no\",\"type\":\"long\"}]}],\"default\":null},{\"name\":\"new_json\",\"type\":[\"null\",\"FactoryATableOp\"],\"default\":null}]}}],\"meta\":{\"compatibility\":\"FORWARD\",\"type\":\"notification\",\"version\":\"0.1.0\",\"source\":\"test\",\"domain\":\"eosio.nft.ft\"}}",
					version: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.sut
			got, err := s.GetCodec(tt.args.name, tt.args.blockNum)
			if (err != nil) != tt.wantErr {
				t.Errorf("StreamedAbiCodec.GetCodec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			expectedAvroCodec, ok := tt.want.(*KafkaAvroCodec)
			if ok {
				actualAvroCodec := got.(KafkaAvroCodec)
				assertFieldsEqual(t, "KafkaAvroCodec.schemaURLTemplate", actualAvroCodec.schemaURLTemplate, expectedAvroCodec.schemaURLTemplate)
				assertFieldsEqual(t, "KafkaAvroCodec.schema.id", actualAvroCodec.schema.id, expectedAvroCodec.schema.id)
				assertFieldsEqual(t, "KafkaAvroCodec.schema.schema", actualAvroCodec.schema.schema, expectedAvroCodec.schema.schema)
				assertFieldsEqual(t, "KafkaAvroCodec.schema.version", actualAvroCodec.schema.version, expectedAvroCodec.schema.version)
				if actualAvroCodec.schema.codec == nil {
					t.Error("KafkaAvroCodec.schema.codec should not be nil")
				}
			} else if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StreamedAbiCodec.GetCodec() = %v, want %v", got, tt.want)
			}
		})
	}
}
