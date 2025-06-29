package dkafka

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	pbabicodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/abicodec/v1"
	"github.com/eoscanada/eos-go"
	"github.com/go-test/deep"
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
				ABI:          &eos.ABI{Version: "1.2.3"},
				Account:      "test",
				AbiBlockNum:  42,
				Irreversible: true,
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
		abi      ABI
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
			sut: &StreamedAbiCodec{
				latestABIs:   make(map[string]*ABI),
				abiHistories: make(map[string][]*ABI),
				codecCache:   make(map[CodecId]Codec),
			},
			args: args{
				abi: ABI{
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: true,
				},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_IRREVERSIBLE,
			},
			want: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: true,
				},
				},
				abiHistories: make(map[string][]*ABI),
				codecCache:   make(map[CodecId]Codec),
			},
		},
		{
			name: "empty-new",
			sut: &StreamedAbiCodec{
				latestABIs:   make(map[string]*ABI),
				abiHistories: make(map[string][]*ABI),
				codecCache:   make(map[CodecId]Codec),
			},
			args: args{
				abi: ABI{
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: false,
				},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_NEW,
			},
			want: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: false,
				},
				},
				abiHistories: make(map[string][]*ABI),
				codecCache:   make(map[CodecId]Codec),
			},
		},
		{
			name: "not-empty-irreversible",
			sut: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: false,
				},
				},
				abiHistories: map[string][]*ABI{"eosio": {{ABI: &eos.ABI{Version: "123"},
					AbiBlockNum:  1,
					Irreversible: true,
					Account:      "eosio",
				}}},
				codecCache: make(map[CodecId]Codec),
			},
			args: args{
				abi: ABI{
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: true,
				},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_IRREVERSIBLE,
			},
			want: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Irreversible: true,
					Account:      "eosio",
				},
				},
				abiHistories: map[string][]*ABI{"eosio": {{
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  1,
					Irreversible: true,
					Account:      "eosio",
				},
					{
						ABI:          &eos.ABI{Version: "456"},
						AbiBlockNum:  42,
						Irreversible: false,
						Account:      "eosio",
					},
				},
				},
				codecCache: make(map[CodecId]Codec),
			},
		},
		{
			name: "irreversible-compaction",
			sut: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Irreversible: true,
					Account:      "eosio",
				}},
				abiHistories: map[string][]*ABI{"eosio": {{
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  1,
					Irreversible: true,
					Account:      "eosio",
				},
					{
						ABI:          &eos.ABI{Version: "456"},
						AbiBlockNum:  42,
						Irreversible: false,
						Account:      "eosio",
					},
				},
				},
			},
			args: args{
				abi: ABI{
					ABI:          &eos.ABI{Version: "789"},
					AbiBlockNum:  64,
					Account:      "eosio",
					Irreversible: false,
				},
				blockNum: 64,
				step:     pbbstream.ForkStep_STEP_NEW,
			},
			want: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "789"},
					AbiBlockNum:  64,
					Irreversible: false,
					Account:      "eosio",
				},
				},
				abiHistories: map[string][]*ABI{"eosio": {{
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  1,
					Irreversible: true,
					Account:      "eosio",
				},
					{
						ABI:          &eos.ABI{Version: "456"},
						AbiBlockNum:  42,
						Irreversible: true,
						Account:      "eosio",
					},
				},
				},
			},
		},
		{
			name: "irreversible-only",
			sut: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Irreversible: true,
					Account:      "eosio",
				},
				},
				abiHistories: map[string][]*ABI{"eosio": {{
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  1,
					Irreversible: true,
					Account:      "eosio",
				},
				},
				},
			},
			args: args{
				// abi:      &eos.ABI{Version: "789"},
				abi: ABI{
					ABI:          &eos.ABI{Version: "789"},
					AbiBlockNum:  64,
					Account:      "eosio",
					Irreversible: true,
				},
				blockNum: 64,
				step:     pbbstream.ForkStep_STEP_IRREVERSIBLE,
			},
			want: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "789"},
					AbiBlockNum:  64,
					Irreversible: true,
					Account:      "eosio",
				},
				},
				abiHistories: map[string][]*ABI{"eosio": {
					{
						ABI:          &eos.ABI{Version: "123"},
						AbiBlockNum:  1,
						Irreversible: true,
						Account:      "eosio",
					},
					{
						ABI:          &eos.ABI{Version: "456"},
						AbiBlockNum:  42,
						Irreversible: true,
						Account:      "eosio",
					},
				},
				},
			},
		},
		{
			name: "undo",
			sut: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Irreversible: false,
					Account:      "eosio",
				},
				},
				abiHistories: map[string][]*ABI{"eosio": {
					{
						ABI:          &eos.ABI{Version: "123"},
						AbiBlockNum:  1,
						Irreversible: true,
						Account:      "eosio",
					},
				},
				},
			},
			args: args{
				abi: ABI{
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: false,
				},

				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  1,
					Irreversible: true,
					Account:      "eosio",
				},
				},
				abiHistories: make(map[string][]*ABI),
			},
		},
		{
			name: "unknown",
			sut: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Irreversible: false,
					Account:      "eosio",
				},
				},
				abiHistories: map[string][]*ABI{"eosio": {
					{
						ABI:          &eos.ABI{Version: "123"},
						AbiBlockNum:  1,
						Irreversible: true,
						Account:      "eosio",
					},
				},
				},
			},
			args: args{
				abi: ABI{
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: false,
				},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNKNOWN,
			},
			want: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Irreversible: false,
					Account:      "eosio",
				},
				},
				abiHistories: map[string][]*ABI{"eosio": {
					{
						ABI:          &eos.ABI{Version: "123"},
						AbiBlockNum:  1,
						Irreversible: true,
						Account:      "eosio",
					},
				},
				},
			},
		},
		{
			name: "undo-empty",
			sut: &StreamedAbiCodec{
				latestABIs:   make(map[string]*ABI),
				abiHistories: make(map[string][]*ABI),
			},
			args: args{
				// abi:      &eos.ABI{Version: "456"},
				abi: ABI{
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: false,
				},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{
				latestABIs:   make(map[string]*ABI),
				abiHistories: make(map[string][]*ABI),
			},
		},
		{
			name: "undo-gt-latest",
			sut: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  24,
					Irreversible: true,
					Account:      "eosio",
				},
				},
				abiHistories: make(map[string][]*ABI),
			},
			args: args{
				abi: ABI{
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: false,
				},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "123"},
					AbiBlockNum:  24,
					Irreversible: true,
					Account:      "eosio",
				},
				},
				abiHistories: make(map[string][]*ABI),
			},
		},
		{
			name: "undo-long-history",
			sut: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "789"},
					AbiBlockNum:  42,
					Irreversible: false,
					Account:      "eosio",
				},
				},
				abiHistories: map[string][]*ABI{"eosio": {
					{
						ABI:          &eos.ABI{Version: "123"},
						AbiBlockNum:  1,
						Irreversible: true,
						Account:      "eosio",
					},
					{
						ABI:          &eos.ABI{Version: "456"},
						AbiBlockNum:  24,
						Irreversible: true,
						Account:      "eosio",
					},
				},
				},
			},
			args: args{
				// abi:      &eos.ABI{Version: "789"},
				abi: ABI{
					ABI:          &eos.ABI{Version: "789"},
					AbiBlockNum:  42,
					Account:      "eosio",
					Irreversible: false,
				},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{
				latestABIs: map[string]*ABI{"eosio": {
					ABI:          &eos.ABI{Version: "456"},
					AbiBlockNum:  24,
					Irreversible: true,
					Account:      "eosio",
				},
				},
				abiHistories: map[string][]*ABI{"eosio": {
					{
						ABI:          &eos.ABI{Version: "123"},
						AbiBlockNum:  1,
						Irreversible: true,
						Account:      "eosio",
					},
				},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.sut.doUpdateABI(tt.args.abi, tt.args.blockNum, tt.args.step)
			// assertEqual(t, tt.sut, tt.want)
			deep.CompareUnexportedFields = true
			if diff := deep.Equal(tt.sut, tt.want); diff != nil {
				t.Error(diff)
			}
		})
	}
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
	}
	type args struct {
		name     CodecId
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
				latestABIs:   make(map[string]*ABI),
				abiHistories: make(map[string][]*ABI),
				getSchema: func(string, *ABI) (MessageSchema, error) {
					return MessageSchema{}, nil
				},
				schemaRegistryClient: nil,
				account:              "test",
				codecCache:           map[CodecId]Codec{{"eosio.nft.ft", "TestTable"}: dummyCodec},
				schemaRegistryURL:    "http://localhost:8083",
			},
			args: args{
				name:     CodecId{"eosio.nft.ft", "TestTable"},
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
				latestABIs:   make(map[string]*ABI),
				abiHistories: make(map[string][]*ABI),
				getSchema: func(string, *ABI) (MessageSchema, error) {
					return MessageSchema{}, nil
				},
				schemaRegistryClient: nil,
				account:              "test",
				codecCache:           map[CodecId]Codec{},
				schemaRegistryURL:    "http://localhost:8083",
			},
			args: args{
				name:     CodecId{"eosio.nft.ft", "TestTable"},
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
				srclient.Forward,
			),
			args: args{
				name:     CheckpointSchema.AsCodecId(),
				blockNum: 42,
			},
			want: &KafkaAvroCodec{
				schemaURLTemplate: "mock://TestKafkaAvroABICodec_GetCodec/schemas/ids/%d",
				schema: RegisteredSchema{
					id:      1,
					schema:  "{\"type\":\"record\",\"name\":\"DKafkaCheckpoint\",\"namespace\":\"io.dkafka\",\"doc\":\"Periodically emitted checkpoint used to save the current position\",\"fields\":[{\"name\":\"step\",\"doc\":\"Step of the current block value can be: 1(New),2(Undo),3(Redo),4(Handoff),5(Irreversible),6(Stalled)\\n - 1(New): First time we're seeing this block\\n - 2(Undo): We are undoing this block (it was done previously)\\n - 4(Redo): We are redoing this block (it was done previously)\\n - 8(Handoff): The block passed a handoff from one producer to another\\n - 16(Irreversible): This block passed the LIB barrier and is in chain\\n - 32(Stalled): This block passed the LIB and is definitely forked out\\n\",\"type\":\"int\"},{\"name\":\"block\",\"type\":{\"type\":\"record\",\"name\":\"BlockRef\",\"namespace\":\"io.dkafka\",\"doc\":\"BlockRef represents a reference to a block and is mainly define as the pair \\u003cBlockID, BlockNum\\u003e\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"num\",\"type\":\"long\"}]}},{\"name\":\"headBlock\",\"type\":\"BlockRef\"},{\"name\":\"lastIrreversibleBlock\",\"type\":\"BlockRef\"},{\"name\":\"time\",\"type\":{\"logicalType\":\"timestamp-millis\",\"type\":\"long\"}}],\"meta\":{\"compatibility\":\"FORWARD\",\"type\":\"notification\",\"version\":\"1.0.0\",\"domain\":\"dkafka\"}}",
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
				srclient.Forward,
			),
			args: args{
				name:     CodecId{"eosio.nft.ft", "factory.a"},
				blockNum: 42,
			},
			want: &KafkaAvroCodec{
				schemaURLTemplate: "mock://TestKafkaAvroABICodec_GetCodec/schemas/ids/%d",
				schema: RegisteredSchema{
					id:      2,
					schema:  "{\"type\":\"record\",\"name\":\"FactoryATableNotification\",\"namespace\":\"test.eosio.nft.ft.tables.v0\",\"fields\":[{\"name\":\"context\",\"type\":{\"type\":\"record\",\"name\":\"NotificationContext\",\"namespace\":\"io.dkafka\",\"fields\":[{\"name\":\"block_num\",\"type\":\"long\"},{\"name\":\"block_id\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"executed\",\"type\":\"boolean\"},{\"name\":\"block_step\",\"type\":\"string\"},{\"name\":\"correlation\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Correlation\",\"namespace\":\"io.dkafka\",\"fields\":[{\"name\":\"payer\",\"type\":\"string\"},{\"name\":\"id\",\"type\":\"string\"}]}],\"default\":null},{\"name\":\"trx_id\",\"type\":\"string\"},{\"name\":\"time\",\"type\":{\"eos.type\":\"block_timestamp_type\",\"logicalType\":\"timestamp-millis\",\"type\":\"long\"}},{\"name\":\"cursor\",\"type\":\"string\"}]}},{\"name\":\"action\",\"type\":{\"type\":\"record\",\"name\":\"ActionInfoBasic\",\"namespace\":\"io.dkafka\",\"fields\":[{\"name\":\"account\",\"type\":\"string\"},{\"name\":\"receiver\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"global_seq\",\"type\":\"long\"},{\"name\":\"authorizations\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"action_ordinal\",\"type\":\"long\"},{\"name\":\"creator_action_ordinal\",\"type\":\"long\"},{\"name\":\"closest_unnotified_ancestor_action_ordinal\",\"type\":\"long\"},{\"name\":\"execution_index\",\"type\":\"long\"}]}},{\"name\":\"db_op\",\"type\":{\"type\":\"record\",\"name\":\"FactoryATableOpInfo\",\"fields\":[{\"name\":\"operation\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"action_index\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"index\",\"type\":\"int\"},{\"name\":\"code\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"scope\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"primary_key\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"old_payer\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"new_payer\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"old_data\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"new_data\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"old_json\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"FactoryATableOp\",\"fields\":[{\"name\":\"id\",\"type\":{\"eos.type\":\"uint64\",\"logicalType\":\"eos.uint64\",\"type\":\"long\"}},{\"name\":\"asset_manager\",\"type\":{\"eos.type\":\"name\",\"type\":\"string\"}},{\"name\":\"asset_creator\",\"type\":{\"eos.type\":\"name\",\"type\":\"string\"}},{\"name\":\"conversion_rate_oracle_contract\",\"type\":{\"eos.type\":\"name\",\"type\":\"string\"}},{\"name\":\"chosen_rate\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Asset\",\"namespace\":\"eosio\",\"convert\":\"eosio.Asset\",\"fields\":[{\"name\":\"amount\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":32,\"scale\":8}},{\"name\":\"symbol\",\"type\":\"string\"},{\"name\":\"precision\",\"type\":\"int\"}]}}},{\"name\":\"minimum_resell_price\",\"type\":\"eosio.Asset\"},{\"name\":\"resale_shares\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ResaleShare\",\"fields\":[{\"name\":\"receiver\",\"type\":{\"eos.type\":\"name\",\"type\":\"string\"}},{\"name\":\"basis_point\",\"type\":{\"eos.type\":\"uint16\",\"type\":\"int\"}}]}}},{\"name\":\"mintable_window_start\",\"type\":[\"null\",{\"eos.type\":\"uint32\",\"type\":\"long\"}],\"default\":null},{\"name\":\"mintable_window_end\",\"type\":[\"null\",{\"eos.type\":\"uint32\",\"type\":\"long\"}],\"default\":null},{\"name\":\"trading_window_start\",\"type\":[\"null\",{\"eos.type\":\"uint32\",\"type\":\"long\"}],\"default\":null},{\"name\":\"trading_window_end\",\"type\":[\"null\",{\"eos.type\":\"uint32\",\"type\":\"long\"}],\"default\":null},{\"name\":\"recall_window_start\",\"type\":[\"null\",{\"eos.type\":\"uint32\",\"type\":\"long\"}],\"default\":null},{\"name\":\"recall_window_end\",\"type\":[\"null\",{\"eos.type\":\"uint32\",\"type\":\"long\"}],\"default\":null},{\"name\":\"lockup_time\",\"type\":[\"null\",{\"eos.type\":\"uint32\",\"type\":\"long\"}],\"default\":null},{\"name\":\"conditionless_receivers\",\"type\":{\"type\":\"array\",\"items\":{\"eos.type\":\"name\",\"type\":\"string\"}}},{\"name\":\"stat\",\"type\":{\"eos.type\":\"uint8\",\"type\":\"int\"}},{\"name\":\"meta_uris\",\"type\":{\"type\":\"array\",\"items\":{\"eos.type\":\"string\",\"type\":\"string\"}}},{\"name\":\"meta_hash\",\"type\":{\"eos.type\":\"checksum256\",\"type\":\"bytes\"}},{\"name\":\"max_mintable_tokens\",\"type\":[\"null\",{\"eos.type\":\"uint32\",\"type\":\"long\"}],\"default\":null},{\"name\":\"minted_tokens_no\",\"type\":{\"eos.type\":\"uint32\",\"type\":\"long\"}},{\"name\":\"existing_tokens_no\",\"type\":{\"eos.type\":\"uint32\",\"type\":\"long\"}}]}],\"default\":null},{\"name\":\"new_json\",\"type\":[\"null\",\"FactoryATableOp\"],\"default\":null}]}}],\"meta\":{\"compatibility\":\"FORWARD\",\"type\":\"notification\",\"version\":\"0.1.0\",\"domain\":\"eosio.nft.ft\"}}",
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
				deep.CompareUnexportedFields = true
				if diff := deep.Equal(expectedAvroCodec, &actualAvroCodec); diff != nil {
					t.Error(diff)
				}
				if actualAvroCodec.schema.codec == nil {
					t.Error("KafkaAvroCodec.schema.codec should not be nil")
				}
			} else if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StreamedAbiCodec.GetCodec() = %v, want %v", got, tt.want)
			}
		})
	}
}
