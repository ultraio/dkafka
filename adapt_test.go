package dkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/eoscanada/eos-go"
	"github.com/golang/protobuf/jsonpb"
	"github.com/riferrei/srclient"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"gotest.tools/assert"
)

func TestCdCAdapter_AdaptJSON(t *testing.T) {
	type fields struct {
		generator GeneratorAtActionLevel
	}
	type args struct {
		rawStep pbbstream.ForkStep
	}
	tests := []struct {
		name    string
		file    string
		schema  string
		fields  fields
		args    args
		want    []*kafka.Message
		wantErr bool
	}{
		{
			name:   "cdc-table",
			file:   "testdata/block-30080032.json",
			schema: tableSchema(t, "testdata/eosio.nft.ft.abi", "factory.a"),
			fields: fields{
				generator: newTableGen4Test(t, "factory.a"),
			},
			args:    args{pbbstream.ForkStep_STEP_NEW},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			byteValue := readFileFromTestdata(t, tt.file)

			block := &pbcodec.Block{}
			// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
			err := json.Unmarshal(byteValue, block)
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}
			m := &CdCAdapter{
				topic:     "test.topic",
				saveBlock: saveBlockNoop,
				generator: transaction2ActionsGenerator{
					actionLevelGenerator: tt.fields.generator,
					topic:                "test.topic",
					headers:              default_headers,
				},
				headers: default_headers,
			}
			blockStep := BlockStep{
				blk:    block,
				step:   tt.args.rawStep,
				cursor: "123",
			}
			got, err := m.Adapt(blockStep)
			if (err != nil) != tt.wantErr {
				t.Errorf("CdCAdapter.Adapt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			kafkaMessage := got[0]

			assert.Equal(t, findHeader("content-type", kafkaMessage.Headers), "application/json")
			assert.Equal(t, findHeader("ce_datacontenttype", kafkaMessage.Headers), "application/json")
		})
	}
}

func findHeader(name string, headers []kafka.Header) string {

	for _, header := range headers {
		if header.Key == name {
			return string(header.Value)
		}
	}
	return ""
}

func newTableGen4Test(t testing.TB, tableName string) TableGenerator {
	var localABIFiles = map[string]string{
		"eosio.nft.ft": "testdata/eosio.nft.ft.abi",
	}
	abiFiles, err := LoadABIFiles(localABIFiles)
	if err != nil {
		t.Fatalf("LoadABIFiles() error: %v", err)
	}
	abiDecoder := NewABIDecoder(abiFiles, nil, context.Background())
	finder, _ := buildTableKeyExtractorFinder([]string{fmt.Sprintf("%s:s+k", tableName)})
	return TableGenerator{
		getExtractKey: finder,
		abiCodec:      NewJsonABICodec(abiDecoder, "eosio.nft.ft"),
	}
}

func tableSchema(t testing.TB, abiFile string, tableName string) string {
	abi, err := LoadABIFile(abiFile)
	if err != nil {
		t.Fatalf("LoadABIFile(abiFile) error: %v", err)
	}
	abiSpec := AbiSpec{
		Account: "eosio.nft.ft",
		Abi:     abi,
	}
	schema, err := GenerateTableSchema(NamedSchemaGenOptions{
		Name:    tableName,
		AbiSpec: abiSpec,
	})

	if err != nil {
		t.Fatalf("GenerateTableSchema() error: %v", err)
	}
	bytes, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}
	return string(bytes)
}

func TestCdCAdapter_Adapt_pb(t *testing.T) {
	eos.NativeType = true

	tests := []struct {
		name       string
		file       string
		abi        string
		table      string
		nbMessages int
	}{
		{
			"accounts",
			"testdata/block-49608395.pb.json",
			"testdata/eosio.token.abi",
			"accounts",
			2,
		},
		{
			"nft-factory",
			"testdata/block-50705256.pb.json",
			"testdata/eosio.nft.ft.abi",
			"factory.a",
			1,
		},
		{
			"nft-factory-b",
			"testdata/block-135283216.pb.json",
			"testdata/eosio.nft.ft-4.0.6-snapshot.abi",
			"factory.b",
			1,
		},
		{
			"eosio.oracle",
			"testdata/block-43922498.pb.json",
			"testdata/eosio.oracle.abi",
			"*",
			4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := &pbcodec.Block{}
			err := jsonpb.UnmarshalString(string(readFileFromTestdata(t, tt.file)), block)
			if err != nil {
				t.Fatalf("jsonpb.UnmarshalString(): %v", err)
			}

			abiAccount := strings.TrimRight(path.Base(tt.abi), ".abi")
			var localABIFiles = map[string]string{
				abiAccount: tt.abi,
			}
			abiFiles, err := LoadABIFiles(localABIFiles)
			if err != nil {
				t.Fatalf("LoadABIFiles() error: %v", err)
			}
			abiDecoder := NewABIDecoder(abiFiles, nil, context.Background())
			msg := MessageSchemaGenerator{
				Namespace: "test.dkafka",
				Version:   "1.2.3",
				Account:   abiAccount,
			}
			// abi, _ := abiDecoder.abi(abiAccount, 0, false)
			// schema, _ := msg.getTableSchema("accounts", abi)
			// jsonSchema, err := json.Marshal(schema)
			// fmt.Println(string(jsonSchema))
			finder, _ := buildTableKeyExtractorFinder([]string{fmt.Sprintf("%s:s+k", tt.table)})
			g := TableGenerator{
				getExtractKey: finder,
				abiCodec: NewStreamedAbiCodec(&DfuseAbiRepository{
					overrides:   abiDecoder.overrides,
					abiCodecCli: abiDecoder.abiCodecCli,
					context:     abiDecoder.context,
				}, msg.getTableSchema, srclient.CreateMockSchemaRegistryClient("mock://bench-adapter"), abiAccount, "mock://bench-adapter"),
			}
			a := &CdCAdapter{
				topic:     "test.topic",
				saveBlock: saveBlockNoop,
				generator: transaction2ActionsGenerator{
					actionLevelGenerator: g,
					topic:                "test.topic",
					headers:              default_headers,
				},
				headers: default_headers,
			}
			blockStep := BlockStep{
				blk:    block,
				step:   pbbstream.ForkStep_STEP_NEW,
				cursor: "123",
			}
			messages, err := a.Adapt(blockStep)
			if err != nil {
				t.Fatalf("Adapt() error: %v", err)
			}
			assert.Equal(t, len(messages), tt.nbMessages)
			for _, m := range messages {
				assert.Equal(t, findHeader("content-type", m.Headers), "application/avro")
				assert.Equal(t, findHeader("ce_datacontenttype", m.Headers), "application/avro")
				assert.Assert(t, findHeader("ce_dataschema", m.Headers) != "")
			}
		})
	}
}

func TestCdCAdapter_Action_pb(t *testing.T) {
	eos.NativeType = true

	tests := []struct {
		name       string
		file       string
		abi        string
		action     string
		nbMessages int
	}{
		{
			"eosio.nft.ft",
			"testdata/block-135283642.pb.json",
			"testdata/eosio.nft.ft-4.0.6-snapshot.abi",
			"*",
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := &pbcodec.Block{}
			err := jsonpb.UnmarshalString(string(readFileFromTestdata(t, tt.file)), block)
			if err != nil {
				t.Fatalf("jsonpb.UnmarshalString(): %v", err)
			}

			abiAccount := strings.TrimRight(path.Base(tt.abi), ".abi")
			var localABIFiles = map[string]string{
				abiAccount: tt.abi,
			}
			abiFiles, err := LoadABIFiles(localABIFiles)
			if err != nil {
				t.Fatalf("LoadABIFiles() error: %v", err)
			}
			abiDecoder := NewABIDecoder(abiFiles, nil, context.Background())
			msg := MessageSchemaGenerator{
				Namespace: "test.dkafka",
				Version:   "1.2.3",
				Account:   abiAccount,
			}
			// abi, _ := abiDecoder.abi(abiAccount, 0, false)
			// schema, _ := msg.getTableSchema("accounts", abi)
			// jsonSchema, err := json.Marshal(schema)
			// fmt.Println(string(jsonSchema))

			expression := fmt.Sprintf(`{"%s":"transaction_id"}`, tt.action)
			actionKeyExpressions, err := createCdcKeyExpressions(expression)
			if err != nil {
				t.Fatalf("createCdcKeyExpressions() error: %v", err)
			}
			g := ActionGenerator2{
				keyExtractors: actionKeyExpressions,
				abiCodec: NewStreamedAbiCodec(&DfuseAbiRepository{
					overrides:   abiDecoder.overrides,
					abiCodecCli: abiDecoder.abiCodecCli,
					context:     abiDecoder.context,
				}, msg.getActionSchema, srclient.CreateMockSchemaRegistryClient("mock://bench-adapter"), abiAccount, "mock://bench-adapter"),
			}
			a := &CdCAdapter{
				topic:     "test.topic",
				saveBlock: saveBlockNoop,
				generator: transaction2ActionsGenerator{
					actionLevelGenerator: g,
					topic:                "test.topic",
					headers:              default_headers,
				},
				headers: default_headers,
			}
			blockStep := BlockStep{
				blk:    block,
				step:   pbbstream.ForkStep_STEP_NEW,
				cursor: "123",
			}
			messages, err := a.Adapt(blockStep)
			if err != nil {
				t.Fatalf("Adapt() error: %v", err)
			}
			assert.Equal(t, len(messages), tt.nbMessages)
			for _, m := range messages {
				assert.Equal(t, findHeader("content-type", m.Headers), "application/avro")
				assert.Equal(t, findHeader("ce_datacontenttype", m.Headers), "application/avro")
				assert.Assert(t, findHeader("ce_dataschema", m.Headers) != "")
			}
		})
	}
}
