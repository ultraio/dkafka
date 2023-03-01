package dkafka

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/riferrei/srclient"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"gotest.tools/assert"
)

var default_headers = []kafka.Header{{
	Key:   "ce_source",
	Value: []byte("dkafka-test"),
}}

func readFileFromTestdata(t testing.TB, file string) []byte {
	f, err := os.Open(file)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer f.Close()
	// read block
	byteValue, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll() error: %v", err)
	}
	return byteValue
}

func Test_adapter_adapt(t *testing.T) {
	tests := []struct {
		name                  string
		file                  string
		expected              string
		failOnUndecodableDBOP bool
		actionBased           bool
		wantErr               bool
	}{
		{
			"filter-out",
			"testdata/block-30080030.json",
			"",
			true,
			false,
			false,
		},
		{
			"filter-in-expr",
			"testdata/block-30080032.json",
			"testdata/block-30080032-expected.json",
			true,
			false,
			false,
		},
		{
			"filter-in-actions",
			"testdata/block-30080032.json",
			"testdata/block-30080032-expected.json",
			true,
			true,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(path.Base(tt.name), func(t *testing.T) {

			byteValue := readFileFromTestdata(t, tt.file)

			block := &pbcodec.Block{}
			// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
			err := json.Unmarshal(byteValue, block)
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}
			var localABIFiles = map[string]string{
				"eosio.nft.ft": "testdata/eosio.nft.ft.abi",
			}
			abiFiles, err := LoadABIFiles(localABIFiles)
			if err != nil {
				t.Fatalf("LoadABIFiles() error: %v", err)
			}
			abiDecoder := NewABIDecoder(abiFiles, nil, context.Background())
			eventTypeProg, err := exprToCelProgram("'TestType'")
			if err != nil {
				t.Fatalf("exprToCelProgram() error: %v", err)
			}
			eventKeyProg, err := exprToCelProgram("[transaction_id]")
			if err != nil {
				t.Fatalf("exprToCelProgram() error: %v", err)
			}
			var adp *adapter
			if tt.actionBased {
				adp, err = newActionsAdapter(
					"test.topic",
					saveBlockNoop,
					abiDecoder.DecodeDBOps,
					true,
					`{"create":[{"key":"transaction_id", "type":"TestType"}]}`,
					default_headers,
				)
				if err != nil {
					t.Fatalf("newActionsAdapter() error: %v", err)
					return
				}
			} else {
				adp = newAdapter(
					"test.topic",
					saveBlockNoop,
					abiDecoder.DecodeDBOps,
					true,
					eventTypeProg,
					eventKeyProg,
					default_headers,
				)
			}
			blockStep := BlockStep{
				blk:    block,
				step:   pbbstream.ForkStep_STEP_NEW,
				cursor: "123",
			}
			if msg, err := adp.Adapt(blockStep); (err != nil) != tt.wantErr {
				t.Errorf("adapter.adapt() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				if tt.expected != "" {
					byteValue := readFileFromTestdata(t, tt.expected)

					var expectedObjectMap map[string]interface{}
					// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
					err := json.Unmarshal(byteValue, &expectedObjectMap)
					if err != nil {
						t.Fatalf("Unmarshal() error: %v", err)
					}

					if rawJSON, err := messageToJSON(msg[0]); err != nil {
						t.Errorf("messageToJSON() error: %v", err)
					} else {
						var actualObjectMap map[string]interface{}
						if err := json.Unmarshal(rawJSON, &actualObjectMap); err != nil {
							t.Fatalf("Unmarshal() error: %v", err)
						} else {
							if !reflect.DeepEqual(actualObjectMap, expectedObjectMap) {
								t.Errorf("adapter.adapt() result diff\nactual:\n%s\nexpected:\n%s", string(rawJSON), string(byteValue))
							}
						}
					}
				}
			}
		})
	}
}

type AdapterType = int

const (
	DEFAULT_ADAPTER AdapterType = iota
	ACTION_ADAPTER
	CDC_TABLE_ADAPTER
	CDC_ACTION_ADAPTER
	CDC_TABLE_ADAPTER_AVRO
	CDC_ACTION_ADAPTER_AVRO
)

func Benchmark_adapter_adapt(b *testing.B) {
	tests := []struct {
		name        string
		file        string
		actionBased AdapterType
	}{
		{
			"filter-out",
			"testdata/block-30080030.json",
			DEFAULT_ADAPTER,
		},
		{
			"filter-in",
			"testdata/block-30080032.json",
			DEFAULT_ADAPTER,
		},
		{
			"filter-in-actions",
			"testdata/block-30080032.json",
			ACTION_ADAPTER,
		},
		{
			"cdc-tables",
			"testdata/block-30080032.json",
			CDC_TABLE_ADAPTER,
		},
		{
			"cdc-actions",
			"testdata/block-30080032.json",
			CDC_ACTION_ADAPTER,
		},
		{
			"cdc-tables-avro",
			"testdata/block-30080032.json",
			CDC_TABLE_ADAPTER_AVRO,
		},
		{
			"cdc-actions-avro",
			"testdata/block-30080032.json",
			CDC_ACTION_ADAPTER_AVRO,
		},
	}

	for _, tt := range tests {
		f, err := os.Open(tt.file)
		if err != nil {
			b.Fatalf("Open() error: %v", err)
		}
		defer f.Close()
		// read block
		byteValue, err := ioutil.ReadAll(f)
		if err != nil {
			b.Fatalf("ReadAll() error: %v", err)
		}
		block := &pbcodec.Block{}
		// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
		err = json.Unmarshal(byteValue, block)
		if err != nil {
			b.Fatalf("Unmarshal() error: %v", err)
		}
		var localABIFiles = map[string]string{
			"eosio.nft.ft": "testdata/eosio.nft.ft.abi",
		}
		abiFiles, err := LoadABIFiles(localABIFiles)
		if err != nil {
			b.Fatalf("LoadABIFiles() error: %v", err)
		}
		abiDecoder := NewABIDecoder(abiFiles, nil, context.Background())
		eventTypeProg, err := exprToCelProgram("'TestType'")
		if err != nil {
			b.Fatalf("exprToCelProgram() error: %v", err)
		}
		eventKeyProg, err := exprToCelProgram("[transaction_id]")
		if err != nil {
			b.Fatalf("exprToCelProgram() error: %v", err)
		}
		var adp Adapter
		switch adapterType := tt.actionBased; adapterType {
		case ACTION_ADAPTER:
			adp, err = newActionsAdapter(
				"test.topic",
				saveBlockNoop,
				abiDecoder.DecodeDBOps,
				true,
				`{"create":[{"key":"transaction_id", "type":"TestType"}]}`,
				default_headers,
			)
			if err != nil {
				b.Fatalf("newActionsAdapter() error: %v", err)
				return
			}
		case DEFAULT_ADAPTER:
			adp = newAdapter(
				"test.topic",
				saveBlockNoop,
				abiDecoder.DecodeDBOps,
				true,
				eventTypeProg,
				eventKeyProg,
				nil,
			)
		case CDC_TABLE_ADAPTER:
			finder, _ := buildTableKeyExtractorFinder([]string{"factory.a:k"})
			adp = &CdCAdapter{
				topic:     "test.topic",
				saveBlock: saveBlockNoop,
				generator: TableGenerator{
					getExtractKey: finder,
					abiCodec:      NewJsonABICodec(abiDecoder, "eosio.nft.ft"),
				},
				headers: default_headers,
			}
		case CDC_ACTION_ADAPTER:
			actionKeyExpressions, err := createCdcKeyExpressions(`{"create":"transaction_id"}`)
			if err != nil {
				b.Fatalf("createCdcKeyExpressions() error: %v", err)
				return
			}
			adp = &CdCAdapter{
				topic:     "test.topic",
				saveBlock: saveBlockNoop,
				generator: &ActionGenerator2{
					keyExtractors: actionKeyExpressions,
					abiCodec:      NewJsonABICodec(abiDecoder, "eosio.nft.ft"),
				},
				headers: default_headers,
			}
		case CDC_TABLE_ADAPTER_AVRO:
			msg := MessageSchemaGenerator{
				Namespace: "test.dkafka",
				Version:   "1.2.3",
				Account:   "eosio.nft.ft",
			}
			abiCodec := NewStreamedAbiCodec(&DfuseAbiRepository{
				overrides:   abiDecoder.overrides,
				abiCodecCli: abiDecoder.abiCodecCli,
				context:     abiDecoder.context,
			}, msg.getTableSchema, srclient.CreateMockSchemaRegistryClient("mock://bench-adapter"), msg.Account, "mock://bench-adapter")
			abiCodec.GetCodec("factory.a", 0)
			finder, _ := buildTableKeyExtractorFinder([]string{"factory.a:k"})
			adp = &CdCAdapter{
				topic:     "test.topic",
				saveBlock: saveBlockNoop,
				generator: TableGenerator{
					getExtractKey: finder,
					abiCodec:      abiCodec,
				},
				headers: default_headers,
			}
		case CDC_ACTION_ADAPTER_AVRO:
			actionKeyExpressions, err := createCdcKeyExpressions(`{"create":"transaction_id"}`)
			if err != nil {
				b.Fatalf("createCdcKeyExpressions() error: %v", err)
				return
			}
			msg := MessageSchemaGenerator{
				Namespace: "test.dkafka",
				Version:   "1.2.3",
				Account:   "eosio.nft.ft",
			}
			abiCodec := NewStreamedAbiCodec(&DfuseAbiRepository{
				overrides:   abiDecoder.overrides,
				abiCodecCli: abiDecoder.abiCodecCli,
				context:     abiDecoder.context,
			}, msg.getActionSchema, srclient.CreateMockSchemaRegistryClient("mock://bench-adapter"), msg.Account, "mock://bench-adapter")
			abiCodec.GetCodec("create", 0)
			adp = &CdCAdapter{
				topic:     "test.topic",
				saveBlock: saveBlockNoop,
				generator: &ActionGenerator2{
					keyExtractors: actionKeyExpressions,
					abiCodec:      abiCodec,
				},
				headers: default_headers,
			}
		}
		blockStep := BlockStep{
			blk:    block,
			step:   pbbstream.ForkStep_STEP_NEW,
			cursor: "123",
		}
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				adp.Adapt(blockStep)
			}
		})
	}
}

func readFileFromTestdataProto(t testing.TB, file string, m proto.Message) {
	t.Helper()
	f, err := os.Open(file)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	err = jsonpb.Unmarshal(f, m)
	if err != nil {
		t.Fatalf("jsonpb.Unmarshal() error: %v", err)
	}
}

func Test_adapter_correlation_id(t *testing.T) {
	block := &pbcodec.Block{}
	readFileFromTestdataProto(t, "testdata/block-49608395.pb.json", block)
	var localABIFiles = map[string]string{
		"eosio.nft.ft": "testdata/eosio.nft.ft.abi",
		"eosio.token":  "testdata/eosio.token.abi",
	}
	abiFiles, err := LoadABIFiles(localABIFiles)
	if err != nil {
		t.Fatalf("LoadABIFiles() error: %v", err)
	}
	abiDecoder := NewABIDecoder(abiFiles, nil, context.Background())
	finder, _ := buildTableKeyExtractorFinder([]string{"accounts:s+k"})
	adp := &CdCAdapter{
		topic:     "test.topic",
		saveBlock: saveBlockNoop,
		generator: TableGenerator{
			getExtractKey: finder,
			abiCodec:      NewJsonABICodec(abiDecoder, "eosio.token"),
		},
		headers: default_headers,
	}
	blockStep := BlockStep{
		blk:    block,
		step:   pbbstream.ForkStep_STEP_NEW,
		cursor: "123",
	}
	if msgs, err := adp.Adapt(blockStep); err != nil {
		t.Errorf("adapter.adapt() error = %v", err)
	} else {
		assert.Equal(t, len(msgs), 2, "should produce 2 messages")
		for _, msg := range msgs {
			value := make(map[string]interface{})
			err := json.Unmarshal(msg.Value, &value)
			if err != nil {
				t.Errorf("json.Unmarshal() error: %v", err)
			}
			context := value["context"].(map[string]interface{})
			correlation := context["correlation"].(map[string]interface{})
			assert.Equal(t, correlation["id"], "ed19191b-3962-4c58-9dee-f41398866ee1", "should provide the correlation id")
		}
	}

}
