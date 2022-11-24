package dkafka

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/riferrei/srclient"
)

func TestABIDecoderOnReload(t *testing.T) {
	abiCodec := ABIDecoder{
		onReload: func() {},
	}
	abiCodec.onReload()
}

func TestKafkaAvroABICodec_GetCodec(t *testing.T) {
	type args struct {
		name     string
		blockNum uint32
	}
	tests := []struct {
		name        string
		args        args
		wantVersion string
		wantErr     bool
	}{
		{
			name: "static",
			args: args{
				name:     dkafkaCheckpoint,
				blockNum: 0,
			},
			wantVersion: "1.0.0",
			wantErr:     false,
		},
		{
			name: "dynamic",
			args: args{
				name:     "factory.a",
				blockNum: 2,
			},
			wantVersion: "0.1.0",
			wantErr:     false,
		},
	}
	var localABIFiles = map[string]string{
		"eosio.nft.ft": "testdata/eosio.nft.ft.abi:1",
	}
	abiFiles, err := LoadABIFiles(localABIFiles)
	if err != nil {
		t.Fatalf("LoadABIFiles() error: %v", err)
	}
	abiDecoder := NewABIDecoder(abiFiles, nil, context.Background())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := MessageSchemaGenerator{
				Namespace: "test",
				Version:   "",
				Account:   "eosio.nft.ft",
			}

			c := NewKafkaAvroABICodec(
				abiDecoder,
				msg.getTableSchema,
				srclient.CreateMockSchemaRegistryClient("mock://TestKafkaAvroABICodec_GetCodec"),
				"eosio.nft.ft",
				"mock://TestKafkaAvroABICodec_GetCodec",
			)
			got, err := c.GetCodec(tt.args.name, tt.args.blockNum)
			if (err != nil) != tt.wantErr {
				t.Errorf("KafkaAvroABICodec.GetCodec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("KafkaAvroABICodec.GetCodec() = %v, want not nil", got)
			}
			avroCodec, ok := got.(KafkaAvroCodec)
			if ok {
				var schema map[string]interface{}
				if err := json.Unmarshal([]byte(avroCodec.schema.schema), &schema); err != nil {
					t.Errorf("json.Unmarshal = %v", err)
				} else {
					if schema["meta"] == nil {
						t.Errorf("Meta field not found")
					} else {
						if version := schema["meta"].(map[string]interface{})["version"]; version != tt.wantVersion {
							t.Errorf("Wrong version number = %v, expecting %v", version, tt.wantVersion)
						}
					}
				}
			} else {
				t.Errorf("Wrong type return")
			}
		})
	}
}

func TestParseABIFileSpec(t *testing.T) {
	type args struct {
		spec string
	}
	tests := []struct {
		name        string
		args        args
		wantAccount string
		wantAbiPath string
		wantErr     bool
	}{
		{
			name: "default",
			args: args{
				spec: "eosio.nft.ft:testdata/eosio.nft.ft.abi",
			},
			wantAccount: "eosio.nft.ft",
			wantAbiPath: "testdata/eosio.nft.ft.abi",
			wantErr:     false,
		},
		{
			name: "with-block-number",
			args: args{
				spec: "eosio.nft.ft:testdata/eosio.nft.ft.abi:1",
			},
			wantAccount: "eosio.nft.ft",
			wantAbiPath: "testdata/eosio.nft.ft.abi:1",
			wantErr:     false,
		},
		{
			name: "invalid",
			args: args{
				spec: "eosio.nft.f",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAccount, gotAbiPath, err := ParseABIFileSpec(tt.args.spec)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseABIFileSpec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotAccount != tt.wantAccount {
				t.Errorf("ParseABIFileSpec() gotAccount = %v, want %v", gotAccount, tt.wantAccount)
			}
			if gotAbiPath != tt.wantAbiPath {
				t.Errorf("ParseABIFileSpec() gotAbiPath = %v, want %v", gotAbiPath, tt.wantAbiPath)
			}
		})
	}
}

func TestLoadABIFile(t *testing.T) {
	type args struct {
		abiFile string
	}
	tests := []struct {
		name            string
		args            args
		wantABIBlockNum uint32
		wantErr         bool
	}{
		{
			name: "default",
			args: args{
				abiFile: "testdata/eosio.nft.ft.abi",
			},
			wantABIBlockNum: uint32(0),
			wantErr:         false,
		},
		{
			name: "with-block-number",
			args: args{
				abiFile: "testdata/eosio.nft.ft.abi:1",
			},
			wantABIBlockNum: uint32(1),
			wantErr:         false,
		},
		{
			name: "invalid",
			args: args{
				abiFile: "eosio.nft.f",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadABIFile(tt.args.abiFile)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadABIFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil && got.AbiBlockNum != tt.wantABIBlockNum {
				t.Errorf("LoadABIFile() got AbiBlockNum = %v, want %v", got, tt.wantABIBlockNum)
			}
		})
	}
}

func TestKafkaAvroABICodec_Reset(t *testing.T) {
	ac := &ABIDecoder{}

	c := &KafkaAvroABICodec{
		ABIDecoder:           ac,
		schemaRegistryClient: srclient.CreateMockSchemaRegistryClient("mock://localhost"),
		codecCache:           map[string]Codec{"dummy-1": NewJSONCodec(), "dummy-2": NewJSONCodec()},
	}
	c.abisCache = map[string]*ABI{"dummy-1": {}, "dummy-2": {}}

	if len(c.codecCache) != 2 && len(c.abisCache) != 2 {
		t.Errorf("Illegal state of the KafkaAvroABICodec before test")
	}
	c.Reset()

	if _, found := c.codecCache["dkafkaCheckpoint"]; !found && len(c.codecCache) != 1 {
		t.Errorf("Reset() must reset the codecCache: %v", c.codecCache)
	}

	if len(c.abisCache) > 0 {
		t.Errorf("Reset() must clear the abisCache: %v", c.abisCache)
	}
}
