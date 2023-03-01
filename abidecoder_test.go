package dkafka

import (
	"encoding/json"
	"testing"

	"github.com/eoscanada/eos-go"
)

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

func TestDecodeABI(t *testing.T) {
	type args struct {
		trxID       string
		account     string
		hexDataPath string
	}
	tests := []struct {
		name    string
		args    args
		wantAbi string
		wantErr bool
	}{
		{
			name: "eosio.nft.ft",
			args: args{
				trxID:       "test",
				account:     "test",
				hexDataPath: "testdata/abi.hex",
			},
			wantAbi: "testdata/abi.json",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hexData := string(readFileFromTestdata(t, tt.args.hexDataPath))
			abiJson := readFileFromTestdata(t, tt.wantAbi)
			expectedAbi := &eos.ABI{}
			json.Unmarshal(abiJson, expectedAbi)
			gotAbi, err := DecodeABI(tt.args.trxID, tt.args.account, hexData)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeABI() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if gotAbi == nil {
				t.Errorf("DecodeABI() = %v, want %v", gotAbi, expectedAbi)
			}

			// if !reflect.DeepEqual(gotAbi, expectedAbi) {
			// 	t.Errorf("DecodeABI() = %v, want %v", gotAbi, expectedAbi)
			// }
		})
	}
}
