package dkafka

import (
	"fmt"
	"reflect"
	"testing"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/eoscanada/eos-go"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
)

func TestNewJSONCode(t *testing.T) {
	tests := []struct {
		name string
		want Codec
	}{
		{
			"json",
			JSONCodec{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewJSONCodec(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewJSONCode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewKafkaAvroCodec(t *testing.T) {
	schema := newSchema(t, 42, UserSchema, nil)
	type args struct {
		schema *srclient.Schema
	}
	tests := []struct {
		name string
		args args
		want Codec
	}{
		{
			"kafka-avro",
			args{
				schema: schema,
			},
			KafkaAvroCodec{
				"mock://test/schemas/ids/%d",
				RegisteredSchema{
					id:      uint32(schema.ID()),
					schema:  schema.Schema(),
					version: schema.Version(),
					codec:   schema.Codec(),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewKafkaAvroCodec("mock://test", tt.args.schema, tt.args.schema.Codec()); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewKafkaAvroCodec() = %v, want %v", got, tt.want)
			}
		})
	}
}

const UserSchema = `
{
	"namespace": "dkafka.test",
	"name": "User",
	"type": "record",
	"fields": [
		{
			"name": "firstName",
			"type": "string"
		},
		{
			"name": "lastName",
			"type": "string"
		},
		{
			"name": "middleName",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "age",
			"type": "int"
		},
		{
			"name": "id",
			"type": "long"
		}
	]
}
`

const TokenATableOpInfo = `
{
	"namespace": "dkafka.test",
	"type": "record",
	"name": "TokenATableOpInfo",
	"fields": [
		{
			"name": "operation",
			"type": [
				"null",
				"int"
			],
			"default": null
		},
		{
			"name": "action_index",
			"type": [
				"null",
				"long"
			],
			"default": null
		},
		{
			"name": "index",
			"type": "int"
		},
		{
			"name": "code",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "scope",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "table_name",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "primary_key",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "old_payer",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "new_payer",
			"type": [
				"null",
				"string"
			],
			"default": null
		},
		{
			"name": "old_data",
			"type": [
				"null",
				"bytes"
			],
			"default": null
		},
		{
			"name": "new_data",
			"type": [
				"null",
				"bytes"
			],
			"default": null
		},
		{
			"name": "old_json",
			"type": [
				"null",
				{
					"type": "record",
					"name": "TokenATableOp",
					"fields": [
						{
							"name": "id",
							"type": "long"
						},
						{
							"name": "token_factory_id",
							"type": "long"
						},
						{
							"name": "mint_date",
							"type": "string"
						},
						{
							"name": "serial_number",
							"type": "long"
						}
					]
				}
			],
			"default": null
		},
		{
			"name": "new_json",
			"type": [
				"null",
				"TokenATableOp"
			],
			"default": null
		}
	]
}
`

func TestCodec_MarshalUnmarshal(t *testing.T) {
	dbOp := &decodedDBOp{
		DBOp: &pbcodec.DBOp{
			Operation: pbcodec.DBOp_OPERATION_INSERT,
			TableName: "token.a",
			NewData:   []byte("aAAAAAAAAAACAAAAAAAAABPuzWEJAAAA"),
		},
		NewJSON: map[string]interface{}{
			"id":               eos.Uint64(104),
			"mint_date":        "2021-12-30T17:36:19",
			"serial_number":    9,
			"token_factory_id": 2,
		},
	}
	uSchema := newSchema(t, 42, UserSchema, nil)
	tSchema := newSchema(t, 42, TokenATableOpInfo, nil)
	type args struct {
		buf   []byte
		value interface{}
	}
	tests := []struct {
		name             string
		c                Codec
		args             args
		wantMarshalErr   bool
		wantUnmarshalErr bool
		expect           interface{}
	}{
		{
			"json",
			JSONCodec{},
			args{
				nil,
				map[string]interface{}{
					"firstName": "Christophe",
					"lastName":  "O",
					"age":       float64(42),
				},
			},
			false,
			false,
			nil,
		},
		{
			"kafka-avro",
			NewKafkaAvroCodec("mock://test", uSchema, uSchema.Codec()),
			args{
				nil,
				map[string]interface{}{
					"firstName": "Christophe",
					"lastName":  "O",
					"age":       int32(24),
					"id":        int64(42),
				},
			},
			false,
			false,
			map[string]interface{}{
				"firstName":  "Christophe",
				"lastName":   "O",
				"middleName": nil,
				"age":        int32(24),
				"id":         int64(42),
			},
		},
		{
			"repeated-avro",
			NewKafkaAvroCodec("mock://test", tSchema, tSchema.Codec()),
			args{
				nil,
				dbOp.asMap("dkafka.test.TokenATableOp", 1),
			},
			false,
			false,
			map[string]interface{}{
				"operation":    map[string]interface{}{"int": int32(1)},
				"action_index": map[string]interface{}{"long": int64(0)},
				"index":        int32(1),
				"code":         nil,
				"scope":        nil,
				"table_name":   map[string]interface{}{"string": "token.a"},
				"primary_key":  nil,
				"old_payer":    nil,
				"new_payer":    nil,
				"old_data":     nil,
				"new_data":     map[string]interface{}{"bytes": []byte("aAAAAAAAAAACAAAAAAAAABPuzWEJAAAA")},
				"old_json":     nil,
				"new_json":     map[string]interface{}{"dkafka.test.TokenATableOp": map[string]interface{}{"id": int64(104), "mint_date": "2021-12-30T17:36:19", "serial_number": int64(9), "token_factory_id": int64(2)}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.c
			gotBytes, err := c.Marshal(tt.args.buf, tt.args.value)
			if (err != nil) != tt.wantMarshalErr {
				t.Errorf("JSONCodec.Marshal() error = %v, wantMarshalErr %v", err, tt.wantMarshalErr)
				return
			}
			gotValue, err := c.Unmarshal(gotBytes)
			if (err != nil) != tt.wantUnmarshalErr {
				t.Errorf("JSONCodec.Unmarshal() error = %v, wantUnmarshalErr %v", err, tt.wantUnmarshalErr)
				return
			}
			expect := tt.expect
			if expect == nil {
				expect = tt.args.value
			}
			if !reflect.DeepEqual(gotValue, expect) {
				t.Errorf("codec Marshal() then Unmarshal() = %v, want %v", gotValue, expect)
			}
		})
	}
}

func newSchema(t testing.TB, id uint32, s string, c *goavro.Codec) *srclient.Schema {
	if c == nil {
		c = newAvroCodec(t, s)
	}
	schema, err := srclient.NewSchema(42, UserSchema, srclient.Avro, 1, nil, c, nil)
	if err != nil {
		t.Fatalf("srclient.NewSchema() on schema: %s, error: %v", s, err)
	}
	return schema
}

func newAvroCodec(t testing.TB, s string) *goavro.Codec {
	// codec, err := goavro.NewCodec(s)
	codec, err := goavro.NewCodecWithConverters(s, map[string]goavro.ConvertBuild{"eosio.Asset": assetConverter})
	if err != nil {
		t.Fatalf("goavro.NewCodec() on schema: %s, error: %v", s, err)
	}
	return codec
}

type TB []byte
type TString string

var Uint8Type reflect.Type = reflect.TypeOf(uint8(0))

// func TestDummy(t *testing.T) {
// 	var value = TString("test")
// 	valueOf := reflect.ValueOf(value)
// 	switch kind := valueOf.Kind(); {
// 	case kind == reflect.Slice:
// 		fmt.Println(valueOf.Type().Elem())
// 		fmt.Println(Uint8Type == valueOf.Type().Elem())
// 	case kind == reflect.String:
// 		fmt.Println(valueOf.String())
// 	}
// 	// fmt.Println(valueOf.Kind())
// 	// fmt.Println(valueOf.Type())
// 	// fmt.Println(valueOf.Type().Elem())
// }

func BenchmarkCodecMarshal(b *testing.B) {
	user := map[string]interface{}{
		"firstName":  "Chris",
		"lastName":   "Otto",
		"middleName": "Tutu",
		"age":        int32(24),
		"id":         int64(42),
	}
	uSchema := newSchema(b, 42, UserSchema, nil)
	tests := []struct {
		name  string
		codec Codec
		value interface{}
	}{
		{
			"json",
			JSONCodec{},
			user,
		},
		{
			"avro",
			NewKafkaAvroCodec("mock://test", uSchema, uSchema.Codec()),
			user,
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				tt.codec.Marshal(nil, tt.value)
			}
		})
	}
}

var AccountSchema = `
{
	"namespace": "dkafka.test",
	"name": "Account",
	"type": "record",
	"fields": [
		{
			"name": "balance",
			
			"type": {
				"namespace": "eosio",
				"name": "Asset",
				"type": "record",
				"convert": "eosio.Asset",
				"fields": [
					{
						"name": "amount",
						"type": {
							"type": "bytes",
							"logicalType": "decimal",
							"precision": 32,
							"scale": 8
						}
					},
					{
						"name": "symbol",
						"type": "string"
					}
				]
			}
		}
	]
}
`
var NullableAccountSchema = `
{
	"namespace": "dkafka.test",
	"name": "Account",
	"type": "record",
	"fields": [
		{
			"name": "balance",
			"type": ["null",{
				"namespace": "eosio",
				"name": "Asset",
				"type": "record",
				"convert": "eosio.Asset",
				"fields": [
					{
						"name": "amount",
						"type": {
							"type": "bytes",
							"logicalType": "decimal",
							"precision": 32,
							"scale": 8
						}
					},
					{
						"name": "symbol",
						"type": "string"
					}
				]
			}],
			"default": null
		}
	]
}
`

func TestAvroCodecAsset(t *testing.T) {

	schema := newSchema(t, 42, AccountSchema, nil)

	codec := KafkaAvroCodec{
		"mock://test/schemas/ids/%d",
		RegisteredSchema{
			id:      uint32(schema.ID()),
			schema:  schema.Schema(),
			version: schema.Version(),
			codec:   schema.Codec(),
		},
	}
	asset := eos.Asset{
		Amount: eos.Int64(100_000_000),
		Symbol: eos.Symbol{
			Precision: uint8(8),
			Symbol:    "UOS",
		},
	}
	bytes, err := codec.Marshal(nil, map[string]interface{}{
		"balance": asset,
	})
	if err != nil {
		t.Fatalf("codec.Marshal() error: %v", err)
	}
	value, err := codec.Unmarshal(bytes)
	t.Log(fmt.Sprintf("%v", value))
	if err != nil {
		t.Fatalf("codec.Unmarshal() error: %v", err)
	}
}

func TestAvroCodecNullableAsset(t *testing.T) {

	schema := newSchema(t, 42, NullableAccountSchema, nil)

	codec := KafkaAvroCodec{
		"mock://test/schemas/ids/%d",
		RegisteredSchema{
			id:      uint32(schema.ID()),
			schema:  schema.Schema(),
			version: schema.Version(),
			codec:   schema.Codec(),
		},
	}
	asset := eos.Asset{
		Amount: eos.Int64(100_000_000),
		Symbol: eos.Symbol{
			Precision: uint8(8),
			Symbol:    "UOS",
		},
	}
	bytes, err := codec.Marshal(nil, map[string]interface{}{
		"balance": asset,
	})
	if err != nil {
		t.Fatalf("codec.Marshal() error: %v", err)
	}
	value, err := codec.Unmarshal(bytes)
	t.Log(fmt.Sprintf("%v", value))
	if err != nil {
		t.Fatalf("codec.Unmarshal() error: %v", err)
	}
}

func TestAvroCodecNullAsset(t *testing.T) {

	schema := newSchema(t, 42, NullableAccountSchema, nil)

	codec := KafkaAvroCodec{
		"mock://test/schemas/ids/%d",
		RegisteredSchema{
			id:      uint32(schema.ID()),
			schema:  schema.Schema(),
			version: schema.Version(),
			codec:   schema.Codec(),
		},
	}
	bytes, err := codec.Marshal(nil, map[string]interface{}{
		"balance": nil,
	})
	if err != nil {
		t.Fatalf("codec.Marshal() error: %v", err)
	}
	value, err := codec.Unmarshal(bytes)
	t.Log(fmt.Sprintf("%v", value))
	if err != nil {
		t.Fatalf("codec.Unmarshal() error: %v", err)
	}
}

var AccountsTableNotificationSchema = `
{
    "type": "record",
    "name": "AccountsTableNotification",
    "namespace": "test.dkafka",
    "fields": [
        {
            "name": "db_op",
            "type": {
                "type": "record",
                "name": "AccountsTableOpInfo",
                "fields": [
                    {
                        "name": "old_json",
                        "type": [
                            "null",
                            {
                                "type": "record",
                                "name": "AccountsTableOp",
                                "fields": [
                                    {
                                        "name": "balance",
                                        "type": {
                                            "namespace": "eosio",
                                            "name": "Asset",
                                            "type": "record",
                                            "convert": "eosio.Asset",
                                            "fields": [
                                                {
                                                    "name": "amount",
                                                    "type": {
                                                        "type": "bytes",
                                                        "logicalType": "decimal",
                                                        "precision": 32,
                                                        "scale": 8
                                                    }
                                                },
                                                {
                                                    "name": "symbol",
                                                    "type": "string"
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        ],
                        "default": null
                    }
                ]
            }
        },
		{
			"name": "minimum_resell_price",
			"type": "eosio.Asset"
		}
    ]
}
`

func TestAccountsTableNotification(t *testing.T) {

	schema := newSchema(t, 42, AccountsTableNotificationSchema, nil)

	codec := KafkaAvroCodec{
		"mock://test/schemas/ids/%d",
		RegisteredSchema{
			id:      uint32(schema.ID()),
			schema:  schema.Schema(),
			version: schema.Version(),
			codec:   schema.Codec(),
		},
	}
	asset := eos.Asset{
		Amount: eos.Int64(100_000_000),
		Symbol: eos.Symbol{
			Precision: uint8(8),
			Symbol:    "UOS",
		},
	}
	// asset := map[string]interface{}{
	// 	"balance": "1.0",
	// }
	bytes, err := codec.Marshal(nil, map[string]interface{}{
		"db_op": map[string]interface{}{
			"old_json": map[string]interface{}{
				"balance": asset,
			},
		},
		"minimum_resell_price": asset,
	})
	if err != nil {
		t.Fatalf("codec.Marshal() error: %v", err)
	}
	value, err := codec.Unmarshal(bytes)
	t.Log(fmt.Sprintf("%v", value))
	if err != nil {
		t.Fatalf("codec.Unmarshal() error: %v", err)
	}
}
