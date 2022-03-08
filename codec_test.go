package dkafka

import (
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
			if got := NewKafkaAvroCodec(tt.args.schema); !reflect.DeepEqual(got, tt.want) {
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
			NewKafkaAvroCodec(newSchema(t, 42, UserSchema, nil)),
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
			NewKafkaAvroCodec(newSchema(t, 42, TokenATableOpInfo, nil)),
			args{
				nil,
				dbOp.asMap("dkafka.test.TokenATableOp"),
			},
			false,
			false,
			map[string]interface{}{
				"operation":    map[string]interface{}{"int": int32(1)},
				"action_index": map[string]interface{}{"long": int64(0)},
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
	codec, err := goavro.NewCodec(s)
	if err != nil {
		t.Fatalf("goavro.NewCodec() on schema: %s, error: %v", s, err)
	}
	return codec
}

// func TestDummy(t *testing.T) {
// 	var value = eos.Uint64(42)
// 	valueOf := reflect.ValueOf(value)
// 	fmt.Println(valueOf.Kind())
// 	valueOf.Int()

// 	fmt.Println(reflect.TypeOf(value))
// 	fmt.Println(reflect.TypeOf(value).ConvertibleTo(reflect.TypeOf(uint64(0))))

// 	// valueType := reflect.TypeOf(value)
// 	// valueType.ConvertibleTo(reflect.Uint64)
// }