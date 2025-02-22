package dkafka

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/eoscanada/eos-go"
)

func Test_resolveFieldTypeSchema(t *testing.T) {
	initBuiltInTypesForTables()
	type args struct {
		fieldType string
		abi       *ABI
	}
	type test struct {
		name    string
		args    args
		want    Schema
		wantErr bool
	}

	newTest := func(eosType string, want string, wantErr bool) test {
		return test{
			name:    fmt.Sprintf("%s->%s", eosType, want),
			args:    args{eosType, nil},
			want:    TypedSchema{Type: want, EosType: eosType},
			wantErr: wantErr,
		}
	}

	tests := []test{
		newTest(
			"bool",
			"boolean",
			false),
		newTest(
			"int8",
			"int",
			false,
		),
		newTest(
			"uint8",
			"int",
			false,
		),
		newTest(
			"int16",
			"int",
			false,
		),
		newTest(
			"uint16",
			"int",
			false,
		),
		newTest(
			"int32",
			"int",
			false,
		),
		{
			name:    "int32[]->[]int",
			args:    args{"int32[]", nil},
			want:    NewArray(TypedSchema{Type: "int", EosType: "int32"}),
			wantErr: false,
		},
		{
			name:    "int32$->['null', 'int']",
			args:    args{"int32$", nil},
			want:    NewOptional(TypedSchema{Type: "int", EosType: "int32"}),
			wantErr: false,
		},
		{
			name:    "int32?$->['null','int']",
			args:    args{"int32?$", nil},
			want:    NewOptional(TypedSchema{Type: "int", EosType: "int32"}),
			wantErr: false,
		},
		{
			name:    "int32[]?->['null',[]int]",
			args:    args{"int32[]?$", nil},
			want:    NewOptional(NewArray(TypedSchema{Type: "int", EosType: "int32"})),
			wantErr: false,
		},
		{
			name:    "int32?->['null','int']",
			args:    args{"int32?", nil},
			want:    NewOptional(TypedSchema{Type: "int", EosType: "int32"}),
			wantErr: false,
		},
		newTest(
			"uint32",
			"long",
			false,
		),
		newTest(
			"int64",
			"long",
			false,
		),
		{
			name:    "uint64->long",
			args:    args{"uint64", nil},
			want:    TypedSchema{Type: "long", EosType: "uint64", LogicalType: "eos.uint64"},
			wantErr: false,
		},
		newTest(
			"varint32",
			"int",
			false,
		),
		newTest(
			"varuint32",
			"long",
			false,
		),
		newTest(
			"float32",
			"float",
			false,
		),
		newTest(
			"float64",
			"double",
			false,
		),
		newTest(
			"name",
			"string",
			false,
		),
		newTest(
			"bytes",
			"bytes",
			false,
		),
		newTest(
			"string",
			"string",
			false,
		),
		newTest(
			"checksum160",
			"bytes",
			false,
		),
		newTest(
			"checksum256",
			"bytes",
			false,
		),
		newTest(
			"checksum512",
			"bytes",
			false,
		),
		newTest(
			"symbol_code",
			"string",
			false,
		),
		{
			name:    "unknown->error",
			args:    args{"unknown", &ABI{ABI: &eos.ABI{}}},
			want:    nil,
			wantErr: true,
		},
		{
			name: "struct->record",
			args: args{"my_struct", &ABI{ABI: &eos.ABI{
				Types: []eos.ABIType{{
					NewTypeName: "int64_alias",
					Type:        "int64",
				}},
				Structs: []eos.StructDef{{
					Name: "my_struct",
					Base: "",
					Fields: []eos.FieldDef{
						{
							Name: "fieldA",
							Type: "uint32",
						},
						{
							Name: "fieldB",
							Type: "int64_alias",
						},
					},
				}},
			}}},
			want: RecordSchema{
				Type: "record",
				Name: "MyStruct",
				Fields: []FieldSchema{
					{
						Name: "fieldA",
						Type: TypedSchema{Type: "long", EosType: "uint32"},
					},
					{
						Name: "fieldB",
						Type: TypedSchema{Type: "long", EosType: "int64"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "struct-inheritance",
			args: args{"my_struct", &ABI{ABI: &eos.ABI{
				Types: []eos.ABIType{{
					NewTypeName: "int64_alias",
					Type:        "int64",
				}},
				Structs: []eos.StructDef{
					{
						Name: "parent",
						Base: "",
						Fields: []eos.FieldDef{
							{
								Name: "parentFieldA",
								Type: "string",
							},
							{
								Name: "parentFieldB",
								Type: "int32",
							},
						},
					},
					{
						Name: "my_struct",
						Base: "parent",
						Fields: []eos.FieldDef{
							{
								Name: "fieldA",
								Type: "uint32",
							},
							{
								Name: "fieldB",
								Type: "int64_alias",
							},
						},
					}},
			}}},
			want: RecordSchema{
				Type: "record",
				Name: "MyStruct",
				Fields: []FieldSchema{
					{
						Name: "parentFieldA",
						Type: TypedSchema{Type: "string", EosType: "string"},
					},
					{
						Name: "parentFieldB",
						Type: TypedSchema{Type: "int", EosType: "int32"},
					},
					{
						Name: "fieldA",
						Type: TypedSchema{Type: "long", EosType: "uint32"},
					},
					{
						Name: "fieldB",
						Type: TypedSchema{Type: "long", EosType: "int64"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "union-multiple-types-nullable",
			args: args{"nullable_union_record", &ABI{ABI: &eos.ABI{
				Types: []eos.ABIType{{
					NewTypeName: "nullable_union",
					Type:        "variant_nullable_union",
				}},
				Structs: []eos.StructDef{{
					Name: "nullable_union_record",
					Base: "",
					Fields: []eos.FieldDef{
						{
							Name: "nullable_union_field",
							Type: "nullable_union?",
						},
					},
				},
				},
				Variants: []eos.VariantDef{{
					Name:  "variant_nullable_union",
					Types: []string{"string", "int8"},
				}},
			}}},
			want: RecordSchema{
				Type: "record",
				Name: "NullableUnionRecord",
				Fields: []FieldSchema{
					{
						Name:    "nullable_union_field",
						Type:    []interface{}{"null", TypedSchema{Type: "string", EosType: "string"}, TypedSchema{Type: "int", EosType: "int8"}},
						Default: _defaultNull,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "uber-variant",
			args: args{"uber_variant", &ABI{ABI: &eos.ABI{
				Types: []eos.ABIType{
					{
						NewTypeName: "INT8_VEC",
						Type:        "int8[]",
					},
					{
						NewTypeName: "INT16_VEC",
						Type:        "int16[]",
					},
					{
						NewTypeName: "INT32_VEC",
						Type:        "int32[]",
					},
					{
						NewTypeName: "INT64_VEC",
						Type:        "int64[]",
					},
					{
						NewTypeName: "UINT8_VEC",
						Type:        "uint8[]",
					},
					{
						NewTypeName: "UINT16_VEC",
						Type:        "uint16[]",
					},
					{
						NewTypeName: "UINT32_VEC",
						Type:        "uint32[]",
					},
					{
						NewTypeName: "UINT64_VEC",
						Type:        "uint64[]",
					},
					{
						NewTypeName: "FLOAT32_VEC",
						Type:        "float32[]",
					},
					{
						NewTypeName: "FLOAT64_VEC",
						Type:        "float64[]",
					},
					{
						NewTypeName: "STRING_VEC",
						Type:        "string[]",
					},
					{
						NewTypeName: "BOOL_VEC",
						Type:        "boolean[]",
					},
					{
						NewTypeName: "key_value_store",
						Type:        HardcodedUberVariant,
					},
				},
				Structs: []eos.StructDef{{
					Name: "uber_variant",
					Base: "",
					Fields: []eos.FieldDef{
						{
							Name: "default_value",
							Type: HardcodedUberVariant,
						},
					},
				},
				},
				Variants: []eos.VariantDef{{
					Name:  HardcodedUberVariant,
					Types: []string{"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "string", "boolean", "INT8_VEC", "INT16_VEC", "INT32_VEC", "INT64_VEC", "UINT8_VEC", "UINT16_VEC", "UINT32_VEC", "UINT64_VEC", "FLOAT32_VEC", "FLOAT64_VEC", "STRING_VEC", "BOOL_VEC"},
				}},
			}}},
			want: RecordSchema{
				Type: "record",
				Name: "UberVariant",
				Fields: []FieldSchema{
					{
						Name: "default_value",
						Type: hardcodedVariantType[HardcodedUberVariant],
					},
				},
			},
			wantErr: false,
		},
		{
			name: "union-multiple-types-nullable",
			args: args{"nullable_union_record", &ABI{ABI: &eos.ABI{
				Types: []eos.ABIType{{
					NewTypeName: "nullable_union",
					Type:        "variant_nullable_union",
				}},
				Structs: []eos.StructDef{{
					Name: "nullable_union_record",
					Base: "",
					Fields: []eos.FieldDef{
						{
							Name: "nullable_union_field",
							Type: "nullable_union?",
						},
					},
				},
				},
				Variants: []eos.VariantDef{{
					Name:  "variant_nullable_union",
					Types: []string{"string", "int8"},
				}},
			}}},
			want: RecordSchema{
				Type: "record",
				Name: "NullableUnionRecord",
				Fields: []FieldSchema{
					{
						Name:    "nullable_union_field",
						Type:    []interface{}{"null", TypedSchema{Type: "string", EosType: "string"}, TypedSchema{Type: "int", EosType: "int8"}},
						Default: _defaultNull,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveFieldTypeSchema(tt.args.abi, tt.args.fieldType, make(map[string]string))
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveFieldTypeSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resolveFieldTypeSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_variantResolveFieldTypeSchema(t *testing.T) {
	initBuiltInTypesForTables()
	type args struct {
		fieldType string
		abi       *ABI
	}
	tests := []struct {
		name    string
		args    args
		want    Schema
		wantErr bool
	}{
		{
			name: "variant->union",
			args: args{"regproducer2", &ABI{ABI: &eos.ABI{
				Types: []eos.ABIType{{
					NewTypeName: "block_signing_authority",
					Type:        "variant_block_signing_authority_v0",
				}},
				Structs: []eos.StructDef{{
					Name: "block_signing_authority_v0",
					Base: "",
					Fields: []eos.FieldDef{
						{
							Name: "threshold",
							Type: "uint32",
						},
					},
				}, {
					Name: "regproducer2",
					Base: "",
					Fields: []eos.FieldDef{
						{
							Name: "producer_authority",
							Type: "block_signing_authority",
						},
					},
				},
				},
				Variants: []eos.VariantDef{{
					Name:  "variant_block_signing_authority_v0",
					Types: []string{"block_signing_authority_v0"},
				}},
			}}},
			want: RecordSchema{
				Type: "record",
				Name: "Regproducer2",
				Fields: []FieldSchema{
					{
						Name: "producer_authority",
						Type: RecordSchema{
							Type: "record",
							Name: "BlockSigningAuthorityV0",
							Fields: []FieldSchema{
								{
									Name: "threshold",
									Type: TypedSchema{Type: "long", EosType: "uint32"},
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "variant->union multiple types",
			args: args{"pair_string_key_value_store", &ABI{ABI: &eos.ABI{
				Types: []eos.ABIType{
					{
						NewTypeName: "key_value_store",
						Type:        "variant_int8_string",
					},
				},
				Structs: []eos.StructDef{{
					Name: "pair_string_key_value_store",
					Base: "",
					Fields: []eos.FieldDef{
						{
							Name: "first",
							Type: "string",
						},
						{
							Name: "second",
							Type: "key_value_store",
						},
					},
				},
				},
				Variants: []eos.VariantDef{{
					Name:  "variant_int8_string",
					Types: []string{"int8", "string"},
				}},
			}}},
			want: RecordSchema{
				Type: "record",
				Name: "PairStringKeyValueStore",
				Fields: []FieldSchema{
					{
						Type: TypedSchema{Type: "string", EosType: "string"},
						Name: "first",
					},
					{
						Type: []interface{}{
							TypedSchema{
								EosType: "int8",
								Type:    "int",
							},
							TypedSchema{
								EosType: "string",
								Type:    "string",
							},
						},
						Name: "second",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveFieldTypeSchema(tt.args.abi, tt.args.fieldType, make(map[string]string))
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveFieldTypeSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resolveFieldTypeSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

var actionABI eos.ABI = eos.ABI{
	Types: []eos.ABIType{{
		NewTypeName: "int64_alias",
		Type:        "int64",
	}},
	Structs: []eos.StructDef{{
		Name: "my_action",
		Base: "",
		Fields: []eos.FieldDef{
			{
				Name: "fieldA",
				Type: "uint32",
			},
			{
				Name: "fieldB",
				Type: "int64_alias",
			},
		},
	}},
	Actions: []eos.ActionDef{{
		Name: "my_action",
		Type: "my_action",
	}},
}

func TestActionToRecord(t *testing.T) {
	type args struct {
		abi  *ABI
		name eos.ActionName
	}
	tests := []struct {
		name    string
		args    args
		want    RecordSchema
		wantErr bool
	}{
		{
			"known_action",
			args{
				&ABI{ABI: &actionABI},
				"my_action",
			},
			RecordSchema{
				Type: "record",
				Name: "MyAction",
				Fields: []FieldSchema{
					{
						Name: "fieldA",
						Type: TypedSchema{Type: "long", EosType: "uint32"},
					},
					{
						Name: "fieldB",
						Type: TypedSchema{Type: "long", EosType: "int64"},
					},
				},
			},
			false,
		},
		{
			"unknown_action",
			args{
				&ABI{ABI: &actionABI},
				"unknown_action",
			},
			RecordSchema{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ActionToRecord(tt.args.abi, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("ActionToRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ActionToRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}

var tableABI eos.ABI = eos.ABI{
	Types: []eos.ABIType{{
		NewTypeName: "int64_alias",
		Type:        "int64",
	}},
	Structs: []eos.StructDef{{
		Name: "my_table_struct",
		Base: "",
		Fields: []eos.FieldDef{
			{
				Name: "fieldA",
				Type: "uint32",
			},
			{
				Name: "fieldB",
				Type: "int64_alias",
			},
		},
	}},
	Tables: []eos.TableDef{{
		Name:      "my.table",
		IndexType: "i64",
		Type:      "my_table_struct",
	}},
}

func TestTableToRecord(t *testing.T) {
	type args struct {
		abi  *ABI
		name eos.TableName
	}
	tests := []struct {
		name    string
		args    args
		want    RecordSchema
		wantErr bool
	}{
		{
			"known table",
			args{
				&ABI{ABI: &tableABI},
				"my.table",
			},
			RecordSchema{
				Type: "record",
				Name: "MyTableStruct",
				Fields: []FieldSchema{
					{
						Name: "fieldA",
						Type: TypedSchema{Type: "long", EosType: "uint32"},
					},
					{
						Name: "fieldB",
						Type: TypedSchema{Type: "long", EosType: "int64"},
					},
				},
			},
			false,
		},
		{
			"unknown table",
			args{
				&ABI{ABI: &actionABI},
				"unknown.table",
			},
			RecordSchema{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TableToRecord(tt.args.abi, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("TableToRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TableToRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}
