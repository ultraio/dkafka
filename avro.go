package dkafka

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/iancoleman/strcase"
)

var namePattern *regexp.Regexp
var namespacePattern *regexp.Regexp

func init() {
	namePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	namespacePattern = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)?(?:\.[A-Za-z_][A-Za-z0-9_]*)*$`)
}

func checkName(name string) (string, error) {
	if namePattern.MatchString(name) {
		return name, nil
	} else {
		return name, fmt.Errorf("invalid Avro name: %s", name)
	}
}

func checkNamespace(np string) (string, error) {
	if namespacePattern.MatchString(np) {
		return np, nil
	} else {
		return np, fmt.Errorf("invalid Avro namespace: %s", np)
	}
}

// Schema is represented in JSON by one of:
// - A JSON string, naming a defined type.
// - A JSON object, of the form:
// - {"type": "typeName" ...attributes...}
// where typeName is either a primitive or derived type name, as defined below. Attributes not defined in this document are permitted as metadata, but must not affect the format of serialized data.
// A JSON array, representing a union of embedded types.
type Schema = interface{}

type TypedSchema struct {
	EosType     string `json:"eos.type,omitempty"`
	LogicalType string `json:"logicalType,omitempty"`
	Type        string `json:"type"`
	Convert     string `json:"convert,omitempty"`
}

type MetaSupplier interface {
	GetVersion() string
	GetDomain() string
	GetCompatibility() string
	GetType() string
}

type MetaSchema struct {
	Compatibility string `json:"compatibility"`
	Type          string `json:"type"`
	Version       string `json:"version,omitempty"`
	Domain        string `json:"domain,omitempty"`
}

type MessageSchema struct {
	RecordSchema
	Meta MetaSchema `json:"meta"`
}

type FieldSchema struct {
	// Name a JSON string providing the name of the field (required)
	Name string `json:"name"`
	// Doc a JSON string describing this field for users (optional).
	Doc string `json:"doc,omitempty"`
	// Type a schema, as defined above
	Type Schema `json:"type"`
	// A default value for this field, only used when reading instances that lack the field for schema evolution purposes.
	Default json.RawMessage `json:"default,omitempty"`
}

type RecordSchema struct {
	// type always equal to "record"
	Type string `json:"type"`
	// Name a JSON string providing the name of the record (required)
	Name string `json:"name"`
	// Namespace a JSON string that qualifies the name
	Namespace string `json:"namespace,omitempty"`
	// Doc a JSON string providing documentation to the user of this schema (optional).
	Doc string `json:"doc,omitempty"`
	// Convert a native type to a goavro
	// map[string]interface{} that follow this specification/format.
	// It's the name of the function that do the convertion and is
	// available in the goavro schema builder context.
	Convert string `json:"convert,omitempty"`
	// Fields a JSON array, listing fields (required). Each field is a JSON object.
	Fields []FieldSchema `json:"fields"`
}

func (r *RecordSchema) AsCodecId() CodecId {
	return CodecId{
		Account: r.Namespace,
		Name:    r.Name,
	}
}

func newRecordS(name string, fields []FieldSchema) RecordSchema {
	return newRecordFQN("", name, fields)
}

func newRecordFQN(np string, name string, fields []FieldSchema) RecordSchema {
	return RecordSchema{
		Type:      "record",
		Name:      strcase.ToCamel(name),
		Namespace: np,
		Fields:    fields,
	}
}

func newMeta(supplier MetaSupplier) MetaSchema {
	return MetaSchema{
		Compatibility: "FORWARD",
		Type:          "notification",
		Version:       supplier.GetVersion(),
		Domain:        supplier.GetDomain(),
	}
}

type ArraySchema struct {
	// type always equal to "array"
	Type string `json:"type"`
	// items the schema of the array's items.
	Items Schema `json:"items"`
	// todo manage default
}

type Union = []Schema

func NewArray(itemType Schema) ArraySchema {
	return ArraySchema{
		Type:  "array",
		Items: itemType,
	}
}

var _defaultNull = json.RawMessage("null")

func NewNullableField(n string, t Schema) FieldSchema {
	return FieldSchema{
		Name:    n,
		Type:    t,
		Default: _defaultNull,
	}
}

func NewOptionalField(n string, t Schema) FieldSchema {
	return NewNullableField(n, NewOptional(t))
}

func NewOptional(schema Schema) Union {
	switch typpedSchema := schema.(type) {
	case []interface{}:
		return append([]interface{}{"null"}, typpedSchema...)
	default:
		return []Schema{"null", schema}
	}
}

func NewTimestampMillisField(name string) FieldSchema {
	return FieldSchema{
		Name: name,
		Type: NewTimestampMillisType("block_timestamp_type"),
	}
}

func NewTimestampMillisType(eosType string) TypedSchema {
	return TypedSchema{
		Type:        "long",
		LogicalType: "timestamp-millis",
		EosType:     eosType,
	}
}

type DecimalLogicalType struct {
	TypedSchema

	Precision int    `json:"precision"`
	Scale     int    `json:"scale"`
	Convert   string `json:"convert,omitempty"`
}

func NewInt128Type() DecimalLogicalType {
	return DecimalLogicalType{
		TypedSchema: TypedSchema{
			Type:        "bytes",
			LogicalType: "decimal",
			EosType:     "int128",
		},
		Scale:     0,
		Precision: 39,
		Convert:   "eos.Int128",
	}
}

func NewUint64Type() DecimalLogicalType {
	return DecimalLogicalType{
		TypedSchema: TypedSchema{
			Type:        "bytes",
			LogicalType: "decimal",
			EosType:     "uint64",
		},
		Precision: 20,
		Scale:     0,
	}
}

func NewUint128Type() DecimalLogicalType {
	return DecimalLogicalType{
		TypedSchema: TypedSchema{
			Type:        "bytes",
			LogicalType: "decimal",
			EosType:     "uint128",
		},
		Precision: 39,
		Scale:     0,
		Convert:   "eos.Uint128",
	}
}

func NewIntField(name string) FieldSchema {
	return FieldSchema{
		Name: name,
		Type: "int",
	}
}

func NewSymbolType() TypedSchema {
	return TypedSchema{
		Type:    "string",
		Convert: "eos.Symbol",
		EosType: "symbol",
	}
}
