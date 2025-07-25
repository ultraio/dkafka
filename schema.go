package dkafka

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"github.com/iancoleman/strcase"
	"github.com/linkedin/goavro/v2"
	"go.uber.org/zap"
)

const dkafkaNamespace = "io.dkafka"

type AvroSchemaGenOptions struct {
	Action    string
	Table     string
	Namespace string
	Type      string
	Version   string
	AbiSpec   *ABI
}

type ActionSchemaGenOptions struct {
	Action    string
	Namespace string
	Version   string
	AbiSpec   *ABI
}

type NamedSchemaGenOptions struct {
	Name      string
	Namespace string
	Version   string
	AbiSpec   *ABI
	Domain    string
}

func (o NamedSchemaGenOptions) GetVersion() string {
	return o.Version
}
func (o NamedSchemaGenOptions) GetDomain() string {
	return o.Domain
}
func (o NamedSchemaGenOptions) GetCompatibility() string {
	return "FORWARD"
}
func (o NamedSchemaGenOptions) GetType() string {
	return "notification"
}

func getNamespace(namespace string, abi *ABI) (string, error) {
	if namespace == "" {
		namespace = strcase.ToDelimited(abi.Account, '.')
	}
	return checkNamespace(namespace)
}

func GenerateActionSchema(options NamedSchemaGenOptions) (MessageSchema, error) {
	actionCamelCase, ceType := actionCeType(options.Name)
	namespace, err := getNamespace(options.Namespace, options.AbiSpec)
	if err != nil {
		return MessageSchema{}, err
	}
	actionInfoRecordName := fmt.Sprintf("%sActionInfo", actionCamelCase)
	actionParamsRecordName := fmt.Sprintf("%sActionParams", actionCamelCase)

	zlog.Debug(
		"generate action avro schema with following names:",
		zap.String("namespace", namespace),
		zap.String("ce_type", ceType),
		zap.String("actionInfo", actionInfoRecordName),
		zap.String("actionParams", actionParamsRecordName),
	)

	actionParamsSchema, err := ActionToRecord(options.AbiSpec, eos.ActionName(options.Name))
	if err != nil {
		return MessageSchema{}, err
	}
	actionParamsSchema.Name = actionParamsRecordName
	schema := newActionNotificationSchema(ceType, namespace, options, newActionInfoSchema(actionInfoRecordName, actionParamsSchema))

	return schema, nil
}

func actionCeType(name string) (actionCamelCase string, ceType string) {
	actionCamelCase = strcase.ToCamel(name)
	ceType = fmt.Sprintf("%sActionNotification", actionCamelCase)
	return
}

func tableCeType(name string) (tableCamelCase string, ceType string) {
	tableCamelCase = strcase.ToCamel(name)
	ceType = fmt.Sprintf("%sTableNotification", tableCamelCase)
	return
}

func dbOpRecordName(tableCamelCaseName string) string {
	return fmt.Sprintf("%sTableOp", tableCamelCaseName)
}

func GenerateTableSchema(options NamedSchemaGenOptions) (MessageSchema, error) {
	tableCamelCase, ceType := tableCeType(options.Name)
	namespace, err := getNamespace(options.Namespace, options.AbiSpec)
	if err != nil {
		return MessageSchema{}, err
	}
	dbOpInfoRecordName := fmt.Sprintf("%sTableOpInfo", tableCamelCase)
	dbOpRecordName := dbOpRecordName(tableCamelCase)

	zlog.Debug(
		"generate table avro schema with following names:",
		zap.String("namespace", namespace),
		zap.String("ce_type", ceType),
		zap.String("TableOpInfo", dbOpInfoRecordName),
		zap.String("TableOp", dbOpRecordName),
	)

	dbOpSchema, err := TableToRecord(options.AbiSpec, eos.TableName(options.Name))
	if err != nil {
		return MessageSchema{}, err
	}
	dbOpSchema.Name = dbOpRecordName
	dbOpInfoSchema := newDBOpInfoRecord(dbOpInfoRecordName, dbOpSchema)
	schema := newTableNotificationSchema(ceType, namespace, options, dbOpInfoSchema)

	return schema, nil
}

func ActionToRecord(abi *ABI, name eos.ActionName) (RecordSchema, error) {
	visited := make(map[string]string)
	initBuiltInTypesForActions()
	actionDef := abi.ActionForName(name)
	if actionDef == nil {
		return RecordSchema{}, fmt.Errorf("action '%s' not found", name)
	}

	return structToRecord(abi, actionDef.Type, visited)
}

func TableToRecord(abi *ABI, name eos.TableName) (RecordSchema, error) {
	visited := make(map[string]string)
	initBuiltInTypesForTables()
	tableDef := abi.TableForName(name)
	if tableDef == nil {
		return RecordSchema{}, fmt.Errorf("table '%s' not found", name)
	}

	return structToRecord(abi, tableDef.Type, visited)
}

func structToRecord(abi *ABI, structName string, visited map[string]string) (RecordSchema, error) {
	s := abi.StructForName(structName)
	if s == nil {
		return RecordSchema{}, fmt.Errorf("struct not found: %s", structName)
	}
	//inheritance
	parentRecord := RecordSchema{}
	if s.Base != "" {
		var err error
		parentRecord, err = structToRecord(abi, s.Base, visited)
		if err != nil {
			return RecordSchema{}, fmt.Errorf("cannot get parent structToRecord() for %s.%s error: %v", structName, s.Base, err)
		}
	}
	fields, err := abiFieldsToRecordFields(abi, s.Fields, visited)
	fields = append(parentRecord.Fields, fields...)
	if cap(fields) == 0 {
		fields = make([]FieldSchema, 0)
	}
	if err != nil {
		return RecordSchema{}, fmt.Errorf("%s abiFieldsToRecordFields() error: %v", structName, err)
	}
	return newRecordS(
		s.Name,
		fields,
	), nil
}

func abiFieldsToRecordFields(abi *ABI, fieldDefs []eos.FieldDef, visited map[string]string) ([]FieldSchema, error) {
	fields := make([]FieldSchema, len(fieldDefs))
	for i, fieldDef := range fieldDefs {
		field, err := abiFieldToRecordField(abi, fieldDef, visited)
		if err != nil {
			return fields, err
		}
		fields[i] = field
	}
	return fields, nil
}

func abiFieldToRecordField(abi *ABI, fieldDef eos.FieldDef, visited map[string]string) (FieldSchema, error) {
	zlog.Debug("convert field", zap.String("name", fieldDef.Name), zap.String("type", fieldDef.Type))
	schema, err := resolveFieldTypeSchema(abi, fieldDef.Type, visited)
	if err != nil {
		return FieldSchema{}, fmt.Errorf("resolve Field type schema error: %v, on field: %s", err, fieldDef.Name)
	}
	if union, ok := schema.(Union); ok && union[0] == "null" {
		return NewNullableField(fieldDef.Name, schema), nil
	} else {
		return FieldSchema{
			Name: fieldDef.Name,
			Type: schema,
		}, nil
	}
}

func variantToUnion(abi *ABI, name string, visited map[string]string) (Schema, error) {
	v := abi.VariantForName(name)
	if v == nil {
		return nil, nil
	}
	if uberVariant, found := hardcodedVariantType[name]; found {
		return uberVariant, nil
	}
	if len(v.Types) == 1 {
		//edge case where there is only one type in the union
		//then return the type ;)
		return resolveType(abi, v.Types[0], visited)
	} else {
		var union = make([]Schema, len(v.Types))
		for i, aType := range v.Types {
			if resolved, err := resolveType(abi, aType, visited); err == nil {
				union[i] = resolved
			} else {
				return nil, err
			}
		}
		return union, nil
	}
}

/*
   built_in_types.emplace("bool",                      pack_unpack<uint8_t>());
   built_in_types.emplace("int8",                      pack_unpack<int8_t>());
   built_in_types.emplace("uint8",                     pack_unpack<uint8_t>());
   built_in_types.emplace("int16",                     pack_unpack<int16_t>());
   built_in_types.emplace("uint16",                    pack_unpack<uint16_t>());
   built_in_types.emplace("int32",                     pack_unpack<int32_t>());
   built_in_types.emplace("uint32",                    pack_unpack<uint32_t>());
   built_in_types.emplace("int64",                     pack_unpack<int64_t>());
   built_in_types.emplace("uint64",                    pack_unpack<uint64_t>());
   built_in_types.emplace("int128",                    pack_unpack<int128_t>());
   built_in_types.emplace("uint128",                   pack_unpack<uint128_t>());
   built_in_types.emplace("varint32",                  pack_unpack<fc::signed_int>());
   built_in_types.emplace("varuint32",                 pack_unpack<fc::unsigned_int>());

   // TODO: Add proper support for floating point types. For now this is good enough.
   built_in_types.emplace("float32",                   pack_unpack<float>());
   built_in_types.emplace("float64",                   pack_unpack<double>());
   built_in_types.emplace("float128",                  pack_unpack<float128_t>());

   built_in_types.emplace("time_point",                pack_unpack<fc::time_point>());
   built_in_types.emplace("time_point_sec",            pack_unpack<fc::time_point_sec>());
   built_in_types.emplace("block_timestamp_type",      pack_unpack<block_timestamp_type>());

   built_in_types.emplace("name",                      pack_unpack<name>());

   built_in_types.emplace("bytes",                     pack_unpack<bytes>());
   built_in_types.emplace("string",                    pack_unpack<string>());

   built_in_types.emplace("checksum160",               pack_unpack<checksum160_type>());
   built_in_types.emplace("checksum256",               pack_unpack<checksum256_type>());
   built_in_types.emplace("checksum512",               pack_unpack<checksum512_type>());

   built_in_types.emplace("public_key",                pack_unpack_deadline<public_key_type>());
   built_in_types.emplace("signature",                 pack_unpack_deadline<signature_type>());

   built_in_types.emplace("symbol",                    pack_unpack<symbol>());
   built_in_types.emplace("symbol_code",               pack_unpack<symbol_code>());
   built_in_types.emplace("asset",                     pack_unpack<asset>());
   built_in_types.emplace("extended_asset",            pack_unpack<extended_asset>());
*/

// var assetSchema RecordSchema = RecordSchema{
// 	Namespace: "eosio",
// 	Name: "Asset",
// 	Type: "record",
// 	Doc: "Stores information for owner of asset",
// 	Convert: "eosio.Asset",
// 	Fields: []FieldSchema {
// 		{
// 			"name": "amount",
// 			"type": {
// 				"type": "bytes",
// 				"logicalType": "decimal",
// 				"precision": 28,
// 				"scale": 8,
// 			}
// 		},
// 		{
// 			"name": "symbol",
// 			"type": "string"
// 		},
// 	},

// }

func assetConverter(f func([]byte, interface{}) ([]byte, error)) func([]byte, interface{}) ([]byte, error) {
	return func(bytes []byte, value interface{}) ([]byte, error) {
		switch valueType := value.(type) {
		case eos.Asset:
			amount := big.NewRat(int64(valueType.Amount), int64(math.Pow10(int(valueType.Symbol.Precision))))
			return f(bytes, map[string]interface{}{"amount": amount, "symbol": valueType.Symbol.Symbol, "precision": valueType.Symbol.Precision})
		case *eos.Asset:
			amount := big.NewRat(int64(valueType.Amount), int64(math.Pow10(int(valueType.Symbol.Precision))))
			return f(bytes, map[string]interface{}{"amount": amount, "symbol": valueType.Symbol.Symbol, "precision": valueType.Symbol.Precision})
		default:
			return bytes, fmt.Errorf("unsupported asset type: %T", value)
		}
	}
}

func extendedAssetConverter(f func([]byte, any) ([]byte, error)) func([]byte, interface{}) ([]byte, error) {
	return func(bytes []byte, value any) ([]byte, error) {
		switch valueType := value.(type) {
		case eos.ExtendedAsset:
			return f(bytes, map[string]interface{}{"quantity": valueType.Asset, "contract": valueType.Contract})
		case *eos.ExtendedAsset:
			return f(bytes, map[string]interface{}{"quantity": valueType.Asset, "contract": valueType.Contract})
		default:
			return bytes, fmt.Errorf("unsupported extended_asset type: %T", value)
		}
	}
}

func publicKeyConverter(f func([]byte, interface{}) ([]byte, error)) func([]byte, interface{}) ([]byte, error) {
	return func(bytes []byte, value interface{}) ([]byte, error) {
		switch valueType := value.(type) {
		case ecc.PublicKey:
			return f(bytes, map[string]interface{}{"curve": valueType.Curve, "content": valueType.Content})
		case *ecc.PublicKey:
			return f(bytes, map[string]interface{}{"curve": valueType.Curve, "content": valueType.Content})
		default:
			return bytes, fmt.Errorf("unsupported public key type type: %T", value)
		}
	}
}

func signatureConverter(f func([]byte, interface{}) ([]byte, error)) func([]byte, interface{}) ([]byte, error) {
	return func(bytes []byte, value interface{}) ([]byte, error) {
		switch valueType := value.(type) {
		case ecc.Signature:
			return f(bytes, map[string]interface{}{"curve": valueType.Curve, "content": valueType.Content})
		case *ecc.Signature:
			return f(bytes, map[string]interface{}{"curve": valueType.Curve, "content": valueType.Content})
		default:
			return bytes, fmt.Errorf("unsupported public key type type: %T", value)
		}
	}
}

func int128Converter(f func([]byte, interface{}) ([]byte, error)) func([]byte, interface{}) ([]byte, error) {
	return func(bytes []byte, value interface{}) ([]byte, error) {
		switch valueType := value.(type) {
		case eos.Int128:
			rat := new(big.Rat).SetInt(valueType.BigInt())
			return f(bytes, rat)
		case *eos.Int128:
			rat := new(big.Rat).SetInt(valueType.BigInt())
			return f(bytes, rat)
		default:
			return bytes, fmt.Errorf("unsupported asset type: %T", value)
		}
	}
}

func uint128Converter(f func([]byte, interface{}) ([]byte, error)) func([]byte, interface{}) ([]byte, error) {
	return func(bytes []byte, value interface{}) ([]byte, error) {
		switch valueType := value.(type) {
		case eos.Uint128:
			rat := new(big.Rat).SetInt(valueType.BigInt())
			return f(bytes, rat)
		case *eos.Uint128:
			rat := new(big.Rat).SetInt(valueType.BigInt())
			return f(bytes, rat)
		default:
			return bytes, fmt.Errorf("unsupported asset type: %T", value)
		}
	}
}

func symbolConverter(f func([]byte, interface{}) ([]byte, error)) func([]byte, interface{}) ([]byte, error) {
	return func(bytes []byte, value interface{}) ([]byte, error) {
		switch valueType := value.(type) {
		case eos.Symbol:
			return f(bytes, valueType.Symbol)
		case *eos.Symbol:
			return f(bytes, valueType.Symbol)
		default:
			return bytes, fmt.Errorf("unsupported symbol type: %T", value)
		}
	}
}

var schemaTypeConverters = map[string]goavro.ConvertBuild{
	"eosio.Asset":         assetConverter,
	"eosio.ExtendedAsset": extendedAssetConverter,
	"ecc.PublicKey":       publicKeyConverter,
	"ecc.Signature":       signatureConverter,
	"eos.Int128":          int128Converter,
	"eos.Uint128":         uint128Converter,
	"eos.Symbol":          symbolConverter,
}

var avroPrimitiveTypeByBuiltInTypes map[string]TypedSchema
var avroDecimalLogicalTypeByBuiltInTypes map[string]DecimalLogicalType

type BuiltInRecordTypeGenerator func(map[string]string) RecordSchema

var avroRecordTypeByBuiltInTypes map[string]BuiltInRecordTypeGenerator

var assetSchema RecordSchema = RecordSchema{
	Type:      "record",
	Name:      "Asset",
	Namespace: "eosio",
	Convert:   "eosio.Asset",
	Fields: []FieldSchema{
		{
			Name: "amount",
			Type: json.RawMessage(`{
				"type": "bytes",
				"logicalType": "decimal",
				"precision": 32,
				"scale": 8
			}`),
		},
		{
			Name: "symbol",
			Type: "string",
		},
		{
			Name: "precision",
			Type: "int",
		},
	},
}

func assetSchemaGenerator(visited map[string]string) RecordSchema {
	return assetSchema
}

func extendedAssetSchemaGenerator(visited map[string]string) RecordSchema {
	var assetType Schema = assetSchema
	if record, found := visited["asset"]; found {
		assetType = record
	}
	visited["asset"] = reference(assetSchema)
	var extendedAssetSchema RecordSchema = RecordSchema{
		Type:      "record",
		Name:      "ExtendedAsset",
		Namespace: "eosio",
		Convert:   "eosio.ExtendedAsset",
		Fields: []FieldSchema{
			{
				Name: "quantity",
				Type: assetType,
			},
			{
				Name: "contract",
				Type: "string",
			},
		},
	}
	return extendedAssetSchema
}

var publicKeySchema RecordSchema = RecordSchema{
	Type:      "record",
	Name:      "PublicKey",
	Namespace: "ecc",
	Convert:   "ecc.PublicKey",
	Fields: []FieldSchema{
		{
			Name: "curve",
			Type: "int",
		},
		{
			Name: "content",
			Type: "bytes",
		},
	},
}

func publicKeySchemaGenerator(visited map[string]string) RecordSchema {
	return publicKeySchema
}

var signatureSchema RecordSchema = RecordSchema{
	Type:      "record",
	Name:      "Signature",
	Namespace: "ecc",
	Convert:   "ecc.Signature",
	Fields: []FieldSchema{
		{
			Name: "curve",
			Type: "int",
		},
		{
			Name: "content",
			Type: "bytes",
		},
	},
}

func signatureSchemaGenerator(visited map[string]string) RecordSchema {
	return signatureSchema
}

const (
	HardcodedUberVariant = "variant_int8_int16_int32_int64_uint8_uint16_uint32_uint64_float32_float64_string_INT8_VEC_INT16_VEC_INT32_VEC_INT64_VEC_UINT8_VEC_UINT16_VEC_UINT32_VEC_UINT64_VEC_FLOAT32_VEC_FLOAT64_VEC_STRING_VEC"
)

var hardcodedVariantType map[string]interface{}

// "uint128":   "",
// "float128",
// "extended_asset",

var allPrimitiveTypes = []TypedSchema{
	{
		Type: "int",
	},
	{
		Type: "long",
	},
	{
		Type: "float",
	},
	{
		Type: "double",
	},
	{
		Type: "string",
	},
	{
		Type: "bytes",
	},
	{
		Type: "boolean",
	},
}

func convertAllPrimitiveTypeToInterface() []interface{} {
	converted := make([]interface{}, len(allPrimitiveTypes))
	for i, v := range allPrimitiveTypes {
		converted[i] = v
	}
	return converted
}

func initBuiltInTypesForTables() {
	avroPrimitiveTypeByBuiltInTypes = map[string]TypedSchema{
		"bool":   {Type: "boolean", EosType: "bool"},
		"int8":   {Type: "int", EosType: "int8"},
		"uint8":  {Type: "int", EosType: "uint8"},
		"int16":  {Type: "int", EosType: "int16"},
		"uint16": {Type: "int", EosType: "uint16"},
		"int32":  {Type: "int", EosType: "int32"},
		"uint32": {Type: "long", EosType: "uint32"},
		"int64":  {Type: "long", EosType: "int64"},
		// FIXME: remove the logicalType
		"uint64":               {Type: "long", EosType: "uint64", LogicalType: "eos.uint64"}, // FIXME maybe use Decimal here see goavro or FIXED
		"varint32":             {Type: "int", EosType: "varint32"},
		"varuint32":            {Type: "long", EosType: "varuint32"},
		"float32":              {Type: "float", EosType: "float32"},
		"float64":              {Type: "double", EosType: "float64"},
		"time_point":           NewTimestampMillisType("time_point"),           // fork/eos-go/abidecoder.go TODO add ABI.nativeTime bool to skip time to string conversion in abidecoder read method
		"time_point_sec":       NewTimestampMillisType("time_point_sec"),       // fork/eos-go/abidecoder.go
		"block_timestamp_type": NewTimestampMillisType("block_timestamp_type"), // fork/eos-go/abidecoder.go
		"name":                 {Type: "string", EosType: "name"},
		"bytes":                {Type: "bytes", EosType: "bytes"},
		"string":               {Type: "string", EosType: "string"},
		"checksum160":          {Type: "bytes", EosType: "checksum160"},
		"checksum256":          {Type: "bytes", EosType: "checksum256"},
		"checksum512":          {Type: "bytes", EosType: "checksum512"},
		"symbol":               NewSymbolType(),
		"symbol_code":          {Type: "string", EosType: "symbol_code"}, // FIXME check with blockchain team
	}
	avroDecimalLogicalTypeByBuiltInTypes = map[string]DecimalLogicalType{
		"int128":  NewInt128Type(),
		"uint128": NewUint128Type(),
	}
	avroRecordTypeByBuiltInTypes = map[string]BuiltInRecordTypeGenerator{
		"asset":          assetSchemaGenerator,
		"extended_asset": extendedAssetSchemaGenerator,
		"public_key":     publicKeySchemaGenerator,
		"signature":      signatureSchemaGenerator,
	}
	hardcodedVariantType = map[string]interface{}{
		HardcodedUberVariant: append(convertAllPrimitiveTypeToInterface(), NewArray(allPrimitiveTypes)),
	}
}

// initBuiltInTypesForActions must rewrite the default types provided by initBuiltInTypesForTables
// because the action details is sent directly in json from the firehouse and the firehouse use the
// string representation of most of the advance type like asset and time based.
// FIXME Can be fixed by using the raw_data of the action trace ;)
func initBuiltInTypesForActions() {
	initBuiltInTypesForTables()
	avroPrimitiveTypeByBuiltInTypes["asset"] = TypedSchema{Type: "string", EosType: "asset"}
	avroPrimitiveTypeByBuiltInTypes["public_key"] = TypedSchema{Type: "string", EosType: "public_key"}
	avroPrimitiveTypeByBuiltInTypes["signature"] = TypedSchema{Type: "string", EosType: "signature"}
	avroPrimitiveTypeByBuiltInTypes["time_point"] = TypedSchema{Type: "string", EosType: "time_point"}
	avroPrimitiveTypeByBuiltInTypes["time_point_sec"] = TypedSchema{Type: "string", EosType: "time_point_sec"}
	avroPrimitiveTypeByBuiltInTypes["block_timestamp_type"] = TypedSchema{Type: "string", EosType: "block_timestamp_type"}
	avroPrimitiveTypeByBuiltInTypes["symbol"] = TypedSchema{Type: "string", EosType: "symbol"}
	avroRecordTypeByBuiltInTypes = map[string]BuiltInRecordTypeGenerator{}
}

func resolveFieldTypeSchema(abi *ABI, fieldType string, visited map[string]string) (Schema, error) {
	zlog.Debug("resolve", zap.String("type", fieldType))
	// remove binary extension marker if any
	var isBinaryExtension bool
	if elementType := strings.TrimSuffix(fieldType, "$"); elementType != fieldType {
		zlog.Debug("binary extension of", zap.String("element", elementType))
		fieldType = elementType
		isBinaryExtension = true
	}
	if elementType := strings.TrimSuffix(fieldType, "[]"); elementType != fieldType {
		// todo array
		zlog.Debug("array of", zap.String("element", elementType))
		itemType, err := resolveFieldTypeSchema(abi, elementType, visited)
		if err != nil {
			return nil, fmt.Errorf("type %s not found, error: %w", elementType, err)
		}
		return NewArray(itemType), nil
	}
	if optionalType := strings.TrimSuffix(fieldType, "?"); optionalType != fieldType || isBinaryExtension {
		zlog.Debug("optional of", zap.String("type", optionalType))
		oType, err := resolveFieldTypeSchema(abi, optionalType, visited)
		if err != nil {
			return nil, fmt.Errorf("type %s not found, error: %w", optionalType, err)
		}
		return NewOptional(oType), nil
	}
	s, err := resolveType(abi, fieldType, visited)
	if err != nil {
		return nil, fmt.Errorf("unknown type: %s, error: %v", fieldType, err)
	}
	return s, nil
}

func resolveType(abi *ABI, name string, visited map[string]string) (Schema, error) {
	zlog.Debug("find type", zap.String("name", name))
	if primitive, found := avroPrimitiveTypeByBuiltInTypes[name]; found {
		return primitive, nil
	}
	if decimal, found := avroDecimalLogicalTypeByBuiltInTypes[name]; found {
		return decimal, nil
	}
	// resolve from types
	if alias, found := abi.TypeNameForNewTypeName(name); found {
		return resolveFieldTypeSchema(abi, alias, visited)
	}
	// resolve from structs
	if referenceName, found := visited[name]; found {
		return referenceName, nil
	}

	if recordGenerator, found := avroRecordTypeByBuiltInTypes[name]; found {
		record := recordGenerator(visited)
		visited[name] = reference(record)
		return record, nil
	}
	union, er := variantToUnion(abi, name, visited)
	if union != nil && er == nil {
		return union, nil
	} else if er != nil {
		return union, er
	}

	if record, err := structToRecord(abi, name, visited); err == nil {
		visited[name] = reference(record)
		return record, nil
	}

	return nil, fmt.Errorf("type not found: %s", name)
}

func reference(record RecordSchema) string {
	if record.Namespace != "" {
		return fmt.Sprintf("%s.%s", record.Namespace, record.Name)
	} else {
		return record.Name
	}
}
