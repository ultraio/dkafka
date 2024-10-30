package goavro

var eosToAvro map[string]string

func init() {
	eosToAvro = map[string]string{
		"bool":                 "boolean",
		"int8":                 "int",
		"uint8":                "int",
		"int16":                "int",
		"uint16":               "int",
		"int32":                "int",
		"uint32":               "long",
		"int64":                "long",
		"uint64":               "long",
		"varint32":             "int",
		"varuint32":            "long",
		"float32":              "float",
		"float64":              "double",
		"time_point":           "long",
		"time_point_sec":       "long",
		"block_timestamp_type": "long",
		"name":                 "string",
		"bytes":                "bytes",
		"string":               "string",
		"checksum160":          "bytes",
		"checksum256":          "bytes",
		"checksum512":          "bytes",
		"symbol":               "string",
		"symbol_code":          "string",
	}
}
