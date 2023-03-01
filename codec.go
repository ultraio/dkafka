package dkafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"go.uber.org/zap"
)

type CodecType = string

const (
	AvroCodec   CodecType = "avro"
	JsonCodec   CodecType = "json"
	schemaByIDs           = "/schemas/ids/"
)

type Codec interface {
	Marshal(buf []byte, value interface{}) ([]byte, error)
	Unmarshal(buf []byte) (interface{}, error)
	GetHeaders() []kafka.Header
}

func NewJSONCodec() Codec {
	return JSONCodec{}
}

type JSONCodec struct{}

var jsonKafkaHeader []kafka.Header = []kafka.Header{
	{
		Key:   "content-type",
		Value: []byte("application/json"),
	},
	{
		Key:   "ce_datacontenttype",
		Value: []byte("application/json"),
	},
}

func (c JSONCodec) Marshal(buf []byte, value interface{}) (bytes []byte, err error) {
	bytes, err = json.Marshal(value)
	if err != nil {
		bytes = buf
	} else {
		bytes = append(buf, bytes...)
	}
	return
}

func (c JSONCodec) Unmarshal(buf []byte) (interface{}, error) {
	value := make(map[string]interface{})
	err := json.Unmarshal(buf, &value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (c JSONCodec) GetHeaders() []kafka.Header {
	return jsonKafkaHeader
}

func NewKafkaAvroCodec(schemaRegistryURL string, schema *srclient.Schema, codec *goavro.Codec) Codec {
	u, _ := url.Parse(schemaRegistryURL)
	u, _ = u.Parse(schemaByIDs)
	t := fmt.Sprint(u, "%d")
	return KafkaAvroCodec{
		schemaURLTemplate: t,
		schema: RegisteredSchema{
			id:      uint32(schema.ID()),
			schema:  schema.Schema(),
			version: schema.Version(),
			codec:   codec,
		},
	}
}

type RegisteredSchema struct {
	id      uint32
	schema  string
	version int
	codec   *goavro.Codec
}

type KafkaAvroCodec struct {
	schemaURLTemplate string
	schema            RegisteredSchema
}

func (c KafkaAvroCodec) Marshal(buf []byte, value interface{}) (bytes []byte, err error) {
	zlog.Debug("marshal value to avro", zap.Uint32("schema_id", c.schema.id))
	schemaHeaderBytes := make([]byte, 5)
	// append magic byte 0
	// append schema id in int32 bigendian
	binary.BigEndian.PutUint32(schemaHeaderBytes[1:5], uint32(c.schema.id))
	bytes = append(bytes, schemaHeaderBytes...)
	// append append value
	bytes, err = c.schema.codec.BinaryFromNative(bytes, value)
	if err != nil {
		zlog.Debug("on error codec.BinaryFromNative()", zap.Error(err))
		bytes = buf
	} else {
		bytes = append(buf, bytes...)
	}
	return
}

func (c KafkaAvroCodec) Unmarshal(buf []byte) (interface{}, error) {
	if len(buf) < 5 {
		return nil, fmt.Errorf("invalid byte buffer it must at least have a length of 5 but: %d", len(buf))
	}
	// check magic byte
	if buf[0] != byte(0) {
		return nil, fmt.Errorf("invalid magic byte at the beginning of the buffer must be 0 but: %d", buf[0])
	}
	schemaId := binary.BigEndian.Uint32(buf[1:5])
	if c.schema.id != schemaId {
		return nil, fmt.Errorf("invalid magic byte at the beginning of the buffer must be %d but: %d", c.schema.id, schemaId)
	}
	value, _, err := c.schema.codec.NativeFromBinary(buf[5:])
	return value, err
}

var avroKafkaHeader []kafka.Header = []kafka.Header{
	{
		Key:   "content-type",
		Value: []byte("application/avro"),
	},
	{
		Key:   "ce_datacontenttype",
		Value: []byte("application/avro"),
	},
}

func (c KafkaAvroCodec) GetHeaders() []kafka.Header {
	u := fmt.Sprintf(c.schemaURLTemplate, c.schema.id)
	return append(avroKafkaHeader, kafka.Header{
		Key:   "ce_dataschema",
		Value: []byte(u),
	})
}
