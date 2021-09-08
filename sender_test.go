package dkafka

import (
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Test_getKafkaProducerWithCompressionConfig(t *testing.T) {
	type args struct {
		conf   kafka.ConfigMap
		config *Config
	}
	tests := []struct {
		name    string
		args    args
		want    kafka.ConfigMap
		wantErr bool
	}{
		{
			name: "gzip",
			args: args{
				conf: kafka.ConfigMap{},
				config: &Config{
					KafkaCompressionType:  "gzip",
					KafkaCompressionLevel: int32(6),
				},
			},
			want: kafka.ConfigMap{
				"compression.type":  "gzip",
				"compression.level": int32(6),
			},
			wantErr: false,
		},
		{
			name: "snappy",
			args: args{
				conf: kafka.ConfigMap{},
				config: &Config{
					KafkaCompressionType:  "snappy",
					KafkaCompressionLevel: int32(-1),
				},
			},
			want: kafka.ConfigMap{
				"compression.type":  "snappy",
				"compression.level": int32(-1),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getKafkaProducerWithCompressionConfig(tt.args.conf, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("getKafkaProducerWithCompressionConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getKafkaProducerWithCompressionConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
