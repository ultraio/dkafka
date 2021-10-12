package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/dfuse-io/dkafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/derr"
	"go.uber.org/zap"
)

var PublishCmd = &cobra.Command{
	Use:   "publish",
	Short: "",
	Long:  "",
	RunE:  publishRunE,
}

var compressionTypes = NewEnumFlag("none", "gzip", "snappy", "lz4", "zstd")

var compressionLevel int8
var compressionType string

func init() {
	RootCmd.AddCommand(PublishCmd)

	PublishCmd.Flags().Var(compressionTypes, "kafka-compression-type", compressionTypes.Help("Specify the compression type to use for compressing message sets."))
	PublishCmd.Flags().Int8("kafka-compression-level", int8(-1), `Compression level parameter for algorithm selected by configuration property
kafka-compression-type. Higher values will result in better compression at the
cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip;
[0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression
level.`)
	PublishCmd.Flags().Int("kafka-message-max-bytes", 1_000_000, `Maximum Kafka protocol request message size.
Due to different framing overhead between protocol versions the producer is
unable to reliably enforce a strict max message limit at produce time,
the message size is checked against your raw uncompressed message.
The broker will also enforce the the topic's max.message.bytes limit
upon receiving your message. So make sure your brokers configuration
match your producers (same apply for consumers)
(see Apache Kafka documentation).`)

	PublishCmd.Flags().Duration("delay-between-commits", time.Second*10, "no commits to kafka blow this delay, except un shutdown")

	PublishCmd.Flags().String("event-source", "dkafka", "custom value for produced cloudevent source")
	PublishCmd.Flags().String("event-keys-expr", "[account]", `CEL expression defining the event keys. More then one key will result in multiple
events being sent. Must resolve to an array of strings`)
	PublishCmd.Flags().String("event-type-expr", "(notif?'!':'')+account+'/'+action", "CEL expression defining the event type. Must resolve to a string")

	PublishCmd.Flags().StringSlice("event-extensions-expr", []string{}, `cloudevent extension definitions in this format:
'{key}:{CEL expression}' (ex: 'blk:string(block_num)')`)

	PublishCmd.Flags().Bool("batch-mode", false, "Batch mode will ignore cursor and always start from {start-block-num}.")
	PublishCmd.Flags().Int64("start-block-num", 0, `If we are in {batch-mode} or no prior cursor exists,
start streaming from this block number (if negative, relative to HEAD)`)
	PublishCmd.Flags().Uint64("stop-block-num", 0, "If non-zero, stop processing before this block number")
	PublishCmd.Flags().String("state-file", "./dkafka.state.json", "progress will be saved into this file")

	PublishCmd.Flags().StringSlice("local-abi-files", []string{}, `repeatable, ABI file definition in this format:
'{account}:{path/to/filename}' (ex: 'eosio.token:/tmp/eosio_token.abi').
ABIs are used to decode DB ops. Provided ABIs have highest priority and
will never be fetched or updated`)
	PublishCmd.Flags().String("abicodec-grpc-addr", "", "if set, will connect to this endpoint to fetch contract ABIs")
	PublishCmd.Flags().Bool("fail-on-undecodable-db-op", false, `If true, program will fail and exit when a db OP cannot be decoded
(ex: missing or incompatible ABI file or invalid ABI fetched from abicodec`)
}

func publishRunE(cmd *cobra.Command, args []string) error {
	SetupLogger()

	extensions := make(map[string]string)
	for _, ext := range viper.GetStringSlice("publish-cmd-event-extensions-expr") {
		kv := strings.SplitN(ext, ":", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid value for extension: %s", ext)
		}
		extensions[kv[0]] = kv[1]
	}

	localABIFiles := make(map[string]string)
	for _, ext := range viper.GetStringSlice("publish-cmd-local-abi-files") {
		kv := strings.SplitN(ext, ":", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid value for local ABI file: %s", ext)
		}
		localABIFiles[kv[0]] = kv[1]
	}

	conf := &dkafka.Config{
		DfuseToken:        viper.GetString("global-dfuse-auth-token"),
		DfuseGRPCEndpoint: viper.GetString("global-dfuse-firehose-grpc-addr"),
		IncludeFilterExpr: viper.GetString("global-dfuse-firehose-include-expr"),

		DryRun:                     viper.GetBool("global-dry-run"),
		KafkaEndpoints:             viper.GetString("global-kafka-endpoints"),
		KafkaSSLEnable:             viper.GetBool("global-kafka-ssl-enable"),
		KafkaSSLCAFile:             viper.GetString("global-kafka-ssl-ca-file"),
		KafkaSSLAuth:               viper.GetBool("global-kafka-ssl-auth"),
		KafkaSSLClientCertFile:     viper.GetString("global-kafka-ssl-client-cert-file"),
		KafkaSSLClientKeyFile:      viper.GetString("global-kafka-ssl-client-key-file"),
		KafkaTopic:                 viper.GetString("global-kafka-topic"),
		KafkaCursorTopic:           viper.GetString("global-kafka-cursor-topic"),
		KafkaCursorPartition:       int32(viper.GetUint32("global-kafka-cursor-partition")),
		KafkaCursorConsumerGroupID: viper.GetString("global-kafka-cursor-consumer-group-id"),
		KafkaTransactionID:         viper.GetString("global-kafka-transaction-id"),
		KafkaCompressionType:       viper.GetString("publish-cmd-kafka-compression-type"),
		KafkaCompressionLevel:      viper.GetInt("publish-cmd-kafka-compression-level"),
		KafkaMessageMaxBytes:       viper.GetInt("publish-cmd-kafka-message-max-bytes"),
		CommitMinDelay:             viper.GetDuration("publish-cmd-delay-between-commits"),

		EventSource:     viper.GetString("publish-cmd-event-source"),
		EventKeysExpr:   viper.GetString("publish-cmd-event-keys-expr"),
		EventTypeExpr:   viper.GetString("publish-cmd-event-type-expr"),
		EventExtensions: extensions,

		BatchMode:     viper.GetBool("publish-cmd-batch-mode"),
		StartBlockNum: viper.GetInt64("publish-cmd-start-block-num"),
		StopBlockNum:  viper.GetUint64("publish-cmd-stop-block-num"),
		StateFile:     viper.GetString("publish-cmd-state-file"),

		LocalABIFiles:         localABIFiles,
		ABICodecGRPCAddr:      viper.GetString("publish-cmd-abicodec-grpc-addr"),
		FailOnUndecodableDBOP: viper.GetBool("publish-cmd-fail-on-undecodable-db-op"),
	}

	cmd.SilenceUsage = true
	signalHandler := derr.SetupSignalHandler(time.Second)

	zlog.Info("starting dkafka publisher", zap.Reflect("config", conf))
	app := dkafka.New(conf)
	go func() { app.Shutdown(app.Run()) }()

	select {
	case <-signalHandler:
		app.Shutdown(fmt.Errorf("shutdown signal received"))
	case <-app.Terminating():
	}
	zlog.Info("terminating", zap.Error(app.Err()))

	<-app.Terminated()
	return app.Err()
}
