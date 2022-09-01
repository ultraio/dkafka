package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/dfuse-io/dkafka"
	"github.com/iancoleman/strcase"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/derr"
	"go.uber.org/zap"
)

var codecTypes = NewEnumFlag(dkafka.JsonCodec, dkafka.AvroCodec)

type GenOptions struct {
	namespace string
	version   string
	outputDir string
	abiSpec   dkafka.AbiSpec
}

var CdCCmd = &cobra.Command{
	Use:   "cdc",
	Short: "Change Data Capture",
	Long:  "Change Data Capture",
}

var CdCTablesCmd = &cobra.Command{
	Use:   dkafka.TABLES_CDC_TYPE,
	Short: "Change Data Capture on smart contract tables",
	Long: `Change Data Capture on smart contract tables.
This argument is the smart contract account to capture.`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"account"},
	RunE:      cdcOnTables,
}

var CdCActionsCmd = &cobra.Command{
	Use:   dkafka.ACTIONS_CDC_TYPE,
	Short: "Change Data Capture on smart contract actions",
	Long: `Change Data Capture on smart contract actions.
This argument is the smart contract account to capture.`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"account"},
	RunE:      cdcOnActions,
}

var CdCSchemasCmd = &cobra.Command{
	Use:   "schemas [-n namespace] [-V version] [-o output-dir] abi-file-def",
	Short: "Generate all tables and actions messages avro schemas from ABI file definition",
	Long: `Generate all tables and actions messages avro schemas from ABI file definition. The ABI file argument (abi-file-def)
must be in this format:
'{account}:{path/to/filename}' (ex: 'eosio.token:/tmp/eosio_token.abi').
The schema type (CamelCase) is for: 
- table: <TableName>Notification
- action: <ActionName>Notification`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"abi-file-path"},
	Run:       generateAllAvroSchema,
}

func init() {
	RootCmd.AddCommand(CdCCmd)
	CdCCmd.PersistentFlags().Var(compressionTypes, "kafka-compression-type", compressionTypes.Help("Specify the compression type to use for compressing message sets."))
	CdCCmd.PersistentFlags().Int8("kafka-compression-level", int8(-1), `Compression level parameter for algorithm selected by configuration property
kafka-compression-type. Higher values will result in better compression at the
cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip;
[0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression
level.`)
	CdCCmd.PersistentFlags().Int("kafka-message-max-bytes", 1_000_000, `Maximum Kafka protocol request message size.
Due to different framing overhead between protocol versions the producer is
unable to reliably enforce a strict max message limit at produce time,
the message size is checked against your raw uncompressed message.
The broker will also enforce the the topic's max.message.bytes limit
upon receiving your message. So make sure your brokers configuration
match your producers (same apply for consumers)
(see Apache Kafka documentation).`)

	CdCCmd.PersistentFlags().Bool("batch-mode", false, "Batch mode will ignore cursor and always start from {start-block-num}.")
	CdCCmd.PersistentFlags().Int64("start-block-num", 0, `If we are in {batch-mode} or no prior cursor exists,
start streaming from this block number (if negative, relative to HEAD)`)
	CdCCmd.PersistentFlags().Uint64("stop-block-num", 0, "If non-zero, stop processing before this block number")
	CdCCmd.PersistentFlags().Bool("capture", false, "Activate the capture mode where blocks are saved on the file system in pb.json format.")

	CdCCmd.PersistentFlags().Duration("delay-between-commits", time.Second*10, "no commits to kafka blow this delay, except un shutdown")
	CdCCmd.PersistentFlags().String("event-source", "dkafka", "custom value for produced cloudevent source")

	CdCCmd.PersistentFlags().Bool("executed", false, "Specify publish messages based only on executed actions => modify the state of the blockchain. This remove the error messages")
	CdCCmd.PersistentFlags().Bool("irreversible", false, "Specify publish messages based only on irreversible actions")

	CdCCmd.PersistentFlags().Var(codecTypes, "codec", codecTypes.Help("Specify the codec to use to encode the messages."))
	CdCCmd.PersistentFlags().String("schema-registry-url", "http://localhost:8081", "Schema registry url whose schemas are pushed to")

	CdCCmd.PersistentFlags().StringP("namespace", "n", "", "namespace of the schema(s). Default: account name")
	CdCCmd.PersistentFlags().StringP("version", "V", "", "Optional but strongly recommended version of the schema(s) in a semver form: 1.2.3.")
	CdCCmd.PersistentFlags().StringSlice("local-abi-files", []string{}, `repeatable, ABI file definition in this format:
'{account}:{path/to/filename}[:{block-number}]' (ex: 'eosio.token:/tmp/eosio_token.abi[:3]').
ABIs are used to decode DB ops. Provided ABIs have highest priority and
will never be fetched or updated. Block number, being the default value: 0, will be used for versioning in the ABI schema.`)
	CdCCmd.PersistentFlags().String("abicodec-grpc-addr", "", "if set, will connect to this endpoint to fetch contract ABIs")

	CdCCmd.AddCommand(CdCActionsCmd)
	CdCActionsCmd.Flags().String("actions-expr", "", "A JSON Object that associate the a name of an action to CEL expression for the message key extration.")

	CdCCmd.AddCommand(CdCTablesCmd)
	CdCTablesCmd.Flags().StringSlice("table-name", []string{}, `table name(s) on which the message must be produced.
The name can include the key extractor pattern as follow:
<table-name>[:{k|s|s+k}]. Where k is for DBOp.PrimaryKey 
and s is for DBOp.Scope.If not specified the ':k' extractor
is used`)

	CdCCmd.AddCommand(CdCSchemasCmd)
	CdCSchemasCmd.Flags().StringP("output-dir", "o", "./", `Optional output directory for the avro schema. The file name pattern is
	the <account>-<schema-type>.avsc in snake-case.`)
}

func cdcOnTables(cmd *cobra.Command, args []string) error {
	SetupLogger()
	account := args[0]
	zlog.Debug(
		"CDC on table",
		zap.String("account", account),
	)
	return executeCdC(cmd, args, dkafka.TABLES_CDC_TYPE, func(c *dkafka.Config) *dkafka.Config {
		c.TableNames = viper.GetStringSlice("cdc-tables-cmd-table-name")
		return c
	})
}

func cdcOnActions(cmd *cobra.Command, args []string) error {
	SetupLogger()
	return executeCdC(cmd, args, dkafka.ACTIONS_CDC_TYPE, func(c *dkafka.Config) *dkafka.Config {
		c.ActionExpressions = viper.GetString("cdc-actions-cmd-actions-expr")
		return c
	})
}

func generateAllAvroSchema(cmd *cobra.Command, args []string) {
	SetupLogger()
	opts, err := genOptions(cmd, args)
	if err != nil {
		zlog.Fatal("action generetion fail", zap.Error(err))
	}
	zlog.Info("generate all schemas for:", zap.String("account", opts.abiSpec.Account))
	zlog.Info("generate all Actions")
	for _, action := range opts.abiSpec.Abi.Actions {
		err = doGenActionAvroSchema(action.Name.String(), opts)
		if err != nil {
			zlog.Fatal("doGenActionAvroSchema()", zap.Error(err))
		}
	}
	zlog.Info("generate all Tables")
	for _, table := range opts.abiSpec.Abi.Tables {
		err = doGenTableAvroSchema(string(table.Name), opts)
		if err != nil {
			zlog.Fatal("doGenTableAvroSchema()", zap.Error(err))
		}

	}
	zlog.Info("generate static dkafka schemas")
	if err = saveSchema(dkafka.CheckpointMessageSchema, "", opts.outputDir); err != nil {
		zlog.Fatal("fail to saveSchema()", zap.String("schema", dkafka.CheckpointMessageSchema.Name), zap.Error(err))
	}
}

func executeCdC(cmd *cobra.Command, args []string,
	cdcType string, f func(*dkafka.Config) *dkafka.Config) error {
	account := args[0]
	zlog.Debug(
		"CDC on action",
		zap.String("account", account),
	)
	localABIFiles, err := dkafka.ParseABIFileSpecs(viper.GetStringSlice("cdc-cmd-local-abi-files"))
	if err != nil {
		return err
	}

	conf := &dkafka.Config{
		DfuseToken:        viper.GetString("global-dfuse-auth-token"),
		DfuseGRPCEndpoint: viper.GetString("global-dfuse-firehose-grpc-addr"),
		IncludeFilterExpr: viper.GetString("global-dfuse-firehose-include-expr"),

		DryRun:                 viper.GetBool("global-dry-run"),
		KafkaEndpoints:         viper.GetString("global-kafka-endpoints"),
		KafkaSSLEnable:         viper.GetBool("global-kafka-ssl-enable"),
		KafkaSSLCAFile:         viper.GetString("global-kafka-ssl-ca-file"),
		KafkaSSLAuth:           viper.GetBool("global-kafka-ssl-auth"),
		KafkaSSLClientCertFile: viper.GetString("global-kafka-ssl-client-cert-file"),
		KafkaSSLClientKeyFile:  viper.GetString("global-kafka-ssl-client-key-file"),
		KafkaTopic:             viper.GetString("global-kafka-topic"),
		KafkaCursorTopic:       viper.GetString("global-kafka-cursor-topic"),
		KafkaCursorPartition:   int32(viper.GetUint32("global-kafka-cursor-partition")),
		KafkaCompressionType:   viper.GetString("cdc-cmd-kafka-compression-type"),
		KafkaCompressionLevel:  viper.GetInt("cdc-cmd-kafka-compression-level"),
		KafkaMessageMaxBytes:   viper.GetInt("cdc-cmd-kafka-message-max-bytes"),
		CommitMinDelay:         viper.GetDuration("cdc-cmd-delay-between-commits"),

		BatchMode:     viper.GetBool("cdc-cmd-batch-mode"),
		StartBlockNum: viper.GetInt64("cdc-cmd-start-block-num"),
		StopBlockNum:  viper.GetUint64("cdc-cmd-stop-block-num"),
		StateFile:     viper.GetString("cdc-cmd-state-file"),
		Capture:       viper.GetBool("cdc-cmd-capture"),

		EventSource: viper.GetString("cdc-cmd-event-source"),

		CdCType:      cdcType,
		Account:      account,
		Irreversible: viper.GetBool("cdc-cmd-irreversible"),
		Executed:     viper.GetBool("cdc-cmd-executed"),

		Codec:             viper.GetString("cdc-cmd-codec"),
		SchemaRegistryURL: viper.GetString("cdc-cmd-schema-registry-url"),
		SchemaNamespace:   viper.GetString("cdc-cmd-namespace"),
		SchemaVersion:     viper.GetString("cdc-cmd-version"),
		LocalABIFiles:     localABIFiles,
		ABICodecGRPCAddr:  viper.GetString("cdc-cmd-abicodec-grpc-addr"),
	}
	conf = f(conf)
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

func genOptions(cmd *cobra.Command, args []string) (opts GenOptions, err error) {
	namespace := viper.GetString("cdc-cmd-namespace")
	outputDir := viper.GetString("cdc-schemas-cmd-output-dir")
	version := viper.GetString("cdc-cmd-version")
	abiString := args[0]

	account, abiFile, err := dkafka.ParseABIFileSpec(abiString)
	if err != nil {
		return
	}

	if _, err = os.Stat(outputDir); err != nil {
		err = fmt.Errorf("cannot reach %s error: %v", outputDir, err)
		return
	}

	zlog.Debug(
		"global avro gen options",
		zap.String("namespace", namespace),
		zap.String("version", version),
		zap.String("outputDir", outputDir),
		zap.String("abi", abiString),
	)

	abi, err := dkafka.LoadABIFile(abiFile)
	if err != nil {
		return
	}
	abiSpec := dkafka.AbiSpec{
		Account: account,
		Abi:     abi,
	}

	return GenOptions{
		namespace: namespace,
		version:   version,
		outputDir: outputDir,
		abiSpec:   abiSpec,
	}, nil
}

func doGenActionAvroSchema(name string, opts GenOptions) error {
	zlog.Info("generate schema for:", zap.String("account", opts.abiSpec.Account), zap.String("action", name))
	return doGenAvroSchema(name, opts, dkafka.GenerateActionSchema)
}

func doGenTableAvroSchema(name string, opts GenOptions) error {
	zlog.Info("generate schema for:", zap.String("account", opts.abiSpec.Account), zap.String("table", name))
	return doGenAvroSchema(name, opts, dkafka.GenerateTableSchema)
}

func doGenAvroSchema(name string, opts GenOptions, f func(dkafka.NamedSchemaGenOptions) (dkafka.MessageSchema, error)) error {
	schema, err := f(dkafka.NamedSchemaGenOptions{
		Name:      name,
		Namespace: opts.namespace,
		Version:   opts.version,
		AbiSpec:   opts.abiSpec,
	})
	if err != nil {
		return fmt.Errorf("generation error: %v", err)
	}
	zlog.Debug("dkafka.GenerateActionSchema()", zap.Any("schema", schema), zap.Error(err))
	err = saveSchema(schema, opts.abiSpec.Account, opts.outputDir)
	if err != nil {
		return fmt.Errorf("saveSchema() error: %v", err)
	}
	return nil
}

func saveSchema(schema dkafka.MessageSchema, prefix string, outputDir string) error {
	fileName := strcase.ToKebab(fmt.Sprintf("%s%s", prefix, schema.Name))
	fileName = fmt.Sprintf("%s.avsc", fileName)
	filePath := filepath.Join(outputDir, fileName)
	zlog.Info("save schema", zap.String("path", filePath))
	jsonString, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("cannot convert schema to json error: %v", err)
	}
	ioutil.WriteFile(filePath, jsonString, 0664)
	if err != nil {
		return fmt.Errorf("cannot write schema to '%s', error: %v", filePath, err)
	}
	return nil
}
