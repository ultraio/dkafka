package dkafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbabicodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/abicodec/v1"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/eoscanada/eos-go"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/cel-go/cel"
	"github.com/riferrei/srclient"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/dgrpc"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	pbhealth "github.com/streamingfast/pbgo/grpc/health/v1"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"

	"github.com/streamingfast/shutter"
)

const TABLES_CDC_TYPE = "tables"
const ACTIONS_CDC_TYPE = "actions"

type Config struct {
	DfuseGRPCEndpoint string
	DfuseToken        string

	DryRun        bool // do not connect to Kafka, just print to stdout
	BatchMode     bool
	Capture       bool
	StartBlockNum int64
	StopBlockNum  uint64
	StateFile     string
	Force         bool

	KafkaEndpoints         string
	KafkaSSLEnable         bool
	KafkaSSLCAFile         string
	KafkaSSLAuth           bool
	KafkaSSLClientCertFile string
	KafkaSSLClientKeyFile  string
	KafkaCompressionType   string
	KafkaCompressionLevel  int
	KafkaMessageMaxBytes   int

	KafkaCursorConsumerGroupID string
	KafkaTransactionID         string
	CommitMinDelay             time.Duration

	KafkaTopic           string
	KafkaCursorTopic     string
	KafkaCursorPartition int32
	EventSource          string

	IncludeFilterExpr string
	EventKeysExpr     string
	EventTypeExpr     string
	ActionsExpr       string

	LocalABIFiles         map[string]string
	ABICodecGRPCAddr      string
	FailOnUndecodableDBOP bool

	CdCType           string
	Account           string
	ActionExpressions string
	TableNames        []string
	Executed          bool
	Irreversible      bool

	Codec             string
	SchemaRegistryURL string
	SchemaNamespace   string
	SchemaVersion     string
}

type App struct {
	*shutter.Shutter
	config         *Config
	readinessProbe pbhealth.HealthClient
}

func New(config *Config) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
	}
}

func (a *App) Run() (err error) {
	go startPrometheusMetrics("/metrics", ":9102")
	// get and setup the dfuse fetcher that gets a stream of blocks, includes the filter, will include the auth token resolver/refresher
	addr := a.config.DfuseGRPCEndpoint
	plaintext := strings.Contains(addr, "*")
	addr = strings.Replace(addr, "*", "", -1)
	var dialOptions []grpc.DialOption
	if plaintext {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	} else {
		transportCreds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(transportCreds))
		credential := oauth.NewOauthAccess(&oauth2.Token{AccessToken: a.config.DfuseToken, TokenType: "Bearer"})
		dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(credential))
	}

	var saveBlock SaveBlock
	saveBlock = saveBlockNoop
	if a.config.Capture {
		saveBlock = saveBlockProto
	}

	var abiFiles map[string]*ABI
	if len(a.config.LocalABIFiles) != 0 {
		abiFiles, err = LoadABIFiles(a.config.LocalABIFiles)
		if err != nil {
			return err
		}
	}

	var abiCodecClient pbabicodec.DecoderClient
	if a.config.ABICodecGRPCAddr != "" {
		abiCodecConn, err := dgrpc.NewInternalClient(a.config.ABICodecGRPCAddr)
		if err != nil {
			return fmt.Errorf("setting up abicodec client: %w", err)
		}

		abiCodecClient = pbabicodec.NewDecoderClient(abiCodecConn)
	}
	source := a.config.EventSource
	if len(source) == 0 {
		if hostname, err := os.Hostname(); err == nil {
			source = hostname
		} else {
			zlog.Warn("cannot get host name", zap.Error(err))
			// use generic name
			source = "dkafka"
		}
	}

	zlog.Info("event source", zap.String("ce_source", source))
	sourceHeader := kafka.Header{
		Key:   "ce_source",
		Value: []byte(source),
	}
	specHeader := kafka.Header{
		Key:   "ce_specversion",
		Value: []byte("1.0"),
	}

	headers := []kafka.Header{
		sourceHeader,
		specHeader,
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.OnTerminating(func(_ error) {
		cancel()
	})
	out := make(chan error, 1)
	defer close(out)
	var producer *kafka.Producer
	if !a.config.DryRun {
		producer, err = getKafkaProducer(createKafkaConfigForMessageProducer(a.config))
		if err != nil {
			return fmt.Errorf("cannot get kafka producer: %w", err)
		}
		go func() {
			for e := range producer.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					// The message delivery report, indicating success or
					// permanent failure after retries have been exhausted.
					// Application level retries won't help since the client
					// is already configured to do that.
					m := ev
					if m.TopicPartition.Error != nil {
						err := m.TopicPartition.Error
						zlog.Error("Delivery failed", zap.Stringp("topic", m.TopicPartition.Topic), zap.Int32("partition", m.TopicPartition.Partition), zap.Int64("offset", int64(m.TopicPartition.Offset)), zap.Error(err))
						out <- err
					} else {
						zlog.Debug("Delivered message", zap.Stringp("topic", m.TopicPartition.Topic), zap.Int32("partition", m.TopicPartition.Partition), zap.Int64("offset", int64(m.TopicPartition.Offset)))
					}
				case kafka.Error:
					// Generic client instance-level errors, such as
					// broker connection failures, authentication issues, etc.
					//
					// These errors should generally be considered informational
					// as the underlying client will automatically try to
					// recover from any errors encountered, the application
					// does not need to take action on them.
					zlog.Error("Kafka client fail", zap.Error(ev))
					out <- ev
				default:
					zlog.Debug("Ignored producer event", zap.Stringer("event", ev.(fmt.Stringer)))
				}
			}
		}()
		defer producer.Close()
	}

	zlog.Info("setting up ABIDecoder")
	abiDecoder := NewABIDecoder(abiFiles, abiCodecClient, ctx)

	if abiDecoder.IsNOOP() && a.config.FailOnUndecodableDBOP {
		return fmt.Errorf("invalid config: no abicodec GRPC address and no local ABI file has been set, but fail-on-undecodable-db-op is enabled")
	}

	var appCtx appCtx
	if a.config.CdCType != "" {
		appCtx, err = a.NewCDCCtx(ctx, producer, headers, source, abiDecoder, saveBlock)
	} else {
		appCtx, err = a.NewLegacyCtx(ctx, producer, headers, abiDecoder, saveBlock)
	}
	if err != nil {
		return err
	}

	if a.config.DryRun {
		appCtx.sender = &DryRunSender{}
	}
	req := NewRequest(appCtx.filter, a.config.StartBlockNum, a.config.StopBlockNum, appCtx.cursor, a.config.Irreversible)

	zlog.Debug("Connect to dfuse grpc", zap.String("address", addr), zap.Any("options", dialOptions))
	conn, err := grpc.Dial(addr,
		dialOptions...,
	)
	if err != nil {
		return fmt.Errorf("connecting to grpc address %s: %w", addr, err)
	}

	zlog.Debug("Create streaming client")
	client := pbbstream.NewBlockStreamV2Client(conn)

	zlog.Info("Filter blocks", zap.Any("request", req))
	executor, err := client.Blocks(ctx, req)
	if err != nil {
		return fmt.Errorf("requesting blocks from dfuse firehose: %w", err)
	}
	return iterate(ctx, cancel, appCtx, a.config.CommitMinDelay, executor, out)
}

type appCtx struct {
	adapter Adapter
	filter  string
	sender  Sender
	cursor  string
}

func (a *App) NewCDCCtx(ctx context.Context, producer *kafka.Producer, headers []kafka.Header, source string, abiDecoder *ABIDecoder, saveBlock SaveBlock) (appCtx, error) {
	var adapter Adapter
	var filter string
	var cursor string
	var abiCodec ABICodec
	var generator Generator2
	var err error
	eos.LegacyJSON4Asset = false
	eos.NativeType = true
	appCtx := appCtx{}
	if cursor, err = a.loadCursor(); err != nil {
		return appCtx, fmt.Errorf("failed to load cursor at startup time for cdc on %s with error: %w", a.config.CdCType, err)
	}

	switch cdcType := a.config.CdCType; cdcType {
	case TABLES_CDC_TYPE:
		msg := MessageSchemaGenerator{
			Namespace: a.config.SchemaNamespace,
			Version:   a.config.SchemaVersion,
			Account:   a.config.Account,
			Source:    source,
		}
		abiCodec, err = newABICodec(
			a.config.Codec, a.config.Account, a.config.SchemaRegistryURL, abiDecoder,
			msg.getTableSchema,
		)
		if err != nil {
			return appCtx, err
		}
		filter = createCdCFilter(a.config.Account, a.config.Executed)
		var finder TableKeyExtractorFinder
		if finder, err = buildTableKeyExtractorFinder(a.config.TableNames); err != nil {
			return appCtx, err
		}
		generator = TableGenerator{
			getExtractKey: finder,
			abiCodec:      abiCodec,
		}
	case ACTIONS_CDC_TYPE:
		filter = createCdCFilter(a.config.Account, a.config.Executed)
		actionKeyExpressions, err := createCdcKeyExpressions(a.config.ActionExpressions)
		if err != nil {
			return appCtx, err
		}
		msg := MessageSchemaGenerator{
			Namespace: a.config.SchemaNamespace,
			Version:   a.config.SchemaVersion,
			Account:   a.config.Account,
			Source:    source,
		}
		abiCodec, err = newABICodec(
			a.config.Codec, a.config.Account, a.config.SchemaRegistryURL, abiDecoder,
			msg.getActionSchema,
		)
		if err != nil {
			return appCtx, err
		}
		generator = ActionGenerator2{
			keyExtractors: actionKeyExpressions,
			abiCodec:      abiCodec,
		}
	default:
		return appCtx, fmt.Errorf("unsupported CDC type %s", cdcType)
	}
	adapter = &CdCAdapter{
		topic:     a.config.KafkaTopic,
		saveBlock: saveBlock,
		headers:   headers,
		generator: generator,
		abiCodec:  abiCodec,
	}
	appCtx.adapter = adapter
	appCtx.cursor = cursor
	appCtx.filter = filter
	appCtx.sender = NewFastSender(ctx, producer, a.config.KafkaTopic, headers, abiCodec)
	return appCtx, nil
}

func buildTableKeyExtractorFinder(tableNamesConfig []string) (finder TableKeyExtractorFinder, err error) {
	tableNames := make(map[string]ExtractKey)
	for _, name := range tableNamesConfig {
		kv := strings.SplitN(name, ":", -1)
		if len(kv) > 2 {
			err = fmt.Errorf("unsupported table key extractor pattern: %s, on support {<name>|*}[:{k|s|s+k}]", name)
			return
		}
		var ek ExtractKey = extractFullKey
		if len(kv) == 2 {
			switch kv[1] {
			case "k":
				ek = extractPrimaryKey
			case "s+k":
				ek = extractFullKey
			case "s":
				ek = extractScope
			default:
				err = fmt.Errorf("unsupported table key extractor pattern: %s, on support {<name>|*}[:{k|s|s+k}]", name)
				return
			}
		}
		tableNames[kv[0]] = ek
	}
	if extractKey, wildcardFound := tableNames["*"]; wildcardFound {

		if len(tableNames) == 1 {
			finder = func(tableName string) (ExtractKey, bool) {
				return extractKey, true
			}
		} else {
			finder = func(tableName string) (extract ExtractKey, found bool) {
				extract, found = tableNames[tableName]
				if !found {
					extract = extractKey
					found = true
				}
				return
			}
		}
	} else {
		finder = func(tableName string) (extract ExtractKey, found bool) {
			extract, found = tableNames[tableName]
			return
		}
	}

	return
}

func createCdcKeyExpressions(cdcExpression string) (finder ActionKeyExtractorFinder, err error) {
	var cdcProgramByKeys map[string]cel.Program
	cdcExpressionMap := make(map[string]string)
	var rawJSON = json.RawMessage(cdcExpression)
	if err = json.Unmarshal(rawJSON, &cdcExpressionMap); err != nil {
		return
	}
	cdcProgramByKeys = make(map[string]cel.Program)
	for k, v := range cdcExpressionMap {
		var prog cel.Program
		prog, err = exprToCelProgramWithEnv(v, ActionDeclarations)
		if err != nil {
			return
		}
		cdcProgramByKeys[k] = prog
	}
	if wildcardProgram, wildcardFound := cdcProgramByKeys["*"]; wildcardFound {
		if len(cdcProgramByKeys) == 1 {
			finder = func(actionName string) (cel.Program, bool) {
				return wildcardProgram, true
			}
		} else {
			finder = func(actionName string) (prog cel.Program, found bool) {
				prog, found = cdcProgramByKeys[actionName]
				if !found {
					prog = wildcardProgram
					found = true
				}
				return
			}
		}
	} else {
		finder = func(actionName string) (prog cel.Program, found bool) {
			prog, found = cdcProgramByKeys[actionName]
			return
		}
	}
	return
}

func LoadCursorFromCursorTopic(config *Config, cp checkpointer) (string, error) {
	cursor, err := cp.Load()
	switch err {
	case ErrNoCursor:
		zlog.Info("no cursor found in cursor topic", zap.String("cursor_topic", config.KafkaCursorTopic))
		return "", nil
	case nil:
		c, err := forkable.CursorFromOpaque(cursor)
		if err != nil {
			zlog.Error("cannot decode cursor", zap.String("cursor", cursor), zap.Error(err))
			return "", err
		}
		zlog.Info("running in live mode, found cursor from cursor topic",
			zap.String("cursor", cursor),
			zap.Stringer("plain_cursor", c),
			zap.Stringer("cursor_block", c.Block),
			zap.Stringer("cursor_head_block", c.HeadBlock),
			zap.Stringer("cursor_LIB", c.LIB),
			zap.String("cursor_topic", config.KafkaCursorTopic),
		)
	default:
		return cursor, fmt.Errorf("error loading cursor: %w", err)
	}
	return cursor, nil
}

func (a *App) loadCursor() (cursor string, err error) {
	if a.config.Force {
		zlog.Info("Force option activated skip loading cursor", zap.String("topic", a.config.KafkaTopic))
		return
	}
	zlog.Info("try to find previous position from message topic", zap.String("topic", a.config.KafkaTopic))
	cursor, err = LoadCursor(createKafkaConfig(a.config), a.config.KafkaTopic)
	if err != nil {
		return "", fmt.Errorf("fail to load cursor on topic: %s, due to: %w", a.config.KafkaTopic, err)
	}
	// UOD-1290 load cursor from legacy cursor topic for dkafka migration
	if cursor == "" && a.config.KafkaCursorTopic != "" {
		zlog.Info("no cursor in message topic try to load it from legacy cursor topic...", zap.String("topic_cursor", a.config.KafkaCursorTopic))
		cp := newKafkaCheckpointer(createKafkaConfig(a.config), a.config.KafkaCursorTopic, a.config.KafkaCursorPartition, a.config.KafkaTopic, a.config.KafkaCursorConsumerGroupID)
		if cursor, err = LoadCursorFromCursorTopic(a.config, cp); err != nil {
			return "", fmt.Errorf("fail to load cursor for legacy topic: %s, due to: %w", a.config.KafkaCursorTopic, err)
		}
	} else {
		zlog.Info("no cursor topic specified skip loading position from it")
	}
	return
}

func (a *App) NewLegacyCtx(ctx context.Context, producer *kafka.Producer, headers []kafka.Header, abiDecoder *ABIDecoder, saveBlock SaveBlock) (appCtx, error) {
	var adapter Adapter
	var filter string = a.config.IncludeFilterExpr
	var sender Sender
	var cursor string
	var err error
	eos.LegacyJSON4Asset = true
	eos.NativeType = false
	appCtx := appCtx{}

	if cursor, err = a.loadCursor(); err != nil {
		return appCtx, fmt.Errorf("failed to load cursor at startup time for json publish message with error: %w", err)
	}

	headers = append(headers,
		kafka.Header{
			Key:   "content-type",
			Value: []byte("application/json"),
		},
		kafka.Header{
			Key:   "ce_datacontenttype",
			Value: []byte("application/json"),
		},
	)
	if a.config.ActionsExpr != "" {
		adapter, err = newActionsAdapter(a.config.KafkaTopic,
			saveBlock,
			abiDecoder.DecodeDBOps,
			a.config.FailOnUndecodableDBOP,
			a.config.ActionsExpr,
			headers,
		)
		if err != nil {
			return appCtx, err
		}
	} else {
		eventTypeProg, err := exprToCelProgram(a.config.EventTypeExpr)
		if err != nil {
			return appCtx, fmt.Errorf("cannot parse event-type-expr: %w", err)
		}
		eventKeyProg, err := exprToCelProgram(a.config.EventKeysExpr)
		if err != nil {
			return appCtx, fmt.Errorf("cannot parse event-keys-expr: %w", err)
		}
		adapter = newAdapter(
			a.config.KafkaTopic,
			saveBlock,
			abiDecoder.DecodeDBOps,
			a.config.FailOnUndecodableDBOP,
			eventTypeProg,
			eventKeyProg,
			headers,
		)
	}
	abiCodec, err := newABICodec(
		JsonCodec, a.config.Account, a.config.SchemaRegistryURL, abiDecoder,
		func(s string, a *ABI) (MessageSchema, error) {
			return MessageSchema{}, fmt.Errorf("json message publisher does not support schema generation. Requested schema: %s", s)
		},
	)
	sender = NewFastSender(ctx, producer, a.config.KafkaTopic, headers, abiCodec)
	if err != nil {
		return appCtx, err
	}

	appCtx.adapter = adapter
	appCtx.cursor = cursor
	appCtx.filter = filter
	appCtx.sender = sender
	return appCtx, nil
}

func iterate(ctx context.Context, cancel context.CancelFunc, appCtx appCtx, tickDuration time.Duration, stream pbbstream.BlockStreamV2_BlocksClient, out chan error) error {
	// loop: receive block,  transform block, send message...
	zlog.Info("Start looping over blocks...")
	in := make(chan BlockStep, 10)
	ticker := time.NewTicker(tickDuration)
	go blockHandler(ctx, appCtx, in, ticker.C, out)
	for {
		select {
		case err := <-out:
			zlog.Error("exit block streaming on error", zap.Error(err))
			ticker.Stop()
			close(in)
			return err
		default:
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("error on receive: %w", err)
			}
			blk := &pbcodec.Block{}
			if err := ptypes.UnmarshalAny(msg.Block, blk); err != nil {
				return fmt.Errorf("decoding any of type %q: %w", msg.Block.TypeUrl, err)
			}
			zlog.Info("Receive new block", zap.Uint32("block_num", blk.Number), zap.String("block_id", blk.Id), zap.String("cursor", msg.Cursor))
			blocksReceived.Inc()
			blkStep := BlockStep{
				blk:    blk,
				step:   msg.Step,
				cursor: msg.Cursor,
			}
			in <- blkStep
		}
	}
}

func blockHandler(ctx context.Context, appCtx appCtx, in <-chan BlockStep, ticks <-chan time.Time, out chan<- error) {
	var lastBlkStep BlockStep = BlockStep{cursor: appCtx.cursor}
	hasFail := false
	var adapter Adapter = appCtx.adapter
	var s Sender = appCtx.sender
	for {
		select {
		case blkStep := <-in:
			if hasFail {
				zlog.Debug("skip incoming block message after failure")
				continue
			}
			blkStep.previousCursor = lastBlkStep.cursor
			kafkaMsgs, err := adapter.Adapt(blkStep)
			if err != nil {
				hasFail = true
				zlog.Debug("fail fast on adapter.Adapt() send message to -> out chan", zap.Error(err))
				out <- fmt.Errorf("transform to kafka message at block_num: %d, cursor: %s, , %w", blkStep.blk.Number, blkStep.cursor, err)
			}
			lastBlkStep = blkStep
			if len(kafkaMsgs) == 0 {
				continue
			}
			if err = s.Send(ctx, kafkaMsgs, blkStep); err != nil {
				hasFail = true
				zlog.Debug("fail fast on sender.Send() send message to -> out chan", zap.Error(err))
				out <- fmt.Errorf("send to kafka message at: %s, %w", blkStep.cursor, err)
			}
			messagesSent.Add(float64(len(kafkaMsgs)))
		case <-ticks:
			if hasFail {
				zlog.Debug("skip incoming tick message after failure")
				continue
			}
			if err := s.SaveCP(ctx, lastBlkStep); err != nil {
				hasFail = true
				zlog.Debug("fail fast on sender.SaveCP() send message to -> out chan", zap.Error(err))
				out <- fmt.Errorf("fail to save check point: %s, %w", lastBlkStep.cursor, err)
			}
		}
	}
}

func createCdCFilter(account string, executed bool) string {
	filter := fmt.Sprintf("(account==\"%s\" && receiver==\"%s\") || (action==\"setabi\" && account==\"eosio\" && data.account==\"%s\")", account, account, account)
	if executed {
		filter = fmt.Sprintf("executed && %s", filter)
	}
	return filter
}

func createKafkaConfig(appConf *Config) kafka.ConfigMap {
	conf := kafka.ConfigMap{
		"bootstrap.servers": appConf.KafkaEndpoints,
	}
	if appConf.KafkaSSLEnable {
		conf["security.protocol"] = "ssl"
		conf["ssl.ca.location"] = appConf.KafkaSSLCAFile
	}
	if appConf.KafkaSSLAuth {
		conf["ssl.certificate.location"] = appConf.KafkaSSLClientCertFile
		conf["ssl.key.location"] = appConf.KafkaSSLClientKeyFile
	}
	return conf
}

func createKafkaConfigForMessageProducer(appConf *Config) kafka.ConfigMap {
	conf := createKafkaConfig(appConf)
	compressionType := appConf.KafkaCompressionType
	conf["compression.type"] = compressionType
	conf["compression.level"] = getCompressionLevel(compressionType, appConf)
	conf["message.max.bytes"] = appConf.KafkaMessageMaxBytes
	return conf
}

// CompressionLevel defines the min and max values
type CompressionLevel struct {
	Min, Max int
}

// see documentation https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
var COMPRESSIONS = map[string]CompressionLevel{
	"none":   {0, 0},
	"gzip":   {0, 9},
	"snappy": {0, 0},
	"lz4":    {0, 12},
	"zstd":   {-1, -1},
}

func (level CompressionLevel) normalize(value int) int {
	if value > level.Max {
		zlog.Warn("Invalid compression cannot be more than 12", zap.Int("current", value), zap.Int("max", level.Max))
		return level.Max
	}
	if value < level.Min {
		zlog.Warn("Invalid compression cannot be less than -1", zap.Int("current", value), zap.Int("min", level.Min))
		return level.Min
	}
	return value
}

func getCompressionLevel(compressionType string, config *Config) int {
	compressionLevel := config.KafkaCompressionLevel
	if compressionLevel == -1 {
		return compressionLevel
	}
	level, ok := COMPRESSIONS[compressionType]
	if !ok {
		return -1
	}
	return level.normalize(compressionLevel)
}

func newABICodec(codec string, account string, schemaRegistryURL string, abiDecoder *ABIDecoder, getSchema MessageSchemaSupplier) (ABICodec, error) {
	switch codec {
	case JsonCodec:
		return NewJsonABICodec(abiDecoder, account), nil
	case AvroCodec:
		schemaRegistryClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)
		return NewKafkaAvroABICodec(abiDecoder, getSchema, schemaRegistryClient, account, schemaRegistryURL), nil
	default:
		return nil, fmt.Errorf("unsupported codec type: '%s'", codec)
	}
}

type MessageSchemaGenerator struct {
	Namespace string
	Version   string
	Account   string
	Source    string
}

func (msg MessageSchemaGenerator) getTableSchema(tableName string, abi *ABI) (MessageSchema, error) {
	return GenerateTableSchema(msg.newNamedSchemaGenOptions(tableName, abi))
}

func schemaVersion(version string, abiBlockNumber uint32) string {
	if version == "" {
		return fmt.Sprintf("0.%d.0", abiBlockNumber)
	} else {
		return version
	}
}

func (msg MessageSchemaGenerator) getActionSchema(actionName string, abi *ABI) (MessageSchema, error) {
	return GenerateActionSchema(msg.newNamedSchemaGenOptions(actionName, abi))
}

func (msg MessageSchemaGenerator) newNamedSchemaGenOptions(name string, abi *ABI) NamedSchemaGenOptions {
	return NamedSchemaGenOptions{
		Name:      name,
		Namespace: msg.Namespace,
		Version:   schemaVersion(msg.Version, abi.AbiBlockNum),
		AbiSpec: AbiSpec{
			Account: msg.Account,
			Abi:     abi,
		},
		Source: msg.Source,
		Domain: msg.Account,
	}
}
