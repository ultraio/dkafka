package dkafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
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

	zlog.Info("setting up ABIDecoder")
	abiDecoder := NewABIDecoder(abiFiles, abiCodecClient)

	if abiDecoder.IsNOOP() && a.config.FailOnUndecodableDBOP {
		return fmt.Errorf("invalid config: no abicodec GRPC address and no local ABI file has been set, but fail-on-undecodable-db-op is enabled")
	}

	// setup the transformer, that will transform incoming blocks

	sourceHeader := kafka.Header{
		Key:   "ce_source",
		Value: []byte(a.config.EventSource),
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

	var producer *kafka.Producer
	if !a.config.DryRun {
		trxID := a.config.KafkaTransactionID
		if a.config.CdCType != "" {
			trxID = ""
		}
		producer, err = getKafkaProducer(createKafkaConfigForMessageProducer(a.config), trxID)
		if err != nil {
			return fmt.Errorf("getting kafka producer: %w", err)
		}
	}

	var appCtx appCtx
	if a.config.CdCType != "" {
		appCtx, err = a.NewCDCCtx(ctx, producer, headers, abiDecoder, saveBlock)
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
	return iterate(ctx, cancel, appCtx.adapter, appCtx.sender, a.config.CommitMinDelay, executor)
}

type appCtx struct {
	adapter Adapter
	filter  string
	sender  Sender
	cursor  string
}

func (a *App) NewCDCCtx(ctx context.Context, producer *kafka.Producer, headers []kafka.Header, abiDecoder *ABIDecoder, saveBlock SaveBlock) (appCtx, error) {
	var adapter Adapter
	var filter string
	var cursor string
	var abiCodec ABICodec
	eos.LegacyJSON4Asset = false
	eos.NativeType = true
	appCtx := appCtx{}
	cursor, err := LoadCursor(createKafkaConfig(a.config), a.config.KafkaTopic)
	if err != nil {
		return appCtx, fmt.Errorf("fail to load cursor on topic: %s, due to: %w", a.config.KafkaTopic, err)
	}
	// UOD-1290 load cursor from legacy cursor topic for dkafka migration
	if cursor == "" && a.config.KafkaCursorTopic != "" {
		zlog.Info("no cursor in message topic try to load it from legacy cursor topic...", zap.String("topic_cursor", a.config.KafkaCursorTopic))
		cp := newKafkaCheckpointer(createKafkaConfig(a.config), a.config.KafkaCursorTopic, a.config.KafkaCursorPartition, a.config.KafkaTopic, a.config.KafkaCursorConsumerGroupID, producer)
		if cursor, err = LoadCursorFromCursorTopic(a.config, cp); err != nil {
			return appCtx, fmt.Errorf("fail to load cursor for legacy topic: %s, due to: %w", a.config.KafkaCursorTopic, err)
		}
	}

	switch cdcType := a.config.CdCType; cdcType {
	case TABLES_CDC_TYPE:
		msg := MessageSchemaGenerator{
			Namespace: a.config.SchemaNamespace,
			Version:   a.config.SchemaVersion,
			Account:   a.config.Account,
		}
		abiCodec, err = newABICodec(
			a.config.Codec, a.config.Account, a.config.SchemaRegistryURL, abiDecoder,
			msg.getTableSchema,
		)
		if err != nil {
			return appCtx, err
		}
		filter = createCdCFilter(a.config.Account, a.config.Executed)
		tableNames := make(map[string]ExtractKey)
		for _, name := range a.config.TableNames {
			kv := strings.SplitN(name, ":", 2)
			var ek ExtractKey = extractPrimaryKey
			if len(kv) == 2 {
				switch kv[1] {
				case "k":
					ek = extractPrimaryKey
				case "s+k":
					ek = extractFullKey
				case "s":
					ek = extractScope
				default:
					return appCtx, fmt.Errorf("unsupported table key extractor pattern: %s, on support <name>[:{k|s|s+k}]", name)
				}
			}
			tableNames[kv[0]] = ek
		}
		generator := TableGenerator{
			tableNames: tableNames,
			abiCodec:   abiCodec,
		}
		adapter = &CdCAdapter{
			topic:     a.config.KafkaTopic,
			saveBlock: saveBlock,
			headers:   headers,
			generator: generator,
		}
	case ACTIONS_CDC_TYPE:
		filter = createCdCFilter(a.config.Account, a.config.Executed)
		actionKeyExpressions, err := createCdcKeyExpressions(a.config.ActionExpressions, ActionDeclarations)
		if err != nil {
			return appCtx, err
		}
		msg := MessageSchemaGenerator{
			Namespace: a.config.SchemaNamespace,
			Version:   a.config.SchemaVersion,
			Account:   a.config.Account,
		}
		abiCodec, err = newABICodec(
			a.config.Codec, a.config.Account, a.config.SchemaRegistryURL, abiDecoder,
			msg.getActionSchema,
		)
		if err != nil {
			return appCtx, err
		}
		generator := ActionGenerator2{
			keyExtractors: actionKeyExpressions,
			abiCodec:      abiCodec,
		}
		adapter = &CdCAdapter{
			topic:     a.config.KafkaTopic,
			saveBlock: saveBlock,
			headers:   headers,
			generator: generator,
		}
	default:
		return appCtx, fmt.Errorf("unsupported CDC type %s", cdcType)
	}
	appCtx.adapter = adapter
	appCtx.cursor = cursor
	appCtx.filter = filter
	appCtx.sender = NewFastSender(ctx, producer, a.config.KafkaTopic, headers, abiCodec)
	return appCtx, nil
}

func LoadCursorFromCursorTopic(config *Config, cp *kafkaCheckpointer) (string, error) {
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

func (a *App) NewLegacyCtx(ctx context.Context, producer *kafka.Producer, headers []kafka.Header, abiDecoder *ABIDecoder, saveBlock SaveBlock) (appCtx, error) {
	var adapter Adapter
	var filter string = a.config.IncludeFilterExpr
	var sender Sender
	var cursor string
	var err error
	eos.LegacyJSON4Asset = true
	eos.NativeType = false
	appCtx := appCtx{}
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
	var cp checkpointer
	if a.config.BatchMode || a.config.DryRun {
		zlog.Info("running in batch mode, ignoring cursors")
		cp = &nilCheckpointer{}
	} else {
		kafkaCp := newKafkaCheckpointer(createKafkaConfig(a.config), a.config.KafkaCursorTopic, a.config.KafkaCursorPartition, a.config.KafkaTopic, a.config.KafkaCursorConsumerGroupID, producer)
		cp = kafkaCp
		cursor, err = LoadCursorFromCursorTopic(a.config, kafkaCp)
		if err != nil {
			return appCtx, fmt.Errorf("error loading cursor: %w", err)
		}
		zlog.Info("running in stream mode", zap.String("cursor", cursor), zap.Int64("start_block_num", a.config.StartBlockNum))
	}
	// s, err = getKafkaSender(producer, cp, a.config.KafkaTransactionID != "")
	sender, err = NewSender(ctx, producer, cp, a.config.KafkaTransactionID != "")
	if err != nil {
		return appCtx, err
	}

	appCtx.adapter = adapter
	appCtx.cursor = cursor
	appCtx.filter = filter
	appCtx.sender = sender
	return appCtx, nil
}

func iterate(ctx context.Context, cancel context.CancelFunc, adapter Adapter, s Sender, tickDuration time.Duration, stream pbbstream.BlockStreamV2_BlocksClient) error {
	// loop: receive block,  transform block, send message...
	zlog.Info("Start looping over blocks...")
	in := make(chan BlockStep, 10)
	out := make(chan error, 1)
	ticker := time.NewTicker(tickDuration)
	go blockHandler(ctx, adapter, s, in, ticker.C, out)
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
			zlog.Debug("Receive new block", zap.String("cursor", msg.Cursor))
			blk := &pbcodec.Block{}
			if err := ptypes.UnmarshalAny(msg.Block, blk); err != nil {
				return fmt.Errorf("decoding any of type %q: %w", msg.Block.TypeUrl, err)
			}
			// step := sanitizeStep(msg.Step.String())
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

func blockHandler(ctx context.Context, adapter Adapter, s Sender, in <-chan BlockStep, ticks <-chan time.Time, out chan<- error) {
	var lastBlkStep BlockStep
	hasFail := false
	for {
		select {
		case blkStep := <-in:
			if hasFail {
				zlog.Debug("skip incoming block message after failure")
				continue
			}
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

func createCdcKeyExpressions(cdcExpression string, env cel.EnvOption) (cdcProgramByKeys map[string]cel.Program, err error) {
	cdcExpressionMap := make(map[string]string)
	var rawJSON = json.RawMessage(cdcExpression)
	err = json.Unmarshal(rawJSON, &cdcExpressionMap)
	if err != nil {
		return
	}
	cdcProgramByKeys = make(map[string]cel.Program)
	for k, v := range cdcExpressionMap {
		var prog cel.Program
		prog, err = exprToCelProgramWithEnv(v, env)
		if err != nil {
			return
		}
		cdcProgramByKeys[k] = prog
	}
	return
}

func createCdCFilter(account string, executed bool) string {
	filter := fmt.Sprintf("account==\"%s\" && receiver==\"%s\"", account, account)
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
		return nil, fmt.Errorf("unsupported codec type: %s", codec)
	}
}

type MessageSchemaGenerator struct {
	Namespace string
	Version   string
	Account   string
}

func (msg MessageSchemaGenerator) getTableSchema(tableName string, abi *ABI) (MessageSchema, error) {
	return GenerateTableSchema(NamedSchemaGenOptions{
		Name:      tableName,
		Namespace: msg.Namespace,
		Version:   schemaVersion(msg.Version, abi.AbiBlockNum),
		AbiSpec: AbiSpec{
			Account: msg.Account,
			Abi:     abi,
		},
	})
}

func schemaVersion(version string, abiBlockNumber uint32) string {
	if version == "" {
		return fmt.Sprintf("0.%d.0", abiBlockNumber)
	} else {
		return version
	}
}

func (msg MessageSchemaGenerator) getActionSchema(actionName string, abi *ABI) (MessageSchema, error) {
	return GenerateActionSchema(NamedSchemaGenOptions{
		Name:      actionName,
		Namespace: msg.Namespace,
		Version:   schemaVersion(msg.Version, abi.AbiBlockNum),
		AbiSpec: AbiSpec{
			Account: msg.Account,
			Abi:     abi,
		},
	})
}
