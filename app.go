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
	"github.com/dfuse-io/dfuse-eosio/filtering"
	pbabicodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/abicodec/v1"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/eoscanada/eos-go"
	"github.com/golang/protobuf/ptypes"
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

type Config struct {
	DfuseGRPCEndpoint string
	DfuseToken        string

	DryRun        bool // do not connect to Kafka, just print to stdout
	BatchMode     bool
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

	KafkaCursorConsumerGroupID string
	KafkaTransactionID         string
	CommitMinDelay             time.Duration

	IncludeFilterExpr    string
	KafkaTopic           string
	KafkaCursorTopic     string
	KafkaCursorPartition int32
	EventSource          string
	EventKeysExpr        string
	EventTypeExpr        string
	EventExtensions      map[string]string

	LocalABIFiles         map[string]string
	ABICodecGRPCAddr      string
	FailOnUndecodableDBOP bool
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

func (a *App) Run() error {
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
	conn, err := grpc.Dial(addr,
		dialOptions...,
	)
	if err != nil {
		return fmt.Errorf("connecting to grpc address %s: %w", addr, err)
	}

	client := pbbstream.NewBlockStreamV2Client(conn)

	req := &pbbstream.BlocksRequestV2{
		IncludeFilterExpr: a.config.IncludeFilterExpr,
		StartBlockNum:     a.config.StartBlockNum,
		StopBlockNum:      a.config.StopBlockNum,
	}

	var producer *kafka.Producer
	if !a.config.BatchMode || !a.config.DryRun {
		producer, err = getKafkaProducer(createKafkaConfigWithCompression(a.config), a.config.KafkaTransactionID)
		if err != nil {
			return fmt.Errorf("getting kafka producer: %w", err)
		}
	}

	var abiFiles map[string]*eos.ABI
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
		return fmt.Errorf("Invalid config: no abicodec GRPC address and no local ABI file has been set, but fail-on-undecodable-db-op is enabled")
	}

	var cp checkpointer
	if a.config.BatchMode {
		zlog.Info("running in batch mode, ignoring cursors")
		cp = &nilCheckpointer{}
	} else {
		cp = newKafkaCheckpointer(createKafkaConfig(a.config), a.config.KafkaCursorTopic, a.config.KafkaCursorPartition, a.config.KafkaTopic, a.config.KafkaCursorConsumerGroupID, producer)

		cursor, err := cp.Load()
		switch err {
		case NoCursorErr:
			zlog.Info("running in live mode, no cursor found: starting from beginning", zap.Int64("start_block_num", a.config.StartBlockNum))
		case nil:
			c, err := forkable.CursorFromOpaque(cursor)
			if err != nil {
				zlog.Error("cannot decode cursor", zap.Error(err))
				return err
			}
			zlog.Info("running in live mode, found cursor",
				zap.String("cursor", cursor),
				zap.Stringer("plain_cursor", c),
				zap.Stringer("cursor_block", c.Block),
				zap.Stringer("cursor_head_block", c.HeadBlock),
				zap.Stringer("cursor_LIB", c.LIB),
			)
			req.StartCursor = cursor
		default:
			return fmt.Errorf("error loading cursor: %w", err)
		}
	}
	if irreversibleOnly {
		req.ForkSteps = []pbbstream.ForkStep{pbbstream.ForkStep_STEP_IRREVERSIBLE}
	}

	var s sender
	if a.config.DryRun {
		s = &dryRunSender{}
	} else {
		s, err = getKafkaSender(producer, cp, a.config.KafkaTransactionID != "")
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.OnTerminating(func(_ error) {
		cancel()
	})

	executor, err := client.Blocks(ctx, req)
	if err != nil {
		return fmt.Errorf("requesting blocks from dfuse firehose: %w", err)
	}

	// setup the transformer, that will transform incoming blocks

	eventTypeProg, err := exprToCelProgram(a.config.EventTypeExpr)
	if err != nil {
		return fmt.Errorf("cannot parse event-type-expr: %w", err)
	}
	eventKeyProg, err := exprToCelProgram(a.config.EventKeysExpr)
	if err != nil {
		return fmt.Errorf("cannot parse event-keys-expr: %w", err)
	}

	var extensions []*extension
	for k, v := range a.config.EventExtensions {
		prog, err := exprToCelProgram(v)
		if err != nil {
			return fmt.Errorf("cannot parse event-extension: %w", err)
		}
		extensions = append(extensions, &extension{
			name: k,
			expr: v,
			prog: prog,
		})

	}

	sourceHeader := kafka.Header{
		Key:   "ce_source",
		Value: []byte(a.config.EventSource),
	}
	specHeader := kafka.Header{
		Key:   "ce_specversion",
		Value: []byte("1.0"),
	}
	contentTypeHeader := kafka.Header{
		Key:   "content-type",
		Value: []byte("application/json"),
	}
	dataContentTypeHeader := kafka.Header{
		Key:   "ce_datacontenttype",
		Value: []byte("application/json"),
	}

	// loop: receive block,  transform block, send message...
	for {
		msg, err := executor.Recv()
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
		blocksReceived.Inc()
		step := sanitizeStep(msg.Step.String())

		if blk.Number%100 == 0 {
			zlog.Info("incoming block 1/100", zap.Uint32("blk_number", blk.Number), zap.String("step", step), zap.Int("length_filtered_trx_traces", len(blk.FilteredTransactionTraces)))
		}
		if blk.Number%10 == 0 {
			zlog.Debug("incoming block 1/10", zap.Uint32("blk_number", blk.Number), zap.String("step", step), zap.Int("length_filtered_trx_traces", len(blk.FilteredTransactionTraces)))
		}

		for _, trx := range blk.TransactionTraces() {
			transactionTracesReceived.Inc()
			status := sanitizeStatus(trx.Receipt.Status.String())
			memoizableTrxTrace := &filtering.MemoizableTrxTrace{TrxTrace: trx}
			for _, act := range trx.ActionTraces {
				if !act.FilteringMatched {
					continue
				}
				actionTracesReceived.Inc()
				var jsonData json.RawMessage
				if act.Action.JsonData != "" {
					jsonData = json.RawMessage(act.Action.JsonData)
				}
				activation := filtering.NewActionTraceActivation(
					act,
					memoizableTrxTrace,
					msg.Step.String(),
				)

				var auths []string
				for _, auth := range act.Action.Authorization {
					auths = append(auths, auth.Authorization())
				}

				var globalSeq uint64
				if act.Receipt != nil {
					globalSeq = act.Receipt.GlobalSequence
				}

				decodedDBOps, err := abiDecoder.DecodeDBOps(trx.DBOpsForAction(act.ExecutionIndex), blk.Number)
				if err != nil {
					if a.config.FailOnUndecodableDBOP {
						return err
					}
					zlog.Warn("cannot decode dbops", zap.Uint32("block_number", blk.Number), zap.Error(err))
				}
				eosioAction := event{
					BlockNum:      blk.Number,
					BlockID:       blk.Id,
					Status:        status,
					Executed:      !trx.HasBeenReverted(),
					Step:          step,
					TransactionID: trx.Id,
					ActionInfo: ActionInfo{
						Account:        act.Account(),
						Receiver:       act.Receiver,
						Action:         act.Name(),
						JSONData:       &jsonData,
						DBOps:          decodedDBOps,
						Authorization:  auths,
						GlobalSequence: globalSeq,
					},
				}

				eventType, err := evalString(eventTypeProg, activation)
				if err != nil {
					return fmt.Errorf("error eventtype eval: %w", err)
				}

				extensionsKV := make(map[string]string)
				for _, ext := range extensions {
					val, err := evalString(ext.prog, activation)
					if err != nil {
						return fmt.Errorf("program: %w", err)
					}
					extensionsKV[ext.name] = val

				}

				eventKeys, err := evalStringArray(eventKeyProg, activation)
				if err != nil {
					return fmt.Errorf("event keyeval: %w", err)
				}

				dedupeMap := make(map[string]bool)
				for _, eventKey := range eventKeys {
					if dedupeMap[eventKey] {
						continue
					}
					dedupeMap[eventKey] = true

					headers := []kafka.Header{
						{
							Key:   "ce_id",
							Value: hashString(fmt.Sprintf("%s%s%d%s%s", blk.Id, trx.Id, act.ExecutionIndex, msg.Step.String(), eventKey)),
						},
						sourceHeader,
						specHeader,
						{
							Key:   "ce_type",
							Value: []byte(eventType),
						},
						contentTypeHeader,
						{
							Key:   "ce_time",
							Value: []byte(blk.MustTime().Format("2006-01-02T15:04:05.9Z")),
						},
						dataContentTypeHeader,
						{
							Key:   "ce_blkstep",
							Value: []byte(step),
						},
					}
					for k, v := range extensionsKV {
						headers = append(headers, kafka.Header{
							Key:   k,
							Value: []byte(v),
						})
					}
					msg := kafka.Message{
						Key:     []byte(eventKey),
						Headers: headers,
						Value:   eosioAction.JSON(),
						TopicPartition: kafka.TopicPartition{
							Topic:     &a.config.KafkaTopic,
							Partition: kafka.PartitionAny,
						},
					}
					if err := s.Send(&msg); err != nil {
						return fmt.Errorf("sending message: %w", err)
					}
					messagesSent.Inc()
				}

			}
		}
		if a.IsTerminating() {
			return s.Commit(context.Background(), msg.Cursor)
		}

		if err := s.CommitIfAfter(context.Background(), msg.Cursor, a.config.CommitMinDelay); err != nil {
			return fmt.Errorf("committing message: %w", err)
		}
	}
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
		//conf["ssl.key.password"] = "keypass"
	}
	return conf
}

func createKafkaConfigWithCompression(appConf *Config) kafka.ConfigMap {
	conf := createKafkaConfig(appConf)
	compressionType := appConf.KafkaCompressionType
	conf["compression.type"] = compressionType
	conf["compression.level"] = getCompressionLevel(compressionType, appConf)
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
