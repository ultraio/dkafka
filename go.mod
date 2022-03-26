module github.com/dfuse-io/dkafka

go 1.14

require (
	cloud.google.com/go/iam v0.2.0 // indirect
	cloud.google.com/go/monitoring v1.3.0 // indirect
	cloud.google.com/go/trace v1.1.0 // indirect
	github.com/blendle/zapdriver v1.3.1
	github.com/confluentinc/confluent-kafka-go v1.8.2
	github.com/dfuse-io/dfuse-eosio v0.9.0-beta9.0.20210812023750-17e5f52111ab
	github.com/eoscanada/eos-go v0.9.1-0.20210812015252-984fc96878b6
	github.com/golang/protobuf v1.5.2
	github.com/google/cel-go v0.6.0
	github.com/google/uuid v1.3.0
	github.com/iancoleman/strcase v0.2.0
	github.com/klauspost/compress v1.11.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/linkedin/goavro/v2 v2.11.0
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/prometheus/client_golang v1.11.0
	github.com/riferrei/srclient v0.5.0
	github.com/spf13/cobra v1.3.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.10.1
	github.com/streamingfast/bstream v0.0.2-0.20210811181043-4c1920a7e3e3
	github.com/streamingfast/derr v0.0.0-20210811180100-9138d738bcec
	github.com/streamingfast/dgrpc v0.0.0-20210811180351-8646818518b2
	github.com/streamingfast/dlauncher v0.0.0-20210811194929-f06e488e63da
	github.com/streamingfast/logging v0.0.0-20210811175431-f3b44b61606a
	github.com/streamingfast/pbgo v0.0.6-0.20210812023556-e996f9c4fb86
	github.com/streamingfast/shutter v1.5.0
	go.uber.org/zap v1.17.0
	golang.org/x/oauth2 v0.0.0-20211104180415-d3ed0bb246c8
	google.golang.org/grpc v1.44.0
	gopkg.in/check.v1 v1.0.0-20200902074654-038fdea0a05b // indirect
	gotest.tools v2.2.0+incompatible

)

replace github.com/eoscanada/eos-go v0.9.1-0.20210812015252-984fc96878b6 => ./fork/eos-go

replace github.com/linkedin/goavro/v2 v2.11.0 => ./fork/goavro
