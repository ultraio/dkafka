package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// RootCmd represents the CLI command
var RootCmd = &cobra.Command{
	Use:   "dkafka",
	Short: "",
	Long:  "",
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().String("dfuse-firehose-grpc-addr", "localhost:13035", "firehose endpoint to connect to")
	RootCmd.PersistentFlags().String("dfuse-firehose-include-expr", "executed", "CEL expression tu use for requests to firehose")
	RootCmd.PersistentFlags().String("dfuse-auth-token", "", "JWT to authenticate to dfuse (empty to skip authentication)")
	RootCmd.PersistentFlags().Bool("dry-run", false, "do not send anything to kafka, just print content")
	RootCmd.PersistentFlags().String("kafka-endpoints", "127.0.0.1:9092", "comma-separated kafka endpoint addresses")
	RootCmd.PersistentFlags().Bool("kafka-ssl-enable", false, "use SSL when connecting to kafka endpoints")
	RootCmd.PersistentFlags().String("kafka-ssl-ca-file", "", "path to certificate authority validating kafka endpoints")
	RootCmd.PersistentFlags().Bool("kafka-ssl-auth", false, "authenticate to kafka endpoints using client certificate (requires {kafka-ssl-enable}")
	RootCmd.PersistentFlags().String("kafka-ssl-client-cert-file", "./client.crt.pem", "path to client certificate to authenticate to kafka endpoint")
	RootCmd.PersistentFlags().String("kafka-ssl-client-key-file", "./client.key.pem", "path to client key to authenticate to kafka endpoint")

	RootCmd.PersistentFlags().String("kafka-transaction-id", "dkafkatransaction", "Unique ID for transactions")

	RootCmd.PersistentFlags().String("kafka-topic", "default", "kafka topic to use for all events writes or reads")
	RootCmd.PersistentFlags().String("kafka-cursor-topic", "_dkafka_cursors", "kafka topic where cursor will be loaded and saved")
	RootCmd.PersistentFlags().Uint32("kafka-cursor-partition", 0, "kafka partition where cursor will be loaded and saved")
	RootCmd.PersistentFlags().String("kafka-cursor-consumer-group-id", "dkafkaconsumer", "Consumer group ID for reading cursor")

	RootCmd.PersistentFlags().String("log-format", "text", "Format for logging to stdout. Either 'text' or 'stackdriver'")
	RootCmd.PersistentFlags().CountP("verbose", "v", "Enables verbose output (-vvvv for max verbosity)")
	RootCmd.PersistentFlags().String("log-level-switcher-listen-addr", "localhost:1065", "If non-empty, the process will listen on this address for json-formatted requests to change different logger levels (see DEBUG.md for more info)")
}

func initConfig() {
	viper.SetEnvPrefix("DKAFKA")
	viper.AutomaticEnv()
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	recurseViperCommands(RootCmd, nil)
}

func recurseViperCommands(root *cobra.Command, segments []string) {
	// Stolen from: github.com/abourget/viperbind
	var segmentPrefix string
	if len(segments) > 0 {
		segmentPrefix = strings.Join(segments, "-") + "-"
	}

	root.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		newVar := segmentPrefix + "global-" + f.Name
		viper.BindPFlag(newVar, f)
	})
	root.Flags().VisitAll(func(f *pflag.Flag) {
		newVar := segmentPrefix + "cmd-" + f.Name
		viper.BindPFlag(newVar, f)
	})

	for _, cmd := range root.Commands() {
		recurseViperCommands(cmd, append(segments, cmd.Name()))
	}
}
