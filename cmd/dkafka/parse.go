package main

import (
	"encoding/json"
	"fmt"

	"github.com/dfuse-io/dkafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/bstream/forkable"
)

var ParseCmd = &cobra.Command{
	Use:   "parse",
	Short: "Debug parsing commands",
	Long:  "",
}

var ParseCursorCmd = &cobra.Command{
	Use:   "cursor <opaque-cursor>",
	Short: "Parse an opaque cursor",
	Long:  "Parse an opaque cursor and return its value",
	RunE:  parseCursor,
}

var ParseAbiCmd = &cobra.Command{
	Use:   "abi <abi-hex-string>",
	Short: "Parse an abi hex-string to json",
	Long:  "Parse an hex-string abi representation and return its json form",
	RunE:  parseAbi,
}

func init() {
	RootCmd.AddCommand(ParseCmd)

	ParseCmd.AddCommand(ParseCursorCmd)
	ParseCursorCmd.Flags().String("cursor", "", "specify the cursor with this flag when it start with a '-'")
	ParseCmd.AddCommand(ParseAbiCmd)
	ParseAbiCmd.Flags().String("abi", "", "specify the abi with this flag if you don't want to use the arg")
}

func parseCursor(cmd *cobra.Command, args []string) error {
	SetupLogger()

	var opaqueCursor string
	if len(args) == 1 {
		opaqueCursor = args[0]
	} else {
		opaqueCursor = viper.GetString("parse-cursor-cmd-cursor")
	}

	if opaqueCursor == "" {
		return fmt.Errorf("un-expected parameter only opaque cursor must be provided")
	}

	cursor, err := forkable.CursorFromOpaque(opaqueCursor)
	if err != nil {
		return err
	}
	fmt.Printf("cursor: { step: %s, block: %s, LIB: %s, head: %s}\n", cursor.Step, cursor.Block, cursor.LIB, cursor.HeadBlock)
	return nil
}

func parseAbi(cmd *cobra.Command, args []string) error {
	SetupLogger()

	var hexData string
	if len(args) == 1 {
		hexData = args[0]
	} else {
		hexData = viper.GetString("parse-abi-cmd-abi")
	}

	if hexData == "" {
		return fmt.Errorf("un-expected parameter an abi hex-string must be provided either as an arg or as a --abi option")
	}

	abi, err := dkafka.DecodeABI("from-cli", "from-cli", hexData)
	if err != nil {
		return err
	}
	abiBytes, err := json.Marshal(abi)
	if err != nil {
		return err
	}
	fmt.Println(string(abiBytes))
	return nil
}
