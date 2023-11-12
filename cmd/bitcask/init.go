package main

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"go.mills.io/bitcask/v2"
)

var initCmd = &cobra.Command{
	Use:     "init",
	Aliases: []string{"create", "init", "new"},
	Short:   "Initialize a new database",
	Long:    `This initializes a new database with persisted options`,
	Args:    cobra.ExactArgs(0),
	PreRun: func(cmd *cobra.Command, args []string) {
		viper.BindPFlag("with-max-datafile-size", cmd.Flags().Lookup("with-max-datafile-size"))
		viper.SetDefault("with-max-datafile-size", bitcask.DefaultMaxDatafileSize)

		viper.BindPFlag("with-max-key-size", cmd.Flags().Lookup("with-max-key-size"))
		viper.SetDefault("with-max-key-size", bitcask.DefaultMaxKeySize)

		viper.BindPFlag("with-max-value-size", cmd.Flags().Lookup("with-max-value-size"))
		viper.SetDefault("with-max-value-size", bitcask.DefaultMaxValueSize)
	},
	Run: func(cmd *cobra.Command, args []string) {
		path := viper.GetString("path")

		maxDatafileSize := viper.GetInt("with-max-datafile-size")
		maxKeySize := viper.GetUint32("with-max-key-size")
		maxValueSize := viper.GetUint64("with-max-value-size")

		db, err := bitcask.Open(
			path,
			bitcask.WithMaxDatafileSize(maxDatafileSize),
			bitcask.WithMaxKeySize(maxKeySize),
			bitcask.WithMaxValueSize(maxValueSize),
		)
		if err != nil {
			log.WithError(err).Error("error opening database")
			os.Exit(1)
		}
		defer db.Close()

		os.Exit(0)
	},
}

func init() {
	RootCmd.AddCommand(initCmd)

	initCmd.PersistentFlags().IntP(
		"with-max-datafile-size", "", bitcask.DefaultMaxDatafileSize,
		"Maximum size of each datafile",
	)
	initCmd.PersistentFlags().Uint32P(
		"with-max-key-size", "", bitcask.DefaultMaxKeySize,
		"Maximum size of each key",
	)
	initCmd.PersistentFlags().Uint64P(
		"with-max-value-size", "", bitcask.DefaultMaxValueSize,
		"Maximum size of each value",
	)
}
