package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"go.mills.io/bitcask/v2"
)

var keysCmd = &cobra.Command{
	Use:     "keys",
	Aliases: []string{"list", "ls"},
	Short:   "Display all keys in Database",
	Long:    `This displays all known keys in the Database"`,
	Args:    cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		path := viper.GetString("path")

		os.Exit(keys(path))
	},
}

func init() {
	RootCmd.AddCommand(keysCmd)
}

func keys(path string) int {
	db, err := bitcask.Open(path)
	if err != nil {
		log.WithError(err).Error("error opening database")
		return 1
	}
	defer db.Close()

	db.ForEach(func(key bitcask.Key) error {
		fmt.Printf("%s\n", string(key))
		return nil
	})

	return 0
}
