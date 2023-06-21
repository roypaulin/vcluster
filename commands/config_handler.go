package commands

import (
	"flag"
	"fmt"
	"os"

	"vertica.com/vcluster/vclusterops/util"
)

type ConfigHandler struct {
	argv      []string
	parser    *flag.FlagSet
	directory *string
}

func (c *ConfigHandler) validateDirectory() error {
	if *c.directory == "" {
		currentDir, err := os.Getwd()
		if err != nil {
			return err
		}
		*c.directory = currentDir
	} else {
		if err := util.AbsPathCheck(*c.directory); err != nil {
			return fmt.Errorf("the directory must be provided as absolute path")
		}
	}
	return nil
}
