package commands

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"vertica.com/vcluster/vclusterops"
	"vertica.com/vcluster/vclusterops/vlog"
)

/* CmdConfig
 *
 * A command managing the YAML config file "vertica_cluster.yaml"
 * under the current or a specified directory.
 *
 * Implements ClusterCommand interface
 */
type CmdConfig struct {
	show *bool
	ConfigHandler
}

func MakeCmdConfig() CmdConfig {
	newCmd := CmdConfig{}
	newCmd.parser = flag.NewFlagSet("config", flag.ExitOnError)
	newCmd.show = newCmd.parser.Bool("show", false, "show the content of the config file")
	newCmd.directory = newCmd.parser.String(
		"directory",
		"",
		"The directory under which the config file was created. "+
			"By default the current directory will be used.",
	)

	return newCmd
}

func (c *CmdConfig) CommandType() string {
	return "config"
}

func (c *CmdConfig) Parse(inputArgv []string) error {
	vlog.LogArgParse(&inputArgv)

	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv

	parserError := c.parser.Parse(c.argv)
	if parserError != nil {
		return parserError
	}

	return c.validateParse()
}

func (c *CmdConfig) validateParse() error {
	vlog.LogInfoln("Called validateParse()")

	// if directory is not provided, then use the current directory
	return c.validateDirectory()
}

func (c *CmdConfig) Analyze() error {
	return nil
}

func (c *CmdConfig) Run() error {
	if *c.show {
		configFilePath := filepath.Join(*c.directory, vclusterops.ConfigFileName)
		fileBytes, err := os.ReadFile(configFilePath)
		if err != nil {
			return fmt.Errorf("fail to read config file, details: %w", err)
		}
		vlog.LogPrintInfo("Content of the config file:\n%s", string(fileBytes))
	}

	return nil
}

func (c *CmdConfig) PrintUsage() {
	thisCommand := c.CommandType()
	fmt.Fprintf(os.Stderr,
		"vcluster %s --show\nExample: vcluster %s --show\n",
		thisCommand,
		thisCommand)
	c.parser.PrintDefaults()
}
