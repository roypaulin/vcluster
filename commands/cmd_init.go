package commands

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"vertica.com/vcluster/vclusterops"
	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
)

/* CmdInit
 *
 * A command creating the YAML config file "vertica_cluster.yaml"
 * under the current or a specified directory.
 *
 * Implements ClusterCommand interface
 */
type CmdInit struct {
	Hosts *string
	ConfigHandler
}

func MakeCmdInit() CmdInit {
	newCmd := CmdInit{}
	newCmd.parser = flag.NewFlagSet("init", flag.ExitOnError)
	newCmd.directory = newCmd.parser.String(
		"directory",
		"",
		"The directory under which the config file will be created. "+
			"By default the current directory will be used.",
	)
	newCmd.Hosts = newCmd.parser.String("hosts", "", "Comma-separated list of hosts to participate in database")
	return newCmd
}

func (c *CmdInit) CommandType() string {
	return "init"
}

func (c *CmdInit) Parse(inputArgv []string) error {
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

func (c *CmdInit) validateParse() error {
	vlog.LogInfoln("Called validateParse()")

	// if directory is not provided, then use the current directory
	err := c.validateDirectory()
	if err != nil {
		return err
	}

	// the host list must be provided
	if *c.Hosts == "" {
		return fmt.Errorf("must provide the host list with --hosts")
	}

	return nil
}

func (c *CmdInit) Analyze() error {
	return nil
}

func (c *CmdInit) Run() error {
	configFilePath := filepath.Join(*c.directory, vclusterops.ConfigFileName)

	// check config file existence
	_, e := os.Stat(configFilePath)
	if e == nil {
		errMsg := fmt.Sprintf("The config file %s already exists", configFilePath)
		vlog.LogPrintErrorln(errMsg)
		return errors.New(errMsg)
	}

	// TODO: this will be improved later with more cluster info
	// build cluster config information
	clusterConfig := vclusterops.MakeClusterConfig()
	hosts, err := util.SplitHosts(*c.Hosts)
	if err != nil {
		return err
	}
	clusterConfig.Hosts = hosts

	// write information to the YAML file
	err = clusterConfig.WriteConfig(configFilePath)
	if err != nil {
		return err
	}

	vlog.LogPrintInfo("Created config file at %s\n", configFilePath)

	return nil
}

func (c *CmdInit) PrintUsage() {
	thisCommand := c.CommandType()
	fmt.Fprintf(os.Stderr,
		"vcluster %s --directory <absolute_directory_path>\nExample: vcluster %s --directory /opt/vertica/config\n",
		thisCommand,
		thisCommand)
	c.parser.PrintDefaults()
}
