/*
 (c) Copyright [2023] Open Text.
 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package commands

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdRemoveNode
 *
 * Implements ClusterCommand interface
 */
type CmdRemoveNode struct {
	removeNodeOptions *vclusterops.VRemoveNodeOptions

	CmdBase
}

func makeCmdRemoveNode() *cobra.Command {
	// CmdRemoveNode
	newCmd := &CmdRemoveNode{}
	opt := vclusterops.VRemoveNodeOptionsFactory()
	newCmd.removeNodeOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		removeNodeSubCmd,
		"Remove host(s) from an existing database",
		`This subcommand removes one or more nodes from an existing database.

You need to provide the --remove option followed by one or more hosts to
remove as a comma-separated list.

You cannot remove nodes from a sandboxed subcluster in an Eon Mode database.

Examples:
  # Remove multiple nodes from the existing database with config file
  vcluster db_remove_node --db-name test_db \
    --remove 10.20.30.40,10.20.30.42 \
    --config /opt/vertica/config/vertica_cluster.yaml

  # Remove a single node from the existing database with user input
  vcluster db_remove_node --db-name test_db --remove 10.20.30.42 \
    --hosts 10.20.30.40 --data-path /data
`,
		[]string{dbNameFlag, configFlag, hostsFlag, catalogPathFlag, dataPathFlag, depotPathFlag, passwordFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// require hosts to remove
	markFlagsRequired(cmd, []string{"remove"})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdRemoveNode) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringSliceVar(
		&c.removeNodeOptions.HostsToRemove,
		"remove",
		[]string{},
		"Comma-separated list of host(s) to remove from the database",
	)
	cmd.Flags().BoolVar(
		&c.removeNodeOptions.ForceDelete,
		"force-delete",
		true,
		"Whether to force clean-up of existing directories if they are not empty",
	)
}

func (c *CmdRemoveNode) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	c.ResetUserInputOptions(&c.removeNodeOptions.DatabaseOptions)
	return c.validateParse(logger)
}

func (c *CmdRemoveNode) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")

	err := c.parseHostToRemoveList()
	if err != nil {
		return err
	}

	err = c.getCertFilesFromCertPaths(&c.removeNodeOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.ValidateParseBaseOptions(&c.removeNodeOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.removeNodeOptions.DatabaseOptions)
}

// parseHostToRemoveList trims and lowercases the hosts in --remove
func (c *CmdRemoveNode) parseHostToRemoveList() error {
	if len(c.removeNodeOptions.HostsToRemove) > 0 {
		err := util.ParseHostList(&c.removeNodeOptions.HostsToRemove)
		if err != nil {
			// the err from util.ParseHostList will be "must specify a host or host list"
			// we overwrite the error here to provide more details
			return fmt.Errorf("must specify at least one host to remove")
		}
	}
	return nil
}

func (c *CmdRemoveNode) Run(vcc vclusterops.ClusterCommands) error {
	vcc.LogInfo("Called method Run()")

	options := c.removeNodeOptions

	vdb, err := vcc.VRemoveNode(options)
	if err != nil {
		return err
	}

	// write db info to vcluster config file
	err = writeConfig(&vdb, vcc.GetLog())
	if err != nil {
		vcc.PrintWarning("fail to write config file, details: %s", err)
	}
	vcc.PrintInfo("Successfully removed nodes %v from database %s", c.removeNodeOptions.HostsToRemove, options.DBName)

	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdRemoveNode
func (c *CmdRemoveNode) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.removeNodeOptions.DatabaseOptions = *opt
}
