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
	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdShowRestorePoints
 *
 * Implements ClusterCommand interface
 */
type CmdShowRestorePoints struct {
	CmdBase
	showRestorePointsOptions *vclusterops.VShowRestorePointsOptions
}

func makeCmdShowRestorePoints() *cobra.Command {
	// CmdShowRestorePoints
	newCmd := &CmdShowRestorePoints{}
	opt := vclusterops.VShowRestorePointsFactory()
	newCmd.showRestorePointsOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		showRestorePointsSubCmd,
		"Query and list restore point(s) in archive(s)",
		`This subcommand queries and lists restore point(s) in archive(s).

Then --start-timestamp and --end-timestamp options both limit the scope of
creation timestamps of listed restore points. Both of them expect a timestamp
in date-time format or date-only format, for example:
"2006-01-02 15:04:05", "2006-01-02", "2006-01-02 15:04:05.000000000".
Both of them expect a timestamp in UTC timezone.

Examples:
  # List restore points without filters with user input
  vcluster show_restore_points --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
	--communal-storage-location /communal

  # List restore points without filters with config file
  vcluster show_restore_points --db-name test_db \
    --config /opt/vertica/config/vertica_cluster.yaml

  # List restore points with archive name filter with user input
  vcluster show_restore_points --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
    --communal-storage-location /communal --restore-point-archive db1

  # List restore points with restore point id filter with user input
  vcluster show_restore_points --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
    --communal-storage-location /communal \
    --restore-point-id 34668031-c63d-4f3b-ba97-70223c4f97d6

  # List restore points with start timestamp and
  # end timestamp filters with user input
  vcluster show_restore_points --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
    --communal-storage-location /communal \
    --start-timestamp 2024-03-04 08:32:33.277569 \
    --end-timestamp 2024-03-04 08:32:34.176391
`,
		[]string{dbNameFlag, configFlag, passwordFlag, hostsFlag,
			communalStorageLocationFlag, configParamFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdShowRestorePoints) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&c.showRestorePointsOptions.FilterOptions.ArchiveName,
		"restore-point-archive",
		"",
		"Archive name to filter restore points with",
	)
	cmd.Flags().StringVar(
		&c.showRestorePointsOptions.FilterOptions.ArchiveID,
		"restore-point-id",
		"",
		"ID to filter restore points with",
	)
	cmd.Flags().StringVar(
		&c.showRestorePointsOptions.FilterOptions.ArchiveIndex,
		"restore-point-index",
		"",
		"Index to filter restore points with",
	)
	cmd.Flags().StringVar(
		&c.showRestorePointsOptions.FilterOptions.StartTimestamp,
		"start-timestamp",
		"",
		"Only show restores points created no earlier than this",
	)
	cmd.Flags().StringVar(
		&c.showRestorePointsOptions.FilterOptions.EndTimestamp,
		"end-timestamp",
		"",
		"Only show restores points created no later than this",
	)
}

func (c *CmdShowRestorePoints) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	c.ResetUserInputOptions(&c.showRestorePointsOptions.DatabaseOptions)

	return c.validateParse(logger)
}

func (c *CmdShowRestorePoints) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")

	err := c.getCertFilesFromCertPaths(&c.showRestorePointsOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.ValidateParseBaseOptions(&c.showRestorePointsOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.showRestorePointsOptions.DatabaseOptions)
}

func (c *CmdShowRestorePoints) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")
	return nil
}

func (c *CmdShowRestorePoints) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	options := c.showRestorePointsOptions

	restorePoints, err := vcc.VShowRestorePoints(options)
	if err != nil {
		vcc.LogError(err, "fail to show restore points", "DBName", options.DBName)
		return err
	}

	vcc.PrintInfo("Successfully show restore points %v in database %s", restorePoints, options.DBName)
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdShowRestorePoints
func (c *CmdShowRestorePoints) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.showRestorePointsOptions.DatabaseOptions = *opt
}
