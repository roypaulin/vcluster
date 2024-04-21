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

package vclusterops

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"golang.org/x/exp/slices"
)

type DatabaseOptions struct {
	/* part 1: basic database info */

	// name of the database
	DBName string
	// expected to be IP addresses or hostnames
	RawHosts []string
	// expected to be IP addresses resolved from RawHosts
	Hosts []string
	// whether using IPv6 for host addresses
	IPv6 bool
	// path of catalog directory
	CatalogPrefix string
	// path of data directory
	DataPrefix string
	// File path to YAML config file
	ConfigPath string

	/* part 2: Eon database info */

	// path of depot directory
	DepotPrefix string
	// whether the database is in Eon mode
	IsEon bool
	// path of the communal storage
	CommunalStorageLocation string
	// database configuration parameters
	ConfigurationParameters map[string]string

	/* part 3: authentication info */

	// user name
	UserName string
	// password
	Password *string
	// TLS Key
	Key string
	// TLS Certificate
	Cert string
	// TLS CA Certificate
	CaCert string

	/* part 4: other info */

	// path of the log file
	LogPath string
	// whether use password
	usePassword bool
}

const (
	descriptionFileName            = "cluster_config.json"
	descriptionFileMetadataFolder  = "metadata"
	currConfigFileDestPath         = "/tmp/curr_config.json"
	restorePointConfigFileDestPath = "/tmp/restore_point_config.json"
	// catalogPath is not used for now, will implement it in VER-88884
	catalogPath = ""
)

const (
	commandCreateDB          = "create_db"
	commandDropDB            = "drop_db"
	commandStopDB            = "stop_db"
	commandStartDB           = "start_db"
	commandAddNode           = "db_add_node"
	commandRemoveNode        = "db_remove_node"
	commandAddCluster        = "db_add_subcluster"
	commandRemoveCluster     = "db_remove_subcluster"
	commandStopCluster       = "stop_subcluster"
	commandSandboxSC         = "sandbox_subcluster"
	commandUnsandboxSC       = "unsandbox_subcluster"
	commandShowRestorePoints = "show_restore_points"
	commandInstallPackages   = "install_packages"
	commandConfigRecover     = "manage_config_recover"
	commandReplicationStart  = "replication_start"
	commandFetchNodesDetails = "fetch_nodes_details"
)

func DatabaseOptionsFactory() DatabaseOptions {
	opt := DatabaseOptions{}
	// set default values to the params
	opt.setDefaultValues()

	return opt
}

func (opt *DatabaseOptions) setDefaultValues() {
	opt.ConfigurationParameters = make(map[string]string)
}

func (opt *DatabaseOptions) validateBaseOptions(commandName string, log vlog.Printer) error {
	// get vcluster commands
	log.WithName(commandName)
	// database name
	if opt.DBName == "" {
		return fmt.Errorf("must specify a database name")
	}
	err := util.ValidateDBName(opt.DBName)
	if err != nil {
		return err
	}

	// raw hosts and password
	err = opt.validateHostsAndPwd(commandName, log)
	if err != nil {
		return err
	}

	// paths
	err = opt.validatePaths(commandName)
	if err != nil {
		return err
	}

	// config directory
	// VER-91801: remove this condition once re_ip supports the config file
	if !slices.Contains([]string{"re_ip"}, commandName) {
		err = opt.validateConfigDir(commandName)
		if err != nil {
			return err
		}
	}

	// log directory
	if log.LogToFileOnly {
		err = util.ValidateAbsPath(opt.LogPath, "log directory")
		if err != nil {
			return err
		}
	}

	return nil
}

// validateHostsAndPwd will validate raw hosts and password
func (opt *DatabaseOptions) validateHostsAndPwd(commandName string, log vlog.Printer) error {
	// hosts
	if len(opt.RawHosts) == 0 && len(opt.Hosts) == 0 {
		return fmt.Errorf("must specify a host or host list")
	}

	// when we create db, we need to set password to "" if user did not provide one
	if opt.Password == nil {
		if commandName == commandCreateDB {
			opt.Password = new(string)
			*opt.Password = ""
		}
		log.PrintInfo("no password specified, using none")
	}
	return nil
}

// validate catalog, data, and depot paths
func (opt *DatabaseOptions) validatePaths(commandName string) error {
	// validate for the following commands only
	commands := []string{commandCreateDB, commandDropDB, commandConfigRecover}
	if !slices.Contains(commands, commandName) {
		return nil
	}

	// catalog prefix path
	err := opt.validateCatalogPath()
	if err != nil {
		return err
	}

	// data prefix
	// `manage_config recover` does not need the data-path
	if commandName != commandConfigRecover {
		err = util.ValidateRequiredAbsPath(opt.DataPrefix, "data path")
		if err != nil {
			return err
		}
	}

	// depot prefix
	if opt.IsEon {
		err = util.ValidateRequiredAbsPath(opt.DepotPrefix, "depot path")
		if err != nil {
			return err
		}
	}
	return nil
}

func (opt *DatabaseOptions) validateCatalogPath() error {
	// catalog prefix path
	return util.ValidateRequiredAbsPath(opt.CatalogPrefix, "catalog path")
}

// validate config directory
func (opt *DatabaseOptions) validateConfigDir(commandName string) error {
	// validate for the following commands only
	// TODO: add other commands into the command list
	commands := []string{commandCreateDB, commandDropDB, commandStopDB, commandStartDB, commandAddCluster, commandRemoveCluster,
		commandSandboxSC, commandUnsandboxSC, commandShowRestorePoints, commandAddNode, commandRemoveNode, commandInstallPackages}
	if slices.Contains(commands, commandName) {
		return nil
	}

	if opt.ConfigPath == "" {
		return nil
	}

	err := util.ValidateAbsPath(opt.ConfigPath, "config")
	if err != nil {
		return err
	}

	return nil
}

func (opt *DatabaseOptions) validateUserName(log vlog.Printer) error {
	if opt.UserName == "" {
		username, err := util.GetCurrentUsername()
		if err != nil {
			return err
		}
		opt.UserName = username
	}
	log.Info("Current username", "username", opt.UserName)

	return nil
}

func (opt *DatabaseOptions) setUsePassword(log vlog.Printer) error {
	// when password is specified,
	// we will use username/password to call https endpoints
	opt.usePassword = false
	if opt.Password != nil {
		opt.usePassword = true
		err := opt.validateUserName(log)
		if err != nil {
			return err
		}
	}

	return nil
}

// normalizePaths replaces all '//' to be '/', and trim
// catalog, data and depot prefixes.
func (opt *DatabaseOptions) normalizePaths() {
	// process correct catalog path, data path and depot path prefixes
	opt.CatalogPrefix = util.GetCleanPath(opt.CatalogPrefix)
	opt.DataPrefix = util.GetCleanPath(opt.DataPrefix)
	opt.DepotPrefix = util.GetCleanPath(opt.DepotPrefix)
}

// getVDBWhenDBIsDown can retrieve db configurations from NMA /nodes endpoint and cluster_config.json when db is down
func (opt *DatabaseOptions) getVDBWhenDBIsDown(vcc VClusterCommands) (vdb VCoordinationDatabase, err error) {
	/*
	 *   1. Get node names for input hosts from NMA /nodes.
	 *   2. Get other node information for input hosts from cluster_config.json.
	 *   3. Build vdb for input hosts using the information from step 1 and step 2.
	 *   From NMA /nodes, we can only get node names and catalog paths so we need the other nodes' info from cluster_config.json.
	 *   In cluster_config.json, we have all nodes' info, however, the node IPs in cluster_config.json could be old and cannot be
	 *   mapped to input hosts so we need node names (retrieved from NMA /nodes) as a bridge to connect cluster_config.json and
	 *   input hosts. For instance, if revive_db changed nodes' IPs, cluster_config.json cannot have the correct nodes' Ips. We
	 *   cannot map input hosts with nodes in cluster_config.json. We need to find node names from NMA /nodes for input hosts
	 *   and use the node names to retrieve other nodes' info from cluster_config.json.
	 */

	// step 1: get node names by calling NMA /nodes on input hosts
	// this step can map input hosts with node names
	vdb1 := VCoordinationDatabase{}
	var instructions1 []clusterOp
	nmaHealthOp := makeNMAHealthOp(opt.Hosts)
	nmaGetNodesInfoOp := makeNMAGetNodesInfoOp(opt.Hosts, opt.DBName, opt.CatalogPrefix,
		false /* report all errors */, &vdb1)
	instructions1 = append(instructions1,
		&nmaHealthOp,
		&nmaGetNodesInfoOp,
	)

	certs := httpsCerts{key: opt.Key, cert: opt.Cert, caCert: opt.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions1, &certs)
	err = clusterOpEngine.run(vcc.Log)
	if err != nil {
		vcc.Log.PrintError("fail to retrieve node names from NMA /nodes: %v", err)
		return vdb, err
	}

	// step 2: get node details from cluster_config.json
	vdb2 := VCoordinationDatabase{}
	var instructions2 []clusterOp
	currConfigFileSrcPath := opt.getCurrConfigFilePath()
	nmaDownLoadFileOp, err := makeNMADownloadFileOp(opt.Hosts, currConfigFileSrcPath, currConfigFileDestPath, catalogPath,
		opt.ConfigurationParameters, &vdb2)
	if err != nil {
		return vdb, err
	}
	instructions2 = append(instructions2, &nmaDownLoadFileOp)

	clusterOpEngine = makeClusterOpEngine(instructions2, &certs)
	err = clusterOpEngine.run(vcc.Log)
	if err != nil {
		vcc.Log.PrintError("fail to retrieve node details from %s: %v", descriptionFileName, err)
		return vdb, err
	}

	// step 3: build vdb for input hosts using node names from step 1 and node details from step 2
	// this step can map input hosts with node details
	vdb.HostList = vdb1.HostList
	vdb.HostNodeMap = makeVHostNodeMap()
	nodeNameVNodeMap := make(map[string]*VCoordinationNode)
	for _, vnode2 := range vdb2.HostNodeMap {
		nodeNameVNodeMap[vnode2.Name] = vnode2
	}
	for h1, vnode1 := range vdb1.HostNodeMap {
		nodeName := vnode1.Name
		vnode2, exists := nodeNameVNodeMap[nodeName]
		if exists {
			vnode := new(VCoordinationNode)
			*vnode = *vnode2
			// Update nodes' addresses using input hosts (the latest nodes' addresses) because
			// cluster_config.json stores the old addresses. revive_db and re_ip can modify
			// the nodes' addresses without syncing the change to cluster_config.json.
			vnode.Address = h1
			vdb.HostNodeMap[h1] = vnode
		} else {
			return vdb, fmt.Errorf("node name %s is not found in %s", nodeName, descriptionFileName)
		}
	}
	return vdb, nil
}

// getCurrConfigFilePath can make the current description file path using db name and communal storage location in the options
func (opt *DatabaseOptions) getCurrConfigFilePath() string {
	// description file will be in the location: {communal_storage_location}/metadata/{db_name}/cluster_config.json
	// an example: s3://tfminio/test_loc/metadata/test_db/cluster_config.json
	descriptionFilePath := filepath.Join(opt.CommunalStorageLocation, descriptionFileMetadataFolder, opt.DBName, descriptionFileName)
	// filepath.Join() will change "://" of the remote communal storage path to ":/"
	// as a result, we need to change the separator back to url format
	descriptionFilePath = strings.Replace(descriptionFilePath, ":/", "://", 1)

	return descriptionFilePath
}

// getRestorePointConfigFilePath can make the restore point description file path using db name, archive name, restore point id,
// and communal storage location in the options
func (options *VReviveDatabaseOptions) getRestorePointConfigFilePath(validatedRestorePointID string) string {
	const (
		archivesFolder = "archives"
	)
	// description file will be in the location:
	// {communal_storage_location}/metadata/{db_name}/archives/{archive_name}/{restore_point_id}/cluster_config.json
	// an example: s3://tfminio/test_loc/metadata/test_db/archives/test_archive_name/2251e5cc-3e16-4fb1-8cd0-e4b8651f5779/cluster_config.json
	descriptionFilePath := filepath.Join(options.CommunalStorageLocation, descriptionFileMetadataFolder,
		options.DBName, archivesFolder, options.RestorePoint.Archive, validatedRestorePointID, descriptionFileName)
	// filepath.Join() will change "://" of the remote communal storage path to ":/"
	// as a result, we need to change the separator back to url format
	descriptionFilePath = strings.Replace(descriptionFilePath, ":/", "://", 1)

	return descriptionFilePath
}

func (opt *DatabaseOptions) isSpreadEncryptionEnabled() (enabled bool, encryptionType string) {
	const EncryptSpreadCommConfigName = "encryptspreadcomm"
	// We cannot use the map lookup because the key name is case insensitive.
	for key, val := range opt.ConfigurationParameters {
		if strings.EqualFold(key, EncryptSpreadCommConfigName) {
			return true, val
		}
	}
	return false, ""
}

func (opt *DatabaseOptions) runClusterOpEngine(log vlog.Printer, instructions []clusterOp) error {
	// Create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: opt.Key, cert: opt.Cert, caCert: opt.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	return clusterOpEngine.run(log)
}
