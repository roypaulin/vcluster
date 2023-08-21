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
	"errors"
	"fmt"
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// VAddNodeOptions are the option arguments for the VAddNode API
type VAddNodeOptions struct {
	DatabaseOptions
	// Hosts to add to database
	NewHosts []string
	// Name of the subcluster that the new nodes will be added to
	SCName *string
	// A primary up host that will be used to execute
	// remove_node operations.
	Initiator string
	DepotSize *string // like 10G
	// Skip rebalance shards if true
	SkipRebalanceShards *bool
}

func VAddNodeOptionsFactory() VAddNodeOptions {
	opt := VAddNodeOptions{}
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (o *VAddNodeOptions) SetDefaultValues() {
	o.DatabaseOptions.SetDefaultValues()

	o.SCName = new(string)
	o.SkipRebalanceShards = new(bool)
	o.DepotSize = new(string)
}

func (o *VAddNodeOptions) validateEonOptions() error {
	if *o.HonorUserInput {
		err := util.ValidateRequiredAbsPath(o.DepotPrefix, "depot path")
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *VAddNodeOptions) validateExtraOptions() error {
	if !*o.HonorUserInput {
		return nil
	}
	// data prefix
	return util.ValidateRequiredAbsPath(o.DataPrefix, "data path")
}

func (o *VAddNodeOptions) validateParseOptions() error {
	// batch 1: validate required parameters
	err := o.ValidateBaseOptions("db_add_node")
	if err != nil {
		return err
	}

	// batch 3: validate all other params
	return o.validateExtraOptions()
}

// analyzeOptions will modify some options based on what is chosen
func (o *VAddNodeOptions) analyzeOptions() (err error) {
	o.NewHosts, err = util.ResolveRawHostsToAddresses(o.NewHosts, o.Ipv6.ToBool())
	if err != nil {
		return err
	}

	// we analyze host names when HonorUserInput is set, otherwise we use hosts in yaml config
	if *o.HonorUserInput {
		// resolve RawHosts to be IP addresses
		o.Hosts, err = util.ResolveRawHostsToAddresses(o.RawHosts, o.Ipv6.ToBool())
		if err != nil {
			return err
		}
		o.normalizePaths()
	}

	return nil
}

// ParseNewHostList converts the string list of hosts, to add, into a slice of strings.
// The hosts should be separated by comma, and will be converted to lower case.
func (o *VAddNodeOptions) ParseNewHostList(hosts string) error {
	inputHostList, err := util.SplitHosts(hosts)
	if err != nil {
		if len(inputHostList) == 0 {
			return fmt.Errorf("must specify at least one host to add")
		}
		return err
	}

	o.NewHosts = inputHostList
	return nil
}

func (o *VAddNodeOptions) validateAnalyzeOptions() error {
	err := o.validateParseOptions()
	if err != nil {
		return err
	}

	return o.analyzeOptions()
}

// VAddNode is the top-level API for adding node(s) to an existing database.
func (vcc *VClusterCommands) VAddNode(options *VAddNodeOptions) (VCoordinationDatabase, error) {
	vdb := MakeVCoordinationDatabase()

	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig()
	if err != nil {
		return vdb, err
	}

	err = options.validateAnalyzeOptions()
	if err != nil {
		return vdb, err
	}

	// get hosts from config file and options.
	// this, as well as all the config file related parts,
	// will be moved to cmd_add_node.go after VER-88122,
	// as the operator does not support config file.
	hosts := options.GetHosts(config)
	options.Hosts = hosts
	// get depot and data prefix from config file or options
	*options.DepotPrefix, *options.DataPrefix = options.getDepotAndDataPrefix(config)

	err = getVDBFromRunningDB(&vdb, &options.DatabaseOptions)
	if err != nil {
		return vdb, err
	}

	err = options.completeVDBSetting(&vdb)
	if err != nil {
		return vdb, err
	}

	if vdb.IsEon {
		// checking this here because now we have got eon value from
		// the running db.
		if e := options.validateEonOptions(); e != nil {
			return vdb, e
		}
	}

	// add_node is aborted if requirements are not met
	err = checkAddNodeRequirements(&vdb, options.NewHosts)
	if err != nil {
		return vdb, err
	}

	err = options.setInitiator(vdb.PrimaryUpNodes)
	if err != nil {
		return vdb, err
	}

	err = vdb.addHosts(options.NewHosts)
	if err != nil {
		return vdb, err
	}

	instructions, err := produceAddNodeInstructions(&vdb, options)
	if err != nil {
		vlog.LogPrintError("fail to produce add node instructions, %s", err)
		return vdb, err
	}

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	if runError := clusterOpEngine.Run(); runError != nil {
		vlog.LogPrintError("fail to complete add node operation, %s", runError)
		return vdb, runError
	}
	return vdb, nil
}

// checkAddNodeRequirements returns an error if at least one of the nodes
// to add already exists in db.
func checkAddNodeRequirements(vdb *VCoordinationDatabase, hostsToAdd []string) error {
	// '0' represents the number of hosts in hostsToAdd
	// we expect to exist in database. For 'add_node', we
	// don't want any of the new host to be part of the db.
	if !vdb.doNodesExist(hostsToAdd, 0) {
		return errors.New("some of the nodes to add already exist in the database")
	}

	return nil
}

// completeVDBSetting sets some VCoordinationDatabase fields we cannot get yet
// from the https endpoints. We set those fields from options.
func (o *VAddNodeOptions) completeVDBSetting(vdb *VCoordinationDatabase) error {
	vdb.DataPrefix = *o.DataPrefix
	vdb.DepotPrefix = *o.DepotPrefix

	hostNodeMap := make(map[string]VCoordinationNode)
	// we set depot/data paths manually because there is not yet an https endpoint for
	// that(VER-88122). This is useful for NMAPrepareDirectoriesOp.
	for h := range vdb.HostNodeMap {
		vnode := vdb.HostNodeMap[h]
		dataPath := vdb.genDataPath(vnode.Name)
		vnode.StorageLocations = append(vnode.StorageLocations, dataPath)
		suffix := "/Catalog"
		index := strings.Index(vnode.CatalogPath, suffix)
		if index != -1 {
			vnode.CatalogPath = vnode.CatalogPath[:index]
		}
		if vdb.DepotPrefix != "" {
			vnode.DepotPath = vdb.genDepotPath(vnode.Name)
		}
		hostNodeMap[h] = vnode
	}
	vdb.HostNodeMap = hostNodeMap

	return nil
}

// produceAddNodeInstructions will build a list of instructions to execute for
// the add node operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful add_node:
//   - Check NMA connectivity
//   - Check NMA versions
//   - If we have subcluster in the input, check if the subcluster exists. If not, we stop.
//     If we do not have a subcluster in the input, fetch the current default subcluster name
//   - Prepare directories
//   - Get network profiles
//   - Create the new node
//   - Reload spread
//   - Transfer config files to the new node
//   - Start the new node
//   - Poll node startup
//   - Create depot on the new node (Eon mode only)
//   - Sync catalog
//   - Rebalance shards on subcluster (Eon mode only)
func produceAddNodeInstructions(vdb *VCoordinationDatabase,
	options *VAddNodeOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp
	var initiatorHost []string
	initiatorHost = append(initiatorHost, options.Initiator)
	newHosts := options.NewHosts
	// hosts that are actively participating to `add_node`
	activeHosts := initiatorHost
	activeHosts = append(activeHosts, newHosts...)
	username := *options.UserName
	usePassword := options.usePassword
	password := options.Password

	nmaHealthOp := makeNMAHealthOp(activeHosts)
	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(activeHosts, true)
	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp)

	if vdb.IsEon {
		httpsFindSubclusterOrDefaultOp, e := makeHTTPSFindSubclusterOrDefaultOp(
			initiatorHost, usePassword, username, password, *options.SCName)
		if e != nil {
			return instructions, e
		}
		instructions = append(instructions, &httpsFindSubclusterOrDefaultOp)
	}

	// this is a copy of the original HostNodeMap that only
	// contains the hosts to add.
	newHostNodeMap := vdb.copyHostNodeMap(options.NewHosts)
	nmaPrepareDirectoriesOp, err := makeNMAPrepareDirectoriesOp(newHostNodeMap)
	if err != nil {
		return instructions, err
	}
	nmaNetworkProfileOp := makeNMANetworkProfileOp(activeHosts)
	httpsCreateNodeOp, err := makeHTTPSCreateNodeOp(newHosts, initiatorHost,
		usePassword, username, password, vdb, *options.SCName)
	if err != nil {
		return instructions, err
	}
	httpsReloadSpreadOp, err := makeHTTPSReloadSpreadOp(initiatorHost, true, username, options.Password)
	if err != nil {
		return instructions, err
	}
	nmaReadCatalogEditorOp, err := makeNMAReadCatalogEditorOp(initiatorHost, vdb)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaPrepareDirectoriesOp,
		&nmaNetworkProfileOp,
		&httpsCreateNodeOp,
		&httpsReloadSpreadOp,
		&nmaReadCatalogEditorOp,
	)

	// we will remove the nil parameters in VER-88401 by adding them in execContext
	produceTransferConfigOps(&instructions,
		initiatorHost,
		nil, /*all existing nodes*/
		newHosts,
		nil /*db configurations retrieved from a running db*/)

	nmaStartNewNodesOp := makeNMAStartNodeOp(newHosts)
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOp(newHosts, usePassword, username, options.Password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaStartNewNodesOp,
		&httpsPollNodeStateOp,
	)

	return prepareAdditionalEonInstructions(vdb, options, instructions,
		username, usePassword, initiatorHost, newHosts)
}

func prepareAdditionalEonInstructions(vdb *VCoordinationDatabase,
	options *VAddNodeOptions,
	instructions []ClusterOp,
	username string, usePassword bool,
	initiatorHost, newHosts []string) ([]ClusterOp, error) {
	if vdb.UseDepot {
		httpsCreateNodesDepotOp, err := makeHTTPSCreateNodesDepotOp(vdb,
			newHosts, usePassword, username, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsCreateNodesDepotOp)
	}

	if vdb.IsEon {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(initiatorHost, true, username, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
		if !*options.SkipRebalanceShards {
			httpsRBSCShardsOp, err := makeHTTPSRebalanceSubclusterShardsOp(
				initiatorHost, usePassword, username, options.Password, *options.SCName)
			if err != nil {
				return instructions, err
			}
			instructions = append(instructions, &httpsRBSCShardsOp)
		}
	}

	return instructions, nil
}

// setInitiator sets the initiator as the first primary up node
func (o *VAddNodeOptions) setInitiator(primaryUpNodes []string) error {
	initiatorHost, err := getInitiatorHost(primaryUpNodes, []string{})
	if err != nil {
		return err
	}
	o.Initiator = initiatorHost
	return nil
}
