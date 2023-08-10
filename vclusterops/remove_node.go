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

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// VRemoveNodeOptions are the option arguments for the VRemoveNode API
type VRemoveNodeOptions struct {
	DatabaseOptions
	// Hosts to remove from database
	HostsToRemove []string
	// A primary up host that will be used to execute
	// remove_node operations.
	Initiator   string
	ForceDelete *bool
	NodeStates  []NodeStateInfo
}

func VRemoveNodeOptionsFactory() VRemoveNodeOptions {
	opt := VRemoveNodeOptions{}
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (o *VRemoveNodeOptions) SetDefaultValues() {
	o.DatabaseOptions.SetDefaultValues()

	o.ForceDelete = new(bool)
	*o.ForceDelete = true
}

// ParseHostToRemoveList converts the string list of hosts, to remove, into a slice of strings.
// The hosts should be separated by comma, and will be converted to lower case.
func (o *VRemoveNodeOptions) ParseHostToRemoveList(hosts string) error {
	inputHostList, err := util.SplitHosts(hosts)
	if err != nil {
		if len(inputHostList) == 0 {
			return fmt.Errorf("must specify at least one host to remove")
		}
	}

	o.HostsToRemove = inputHostList
	return nil
}

func (o *VRemoveNodeOptions) validateRequiredOptions() error {
	err := o.ValidateBaseOptions("db_remove_node")
	if err != nil {
		return err
	}
	return nil
}

func (o *VRemoveNodeOptions) validateExtraOptions() error {
	if !*o.HonorUserInput {
		return nil
	}
	// data prefix
	return util.ValidateRequiredAbsPath(o.DataPrefix, "data path")
}

func (o *VRemoveNodeOptions) validateParseOptions() error {
	// batch 1: validate required params
	err := o.validateRequiredOptions()
	if err != nil {
		return err
	}
	// batch 2: validate all other params
	err = o.validateExtraOptions()
	return err
}

func (o *VRemoveNodeOptions) analyzeOptions() (err error) {
	o.HostsToRemove, err = util.ResolveRawHostsToAddresses(o.HostsToRemove, o.Ipv6.ToBool())
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
		o.cleanPaths()
	}
	return nil
}

func (o *VRemoveNodeOptions) ValidateAnalyzeOptions() error {
	if err := o.validateParseOptions(); err != nil {
		return err
	}
	err := o.analyzeOptions()
	if err != nil {
		return err
	}
	return o.SetUsePassword()
}

func (vcc *VClusterCommands) VRemoveNode(options *VRemoveNodeOptions) (*VCoordinationDatabase, error) {
	config, err := options.GetDBConfig()
	if err != nil {
		return nil, err
	}

	// validate and analyze options
	err = options.ValidateAnalyzeOptions()
	if err != nil {
		return nil, err
	}

	// get db name and hosts from config file and options
	dbName, hosts := options.GetNameAndHosts(config)
	options.Name = &dbName
	options.Hosts = hosts
	// get depot and data prefix from config file or options
	*options.DepotPrefix, *options.DataPrefix = options.getDepotAndDataPrefix(config)
	*options.CatalogPrefix = options.GetCatalogPrefix(config)

	vdb := MakeVCoordinationDatabase()
	err = GetVDBFromRunningDB(&vdb, &options.DatabaseOptions)
	if err != nil {
		return nil, err
	}
	if *options.HonorUserInput && vdb.IsEon {
		// checking this here because now we have got eon value from
		// the running db. This will be removed once we are able to get
		// depotPrefix from the db.
		err = util.ValidateRequiredAbsPath(options.DepotPrefix, "depot path")
		if err != nil {
			return nil, err
		}
	}

	vdb.DataPrefix = *options.DataPrefix
	if *options.DepotPrefix != "" {
		vdb.UseDepot = true
		vdb.DepotPrefix = *options.DepotPrefix
	}

	err = options.setInitiator(vdb.UpPrimaryNodes)
	if err != nil {
		return nil, err
	}

	instructions, err := produceRemoveNodeInstructions(&vdb, options)
	if err != nil {
		vlog.LogPrintError("fail to produce remove node instructions, %s", err)
		return nil, err
	}

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	if runError := clusterOpEngine.Run(); runError != nil {
		vlog.LogPrintError("fail to complete remove node operation, %s", runError)
		return nil, runError
	}
	return &vdb, nil
}

// produceRemoveNodeInstructions will build a list of instructions to execute for
// the remove node operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful remove_node:
//   - Check if nodes to remove exist
//   - Update ksafety if needed
//   - Mark nodes to remove as ephemeral
//   - Rebalance cluster for Enterprise mode, rebalance shards for Eon mode
//   - Remove nodes from Spread
//   - Drop Nodes
//   - Delete catalog and data directories
//   - Reload spread
//   - Sync catalog (eon only)
func produceRemoveNodeInstructions(vdb *VCoordinationDatabase, options *VRemoveNodeOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	var inputHost []string
	inputHost = append(inputHost, options.Initiator)

	username := *options.UserName
	usePassword := options.usePassword

	httpCheckNodesExistOp, err := makeHTTPCheckNodesExistOp(inputHost,
		options.HostsToRemove, options.usePassword, username, options.Password, RemoveNode)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&httpCheckNodesExistOp)

	if (len(vdb.HostList) - len(options.HostsToRemove)) < ksafetyThreshold {
		httpsMarkDesignKSafeOp, e := makeHTTPSMarkDesignKSafeOp(inputHost, usePassword, username,
			options.Password, ksafeValueZero)
		if e != nil {
			return instructions, e
		}
		instructions = append(instructions, &httpsMarkDesignKSafeOp)
	}

	err = produceMarkEphemeralNodeOps(&instructions, options.HostsToRemove, inputHost,
		usePassword, username, options.Password, vdb.HostNodeMap)
	if err != nil {
		return instructions, err
	}

	httpsRebalanceClusterOp, err := makeHTTPSRebalanceClusterOp(inputHost, usePassword, username,
		options.Password)
	if err != nil {
		return instructions, err
	}
	httpSpreadRemoveNodeOp, err := makeHTTPSpreadRemoveNodeOp(options.HostsToRemove, inputHost, usePassword,
		username, options.Password, vdb.HostNodeMap)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&httpsRebalanceClusterOp,
		&httpSpreadRemoveNodeOp)

	err = produceDropNodeOps(&instructions, options.HostsToRemove, inputHost,
		usePassword, username, options.Password, vdb.HostNodeMap, vdb.IsEon)
	if err != nil {
		return instructions, err
	}

	v := MakeVCoordinationDatabase()
	// we create a VCoordinationDatabase that contains only hosts to remove
	v.SetFromVCoordinationDatabase(vdb, options.HostsToRemove)
	nmaDeleteDirectoriesOp, err := makeNMADeleteDirectoriesOp(&v, *options.ForceDelete)
	if err != nil {
		return instructions, err
	}
	httpsReloadSpreadOp, err := makeHTTPSReloadSpreadOp(inputHost, true, username, options.Password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaDeleteDirectoriesOp,
		&httpsReloadSpreadOp)

	if vdb.IsEon {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(inputHost, true, username, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
	}
	return instructions, nil
}

// produceMarkEphemeralNodeOps produces one MakeHTTPSMarkEphemeralNodeOp for each node.
func produceMarkEphemeralNodeOps(instructions *[]ClusterOp, targetHosts, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	hostNodeMap map[string]VCoordinationNode) error {
	for _, host := range targetHosts {
		httpsMarkEphemeralNodeOp, err := makeHTTPSMarkEphemeralNodeOp(hostNodeMap[host].Name, hosts, useHTTPPassword, userName,
			httpsPassword)
		if err != nil {
			return err
		}
		*instructions = append(*instructions, &httpsMarkEphemeralNodeOp)
	}
	return nil
}

// produceDropNodeOps produces one MakeHTTPSDropNodeOp for each node.
// This is because we must drop node one by one to avoid losing quorum.
func produceDropNodeOps(instructions *[]ClusterOp, targetHosts, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	hostNodeMap map[string]VCoordinationNode, isEon bool) error {
	for _, host := range targetHosts {
		httpsDropNodeOp, err := makeHTTPSDropNodeOp(hostNodeMap[host].Name, hosts, useHTTPPassword, userName,
			httpsPassword, isEon)
		if err != nil {
			return err
		}
		*instructions = append(*instructions, &httpsDropNodeOp)
	}

	return nil
}

// setInitiator chooses as initiator the first primary up node that is not
// in the list of hosts to remove.
func (o *VRemoveNodeOptions) setInitiator(upPrimaryNodes []string) error {
	initiatorHost := getInitiatorHost(upPrimaryNodes, o.HostsToRemove)
	if initiatorHost == "" {
		return fmt.Errorf("could not find any primary up nodes that is not to be removed")
	}
	o.Initiator = initiatorHost
	return nil
}
