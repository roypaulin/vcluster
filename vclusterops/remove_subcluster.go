/*
 (c) Copyright [2023-2024] Open Text.
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
	"strings"

	"github.com/vertica/vcluster/rfc7807"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// VRemoveScOptions represents the available options when you remove a subcluster from a
// database.
type VRemoveScOptions struct {
	DatabaseOptions
	SCName      string // subcluster to remove from database
	ForceDelete bool   // whether force delete directories
}

func VRemoveScOptionsFactory() VRemoveScOptions {
	options := VRemoveScOptions{}
	// set default values to the params
	options.setDefaultValues()

	return options
}

func (options *VRemoveScOptions) setDefaultValues() {
	options.DatabaseOptions.setDefaultValues()
}

func (options *VRemoveScOptions) validateRequiredOptions(logger vlog.Printer) error {
	err := options.validateBaseOptions(commandRemoveSubcluster, logger)
	if err != nil {
		return err
	}

	if options.SCName == "" {
		return fmt.Errorf("must specify a subcluster name")
	}

	err = util.ValidateScName(options.SCName)
	if err != nil {
		return err
	}
	return nil
}

func (options *VRemoveScOptions) validateEonOptions() error {
	if !options.IsEon {
		return fmt.Errorf(`cannot remove subcluster from an enterprise database '%s'`,
			options.DBName)
	}
	return nil
}

func (options *VRemoveScOptions) validateExtraOptions() error {
	// VER-88096 will get data path and depot path from /nodes
	// so the validation below may be removed
	// data prefix
	err := util.ValidateRequiredAbsPath(options.DataPrefix, "data path")
	if err != nil {
		return err
	}

	// depot path
	return util.ValidateRequiredAbsPath(options.DepotPrefix, "depot path")
}

func (options *VRemoveScOptions) validateParseOptions(logger vlog.Printer) error {
	// batch 1: validate required params
	err := options.validateRequiredOptions(logger)
	if err != nil {
		return err
	}

	// batch 2: validate eon params
	err = options.validateEonOptions()
	if err != nil {
		return err
	}

	// batch 3: validate all other params
	err = options.validateExtraOptions()
	if err != nil {
		return err
	}
	return nil
}

func (options *VRemoveScOptions) analyzeOptions() (err error) {
	// we analyze host names when it is set in user input, otherwise we use hosts in yaml config
	if len(options.RawHosts) > 0 {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.IPv6)
		if err != nil {
			return err
		}
		options.normalizePaths()
	}
	return nil
}

func (options *VRemoveScOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	if err := options.validateParseOptions(logger); err != nil {
		return err
	}
	err := options.analyzeOptions()
	if err != nil {
		return err
	}
	return options.setUsePassword(logger)
}

// VRemoveSubcluster removes a subcluster. It returns updated database catalog information and any error encountered.
// VRemoveSubcluster has three major phases:
//  1. Pre-check: check the subcluster name and get nodes for the subcluster.
//  2. Removes nodes: Optional. If there are any nodes still associated with the subcluster, runs VRemoveNode.
//  3. Drop the subcluster: Remove the subcluster name from the database catalog.
func (vcc VClusterCommands) VRemoveSubcluster(removeScOpt *VRemoveScOptions) (VCoordinationDatabase, error) {
	vdb := makeVCoordinationDatabase()

	// validate and analyze options
	err := removeScOpt.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return vdb, err
	}

	// pre-check: should not remove the default subcluster
	vcc.PrintInfo("Performing remove_subcluster pre-checks")
	hostsToRemove, err := vcc.removeScPreCheck(&vdb, removeScOpt)
	if err != nil {
		return vdb, err
	}

	// proceed to run remove_node only if
	// the number of nodes to remove is greater than zero
	var needRemoveNodes bool
	vcc.Log.V(1).Info("Nodes to be removed: %+v", hostsToRemove)
	if len(hostsToRemove) == 0 {
		vcc.Log.PrintInfo("no node found in subcluster %s",
			removeScOpt.SCName)
		needRemoveNodes = false
	} else {
		needRemoveNodes = true
	}

	if needRemoveNodes {
		// Remove nodes from the target subcluster
		removeNodeOpt := VRemoveNodeOptionsFactory()
		removeNodeOpt.DatabaseOptions = removeScOpt.DatabaseOptions
		removeNodeOpt.HostsToRemove = hostsToRemove
		removeNodeOpt.ForceDelete = removeScOpt.ForceDelete
		removeNodeOpt.IsSubcluster = true

		vcc.Log.PrintInfo("Removing nodes %q from subcluster %s",
			hostsToRemove, removeScOpt.SCName)
		vdb, err = vcc.VRemoveNode(&removeNodeOpt)
		if err != nil {
			return vdb, err
		}
	}

	// drop subcluster (i.e., remove the sc name from catalog)
	vcc.Log.PrintInfo("Removing the subcluster name from catalog")
	err = vcc.dropSubcluster(&vdb, removeScOpt)
	if err != nil {
		return vdb, err
	}

	return vdb, nil
}

type removeDefaultSubclusterError struct {
	Name string
}

func (e *removeDefaultSubclusterError) Error() string {
	return fmt.Sprintf("cannot remove the default subcluster '%s'", e.Name)
}

// removeScPreCheck will build a list of instructions to perform
// remove_subcluster pre-checks
//
// The generated instructions will later perform the following operations necessary
// for a successful remove_node:
//   - Get cluster and nodes info (check if the target DB is Eon and get to-be-removed node list)
//   - Get the subcluster info (check if the target sc exists and if it is the default sc)
func (vcc VClusterCommands) removeScPreCheck(vdb *VCoordinationDatabase, options *VRemoveScOptions) ([]string, error) {
	var hostsToRemove []string
	const preCheckErrMsg = "while performing remove_subcluster pre-checks"

	// get cluster and nodes info
	err := vcc.getVDBFromRunningDB(vdb, &options.DatabaseOptions)
	if err != nil {
		return hostsToRemove, err
	}

	// remove_subcluster only works with Eon database
	if !vdb.IsEon {
		// info from running db confirms that the db is not Eon
		return hostsToRemove, fmt.Errorf(`cannot remove subcluster from an enterprise database '%s'`,
			options.DBName)
	}

	err = options.completeVDBSetting(vdb)
	if err != nil {
		return hostsToRemove, err
	}

	// get default subcluster
	// cannot remove sandbox subcluster
	httpsFindSubclusterOp, err := makeHTTPSFindSubclusterOp(options.Hosts,
		options.usePassword, options.UserName, options.Password,
		options.SCName,
		false /*do not ignore not found*/, RemoveSubclusterCmd)
	if err != nil {
		return hostsToRemove, fmt.Errorf("fail to get default subcluster %s, details: %w",
			preCheckErrMsg, err)
	}

	var instructions []clusterOp
	instructions = append(instructions,
		&httpsFindSubclusterOp,
	)

	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)
	err = clusterOpEngine.run(vcc.Log)
	if err != nil {
		// VER-88585 will improve this rfc error flow
		if strings.Contains(err.Error(), "does not exist in the database") {
			vcc.Log.PrintError("fail to get subclusters' information %s, %v", preCheckErrMsg, err)
			rfcErr := rfc7807.New(rfc7807.SubclusterNotFound).WithHost(options.Hosts[0])
			return hostsToRemove, rfcErr
		}
		return hostsToRemove, err
	}

	// the default subcluster should not be removed
	if options.SCName == clusterOpEngine.execContext.defaultSCName {
		return hostsToRemove, &removeDefaultSubclusterError{Name: options.SCName}
	}

	// get nodes of the to-be-removed subcluster
	for h, vnode := range vdb.HostNodeMap {
		if vnode.Subcluster == options.SCName {
			hostsToRemove = append(hostsToRemove, h)
		}
	}

	return hostsToRemove, nil
}

// completeVDBSetting sets some VCoordinationDatabase fields we cannot get yet
// from the https endpoints. We set those fields from options.
func (options *VRemoveScOptions) completeVDBSetting(vdb *VCoordinationDatabase) error {
	vdb.DataPrefix = options.DataPrefix
	vdb.DepotPrefix = options.DepotPrefix

	hostNodeMap := makeVHostNodeMap()
	// TODO: we set the depot path from /nodes rather than manually
	// (VER-92725). This is useful for nmaDeleteDirectoriesOp.
	for h, vnode := range vdb.HostNodeMap {
		vnode.DepotPath = vdb.GenDepotPath(vnode.Name)
		hostNodeMap[h] = vnode
	}
	vdb.HostNodeMap = hostNodeMap
	return nil
}

func (vcc VClusterCommands) dropSubcluster(vdb *VCoordinationDatabase, options *VRemoveScOptions) error {
	dropScErrMsg := fmt.Sprintf("fail to drop subcluster %s", options.SCName)

	// the initiator is a list of one primary up host
	// that will call the https /v1/subclusters/{scName}/drop endpoint
	// as the endpoint will drop a subcluster, we only need one host to do so
	initiator, err := getInitiatorHost(vdb.PrimaryUpNodes, []string{})
	if err != nil {
		return err
	}

	httpsDropScOp, err := makeHTTPSDropSubclusterOp([]string{initiator},
		options.SCName,
		options.usePassword, options.UserName, options.Password)
	if err != nil {
		vcc.Log.Error(err, "details: %v", dropScErrMsg)
		return err
	}

	var instructions []clusterOp
	instructions = append(instructions, &httpsDropScOp)

	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)
	err = clusterOpEngine.run(vcc.Log)
	if err != nil {
		vcc.Log.Error(err, "fail to drop subcluster, details: %v", dropScErrMsg)
		return err
	}

	return nil
}
