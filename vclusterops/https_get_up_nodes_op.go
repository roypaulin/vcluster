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
	"sort"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/vertica/vcluster/vclusterops/util"
)

const (
	SandboxCmd = iota
	StartNodeCommand
	StopDBCmd
	ScrutinizeCmd
	AddSubclusterCmd
	StopSubclusterCmd
	InstallPackageCmd
	UnsandboxCmd
)

type CommandType int

type httpsGetUpNodesOp struct {
	opBase
	opHTTPSBase
	DBName      string
	noUpHostsOk bool
	cmdType     CommandType
	sandbox     string
	mainCluster bool
	scName      string
}

func makeHTTPSGetUpNodesOp(dbName string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string, cmdType CommandType,
) (httpsGetUpNodesOp, error) {
	op := httpsGetUpNodesOp{}
	op.name = "HTTPSGetUpNodesOp"
	op.description = "Collect information for all up nodes"
	op.hosts = hosts
	op.useHTTPPassword = useHTTPPassword
	op.DBName = dbName
	op.cmdType = cmdType
	op.sandbox = ""
	op.mainCluster = false

	if useHTTPPassword {
		err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
		if err != nil {
			return op, err
		}
		op.userName = userName
		op.httpsPassword = httpsPassword
	}
	return op, nil
}

func makeHTTPSGetUpNodesWithSandboxOp(dbName string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string, cmdType CommandType,
	sandbox string, mainCluster bool) (httpsGetUpNodesOp, error) {
	op, err := makeHTTPSGetUpNodesOp(dbName, hosts, useHTTPPassword, userName, httpsPassword, cmdType)
	op.sandbox = sandbox
	op.mainCluster = mainCluster
	return op, err
}

func makeHTTPSGetUpScNodesOp(dbName string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string, cmdType CommandType,
	scName string) (httpsGetUpNodesOp, error) {
	op, err := makeHTTPSGetUpNodesOp(dbName, hosts, useHTTPPassword, userName, httpsPassword, cmdType)
	op.scName = scName
	return op, err
}

func (op *httpsGetUpNodesOp) allowNoUpHosts() {
	op.noUpHostsOk = true
}

func (op *httpsGetUpNodesOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.buildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsGetUpNodesOp) prepare(execContext *opEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsGetUpNodesOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

/* httpsNodeStateResponse example:
   {'details':[]
	'node_list':[{ 'name': 'v_test_db_running_node0001',
	               'node_id':'45035996273704982',
		           'address': '192.168.1.101',
		           'state' : 'UP'
		           'database' : 'test_db',
		           'is_primary' : true,
		           'is_readonly' : false,
		           'catalog_path' : "\/data\/test_db\/v_test_db_node0001_catalog\/Catalog"
		           'subcluster_name' : ''
		           'last_msg_from_node_at':'2023-01-23T15:18:18.44866"
		           'down_since' : null
		           'build_info' : "v12.0.4-7142c8b01f373cc1aa60b1a8feff6c40bfb7afe8"
	}]}
     or an rfc error if the endpoint does not return a well-structured JSON, an example:
    {
    "type": "https:\/\/integrators.vertica.com\/rest\/errors\/unauthorized-request",
    "title": "Unauthorized-request",
    "detail": "Local node has not joined cluster yet, HTTP server will accept connections when the node has joined the cluster\n",
    "host": "0.0.0.0",
    "status": 401
    }
*/

func (op *httpsGetUpNodesOp) processResult(execContext *opEngineExecContext) error {
	var allErrs error
	upHosts := mapset.NewSet[string]()
	upScInfo := make(map[string]string)
	exceptionHosts := []string{}
	downHosts := []string{}
	sandboxInfo := make(map[string]string)
	upScNodes := mapset.NewSet[NodeInfo]()
	scNodes := mapset.NewSet[NodeInfo]()
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
		}

		// We assume all the hosts are in the same db cluster
		// If any of the hosts reject the request, other hosts will reject the request too
		// Do not try other hosts when we see a http failure
		if result.isFailing() && result.isHTTPRunning() {
			exceptionHosts = append(exceptionHosts, host)
			continue
		}

		if !result.isPassing() {
			downHosts = append(downHosts, host)
			continue
		}

		nodesStates := nodesStateInfo{}
		err := op.parseAndCheckResponse(host, result.content, &nodesStates)
		if err != nil {
			err = fmt.Errorf(`[%s] fail to parse result on host %s, details: %w`, op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		if op.cmdType == StopDBCmd || op.cmdType == StopSubclusterCmd {
			err = op.validateHosts(nodesStates)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				break
			}
		}

		// collect all the up hosts
		err = op.collectUpHosts(nodesStates, host, upHosts, upScInfo, sandboxInfo, upScNodes, scNodes)
		if err != nil {
			allErrs = errors.Join(allErrs, err)
			return allErrs
		}

		if op.cmdType == UnsandboxCmd {
			op.collectUnsandboxingHosts(nodesStates, sandboxInfo)
		}

		if upHosts.Cardinality() > 0 && !isCompleteScanRequired(op.cmdType) {
			break
		}
	}
	execContext.nodesInfo = upScNodes.ToSlice()
	execContext.scNodesInfo = scNodes.ToSlice()
	execContext.upHostsToSandboxes = sandboxInfo
	ignoreErrors := op.processHostLists(upHosts, upScInfo, exceptionHosts, downHosts, sandboxInfo, execContext)
	if ignoreErrors {
		return nil
	}

	return errors.Join(allErrs, fmt.Errorf("no up nodes detected"))
}

// Return true if all the results need to be scanned to figure out UP hosts
func isCompleteScanRequired(cmdType CommandType) bool {
	return cmdType == SandboxCmd || cmdType == StopDBCmd || cmdType == UnsandboxCmd || cmdType == StopSubclusterCmd
}

func (op *httpsGetUpNodesOp) finalize(_ *opEngineExecContext) error {
	return nil
}

func (op *httpsGetUpNodesOp) checkSandboxUp(sandboxingInfo map[string]string, sandbox string) bool {
	for _, sb := range sandboxingInfo {
		if sb == sandbox {
			return true
		}
	}
	return false
}

// processHostLists stashes the up hosts, and if there are no up hosts, prints and logs
// down or erratic hosts.  Additionally, it determines if the op should fail or not.
func (op *httpsGetUpNodesOp) processHostLists(upHosts mapset.Set[string], upScInfo map[string]string,
	exceptionHosts, downHosts []string, sandboxInfo map[string]string,
	execContext *opEngineExecContext) (ignoreErrors bool) {
	execContext.upScInfo = upScInfo

	// when we found up nodes in the database, but cannot found up nodes in subcluster, we throw an error
	if op.cmdType == StopSubclusterCmd && upHosts.Cardinality() > 0 && len(execContext.nodesInfo) == 0 {
		op.logger.PrintError(`[%s] There are no UP nodes in subcluster %s. The subcluster is already down`, op.name, op.scName)
		return false
	}
	if op.sandbox != "" && op.cmdType != UnsandboxCmd {
		upSandbox := op.checkSandboxUp(sandboxInfo, op.sandbox)
		if !upSandbox {
			op.logger.PrintError(`[%s] There are no UP nodes in the sandbox %s. The db %s is already down`, op.name, op.sandbox, op.DBName)
		}
	}
	if op.mainCluster {
		upMainCluster := op.checkSandboxUp(sandboxInfo, "")
		if !upMainCluster {
			op.logger.PrintError(`[%s] There are no UP nodes in the main cluster. The db %s is already down`, op.name, op.DBName)
		}
	}
	if upHosts.Cardinality() > 0 {
		execContext.upHosts = upHosts.ToSlice()
		// sorting the up hosts will be helpful for picking up the initiator in later instructions
		sort.Strings(execContext.upHosts)
		return true
	}
	if len(exceptionHosts) > 0 {
		op.logger.PrintError(`[%s] fail to call https endpoint of database %s on hosts %s`, op.name, op.DBName, exceptionHosts)
	}

	if len(downHosts) > 0 {
		op.logger.PrintError(`[%s] did not detect database %s running on hosts %v`, op.name, op.DBName, downHosts)
		op.updateSpinnerStopFailMessage("did not detect database %s running on hosts %v", op.DBName, downHosts)
	}

	return op.noUpHostsOk
}

// validateHosts can validate if hosts in user input matches the ones in GET /nodes response
func (op *httpsGetUpNodesOp) validateHosts(nodesStates nodesStateInfo) error {
	var dbHosts []string
	dbUnexpected := false
	unexpectedDBName := ""
	for _, node := range nodesStates.NodeList {
		if node.Database != op.DBName {
			unexpectedDBName = node.Database
			dbUnexpected = true
		}
		dbHosts = append(dbHosts, node.Address)
	}
	// when db name does not match, we throw an error
	if dbUnexpected {
		unexpectedHosts := util.SliceCommon(op.hosts, dbHosts)
		return fmt.Errorf(`[%s] unexpected database %q is running on hosts %v. Please ensure the provided hosts or database name are correct`,
			op.name, unexpectedDBName, unexpectedHosts)
	}
	// when hosts from user input do not match the ones from running db, we throw an error
	unexpectedHosts := util.SliceDiff(op.hosts, dbHosts)
	if len(unexpectedHosts) > 0 {
		return fmt.Errorf(`[%s] database %q does not contain any nodes on the hosts %v. Please ensure the hosts are correct`,
			op.name, op.DBName, unexpectedHosts)
	}

	return nil
}

func (op *httpsGetUpNodesOp) collectUpHosts(nodesStates nodesStateInfo, host string, upHosts mapset.Set[string],
	upScInfo, sandboxInfo map[string]string, upScNodes, scNodes mapset.Set[NodeInfo]) (err error) {
	upMainNodeFound := false
	foundSC := false
	for _, node := range nodesStates.NodeList {
		if node.Database != op.DBName {
			err = fmt.Errorf(`[%s] database %s is running on host %s, rather than database %s`, op.name, node.Database, host, op.DBName)
			return err
		}
		if op.scName != "" && node.Subcluster == op.scName {
			foundSC = true
		}
		if node.State == util.NodeUpState {
			upHosts.Add(node.Address)
			upScInfo[node.Address] = node.Subcluster
			if op.cmdType == StopDBCmd {
				if node.Sandbox != util.MainClusterSandbox || !upMainNodeFound {
					sandboxInfo[node.Address] = node.Sandbox
					// We still need one main cluster UP node, when there are sandboxes
					upMainNodeFound = true
				}
			}
		}
		if op.scName == node.Subcluster {
			op.sandbox = node.Sandbox
			var n NodeInfo
			// collect info for "UP" and "DOWN" nodes, ignore "UNKNOWN" nodes here
			// because we want to avoid getting duplicate nodes' info. For a sandbox node,
			// we will get two duplicate NodeInfo entries if we do not ignore "UNKNOWN" nodes:
			// one with state "UNKNOWN" from main cluster, and the other with state "UP"
			// from sandboxes.
			if node.State == util.NodeUpState {
				if n, err = node.asNodeInfo(); err != nil {
					op.logger.PrintError("[%s] %s", op.name, err.Error())
				} else {
					upScNodes.Add(n)
					scNodes.Add(n)
				}
			} else if node.State == util.NodeDownState {
				// for "DOWN" node, we cannot get its version from https response
				n = node.asNodeInfoWoVer()
				scNodes.Add(n)
			}
		}
	}
	if !foundSC && op.cmdType == StopSubclusterCmd {
		return fmt.Errorf(`[%s] cannot find subcluster %s in database %s`, op.name, op.scName, op.DBName)
	}
	return err
}

func (op *httpsGetUpNodesOp) collectUnsandboxingHosts(nodesStates nodesStateInfo, sandboxInfo map[string]string) {
	mainNodeFound := false
	sandboxNodeFound := false
	for _, node := range nodesStates.NodeList {
		if node.State == util.NodeUpState {
			// A sandbox could consist of multiple subclusters.
			// We need to run unsandbox command on the other subcluster node in the same sandbox
			// Find a node from same sandbox but different subcluster, if exists
			if node.Sandbox == op.sandbox && node.Subcluster != op.scName {
				sandboxInfo[node.Address] = node.Sandbox
			}
			// Get one main cluster host
			if node.Sandbox == "" && !mainNodeFound {
				sandboxInfo[node.Address] = ""
				mainNodeFound = true
			}
			if sandboxNodeFound && mainNodeFound {
				break
			}
		}
	}
}
