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

	"github.com/vertica/vcluster/vclusterops/util"
)

type CheckNodesExistCallerType int

const (
	AddNode CheckNodesExistCallerType = iota
	RemoveNode
)

const emptySubcluster = ""

// HTTPCheckNodesExistOp defines an operation to get the
// node states and check if some hosts are already part
// of the database.
type HTTPCheckNodesExistOp struct {
	OpBase
	OpHTTPBase
	// The IP addresses of the hosts whose existence we want to check
	targetHosts []string
	caller      CheckNodesExistCallerType
}

type NodeInfoEon struct {
	NodeInfo
	Subcluster string `json:"subcluster_name"`
}

type NodesInfoEon struct {
	NodeList []NodeInfoEon `json:"node_list"`
}

// makeHTTPCheckNodesExistOp will make a https op that check if new nodes exists in current database
func makeHTTPCheckNodesExistOp(
	hosts []string,
	targetHosts []string,
	useHTTPPassword bool,
	userName string,
	httpsPassword *string,
	caller CheckNodesExistCallerType) (HTTPCheckNodesExistOp, error) {
	nodeStateChecker := HTTPCheckNodesExistOp{}
	nodeStateChecker.name = "HTTPCheckNodesExistOp"
	nodeStateChecker.caller = caller
	// The hosts are the ones we are going to talk to.
	// as if any of the hosts is responsive, spread can give us the info of all nodes.
	nodeStateChecker.hosts = hosts
	nodeStateChecker.targetHosts = targetHosts
	nodeStateChecker.useHTTPPassword = useHTTPPassword

	err := util.ValidateUsernameAndPassword(nodeStateChecker.name, useHTTPPassword, userName)
	if err != nil {
		return nodeStateChecker, err
	}

	nodeStateChecker.userName = userName
	nodeStateChecker.httpsPassword = httpsPassword
	return nodeStateChecker, nil
}

func (op *HTTPCheckNodesExistOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *HTTPCheckNodesExistOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *HTTPCheckNodesExistOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPCheckNodesExistOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			// return here because we assume that
			// we will get the same error across other nodes
			return fmt.Errorf("[%s] unauthorized request: %w", op.name, result.err)
		}

		if result.err != nil {
			err := fmt.Errorf("[%s] error of the /nodes endpoint: %w", op.name, result.err)
			allErrs = errors.Join(allErrs, err)
			// for any error, we use "continue" to try the next node
			continue
		}

		// parse the /nodes endpoint response
		nodesInfo := NodesInfoEon{}
		err := op.parseAndCheckResponse(host, result.content, &nodesInfo)
		if err != nil {
			err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w",
				op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		doNodesExist := op.checkNodesExist(nodesInfo.NodeList)
		switch {
		case op.caller == AddNode:
			if doNodesExist {
				return errors.New("some of the hosts to add already exists in the database")
			}
		case op.caller == RemoveNode:
			if !doNodesExist {
				return errors.New("some of the nodes to remove do not exist in the database")
			}
			// in Eon mode, every node is assigned to a subcluster, so an empty
			// subcluster means it is not eon.
			if nodesInfo.NodeList[0].Subcluster == emptySubcluster {
				// In enterprise mode, we need all nodes to be up or in
				// standby in order to drop a node.
				if op.checkDownNode(nodesInfo.NodeList) {
					return errors.New("all nodes must be up or standby")
				}
			}
		}
		return nil
	}
	return allErrs
}

func (op *HTTPCheckNodesExistOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

// checkNodesExist return true if at least one of the target hosts
// already exists in the database.
func (op *HTTPCheckNodesExistOp) checkNodesExist(nodes []NodeInfoEon) bool {
	// verify the new nodes do not exist in current database
	hostSet := make(map[string]struct{})
	for _, host := range op.targetHosts {
		hostSet[host] = struct{}{}
	}
	dupHosts := []string{}
	for _, host := range nodes {
		if _, exist := hostSet[host.Address]; exist {
			dupHosts = append(dupHosts, host.Address)
		}
	}

	return len(dupHosts) != 0
}

// checkDownNode returns true if at least one of the db nodes
// is down.
func (op *HTTPCheckNodesExistOp) checkDownNode(nodes []NodeInfoEon) bool {
	for _, host := range nodes {
		if host.State == util.NodeDownState {
			return true
		}
	}
	return false
}
