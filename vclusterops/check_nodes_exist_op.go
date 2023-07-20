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
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type CheckNodesExistOp struct {
	OpBase
}

// MakeCheckNodesExistOp will make a non-https op that check if new nodes exists in current database
func MakeCheckNodesExistOp(opName string, nodes []string) CheckNodesExistOp {
	nodeStateChecker := CheckNodesExistOp{}
	nodeStateChecker.name = opName
	nodeStateChecker.hosts = nodes

	return nodeStateChecker
}

// use an empty implementation of setupClusterHTTPRequest to make the compiler happy
func (op *CheckNodesExistOp) setupClusterHTTPRequest(hosts []string) {
}

func (op *CheckNodesExistOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	if len(execContext.nodeStates) == 0 {
		vlog.LogError(`[%s] Cannot find any nodes' information in OpEngineExecContext`, op.name)
		return MakeClusterOpResultFail()
	}
	return MakeClusterOpResultPass()
}

func (op *CheckNodesExistOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	return op.processResult(execContext)
}

func (op *CheckNodesExistOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := false
	newNodes := op.hosts
	currentNodes := []string{}
	for _, node := range execContext.nodeStates {
		currentNodes = append(currentNodes, node.Address)
	}

	// verify the new nodes do not exist in current database
	hostSet := make(map[string]struct{})
	for _, host := range newNodes {
		hostSet[host] = struct{}{}
	}
	dupHosts := []string{}
	for _, host := range currentNodes {
		if _, exist := hostSet[host]; exist {
			dupHosts = append(dupHosts, host)
		}
	}
	if len(dupHosts) == 0 {
		success = true
	}

	if success {
		return MakeClusterOpResultPass()
	}
	vlog.LogPrintError("[%s] new nodes %v already exist in the database", op.name, dupHosts)
	return MakeClusterOpResultFail()
}

func (op *CheckNodesExistOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}
