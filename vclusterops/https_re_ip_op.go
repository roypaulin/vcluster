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
	"errors"
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
)

// ReIPNoClusterQuorumError is an error to indicate
// that cluster quorum was lost before a re-ip.
// This is emitted from this op. Callers can do type checking to perform an
// action based on the error.
type ReIPNoClusterQuorumError struct {
	Detail string
}

func (e *ReIPNoClusterQuorumError) Error() string {
	return e.Detail
}

type httpsReIPOp struct {
	opBase
	opHTTPSBase
	hostToReIP      []string
	reIPList        map[string]ReIPInfo
	nodeNamesToReIP []string
	upHosts         []string
}

func makeHTTPSReIPOp(nodeNamesToReIP, hostToReIP []string,
	useHTTPPassword bool, userName string, httpsPassword *string) (httpsReIPOp, error) {
	op := httpsReIPOp{}
	op.name = "HTTPSReIpOp"
	op.description = "Change host IPs in the catalog"
	op.useHTTPPassword = useHTTPPassword
	op.nodeNamesToReIP = nodeNamesToReIP
	op.hostToReIP = hostToReIP

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

func (op *httpsReIPOp) setupClusterHTTPRequest(hostsToReIP []string) error {
	// At this point there must be more up nodes than hosts to re-ip.
	// Failure to meet that requirement would most likely mean that we have lost
	// quorum and a cluster restart is needed
	if len(op.upHosts) < len(hostsToReIP) {
		return &ReIPNoClusterQuorumError{
			Detail: fmt.Sprintf("[%s] %d up nodes are not enough for re-ip", op.name, len(op.upHosts)),
		}
	}
	for i, host := range hostsToReIP {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = PutMethod
		nodesInfo, ok := op.reIPList[host]
		if !ok {
			return fmt.Errorf("[%s] cannot find node information for address %s", op.name, host)
		}
		httpRequest.buildHTTPSEndpoint("nodes/" + nodesInfo.NodeName + "/ip")
		httpRequest.QueryParams = make(map[string]string)
		httpRequest.QueryParams["host"] = nodesInfo.TargetAddress
		httpRequest.QueryParams["control-host"] = nodesInfo.TargetControlAddress
		httpRequest.QueryParams["broadcast"] = nodesInfo.TargetControlBroadcast

		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[op.upHosts[i]] = httpRequest
	}

	return nil
}

func (op *httpsReIPOp) prepare(execContext *opEngineExecContext) error {
	op.reIPList = make(map[string]ReIPInfo)
	// update reIPList from input node names and execContext.networkProfiles
	for i := 0; i < len(op.nodeNamesToReIP); i++ {
		nodeNameToReIP := op.nodeNamesToReIP[i]
		targetAddress := op.hostToReIP[i]
		profile, ok := execContext.networkProfiles[targetAddress]
		if !ok {
			return fmt.Errorf("[%s] unable to find network profile for address %s", op.name, targetAddress)
		}
		info := ReIPInfo{
			NodeName:               nodeNameToReIP,
			TargetAddress:          targetAddress,
			TargetControlAddress:   profile.Address,
			TargetControlBroadcast: profile.Broadcast,
		}
		op.reIPList[nodeNameToReIP] = info
	}

	// use up hosts to execute the HTTP re-IP endpoint
	op.upHosts = execContext.upHosts
	execContext.dispatcher.setup(op.upHosts)
	return op.setupClusterHTTPRequest(op.nodeNamesToReIP)
}

func (op *httpsReIPOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsReIPOp) processResult(_ *opEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isUnauthorizedRequest() {
			return fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}

		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		// decode the json-format response
		// The successful response object will be a dictionary as below:
		// {"detail": ""}
		reIPRsp, err := op.parseAndCheckMapResponse(host, result.content)
		if err != nil {
			err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w", op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			break
		}

		// verify if the response content is correct
		v, ok := reIPRsp["detail"]
		if !ok {
			err = fmt.Errorf(`[%s] response does not contain field "detail"`, op.name)
			allErrs = errors.Join(allErrs, err)
			break
		}
		if v != "" {
			err = fmt.Errorf(`[%s] response detail should be '' but got '%s'`, op.name, reIPRsp["detail"])
			allErrs = errors.Join(allErrs, err)
			break
		}
	}
	return allErrs
}

func (op *httpsReIPOp) finalize(_ *opEngineExecContext) error {
	return nil
}
