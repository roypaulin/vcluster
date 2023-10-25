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

type httpsDropSubclusterOp struct {
	OpBase
	OpHTTPSBase
	scName string
}

func makeHTTPSDropSubclusterOp(hosts []string, scName string,
	useHTTPPassword bool, userName string, httpsPassword *string,
) (httpsDropSubclusterOp, error) {
	op := httpsDropSubclusterOp{}
	op.name = "HTTPSDropSubclusterOp"
	op.hosts = hosts
	op.useHTTPPassword = useHTTPPassword
	op.scName = scName

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

func (op *httpsDropSubclusterOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("subclusters/" + op.scName + "/drop")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsDropSubclusterOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsDropSubclusterOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsDropSubclusterOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			return fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}

		if result.isPassing() {
			// the successful result should look like
			// {"detail": ""}
			return nil
		}

		allErrs = errors.Join(allErrs, result.err)
	}
	return appendHTTPSFailureError(allErrs)
}

func (op *httpsDropSubclusterOp) finalize(_ *OpEngineExecContext) error {
	return nil
}
