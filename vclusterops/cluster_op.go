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

// vclusterops is a Go library to administer a Vertica cluster with HTTP RESTful
// interfaces. These interfaces are exposed through the Node Management Agent
// (NMA) and an HTTPS service embedded in the server. With this library you can
// perform administrator-level operations, including: creating a database,
// scaling up/down, restarting the cluster, and stopping the cluster.
package vclusterops

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* Op and host http result status
 */

// ResultStatus is the data type for the status of
// ClusterOpResult and HostHTTPResult
type ResultStatus int

var wrongCredentialErrMsg = []string{"Wrong password", "Wrong certificate"}

const (
	SUCCESS   ResultStatus = 0
	FAILURE   ResultStatus = 1
	EXCEPTION ResultStatus = 2
)

const (
	GetMethod    = "GET"
	PutMethod    = "PUT"
	PostMethod   = "POST"
	DeleteMethod = "DELETE"
)

const (
	// track endpoint versions and the current version
	NMAVersion1    = "v1/"
	HTTPVersion1   = "v1/"
	NMACurVersion  = NMAVersion1
	HTTPCurVersion = HTTPVersion1
)

const (
	SuccessResult   = "SUCCESS"
	FailureResult   = "FAILURE"
	ExceptionResult = "FAILURE"
)

const (
	SuccessCode        = 200
	MultipleChoiceCode = 300
	UnauthorizedCode   = 401
	InternalErrorCode  = 500
)

// HostHTTPResult is used to save result of an Adapter's sendRequest(...) function
// it is the element of the adapter pool's channel
type HostHTTPResult struct {
	status     ResultStatus
	statusCode int
	host       string
	content    string
	err        error // This is set if the http response ends in a failure scenario
}

type httpsResponseStatus struct {
	StatusCode int `json:"status"`
}

const respSuccStatusCode = 0

// The HTTP response with a 401 status code can have several scenarios:
// 1. Wrong password
// 2. Wrong certificate
// 3. The local node has not yet joined the cluster; the HTTP server will accept connections once the node joins the cluster.
// HTTPCheckDBRunningOp in create_db need to check all scenarios to see any HTTP running
// For HTTPSPollNodeStateOp in start_db, it requires only handling the first and second scenarios
func (hostResult *HostHTTPResult) IsUnauthorizedRequest() bool {
	return hostResult.statusCode == UnauthorizedCode
}

// IsSuccess returns true if status code is 200
func (hostResult *HostHTTPResult) IsSuccess() bool {
	return hostResult.statusCode == SuccessCode
}

// check only password and certificate for start_db
func (hostResult *HostHTTPResult) IsPasswordAndCertificateError(log vlog.Printer) bool {
	if !hostResult.IsUnauthorizedRequest() {
		return false
	}
	resultString := fmt.Sprintf("%v", hostResult)
	for _, msg := range wrongCredentialErrMsg {
		if strings.Contains(resultString, msg) {
			log.Error(errors.New(msg), "the user has provided")
			return true
		}
	}
	return false
}

func (hostResult *HostHTTPResult) IsInternalError() bool {
	return hostResult.statusCode == InternalErrorCode
}

func (hostResult *HostHTTPResult) IsHTTPRunning() bool {
	if hostResult.isPassing() || hostResult.IsUnauthorizedRequest() || hostResult.IsInternalError() {
		return true
	}
	return false
}

func (hostResult *HostHTTPResult) isPassing() bool {
	return hostResult.err == nil
}

func (hostResult *HostHTTPResult) isFailing() bool {
	return hostResult.status == FAILURE
}

func (hostResult *HostHTTPResult) isException() bool {
	return hostResult.status == EXCEPTION
}

func (hostResult *HostHTTPResult) isTimeout() bool {
	if hostResult.err != nil {
		var netErr net.Error
		if errors.As(hostResult.err, &netErr) && netErr.Timeout() {
			return true
		}
	}
	return false
}

// getStatusString converts ResultStatus to string
func (status ResultStatus) getStatusString() string {
	if status == FAILURE {
		return FailureResult
	} else if status == EXCEPTION {
		return ExceptionResult
	}
	return SuccessResult
}

/* Cluster ops interface
 */

// ClusterOp interface requires that all ops implements
// the following functions
// log* implemented by embedding OpBase, but overrideable
type ClusterOp interface {
	getName() string
	prepare(execContext *OpEngineExecContext) error
	execute(execContext *OpEngineExecContext) error
	finalize(execContext *OpEngineExecContext) error
	processResult(execContext *OpEngineExecContext) error
	logResponse(host string, result HostHTTPResult)
	logPrepare()
	logExecute()
	logFinalize()
	setupBasicInfo()
	loadCertsIfNeeded(certs *HTTPSCerts, findCertsInOptions bool) error
	isSkipExecute() bool
}

/* Cluster ops basic fields and functions
 */

// OpBase defines base fields and implements basic functions
// for all ops
type OpBase struct {
	log                vlog.Printer
	name               string
	hosts              []string
	clusterHTTPRequest ClusterHTTPRequest
	skipExecute        bool // This can be set during prepare if we determine no work is needed
}

type OpResponseMap map[string]string

func (op *OpBase) getName() string {
	return op.name
}

func (op *OpBase) parseAndCheckResponse(host, responseContent string, responseObj any) error {
	err := util.GetJSONLogErrors(responseContent, &responseObj, op.name, op.log)
	if err != nil {
		op.log.Error(err, "fail to parse response on host, detail", "host", host)
		return err
	}
	op.log.Info("JSON response", "host", host, "responseObj", responseObj)
	return nil
}

func (op *OpBase) parseAndCheckMapResponse(host, responseContent string) (OpResponseMap, error) {
	var responseObj OpResponseMap
	err := op.parseAndCheckResponse(host, responseContent, &responseObj)

	return responseObj, err
}

func (op *OpBase) setClusterHTTPRequestName() {
	op.clusterHTTPRequest.Name = op.name
}

func (op *OpBase) setVersionToSemVar() {
	op.clusterHTTPRequest.SemVar = SemVer{Ver: "1.0.0"}
}

func (op *OpBase) setupBasicInfo() {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setClusterHTTPRequestName()
	op.setVersionToSemVar()
}

func (op *OpBase) logResponse(host string, result HostHTTPResult) {
	op.log.PrintInfo("[%s] result from host %s summary %s, details: %+v",
		op.name, host, result.status.getStatusString(), result)
}

func (op *OpBase) logPrepare() {
	op.log.Info("Prepare() called", "name", op.name)
}

func (op *OpBase) logExecute() {
	op.log.Info("Execute() called", "name", op.name)
	op.log.PrintInfo("[%s] is running", op.name)
}

func (op *OpBase) logFinalize() {
	op.log.Info("Finalize() called", "name", op.name)
}

func (op *OpBase) runExecute(execContext *OpEngineExecContext) error {
	err := execContext.dispatcher.sendRequest(&op.clusterHTTPRequest)
	if err != nil {
		op.log.Error(err, "Fail to dispatch request, detail", "dispatch request", op.clusterHTTPRequest)
		return err
	}
	return nil
}

// if found certs in the options, we add the certs to http requests of each instruction
func (op *OpBase) loadCertsIfNeeded(certs *HTTPSCerts, findCertsInOptions bool) error {
	if !findCertsInOptions {
		return nil
	}

	// this step is executed after Prepare() so all http requests should be set up
	if len(op.clusterHTTPRequest.RequestCollection) == 0 {
		return fmt.Errorf("[%s] has not set up a http request", op.name)
	}

	for host := range op.clusterHTTPRequest.RequestCollection {
		request := op.clusterHTTPRequest.RequestCollection[host]
		request.UseCertsInOptions = true
		request.Certs.key = certs.key
		request.Certs.cert = certs.cert
		request.Certs.caCert = certs.caCert
		op.clusterHTTPRequest.RequestCollection[host] = request
	}
	return nil
}

// isSkipExecute will check state to see if the Execute() portion of the
// operation should be skipped. Some operations can choose to implement this if
// they can only determine at runtime where the operation is needed. One
// instance of this is the nma_upload_config.go. If all nodes already have the
// latest catalog information, there is nothing to be done during execution.
func (op *OpBase) isSkipExecute() bool {
	return op.skipExecute
}

// hasQuorum checks if we have enough working primary nodes to maintain data integrity
// quorumCount = (1/2 * number of primary nodes) + 1
func (op *OpBase) hasQuorum(hostCount, primaryNodeCount uint) bool {
	quorumCount := (primaryNodeCount + 1) / 2
	if hostCount < quorumCount {
		op.log.PrintError("[%s] Quorum check failed: "+
			"number of hosts with latest catalog (%d) is not "+
			"greater than or equal to 1/2 of number of the primary nodes (%d)\n",
			op.name, hostCount, primaryNodeCount)
		return false
	}

	return true
}

// checkResponseStatusCode will verify if the status code in https response is a successful code
func (op *OpBase) checkResponseStatusCode(resp httpsResponseStatus, host string) (err error) {
	if resp.StatusCode != respSuccStatusCode {
		err = fmt.Errorf(`[%s] fail to execute HTTPS request on host %s, status code in HTTPS response is %d`, op.name, host, resp.StatusCode)
		op.log.Error(err, "fail to execute HTTPS request, detail")
		return err
	}
	return nil
}

/* Sensitive fields in request body
 */
type SensitiveFields struct {
	DBPassword         string            `json:"db_password"`
	AWSAccessKeyID     string            `json:"aws_access_key_id"`
	AWSSecretAccessKey string            `json:"aws_secret_access_key"`
	Parameters         map[string]string `json:"parameters"`
}

func (maskedData *SensitiveFields) maskSensitiveInfo() {
	const maskedValue = "******"
	sensitiveKeyParams := map[string]bool{
		"awsauth":                 true,
		"awssessiontoken":         true,
		"gcsauth":                 true,
		"azurestoragecredentials": true,
	}
	maskedData.DBPassword = maskedValue
	maskedData.AWSAccessKeyID = maskedValue
	maskedData.AWSSecretAccessKey = maskedValue
	for key := range maskedData.Parameters {
		// Mask the value if the keys are credentials
		keyLowerCase := strings.ToLower(key)
		if sensitiveKeyParams[keyLowerCase] {
			maskedData.Parameters[key] = maskedValue
		}
	}
}

/* Cluster HTTPS ops basic fields
 * which are needed for https requests using password auth
 * specify whether to use password auth explicitly
 * for the case where users do not specify a password, e.g., create db
 * we need the empty password "" string
 */
type OpHTTPSBase struct {
	useHTTPPassword bool
	httpsPassword   *string
	userName        string
}

// we may add some common functions for OpHTTPSBase here

func (opb *OpHTTPSBase) validateAndSetUsernameAndPassword(opName string, useHTTPPassword bool,
	userName string, httpsPassword *string) error {
	opb.useHTTPPassword = useHTTPPassword
	if opb.useHTTPPassword {
		err := util.ValidateUsernameAndPassword(opName, opb.useHTTPPassword, userName)
		if err != nil {
			return err
		}
		opb.userName = userName
		opb.httpsPassword = httpsPassword
	}

	return nil
}

// VClusterCommands is struct for all top-level admin commands (e.g. create db,
// add node, etc.). This is used to pass state around for the various APIs. We
// also use it for mocking in our unit test.
type VClusterCommands struct {
	Log vlog.Printer
}
