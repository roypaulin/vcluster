package vclusterops

import (
	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
)

/* Op and host http result status
 */

// ResultStatus is the data type for the status of
// ClusterOpResult and HostHTTPResult
type ResultStatus int

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

// ClusterOpResult is used to hold the ClusterOp's result
// at the steps of prepare, execute, and finalize
type ClusterOpResult struct {
	status ResultStatus
}

// HostHTTPResult is used to save result of an Adapter's sendRequest(...) function
// it is the element of the adapter pool's channel
type HostHTTPResult struct {
	status     ResultStatus
	statusCode int
	host       string
	content    string
	errMsg     string
}

func MakeClusterOpResultPass() ClusterOpResult {
	return ClusterOpResult{status: SUCCESS}
}

func MakeClusterOpResultFail() ClusterOpResult {
	return ClusterOpResult{status: FAILURE}
}

func MakeClusterOpResultException() ClusterOpResult {
	return ClusterOpResult{status: EXCEPTION}
}

func (clusterOpResult *ClusterOpResult) isPassing() bool {
	return clusterOpResult.status == SUCCESS
}

func (clusterOpResult *ClusterOpResult) isFailing() bool {
	return clusterOpResult.status == FAILURE
}

func (clusterOpResult *ClusterOpResult) isException() bool {
	return clusterOpResult.status == EXCEPTION
}

func (hostResult *HostHTTPResult) isPassing() bool {
	return hostResult.status == SUCCESS
}

func (hostResult *HostHTTPResult) isFailing() bool {
	return hostResult.status == FAILURE
}

func (hostResult *HostHTTPResult) isException() bool {
	return hostResult.status == EXCEPTION
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
	setupClusterHTTPRequest(hosts []string)
	Prepare(execContext *OpEngineExecContext) ClusterOpResult
	Execute(execContext *OpEngineExecContext) ClusterOpResult
	Finalize(execContext *OpEngineExecContext) ClusterOpResult
	processResult(execContext *OpEngineExecContext) ClusterOpResult
	logResponse(host string, result HostHTTPResult)
	logPrepare()
	logExecute()
	logFinalize()
}

/* Cluster ops basic fields and functions
 */

// OpBase defines base fields and implements basic functions
// for all ops
type OpBase struct {
	name               string
	hosts              []string
	clusterHTTPRequest ClusterHTTPRequest
}

type OpResponseMap map[string]string

func (op *OpBase) getName() string {
	return op.name
}

func (op *OpBase) parseAndCheckResponse(host, responseContent string, responseObj any) error {
	err := util.GetJSONLogErrors(responseContent, &responseObj, op.name)
	if err != nil {
		return err
	}
	vlog.LogInfo("[%s] JSON response from %s is %+v\n", op.name, host, responseObj)

	return nil
}

func (op *OpBase) parseAndCheckMapResponse(host, responseContent string) (OpResponseMap, error) {
	var responseObj OpResponseMap
	err := op.parseAndCheckResponse(host, responseContent, &responseObj)

	return responseObj, err
}

func (op *OpBase) setVersionToSemVar() {
	op.clusterHTTPRequest.SemVar = SemVer{Ver: "1.0.0"}
}

// TODO: implement another parse function for list response

func (op *OpBase) logResponse(host string, result HostHTTPResult) {
	vlog.LogPrintInfo("[%s] result from host %s summary %s, details: %+v",
		op.name, host, result.status.getStatusString(), result)
}

func (op *OpBase) logPrepare() {
	vlog.LogInfo("[%s] Prepare() called\n", op.name)
}

func (op *OpBase) logExecute() {
	vlog.LogInfo("[%s] Execute() called\n", op.name)
}

func (op *OpBase) logFinalize() {
	vlog.LogInfo("[%s] Finalize() called\n", op.name)
}

func (op *OpBase) execute(execContext *OpEngineExecContext) error {
	err := execContext.dispatcher.sendRequest(&op.clusterHTTPRequest)
	if err != nil {
		vlog.LogError("Fail to dispatch request %v", op.clusterHTTPRequest)
		return err
	}
	return nil
}

/* Sensitive fields in request body
 */
type SensitiveFields struct {
	DBPassword         string `json:"db_password"`
	AWSAccessKeyID     string `json:"aws_access_key_id"`
	AWSSecretAccessKey string `json:"aws_secret_access_key"`
}

func (maskedData *SensitiveFields) maskSensitiveInfo() {
	const maskedValue = "******"

	maskedData.DBPassword = maskedValue
	maskedData.AWSAccessKeyID = maskedValue
	maskedData.AWSSecretAccessKey = maskedValue
}

/* Cluster HTTPS ops basic fields
 * which are needed for https requests using password auth
 * specify whether to use password auth explicitly
 * for the case where users do not specify a password, e.g., create db
 * we need the empty password "" string
 */
type OpHTTPBase struct {
	useHTTPPassword bool
	httpsPassword   *string
	userName        string
}

// we may add some common functions for OpHTTPBase here
