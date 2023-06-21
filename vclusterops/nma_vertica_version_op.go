package vclusterops

import (
	"errors"
	"fmt"

	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
)

const NoVersion = "NO_VERSION"

type NMAVerticaVersionOp struct {
	OpBase
	RequireSameVersion bool
	HostVersionMap     map[string]string
}

func MakeNMAVerticaVersionOp(name string, hosts []string, sameVersion bool) NMAVerticaVersionOp {
	nmaVerticaVersionOp := NMAVerticaVersionOp{}
	nmaVerticaVersionOp.name = name
	nmaVerticaVersionOp.hosts = hosts
	nmaVerticaVersionOp.RequireSameVersion = sameVersion
	nmaVerticaVersionOp.HostVersionMap = map[string]string{}
	return nmaVerticaVersionOp
}

func (op *NMAVerticaVersionOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildNMAEndpoint("vertica/version")
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *NMAVerticaVersionOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *NMAVerticaVersionOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *NMAVerticaVersionOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

type NMAVerticaVersionOpResponse map[string]string

func (op *NMAVerticaVersionOp) parseAndCheckResponse(host, resultContent string) error {
	// each result is a pair {"vertica_version": <vertica version string>}
	// example result:
	// {"vertica_version": "Vertica Analytic Database v12.0.3"}
	var responseObj NMAVerticaVersionOpResponse
	err := util.GetJSONLogErrors(resultContent, &responseObj, op.name)
	if err != nil {
		return err
	}

	version, ok := responseObj["vertica_version"]
	// missing key "vertica_version"
	if !ok {
		return errors.New("Unable to get vertica version from host " + host)
	}

	vlog.LogInfo("[%s] JSON response from %s is %v\n", op.name, host, responseObj)
	op.HostVersionMap[host] = version
	return nil
}

func (op *NMAVerticaVersionOp) logResponseCollectVersions() error {
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		if !result.isPassing() {
			errStr := fmt.Sprintf("[%s] result from host %s summary %s, details: %+v\n",
				op.name, host, FailureResult, result)
			return errors.New(errStr)
		}

		err := op.parseAndCheckResponse(host, result.content)
		if err != nil {
			vlog.LogInfo("[%s] result from host %s summary %s, details: %+v, parsing failure details: %s\n",
				op.name, host, FailureResult, result, err.Error())
			return err
		}

		vlog.LogPrintInfo("[%s] result from host %s summary %s, details: %+v",
			op.name, host, SuccessResult, result)
	}
	return nil
}

func (op *NMAVerticaVersionOp) logCheckVersionMatch() ClusterOpResult {
	versionStr := NoVersion
	for host, version := range op.HostVersionMap {
		vlog.LogInfo("[%s] Host {%s}: version {s%}", op.name, host, version)
		if version == "" {
			vlog.LogError("[%s] No Version version collected for host: [%s]", op.name, host)
			return MakeClusterOpResultFail()
		} else if versionStr == NoVersion {
			// first time seeing a valid version, set it as the versionStr
			versionStr = version
		} else if version != versionStr && op.RequireSameVersion {
			vlog.LogError("[%s] Found mismatched versions: [%s] and [%s]", op.name, versionStr, version)
			return MakeClusterOpResultFail()
		}
	}
	// no version collected at all
	if versionStr == NoVersion {
		vlog.LogError("[s%] No Version version collected for all hosts", op.name)
		return MakeClusterOpResultFail()
	}
	return MakeClusterOpResultPass()
}

func (op *NMAVerticaVersionOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	err := op.logResponseCollectVersions()
	if err != nil {
		vlog.LogError(err.Error())
		return MakeClusterOpResultFail()
	}
	return op.logCheckVersionMatch()
}
