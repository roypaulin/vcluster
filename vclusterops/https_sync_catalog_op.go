package vclusterops

import (
	"strconv"

	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
)

type HTTPSSyncCatalogOp struct {
	OpBase
	OpHTTPBase
}

func MakeHTTPSSyncCatalogOp(name string, hosts []string, useHTTPPassword bool,
	userName string, httpsPassword *string) HTTPSSyncCatalogOp {
	httpsSyncCatalogOp := HTTPSSyncCatalogOp{}
	httpsSyncCatalogOp.name = name
	httpsSyncCatalogOp.hosts = hosts
	httpsSyncCatalogOp.useHTTPPassword = useHTTPPassword

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	httpsSyncCatalogOp.userName = userName
	httpsSyncCatalogOp.httpsPassword = httpsPassword
	return httpsSyncCatalogOp
}

func (op *HTTPSSyncCatalogOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("cluster/catalog/sync")
		httpRequest.QueryParams = make(map[string]string)
		httpRequest.QueryParams["retry-count"] = strconv.Itoa(util.DefaultRetryCount)
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPSSyncCatalogOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPSSyncCatalogOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPSSyncCatalogOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// decode the json-format response
			// The response object will be a dictionary, an example:
			// {"new_truncation_version": "18"}
			syncCatalogRsp, err := op.parseAndCheckMapResponse(host, result.content)
			if err != nil {
				success = false
				continue
			}
			version, ok := syncCatalogRsp["new_truncation_version"]
			if !ok {
				vlog.LogError(`[%s] response does not contain field "new_truncation_version"`, op.name)
				success = false
			}
			vlog.LogPrintInfo(`[%s] the_latest_truncation_catalog_version: %s"`, op.name, version)
		} else {
			success = false
		}
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}

func (op *HTTPSSyncCatalogOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}
