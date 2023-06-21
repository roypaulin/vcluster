package vclusterops

import (
	"fmt"
)

type NMADownloadConfigOp struct {
	OpBase
	catalogPathMap map[string]string
	endpoint       string
	fileContent    *string
}

func MakeNMADownloadConfigOp(
	name string,
	vdb *VCoordinationDatabase,
	bootstrapHosts []string,
	endpoint string,
	fileContent *string,
) NMADownloadConfigOp {
	nmaDownloadConfigOp := NMADownloadConfigOp{}
	nmaDownloadConfigOp.name = name
	nmaDownloadConfigOp.hosts = bootstrapHosts
	nmaDownloadConfigOp.endpoint = endpoint
	nmaDownloadConfigOp.fileContent = fileContent

	nmaDownloadConfigOp.catalogPathMap = make(map[string]string)
	for _, host := range bootstrapHosts {
		vnode, ok := vdb.HostNodeMap[host]
		if !ok {
			msg := fmt.Errorf("[%s] fail to get catalog path from host %s", name, host)
			panic(msg)
		}
		nmaDownloadConfigOp.catalogPathMap[host] = vnode.CatalogPath
	}

	return nmaDownloadConfigOp
}

func (op *NMADownloadConfigOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildNMAEndpoint(op.endpoint)

		catalogPath, ok := op.catalogPathMap[host]
		if !ok {
			msg := fmt.Errorf("[%s] fail to get catalog path from host %s", op.name, host)
			panic(msg)
		}
		httpRequest.QueryParams = map[string]string{"catalog_path": catalogPath}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *NMADownloadConfigOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *NMADownloadConfigOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *NMADownloadConfigOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

func (op *NMADownloadConfigOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			// The content of config file will be stored as content of the response
			*op.fileContent = result.content
			return MakeClusterOpResultPass()
		}
	}

	return MakeClusterOpResultFail()
}
