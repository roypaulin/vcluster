package vclusterops

import (
	"fmt"

	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
)

type HTTPCreateNodeOp struct {
	OpBase
	OpHTTPBase
	RequestParams map[string]string
}

func MakeHTTPCreateNodeOp(name string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	vdb *VCoordinationDatabase) HTTPCreateNodeOp {
	createNodeOp := HTTPCreateNodeOp{}
	createNodeOp.name = name
	createNodeOp.hosts = hosts
	createNodeOp.RequestParams = make(map[string]string)
	// HTTPS create node endpoint requires passing everything before node name
	createNodeOp.RequestParams["catalog-prefix"] = vdb.CatalogPrefix + "/" + vdb.Name
	createNodeOp.RequestParams["data-prefix"] = vdb.DataPrefix + "/" + vdb.Name
	// need to create new nodes for every node except for the nodes
	// that we run the https endpoint call on
	// e.g., for create db case, newNodesHost will be [all hosts - bootstrap host]
	newNodeHosts := util.SliceDiff(vdb.HostList, hosts)
	createNodeOp.RequestParams["hosts"] = util.ArrayToString(newNodeHosts, ",")
	createNodeOp.useHTTPPassword = useHTTPPassword

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)

	createNodeOp.userName = userName
	createNodeOp.httpsPassword = httpsPassword
	return createNodeOp
}

func (op *HTTPCreateNodeOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		// note that this will be updated in Prepare()
		// because the endpoint only accept parameters in query
		httpRequest.BuildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.QueryParams = op.RequestParams
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPCreateNodeOp) updateQueryParams(execContext *OpEngineExecContext) {
	for _, host := range op.hosts {
		profile, ok := execContext.networkProfiles[host]
		if !ok {
			msg := fmt.Sprintf("[%s] unable to find network profile for host %s", op.name, host)
			panic(msg)
		}
		op.RequestParams["broadcast"] = profile.Broadcast
	}
}

func (op *HTTPCreateNodeOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	op.updateQueryParams(execContext)
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPCreateNodeOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPCreateNodeOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

type HTTPCreateNodeResponse map[string][]map[string]string

func (op *HTTPCreateNodeOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// The response object will be a dictionary, an example:
			// {'created_nodes': [{'name': 'v_running_db_node0002', 'catalog_path': '/data/v_running_db_node0002_catalog'},
			//                    {'name': 'v_running_db_node0003', 'catalog_path': '/data/v_running_db_node0003_catalog'}]}
			var responseObj HTTPCreateNodeResponse
			err := op.parseAndCheckResponse(host, result.content, &responseObj)

			if err != nil {
				success = false
				continue
			}
			_, ok := responseObj["created_nodes"]
			if !ok {
				vlog.LogError(`[%s] response does not contain field "created_nodes"`, op.name)
				success = false
			}
		} else {
			success = false
		}
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}
