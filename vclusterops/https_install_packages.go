package vclusterops

import (
	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
)

type HTTPSInstallPackagesOp struct {
	OpBase
	OpHTTPBase
}

func MakeHTTPSInstallPackagesOp(name string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string) HTTPSInstallPackagesOp {
	installPackagesOp := HTTPSInstallPackagesOp{}
	installPackagesOp.name = name
	installPackagesOp.hosts = hosts

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	installPackagesOp.useHTTPPassword = useHTTPPassword
	installPackagesOp.userName = userName
	installPackagesOp.httpsPassword = httpsPassword
	return installPackagesOp
}

func (op *HTTPSInstallPackagesOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("packages")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPSInstallPackagesOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPSInstallPackagesOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPSInstallPackagesOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

/*
	HTTPSInstallPackagesResponse example:

{'packages': [

	             {
	               'package_name': 'ComplexTypes',
	               'install_status': 'skipped'
	             },
	             {
	               'package_name': 'DelimitedExport',
	               'install_status': 'skipped'
	             },
	           ...
	           ]
	}
*/
type HTTPSInstallPackagesResponse map[string][]map[string]string

func (op *HTTPSInstallPackagesOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			success = false
			continue
		}

		var responseObj HTTPSInstallPackagesResponse
		err := op.parseAndCheckResponse(host, result.content, &responseObj)

		if err != nil {
			success = false
			continue
		}

		installedPackages, ok := responseObj["packages"]
		if !ok {
			vlog.LogError(`[%s] response does not contain field "packages"`, op.name)
			success = false
		}
		vlog.LogPrintInfo("[%s] installed packages: %v", op.name, installedPackages)
	}
	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}
