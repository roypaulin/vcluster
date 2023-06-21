package vclusterops

import (
	"sort"
	"strconv"
	"time"

	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
)

type HTTPSPollNodeStateOp struct {
	OpBase
	OpHTTPBase
	allHosts   map[string]interface{}
	upHosts    map[string]interface{}
	notUpHosts []string
}

func MakeHTTPSPollNodeStateOp(name string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string) HTTPSPollNodeStateOp {
	httpsPollNodeStateOp := HTTPSPollNodeStateOp{}
	httpsPollNodeStateOp.name = name
	httpsPollNodeStateOp.hosts = hosts
	httpsPollNodeStateOp.useHTTPPassword = useHTTPPassword

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	httpsPollNodeStateOp.userName = userName
	httpsPollNodeStateOp.httpsPassword = httpsPassword

	httpsPollNodeStateOp.upHosts = make(map[string]interface{})
	httpsPollNodeStateOp.allHosts = make(map[string]interface{})
	for _, h := range hosts {
		httpsPollNodeStateOp.allHosts[h] = struct{}{}
	}

	return httpsPollNodeStateOp
}

func (op *HTTPSPollNodeStateOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPSPollNodeStateOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPSPollNodeStateOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPSPollNodeStateOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

func (op *HTTPSPollNodeStateOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	startTime := time.Now()
	timeoutSecondStr := util.GetEnv("NODE_STATE_POLLING_TIMEOUT", strconv.Itoa(StartupPollingTimeout))
	timeoutSecond, err := strconv.Atoi(timeoutSecondStr)
	if err != nil {
		vlog.LogPrintError("invalid timeout value %s", timeoutSecondStr)
		return MakeClusterOpResultFail()
	}

	duration := time.Duration(timeoutSecond) * time.Second
	count := 0
	for endTime := startTime.Add(duration); ; {
		if time.Now().After(endTime) {
			break
		}

		if count > 0 {
			time.Sleep(PollingInterval * time.Second)
		}

		shouldStopPoll, err := op.shouldStopPolling()
		if err != nil {
			return MakeClusterOpResultException()
		}

		if shouldStopPoll {
			return MakeClusterOpResultPass()
		}

		if err := op.execute(execContext); err != nil {
			return MakeClusterOpResultException()
		}

		count++
	}

	// show the hosts that are not UP
	sort.Strings(op.notUpHosts)
	vlog.LogPrintError("The following hosts are not up after %d seconds: %v",
		timeoutSecond, op.notUpHosts)

	return MakeClusterOpResultFail()
}

// the following structs only hosts necessary information for this op
type NodeInfo struct {
	Address string `json:"address"`
	State   string `json:"state"`
}

type NodesInfo struct {
	NodeList []NodeInfo `json:"node_list"`
}

func (op *HTTPSPollNodeStateOp) shouldStopPolling() (bool, error) {
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// parse the /nodes endpoint response
			nodesInfo := NodesInfo{}
			err := op.parseAndCheckResponse(host, result.content, &nodesInfo)
			if err != nil {
				vlog.LogPrintError("[%s] fail to parse result on host %s, details: %w",
					op.name, host, err)
				return false, err
			}

			// check whether all nodes are up
			for _, n := range nodesInfo.NodeList {
				if n.State == "UP" {
					op.upHosts[n.Address] = struct{}{}
				}
			}

			// the HTTPS /nodes endpoint will return the states of all nodes
			// we only need to read info from one responding node
			break
		}
	}

	op.notUpHosts = util.MapKeyDiff(op.allHosts, op.upHosts)
	if len(op.notUpHosts) == 0 {
		vlog.LogPrintInfoln("All nodes are up")
		return true, nil
	}

	return false, nil
}
