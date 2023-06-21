package vclusterops

import (
	"vertica.com/vcluster/vclusterops/vlog"
)

type HTTPRequestDispatcher struct {
	pool AdapterPool
}

func MakeHTTPRequestDispatcher() HTTPRequestDispatcher {
	newHTTPRequestDispatcher := HTTPRequestDispatcher{}

	return newHTTPRequestDispatcher
}

// set up the pool connection for each host
func (dispatcher *HTTPRequestDispatcher) Setup(hosts []string) {
	dispatcher.pool = getPoolInstance()

	for _, host := range hosts {
		adapter := MakeHTTPAdapter()
		adapter.host = host
		dispatcher.pool.connections[host] = &adapter
	}
}

func (dispatcher *HTTPRequestDispatcher) sendRequest(clusterHTTPRequest *ClusterHTTPRequest) error {
	vlog.LogInfoln("HTTP request dispatcher's sendRequest is called")
	return dispatcher.pool.sendRequest(clusterHTTPRequest)
}
