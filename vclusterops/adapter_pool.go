package vclusterops

import (
	"fmt"
	"sync"

	"vertica.com/vcluster/vclusterops/vlog"
)

type AdapterPool struct {
	// map from host to HTTPAdapter
	connections map[string]Adapter
}

var (
	poolInstance AdapterPool
	once         sync.Once
)

// return a singleton instance of the AdapterPool
func getPoolInstance() AdapterPool {
	/* if once.Do(f) is called multiple times,
	 * only the first call will invoke f,
	 * even if f has a different value in each invocation.
	 * Reference: https://pkg.go.dev/sync#Once
	 */
	once.Do(func() {
		poolInstance = makeAdapterPool()
	})

	return poolInstance
}

func makeAdapterPool() AdapterPool {
	newAdapterPool := AdapterPool{}
	newAdapterPool.connections = make(map[string]Adapter)
	return newAdapterPool
}

type adapterToRequest struct {
	adapter Adapter
	request HostHTTPRequest
}

func (pool *AdapterPool) sendRequest(clusterHTTPRequest *ClusterHTTPRequest) error {
	vlog.LogInfoln("Adapter pool's sendRequest is called")
	// build a collection of adapter to request
	// we need this step as a host may not be in the pool
	// in that case, we should not proceed
	var adapterToRequestCollection []adapterToRequest
	for host, request := range clusterHTTPRequest.RequestCollection {
		adapter, ok := pool.connections[host]
		if !ok {
			return fmt.Errorf("host %s is not found in the adapter pool", host)
		}
		ar := adapterToRequest{adapter: adapter, request: request}
		adapterToRequestCollection = append(adapterToRequestCollection, ar)
	}

	hostCount := len(adapterToRequestCollection)

	// result channel to collect result from each host
	resultChannel := make(chan HostHTTPResult, hostCount)

	for _, ar := range adapterToRequestCollection {
		// send request to the hosts
		// each goroutine will handle one request for one host
		request := ar.request
		go ar.adapter.sendRequest(&request, resultChannel)
	}

	// TODO: the status check code comes here

	// handle results
	// we expect to receive the same number of results from the channel as the number of hosts
	// before proceeding to the next steps
	clusterHTTPRequest.ResultCollection = make(map[string]HostHTTPResult)
	for i := 0; i < hostCount; i++ {
		result, ok := <-resultChannel
		if ok {
			clusterHTTPRequest.ResultCollection[result.host] = result
		}
	}
	close(resultChannel)

	return nil
}
