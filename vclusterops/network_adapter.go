package vclusterops

import "net/http"

type Adapter interface {
	sendRequest(*HostHTTPRequest, chan<- HostHTTPResult)
	processResult(*http.Response, chan<- HostHTTPResult)
}
