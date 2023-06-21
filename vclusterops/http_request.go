package vclusterops

type HostHTTPRequest struct {
	Method       string
	Endpoint     string
	IsNMACommand bool
	QueryParams  map[string]string
	RequestData  string // the data must be a JSON-encoded string
	Username     string // optional, for HTTPS endpoints only
	// string pointer is used here as we need to check whether the password has been set
	Password *string // optional, for HTTPS endpoints only
	Timeout  int     // optional, set it if an Op needs longer time to complete
}

func (req *HostHTTPRequest) BuildNMAEndpoint(url string) {
	req.IsNMACommand = true
	req.Endpoint = NMACurVersion + url
}

func (req *HostHTTPRequest) BuildHTTPSEndpoint(url string) {
	req.IsNMACommand = false
	req.Endpoint = HTTPCurVersion + url
}

// this is used as the "ATModuleBase" in Admintools
type ClusterHTTPRequest struct {
	RequestCollection map[string]HostHTTPRequest
	ResultCollection  map[string]HostHTTPResult
	SemVar            SemVer
}
