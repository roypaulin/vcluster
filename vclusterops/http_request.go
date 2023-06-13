/*
 (c) Copyright [2023] Open Text.
 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

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