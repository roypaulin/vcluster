package util

// this file defines basic default values
const (
	DefaultClientPort           = 5433
	DefaultHTTPPortOffset       = 3010
	DefaultHTTPPort             = DefaultClientPort + DefaultHTTPPortOffset
	DefaultControlAddressFamily = "ipv4"
	IPv6ControlAddressFamily    = "ipv6"
	DefaultRestartPolicy        = "ksafe"
	DefaultDBDir                = "/opt/vertica"
	DefaultShareDir             = DefaultDBDir + "/share"
	DefaultLicenseKey           = DefaultShareDir + "/license.key"
	DefaultConfigDir            = DefaultDBDir + "/config"
	DefaultRetryCount           = 3
	DefaultTimeoutSeconds       = 300
	DefaultLargeCluster         = -1
	MaxLargeCluster             = 120
	MinDepotSize                = 0
	MaxDepotSize                = 100
)

var RestartPolicyList = []string{"never", DefaultRestartPolicy, "always"}
