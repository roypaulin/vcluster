package vclusterops

import (
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type VFetchNodeStateOptions struct {
	DatabaseOptions
}

func VFetchNodeStateOptionsFactory() VFetchNodeStateOptions {
	opt := VFetchNodeStateOptions{}
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (options *VFetchNodeStateOptions) validateParseOptions() error {
	if len(options.RawHosts) == 0 {
		return fmt.Errorf("must specify a host or host list")
	}

	if options.Password == nil {
		vlog.LogPrintInfoln("no password specified, using none")
	}

	return nil
}

func (options *VFetchNodeStateOptions) analyzeOptions() error {
	hostAddresses, err := util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

	options.Hosts = hostAddresses
	return nil
}

func (options *VFetchNodeStateOptions) ValidateAnalyzeOptions() error {
	if err := options.validateParseOptions(); err != nil {
		return err
	}
	if err := options.analyzeOptions(); err != nil {
		return err
	}
	return nil
}

// VFetchNodeState fetches node states (e.g., up or down) in the cluster
func VFetchNodeState(options *VFetchNodeStateOptions) ([]NodeInfo, error) {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err := options.ValidateAnalyzeOptions()
	if err != nil {
		return nil, err
	}

	// TODO: we need to support reading hosts from config for Go client

	// produce list_allnodes instructions
	instructions, err := produceListAllNodesInstructions(options)
	if err != nil {
		vlog.LogPrintError("fail to produce instructions, %w", err)
		return nil, err
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run()
	nodeStates := clusterOpEngine.execContext.nodeStates

	return nodeStates, runError
}

func produceListAllNodesInstructions(options *VFetchNodeStateOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	// get hosts
	hosts := options.Hosts

	// validate user name
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.ValidateUserName()
		if err != nil {
			return instructions, err
		}
	}

	httpsCheckNodeStateOp := MakeHTTPCheckNodeStateOp("HTTPCheckNodeStateOp", hosts,
		usePassword, *options.UserName, options.Password)

	instructions = append(instructions, &httpsCheckNodeStateOp)

	return instructions, nil
}