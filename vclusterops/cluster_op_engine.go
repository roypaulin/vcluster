package vclusterops

import (
	"fmt"

	"vertica.com/vcluster/vclusterops/vlog"
)

type VClusterOpEngine struct {
	instructions []ClusterOp
}

func MakeClusterOpEngine(instructions []ClusterOp) VClusterOpEngine {
	newClusterOpEngine := VClusterOpEngine{}
	newClusterOpEngine.instructions = instructions
	return newClusterOpEngine
}

func (opEngine *VClusterOpEngine) Run() error {
	var statusCode = SUCCESS

	execContext := MakeOpEngineExecContext()

	for _, op := range opEngine.instructions {
		op.logPrepare()
		prepareResult := op.Prepare(&execContext)

		// execute an instruction if prepare succeed
		if prepareResult.isPassing() {
			op.logExecute()
			executeResult := op.Execute(&execContext)
			statusCode = executeResult.status
			if executeResult.isFailing() {
				vlog.LogPrintInfo("Execute %s failed, details: %+v\n", op.getName(), executeResult)
			}
			if executeResult.isException() {
				vlog.LogPrintInfo("An exception happened during executing %s, details: %+v\n", op.getName(), executeResult)
			}
		} else if prepareResult.isFailing() {
			vlog.LogPrintInfo("Prepare %s failed, details: %+v\n", op.getName(), prepareResult)
		} else if prepareResult.isException() {
			vlog.LogPrintInfo("Prepare %s got exception, details: %+v\n", op.getName(), prepareResult)
		}

		op.logFinalize()
		op.Finalize(&execContext)
		if statusCode != SUCCESS {
			return fmt.Errorf("status code %d (%s)", statusCode, statusCode.getStatusString())
		}
	}

	return nil
}
