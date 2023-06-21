package vclusterops

type OpEngineExecContext struct {
	dispatcher      HTTPRequestDispatcher
	networkProfiles map[string]NetworkProfile
	nmaVDatabase    NmaVDatabase
}

func MakeOpEngineExecContext() OpEngineExecContext {
	newOpEngineExecContext := OpEngineExecContext{}
	newOpEngineExecContext.dispatcher = MakeHTTPRequestDispatcher()

	return newOpEngineExecContext
}
