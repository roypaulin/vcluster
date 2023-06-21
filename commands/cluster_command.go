package commands

const (
	EonOnlyOption = "[Eon only] "
)

type ClusterCommand interface {
	CommandType() string
	Parse(argv []string) error

	/* TODO: Analyze information about the state of
	 * the cluster. The information could be
	 * cached in a config file or constructed through
	 * cluster discovery.
	 */
	Analyze() error
	Run() error
	PrintUsage()
}
