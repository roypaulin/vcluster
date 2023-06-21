package commands

import (
	"flag"
	"fmt"
	"os"

	"vertica.com/vcluster/vclusterops/vlog"
)

/* CmdHelp
 *
 * A command providing top-level help on
 * various topics. PrintUsage() will print
 * the requested help.
 *
 * Implements ClusterCommand interface
 */
type CmdHelp struct {
	argv   []string
	parser *flag.FlagSet
	topic  *string
}

func MakeCmdHelp() CmdHelp {
	newCmd := CmdHelp{}
	newCmd.parser = flag.NewFlagSet("help", flag.ExitOnError)
	newCmd.topic = newCmd.parser.String("topic", "", "The topic for more help")
	return newCmd
}

func (c CmdHelp) CommandType() string {
	return "help"
}

func (c *CmdHelp) Parse(inputArgv []string) error {
	vlog.LogArgParse(&inputArgv)

	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv

	parserError := c.parser.Parse(c.argv)
	if parserError != nil {
		return parserError
	}

	return c.validateParse()
}

func (c *CmdHelp) validateParse() error {
	vlog.LogInfoln("Called validateParse()")
	return nil
}

func (c *CmdHelp) Analyze() error {
	return nil
}

func (c *CmdHelp) Run() error {
	return nil
}

func (c *CmdHelp) PrintUsage() {
	fmt.Fprintf(os.Stderr,
		"vcluster %s\nExample: vcluster %s --topic create_db\n",
		c.CommandType(),
		c.CommandType())
	c.parser.PrintDefaults()
}
