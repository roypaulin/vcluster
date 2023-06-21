package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseConfigParams(t *testing.T) {
	var c CmdCreateDB

	// positive cases
	emptyStr := ""
	c.configParamListStr = &emptyStr
	err := c.parseConfigParams()
	assert.Nil(t, err)

	// negative case
	emptyStr2 := "       "
	c.configParamListStr = &emptyStr2
	err = c.parseConfigParams()
	assert.NotNil(t, err)
}
