package commands

import (
	"bytes"
	"log"
	"os"
	"testing"

	"vertica.com/vcluster/vclusterops"

	"github.com/stretchr/testify/assert"
)

func TestInitCmd(t *testing.T) {
	// no hosts provided, the case should fail
	c := MakeCmdInit()
	err := c.Parse([]string{})
	assert.ErrorContains(t, err, "must provide the host list with --hosts")

	// hosts provided, the case should pass
	err = c.Parse([]string{"--hosts", "vnode1,vnode2,vnode3"})
	assert.Nil(t, err)

	// no directory provided, current directory will be used
	currentDir, _ := os.Getwd()
	assert.Equal(t, currentDir, *c.directory)

	// directory provided, the given directory will be used
	const configDir = "/opt/vertica/config"
	c = MakeCmdInit()
	err = c.Parse([]string{
		"--hosts", "vnode1,vnode2,vnode3",
		"--directory", configDir})
	assert.Nil(t, err)
	assert.Equal(t, "/opt/vertica/config", *c.directory)
}

func TestConfigCmd(t *testing.T) {
	// redirect log to a local bytes.Buffer
	var logStr bytes.Buffer
	log.SetOutput(&logStr)

	// create a stub YAML file
	const yamlPath = "vertica_cluster.yaml"
	const yamlStr = "hosts\n  - vnode1\n  - vnode2\n  - vnode3"
	_ = os.WriteFile(yamlPath, []byte(yamlStr), vclusterops.ConfigFilePerm)
	defer os.Remove(yamlPath)

	// if `--show` is not specified, the config content should not show
	c := MakeCmdConfig()
	err := c.Parse([]string{})
	assert.Nil(t, err)

	err = c.Run()
	assert.Nil(t, err)
	assert.NotContains(t, logStr.String(), yamlStr)

	// if `--show` is specified, the config content should show
	c = MakeCmdConfig()
	err = c.Parse([]string{"--show"})
	assert.Nil(t, err)

	err = c.Run()
	assert.Nil(t, err)
	assert.Contains(t, logStr.String(), yamlStr)

	// now run `init`, the command should fail
	// because the config file under the current directory already exists
	cmdInit := MakeCmdInit()
	err = cmdInit.Parse([]string{"--hosts", "vnode1,vnode2,vnode3"})
	assert.Nil(t, err)

	err = cmdInit.Run()
	assert.ErrorContains(t, err, "vertica_cluster.yaml already exists")
}
