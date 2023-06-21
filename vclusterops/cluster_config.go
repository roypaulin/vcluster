package vclusterops

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
)

const (
	ConfigDirPerm  = 0755
	ConfigFilePerm = 0600
)

const ConfigFileName = "vertica_cluster.yaml"
const ConfigBackupName = "vertica_cluster.yaml.backup"

type ClusterConfig struct {
	DBName string `yaml:"db_name"`
	Hosts  []string
	Nodes  []NodeConfig
	IsEon  bool `yaml:"eon_mode"`
}

type NodeConfig struct {
	Name    string `yaml:"name"`
	Address string `yaml:"address"`
}

func MakeClusterConfig() ClusterConfig {
	return ClusterConfig{}
}

// write config information to the YAML file
func (clusterConfig *ClusterConfig) WriteConfig(configFilePath string) error {
	configBytes, err := yaml.Marshal(&clusterConfig)
	if err != nil {
		return fmt.Errorf("fail to marshal config data, details: %w", err)
	}
	err = os.WriteFile(configFilePath, configBytes, ConfigFilePerm)
	if err != nil {
		return fmt.Errorf("fail to write config file, details: %w", err)
	}

	return nil
}

func GetConfigFilePath(dbName string, inputConfigDir *string) (string, error) {
	var configParentPath string

	// if the input config directory is given and has write permission,
	// write the YAML config file under this directory
	if inputConfigDir != nil {
		if err := os.MkdirAll(*inputConfigDir, ConfigDirPerm); err != nil {
			return "", fmt.Errorf("fail to create config path at %s, detail: %w", *inputConfigDir, err)
		}

		return filepath.Join(*inputConfigDir, ConfigFileName), nil
	}

	// otherwise write it under the user home directory
	// as <current_dir or home_dir>/<db_name>/vertica_cluster.yaml
	currentDir, err := os.Getwd()
	if err != nil {
		vlog.LogWarning("Fail to get current directory\n")
		configParentPath = currentDir
	}

	// create a directory with the database name
	// then write the config content inside this directory
	configDirPath := filepath.Join(configParentPath, dbName)
	if err := os.MkdirAll(configDirPath, ConfigDirPerm); err != nil {
		return "", fmt.Errorf("fail to create config path at %s, detail: %w", configDirPath, err)
	}

	configFilePath := filepath.Join(configDirPath, ConfigFileName)
	return configFilePath, nil
}

func BackupConfigFile(configFilePath string) error {
	if util.CanReadAccessDir(configFilePath) == nil {
		// copy file to vertica_cluster.yaml.backup
		configDirPath := filepath.Dir(configFilePath)
		configFileBackup := filepath.Join(configDirPath, ConfigBackupName)
		vlog.LogInfo("Config file exists at %s, creating a backup at %s",
			configFilePath, configFileBackup)
		err := util.CopyFile(configFilePath, configFileBackup, ConfigFilePerm)
		if err != nil {
			return err
		}
	}

	return nil
}
