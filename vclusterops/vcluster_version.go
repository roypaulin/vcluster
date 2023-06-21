package vclusterops

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

type SemVer struct {
	Ver   string `json:"ver"`
	Major string `json:"-"`
	Minor string `json:"-"`
	Patch string `json:"-"`
}

type VclusterOpVersion struct {
	Origin string `json:"origin"`
	SemVer SemVer
}

func (semVer *SemVer) parseComponentsIfNecessary() error {
	cleanSize := strings.TrimSpace(semVer.Ver)
	r := regexp.MustCompile(`^(\d+)\.(\d+).(\d+)$`)
	matches := r.FindAllStringSubmatch(cleanSize, -1)
	if len(matches) != 1 {
		return fmt.Errorf("parse error for version %s: It is not a valid version", semVer.Ver)
	}
	semVer.Major = matches[0][1]
	semVer.Minor = matches[0][2]
	semVer.Patch = matches[0][3]
	return nil
}

func (semVer *SemVer) incompatibleVersion(otherVer *SemVer) (bool, error) {
	err := semVer.parseComponentsIfNecessary()
	if err != nil {
		return false, err
	}
	majorStr := semVer.Major
	err = otherVer.parseComponentsIfNecessary()
	if err != nil {
		return false, err
	}
	majorOtherVerStr := otherVer.Major
	return majorStr == majorOtherVerStr, nil
}

func (semVer *SemVer) equalVersion(otherVer *SemVer) bool {
	return otherVer.Ver == semVer.Ver
}

func (opVersion *VclusterOpVersion) equalVclusterVersion(otherVer *VclusterOpVersion) bool {
	return opVersion.Origin == otherVer.Origin && opVersion.SemVer.equalVersion(&otherVer.SemVer)
}

func (opVersion *VclusterOpVersion) convertVclusterVersionToJSON() (string, error) {
	SemVer := &SemVer{Ver: opVersion.SemVer.Ver}
	vclusterVersionData := map[string]any{
		"origin": opVersion.Origin,
		"semver": SemVer,
	}
	jsonFile, err := json.Marshal(vclusterVersionData)
	if err != nil {
		return "", fmt.Errorf("could not marshal json: %w", err)
	}
	return string(jsonFile), nil
}

func vclusterVersionFromDict(vclusterVersionDict map[string]string) (VclusterOpVersion, error) {
	requiredKeys := []string{"origin", "semver"}
	for _, key := range requiredKeys {
		if _, ok := vclusterVersionDict[key]; !ok {
			return VclusterOpVersion{}, fmt.Errorf("%s is missing one or more required fields", vclusterVersionDict)
		}
	}
	return VclusterOpVersion{Origin: vclusterVersionDict["origin"], SemVer: SemVer{Ver: vclusterVersionDict["semver"]}}, nil
}
