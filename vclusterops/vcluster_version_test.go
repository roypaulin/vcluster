package vclusterops

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestForVersionEquality(t *testing.T) {
	v1 := &SemVer{Ver: "1.5.3"}
	_ = v1.parseComponentsIfNecessary()
	assert.Equal(t, v1.Major, "1")
	assert.Equal(t, v1.Minor, "5")
	assert.Equal(t, v1.Patch, "3")
}

func TestForIncompatibleVersion(t *testing.T) {
	v1 := &SemVer{Ver: "2.1.0"}
	v2 := &SemVer{Ver: "1.9.0"}
	result, err := v1.incompatibleVersion(v2)
	assert.NoError(t, err)
	assert.Equal(t, result, false)
}

func TestForDifferentButCompatibleVersion(t *testing.T) {
	v1 := &SemVer{Ver: "1.5.4"}
	v2 := &SemVer{Ver: "1.5.3"}
	result, err := v1.incompatibleVersion(v2)
	assert.NoError(t, err)
	assert.Equal(t, result, true)
}

func TestForEqualVersion(t *testing.T) {
	v1 := &SemVer{Ver: "1.5.3"}
	v2 := &SemVer{Ver: "1.5.3"}
	result := v1.equalVersion(v2)
	assert.Equal(t, result, true)
}

func TestForVclusterVersionEquality(t *testing.T) {
	v1 := &VclusterOpVersion{Origin: "root", SemVer: SemVer{Ver: "1.0.0"}}
	assert.NotEqual(t, v1, "blah")
	v2 := &VclusterOpVersion{Origin: "root", SemVer: SemVer{Ver: "1.0.1"}}
	result := v1.equalVclusterVersion(v2)
	assert.Equal(t, result, false)
	v3 := &VclusterOpVersion{Origin: "root", SemVer: SemVer{Ver: "1.0.0"}}
	result = v3.equalVclusterVersion(v1)
	assert.Equal(t, result, true)
}
func TestForVclusterVersionJSON(t *testing.T) {
	v1 := &VclusterOpVersion{Origin: "root", SemVer: SemVer{Ver: "1.0.0"}}
	result, err := v1.convertVclusterVersionToJSON()
	assert.NoError(t, err)
	data := VclusterOpVersion{}
	_ = json.Unmarshal([]byte(result), &data)
	assert.Equal(t, data.Origin, "root")
	assert.Equal(t, data.SemVer, SemVer{Ver: "1.0.0"})
}

func TestForConvertVclusterVersionToJSONString(t *testing.T) {
	v1 := &VclusterOpVersion{Origin: "root", SemVer: SemVer{Ver: "1.0.0"}}
	result, err := v1.convertVclusterVersionToJSON()
	assert.NoError(t, err)
	assert.Equal(t, result, "{\"origin\":\"root\",\"semver\":{\"ver\":\"1.0.0\"}}")
}

func TestForVclusterVersionDict(t *testing.T) {
	VclusterVersionDict := map[string]string{"origin": "root", "semver": "1.0.0"}
	v1, _ := vclusterVersionFromDict(VclusterVersionDict)
	expectedVer := VclusterOpVersion{Origin: "root", SemVer: SemVer{Ver: "1.0.0"}}
	result := v1.equalVclusterVersion(&expectedVer)
	assert.Equal(t, result, true)
	// negative case - missing semver field
	VclusterVersionDict = map[string]string{"origin": "root"}
	_, err := vclusterVersionFromDict(VclusterVersionDict)
	assert.Error(t, err)
}
