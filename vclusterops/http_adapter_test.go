package vclusterops

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildQueryParams(t *testing.T) {
	queryParams := make(map[string]string)

	// empty query params should produce an empty string
	queryParamString := buildQueryParamString(queryParams, true)
	assert.Empty(t, queryParamString)

	// non-empty query params should produce a string like
	// "?key1=value1&key2=value2"
	queryParams["key1"] = "value1"
	queryParams["key2"] = "value2"
	queryParamString = buildQueryParamString(queryParams, true)
	queryParamStringHTTP := buildQueryParamString(queryParams, false)
	assert.Equal(t, queryParamString, "?key1=value1&key2=value2")
	assert.Contains(t, queryParamStringHTTP, "key1=value1")
	assert.Contains(t, queryParamStringHTTP, "key2=value2")

	// query params with special characters, such as %
	// which is used by the create depot endpoint
	queryParams = make(map[string]string)
	queryParams["size"] = "10%"
	queryParams["path"] = "/the/depot/path"
	queryParamString = buildQueryParamString(queryParams, true)
	// `/` is escaped with `%2F`
	// `%` is escaped with `%25`
	assert.Equal(t, queryParamString, "?path=%2Fthe%2Fdepot%2Fpath&size=10%25")

	queryParams["hosts"] = "192.168.1.102,192.168.1.103"
	queryParams["size"] = "25%"
	queryParams["path"] = "/the/path"
	queryParamStringHTTP = buildQueryParamString(queryParams, false)
	assert.Contains(t, queryParamStringHTTP, "hosts=192.168.1.102,192.168.1.103")
	assert.Contains(t, queryParamStringHTTP, "path=/the/path")
	assert.Contains(t, queryParamStringHTTP, "size=25%")
}
