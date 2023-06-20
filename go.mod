module github.com/vertica/vcluster

go 1.20

require (
	github.com/stretchr/testify v1.8.2
	golang.org/x/exp v0.0.0-20230510235704-dd950f8aeaea
	golang.org/x/sys v0.8.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
)

replace github.com/rpaulin/vcluster => github.com/vertica/vcluster v0.0.0-20230619120328-5d8c9008e000
