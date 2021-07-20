module github.com/addreas/cuebuild-controller

go 1.15

replace github.com/addreas/cuebuild-controller/api => ./api

require (
	cuelang.org/go v0.4.0
	github.com/addreas/cuebuild-controller/api v0.0.0-00010101000000-000000000000
	github.com/fluxcd/pkg/apis/meta v0.9.0
	github.com/fluxcd/pkg/runtime v0.11.0
	github.com/fluxcd/pkg/testserver v0.0.2
	github.com/fluxcd/pkg/untar v0.0.5
	github.com/fluxcd/source-controller/api v0.12.1
	github.com/go-logr/logr v0.3.0
	github.com/hashicorp/go-retryablehttp v0.6.8
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/lib/pq v1.2.0 // indirect
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.2
	github.com/spf13/pflag v1.0.5
	golang.org/x/crypto v0.0.0-20210322153248-0c34fe9e7dc2 // indirect
	golang.org/x/net v0.0.0-20210326060303-6b1517762897 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107172259-749611fa9fcc // indirect
	k8s.io/api v0.20.4
	k8s.io/apiextensions-apiserver v0.20.4 // indirect
	k8s.io/apimachinery v0.20.4
	k8s.io/cli-runtime v0.20.4 // indirect
	k8s.io/client-go v0.20.4
	sigs.k8s.io/cli-utils v0.22.4
	sigs.k8s.io/controller-runtime v0.8.3
	sigs.k8s.io/kustomize/kyaml v0.10.9 // indirect
)
