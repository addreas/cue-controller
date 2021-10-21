module github.com/addreas/cuebuild-controller

go 1.16

replace github.com/addreas/cuebuild-controller/api => ./api

require (
	cuelang.org/go v0.4.0
	github.com/addreas/cuebuild-controller/api v0.0.0-00010101000000-000000000000
	github.com/cyphar/filepath-securejoin v0.2.2
	github.com/fluxcd/kustomize-controller/api v0.16.0
	github.com/fluxcd/pkg/apis/kustomize v0.2.1
	github.com/fluxcd/pkg/apis/meta v0.10.1
	github.com/fluxcd/pkg/runtime v0.12.2
	github.com/fluxcd/pkg/ssa v0.2.0
	github.com/fluxcd/pkg/testserver v0.1.0
	github.com/fluxcd/pkg/untar v0.1.0
	github.com/fluxcd/source-controller/api v0.16.0
	github.com/go-errors/errors v1.4.0 // indirect
	github.com/go-logr/logr v0.4.0
	github.com/hashicorp/go-retryablehttp v0.6.8
	github.com/kr/pretty v0.2.1 // indirect
	github.com/lib/pq v1.2.0 // indirect
	github.com/onsi/gomega v1.15.0
	github.com/spf13/pflag v1.0.5
	k8s.io/api v0.22.2
	k8s.io/apiextensions-apiserver v0.22.2
	k8s.io/apimachinery v0.22.2
	k8s.io/client-go v0.22.2
	sigs.k8s.io/cli-utils v0.26.0
	sigs.k8s.io/controller-runtime v0.10.2
	sigs.k8s.io/kustomize/api v0.10.0 // indirect
)

// pin kustomize to v4.4.0
replace (
	sigs.k8s.io/kustomize/api => sigs.k8s.io/kustomize/api v0.10.0
	sigs.k8s.io/kustomize/kyaml => sigs.k8s.io/kustomize/kyaml v0.12.0
)

// fix CVE-2021-30465
replace github.com/opencontainers/runc => github.com/opencontainers/runc v1.0.0-rc95
