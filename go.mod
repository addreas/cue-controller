module github.com/addreas/cuebuild-controller

go 1.15

replace github.com/addreas/cuebuild-controller/api => ./api

require (
	github.com/cyphar/filepath-securejoin v0.2.2
	github.com/addreas/cuebuild-controller/api v0.0.0-00010101000000-000000000000
	github.com/fluxcd/pkg/apis/meta v0.8.0
	github.com/fluxcd/pkg/runtime v0.8.3
	github.com/fluxcd/pkg/testserver v0.0.2
	github.com/fluxcd/pkg/untar v0.0.5
	github.com/fluxcd/source-controller/api v0.9.0
	github.com/go-logr/logr v0.3.0
	github.com/hashicorp/go-retryablehttp v0.6.8
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.2
	github.com/spf13/pflag v1.0.5
	k8s.io/api v0.20.2
	k8s.io/apimachinery v0.20.2
	k8s.io/cli-runtime v0.20.2 // indirect
	k8s.io/client-go v0.20.2
	sigs.k8s.io/cli-utils v0.22.2
	sigs.k8s.io/controller-runtime v0.8.2
	sigs.k8s.io/kustomize v2.0.3+incompatible
	sigs.k8s.io/kustomize/api v0.8.3
	sigs.k8s.io/yaml v1.2.0
)
