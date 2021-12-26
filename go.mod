module github.com/addreas/cuebuild-controller

go 1.16

replace github.com/addreas/cuebuild-controller/api => ./api

require (
	cuelang.org/go v0.4.0
	github.com/addreas/cuebuild-controller/api v0.0.0-00010101000000-000000000000
	github.com/fluxcd/pkg/apis/meta v0.10.1
	github.com/fluxcd/pkg/runtime v0.12.2
	github.com/fluxcd/pkg/ssa v0.6.0
	github.com/fluxcd/pkg/testserver v0.1.0
	github.com/fluxcd/pkg/untar v0.1.0
	github.com/fluxcd/source-controller/api v0.19.2
	github.com/go-logr/logr v0.4.0
	github.com/hashicorp/go-retryablehttp v0.7.0
	github.com/spf13/pflag v1.0.5
	k8s.io/api v0.22.2
	k8s.io/apimachinery v0.22.2
	k8s.io/client-go v0.22.2
	sigs.k8s.io/cli-utils v0.26.0
	sigs.k8s.io/controller-runtime v0.10.2
)

// fix CVE-2021-30465
// fix CVE-2021-43784
replace github.com/opencontainers/runc => github.com/opencontainers/runc v1.0.3

// fix CVE-2021-41190
replace github.com/opencontainers/image-spec => github.com/opencontainers/image-spec v1.0.2
