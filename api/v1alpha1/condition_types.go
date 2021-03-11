/*
Copyright 2020 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

const (
	// HealthyCondition is the condition type used
	// to record the last health assessment result.
	HealthyCondition string = "Healthy"

	// PruneFailedReason represents the fact that the
	// pruning of the CueBuild failed.
	PruneFailedReason string = "PruneFailed"

	// ArtifactFailedReason represents the fact that the
	// artifact download of the CueBuild failed.
	ArtifactFailedReason string = "ArtifactFailed"

	// BuildFailedReason represents the fact that the
	// cue export of the CueBuild failed.
	BuildFailedReason string = "BuildFailed"

	// HealthCheckFailedReason represents the fact that
	// one of the health checks of the CueBuild failed.
	HealthCheckFailedReason string = "HealthCheckFailed"

	// ValidationFailedReason represents the fact that the
	// validation of the CueBuild manifests has failed.
	ValidationFailedReason string = "ValidationFailed"
)
