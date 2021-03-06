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

import (
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/runtime/dependency"
)

const (
	CueBuildKind              = "CueBuild"
	CueBuildFinalizer         = "finalizers.fluxcd.io"
	MaxConditionMessageLength = 20000
	DisabledValue             = "disabled"
)

// CueBuildSpec defines the desired state of a cueBuild.
type CueBuildSpec struct {
	// DependsOn may contain a dependency.CrossNamespaceDependencyReference slice
	// with references to CueBuild resources that must be ready before this
	// CueBuild can be reconciled.
	// +optional
	DependsOn []dependency.CrossNamespaceDependencyReference `json:"dependsOn,omitempty"`

	// The interval at which to reconcile the CueBuild.
	// +required
	Interval metav1.Duration `json:"interval"`

	// The interval at which to retry a previously failed reconciliation.
	// When not specified, the controller uses the CueBuildSpec.Interval
	// value to retry failures.
	// +optional
	RetryInterval *metav1.Duration `json:"retryInterval,omitempty"`

	// The KubeConfig for reconciling the CueBuild on a remote cluster.
	// When specified, KubeConfig takes precedence over ServiceAccountName.
	// +optional
	KubeConfig *KubeConfig `json:"kubeConfig,omitempty"`

	// Package to include in the cue build.
	// Defaults to 'None', which translates to everything via './...'.
	// +optional
	Packages []string `json:"packages,omitempty"`

	// Prune enables garbage collection.
	// +required
	Prune bool `json:"prune"`

	// A list of resources to be included in the health assessment.
	// +optional
	HealthChecks []meta.NamespacedObjectKindReference `json:"healthChecks,omitempty"`

	// The name of the Kubernetes service account to impersonate
	// when reconciling this CueBuild.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// Reference of the source where the cue packages are.
	// +required
	SourceRef CrossNamespaceSourceReference `json:"sourceRef"`

	// This flag tells the controller to suspend subsequent cue build executions,
	// it does not apply to already started executions. Defaults to false.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// Timeout for validation, apply and health checking operations.
	// Defaults to 'Interval' duration.
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// Validate the Kubernetes objects before applying them on the cluster.
	// The validation strategy can be 'client' (local dry-run), 'server'
	// (APIServer dry-run) or 'none'.
	// When 'Force' is 'true', validation will fallback to 'client' if set to
	// 'server' because server-side validation is not supported in this scenario.
	// +kubebuilder:validation:Enum=none;client;server
	// +optional
	Validation string `json:"validation,omitempty"`

	// Force instructs the controller to recreate resources
	// when patching fails due to an immutable field change.
	// +kubebuilder:default:=false
	// +optional
	Force bool `json:"force,omitempty"`
}

// KubeConfig references a Kubernetes secret that contains a kubeconfig file.
type KubeConfig struct {
	// SecretRef holds the name to a secret that contains a 'value' key with
	// the kubeconfig file as the value. It must be in the same namespace as
	// the CueBuild.
	// It is recommended that the kubeconfig is self-contained, and the secret
	// is regularly updated if credentials such as a cloud-access-token expire.
	// Cloud specific `cmd-path` auth helpers will not function without adding
	// binaries and credentials to the Pod that is responsible for reconciling
	// the CueBuild.
	// +required
	SecretRef meta.LocalObjectReference `json:"secretRef,omitempty"`
}

// CueBuildStatus defines the observed state of a cueBuild.
type CueBuildStatus struct {
	// ObservedGeneration is the last reconciled generation.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// The last successfully applied revision.
	// The revision format for Git sources is <branch|tag>/<commit-sha>.
	// +optional
	LastAppliedRevision string `json:"lastAppliedRevision,omitempty"`

	// LastAttemptedRevision is the revision of the last reconciliation attempt.
	// +optional
	LastAttemptedRevision string `json:"lastAttemptedRevision,omitempty"`

	meta.ReconcileRequestStatus `json:",inline"`

	// The last successfully applied revision metadata.
	// +optional
	Snapshot *Snapshot `json:"snapshot,omitempty"`
}

// CueBuildProgressing resets the conditions of the given CueBuild to a single
// ReadyCondition with status ConditionUnknown.
func CueBuildProgressing(k CueBuild) CueBuild {
	meta.SetResourceCondition(&k, meta.ReadyCondition, metav1.ConditionUnknown, meta.ProgressingReason, "reconciliation in progress")
	return k
}

// SetCueBuildHealthiness sets the HealthyCondition status for a CueBuild.
func SetCueBuildHealthiness(k *CueBuild, status metav1.ConditionStatus, reason, message string) {
	switch len(k.Spec.HealthChecks) {
	case 0:
		apimeta.RemoveStatusCondition(k.GetStatusConditions(), HealthyCondition)
	default:
		meta.SetResourceCondition(k, HealthyCondition, status, reason, trimString(message, MaxConditionMessageLength))
	}
}

// SetCueBuildReadiness sets the ReadyCondition, ObservedGeneration, and LastAttemptedRevision,
// on the CueBuild.
func SetCueBuildReadiness(k *CueBuild, status metav1.ConditionStatus, reason, message string, revision string) {
	meta.SetResourceCondition(k, meta.ReadyCondition, status, reason, trimString(message, MaxConditionMessageLength))
	k.Status.ObservedGeneration = k.Generation
	k.Status.LastAttemptedRevision = revision
}

// CueBuildNotReady registers a failed apply attempt of the given CueBuild.
func CueBuildNotReady(k CueBuild, revision, reason, message string) CueBuild {
	SetCueBuildReadiness(&k, metav1.ConditionFalse, reason, trimString(message, MaxConditionMessageLength), revision)
	if revision != "" {
		k.Status.LastAttemptedRevision = revision
	}
	return k
}

// CueBuildNotReadySnapshot registers a failed apply attempt of the given CueBuild including a Snapshot.
func CueBuildNotReadySnapshot(k CueBuild, snapshot *Snapshot, revision, reason, message string) CueBuild {
	SetCueBuildReadiness(&k, metav1.ConditionFalse, reason, trimString(message, MaxConditionMessageLength), revision)
	SetCueBuildHealthiness(&k, metav1.ConditionFalse, reason, reason)
	k.Status.Snapshot = snapshot
	k.Status.LastAttemptedRevision = revision
	return k
}

// CueBuildReady registers a successful apply attempt of the given CueBuild.
func CueBuildReady(k CueBuild, snapshot *Snapshot, revision, reason, message string) CueBuild {
	SetCueBuildReadiness(&k, metav1.ConditionTrue, reason, trimString(message, MaxConditionMessageLength), revision)
	SetCueBuildHealthiness(&k, metav1.ConditionTrue, reason, reason)
	k.Status.Snapshot = snapshot
	k.Status.LastAppliedRevision = revision
	return k
}

// GetTimeout returns the timeout with default.
func (in CueBuild) GetTimeout() time.Duration {
	duration := in.Spec.Interval.Duration
	if in.Spec.Timeout != nil {
		duration = in.Spec.Timeout.Duration
	}
	if duration < time.Minute {
		return time.Minute
	}
	return duration
}

// GetRetryInterval returns the retry interval
func (in CueBuild) GetRetryInterval() time.Duration {
	if in.Spec.RetryInterval != nil {
		return in.Spec.RetryInterval.Duration
	}
	return in.Spec.Interval.Duration
}

func (in CueBuild) GetDependsOn() (types.NamespacedName, []dependency.CrossNamespaceDependencyReference) {
	return types.NamespacedName{
		Namespace: in.Namespace,
		Name:      in.Name,
	}, in.Spec.DependsOn
}

// GetStatusConditions returns a pointer to the Status.Conditions slice
func (in *CueBuild) GetStatusConditions() *[]metav1.Condition {
	return &in.Status.Conditions
}

const (
	// GitRepositoryIndexKey is the key used for indexing cueBuilds
	// based on their Git sources.
	GitRepositoryIndexKey string = ".metadata.gitRepository"
	// BucketIndexKey is the key used for indexing cueBuilds
	// based on their S3 sources.
	BucketIndexKey string = ".metadata.bucket"
)

// +genclient
// +genclient:Namespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=cb
// +kubebuilder:resource:path=cuebuilds
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status",description=""
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].message",description=""
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description=""

// CueBuild is the Schema for the CueBuilds API.
type CueBuild struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CueBuildSpec   `json:"spec,omitempty"`
	Status CueBuildStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CueBuildList contains a list of CueBuilds.
type CueBuildList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CueBuild `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CueBuild{}, &CueBuildList{})
}

func trimString(str string, limit int) string {
	result := str
	chars := 0
	for i := range str {
		if chars >= limit {
			result = str[:i] + "..."
			break
		}
		chars++
	}
	return result
}
