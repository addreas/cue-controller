/*
Copyright 2021 The Flux authors

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

package v1beta2

import (
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	CueExportKind             = "CueExport"
	CueExportFinalizer        = "finalizers.fluxcd.io"
	MaxConditionMessageLength = 20000
	EnabledValue              = "enabled"
	DisabledValue             = "disabled"
	MergeValue                = "merge"
)

// CueExportSpec defines the configuration to calculate the desired state from a Source using Kustomize.
type CueExportSpec struct {
	// DependsOn may contain a meta.NamespacedObjectReference slice
	// with references to CueExport resources that must be ready before this
	// CueExport can be reconciled.
	// +optional
	DependsOn []meta.NamespacedObjectReference `json:"dependsOn,omitempty"`

	// The interval at which to reconcile the CueExport.
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern="^([0-9]+(\\.[0-9]+)?(ms|s|m|h))+$"
	// +required
	Interval metav1.Duration `json:"interval"`

	// The interval at which to retry a previously failed reconciliation.
	// When not specified, the controller uses the CueExportSpec.Interval
	// value to retry failures.
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern="^([0-9]+(\\.[0-9]+)?(ms|s|m|h))+$"
	// +optional
	RetryInterval *metav1.Duration `json:"retryInterval,omitempty"`

	// The KubeConfig for reconciling the CueExport on a remote cluster.
	// When used in combination with CueExportSpec.ServiceAccountName,
	// forces the controller to act on behalf of that Service Account at the
	// target cluster.
	// If the --default-service-account flag is set, its value will be used as
	// a controller level fallback for when CueExportSpec.ServiceAccountName
	// is empty.
	// +optional
	KubeConfig *meta.KubeConfigReference `json:"kubeConfig,omitempty"`

	// The module root of the CUE instance.
	// +optional
	Root string `json:"root,omitempty"`

	// The paths at which the CUE instances will be built from.
	// +optional
	Paths []string `json:"paths,omitempty"`

	// The CUE package to use for the CUE instance. This is useful when applying
	// a CUE schema to plain yaml files.
	// +optional
	Package string `json:"package,omitempty"`

	// Tags that will be injected into the CUE instance.
	// +optional
	Tags []TagVar `json:"tags,omitempty"`

	// TagVars that will be available to the CUE instance.
	// +optional
	TagVars []TagVar `json:"tagVars,omitempty"`

	// The CUE expression(s) to execute.
	// +optional
	Exprs []string `json:"expressions,omitempty"`

	// A list of CUE expressions that must be true for the CUE instance to be
	// reconciled
	// +optional
	Gates []GateExpr `json:"gates,omitempty"`

	// Prune enables garbage collection.
	// +required
	Prune bool `json:"prune"`

	// A list of resources to be included in the health assessment.
	// +optional
	HealthChecks []meta.NamespacedObjectKindReference `json:"healthChecks,omitempty"`

	// The name of the Kubernetes service account to impersonate
	// when reconciling this CueExport.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// Reference of the source where the cue files are.
	// +required
	SourceRef CrossNamespaceSourceReference `json:"sourceRef"`

	// This flag tells the controller to suspend subsequent cue exports,
	// it does not apply to already started executions. Defaults to false.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// Timeout for validation, apply and health checking operations.
	// Defaults to 'Interval' duration.
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern="^([0-9]+(\\.[0-9]+)?(ms|s|m|h))+$"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// Force instructs the controller to recreate resources
	// when patching fails due to an immutable field change.
	// +kubebuilder:default:=false
	// +optional
	Force bool `json:"force,omitempty"`

	// Wait instructs the controller to check the health of all the reconciled resources.
	// When enabled, the HealthChecks are ignored. Defaults to false.
	// +optional
	Wait bool `json:"wait,omitempty"`
}

// GateExpr defines a CUE expression that must be true for the CUE instance to be reconciled
type GateExpr struct {
	// The CUE expression to evaluate.
	// +required
	Expr string `json:"expr"`

	// The name of the gate.
	// +required
	Name string `json:"name"`
}

// TagVar is a tag variable with a required name and optional value
type TagVar struct {
	// +required
	Name string `json:"name"`

	// +optional
	Value string `json:"value,omitempty"`

	// Source for the environment variable's value. Cannot be used if value is not empty.
	// +optional
	ValueFrom *TagVarSource `json:"valueFrom,omitempty"`
}

// TagVarSource represents a source for the value of an EnvVar.
type TagVarSource struct {
	// Selects a key of a ConfigMap.
	// +optional
	ConfigMapKeyRef *v1.ConfigMapKeySelector `json:"configMapKeyRef,omitempty"`
	// Selects a key of a secret in the pod's namespace
	// +optional
	SecretKeyRef *v1.SecretKeySelector `json:"secretKeyRef,omitempty"`
}

// CueExportStatus defines the observed state of a CueExport.
type CueExportStatus struct {
	meta.ReconcileRequestStatus `json:",inline"`

	// ObservedGeneration is the last reconciled generation.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// The last successfully applied revision.
	// Equals the Revision of the applied Artifact from the referenced Source.
	// +optional
	LastAppliedRevision string `json:"lastAppliedRevision,omitempty"`

	// LastAttemptedRevision is the revision of the last reconciliation attempt.
	// +optional
	LastAttemptedRevision string `json:"lastAttemptedRevision,omitempty"`

	// Inventory contains the list of Kubernetes resource object references that have been successfully applied.
	// +optional
	Inventory *ResourceInventory `json:"inventory,omitempty"`
}

// GetTimeout returns the timeout with default.
func (in CueExport) GetTimeout() time.Duration {
	duration := in.Spec.Interval.Duration - 30*time.Second
	if in.Spec.Timeout != nil {
		duration = in.Spec.Timeout.Duration
	}
	if duration < 30*time.Second {
		return 30 * time.Second
	}
	return duration
}

// GetRetryInterval returns the retry interval
func (in CueExport) GetRetryInterval() time.Duration {
	if in.Spec.RetryInterval != nil {
		return in.Spec.RetryInterval.Duration
	}
	return in.GetRequeueAfter()
}

// GetRequeueAfter returns the duration after which the CueExport must be
// reconciled again.
func (in CueExport) GetRequeueAfter() time.Duration {
	return in.Spec.Interval.Duration
}

// GetDependsOn returns the list of dependencies across-namespaces.
func (in CueExport) GetDependsOn() []meta.NamespacedObjectReference {
	return in.Spec.DependsOn
}

// GetConditions returns the status conditions of the object.
func (in CueExport) GetConditions() []metav1.Condition {
	return in.Status.Conditions
}

// SetConditions sets the status conditions on the object.
func (in *CueExport) SetConditions(conditions []metav1.Condition) {
	in.Status.Conditions = conditions
}

// GetStatusConditions returns a pointer to the Status.Conditions slice.
// Deprecated: use GetConditions instead.
func (in *CueExport) GetStatusConditions() *[]metav1.Condition {
	return &in.Status.Conditions
}

// +genclient
// +genclient:Namespaced
// +kubebuilder:storageversion
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=ks
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description=""
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status",description=""
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].message",description=""

// CueExport is the Schema for the cue controller API.
type CueExport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CueExportSpec `json:"spec,omitempty"`
	// +kubebuilder:default:={"observedGeneration":-1}
	Status CueExportStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CueExportList contains a list of CueExports.
type CueExportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CueExport `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CueExport{}, &CueExportList{})
}
