<h1>Kustomize API reference</h1>
<p>Packages:</p>
<ul class="simple">
<li>
<a href="#cue.toolkit.fluxcd.io%2fv1beta2">cue.toolkit.fluxcd.io/v1beta2</a>
</li>
</ul>
<h2 id="cue.toolkit.fluxcd.io/v1beta2">cue.toolkit.fluxcd.io/v1beta2</h2>
<p>Package v1beta2 contains API Schema definitions for the kustomize.toolkit.fluxcd.io v1beta2 API group.</p>
Resource Types:
<ul class="simple"><li>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CueExport">CueExport</a>
</li></ul>
<h3 id="cue.toolkit.fluxcd.io/v1beta2.CueExport">CueExport
</h3>
<p>CueExport is the Schema for the cue controller API.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>apiVersion</code><br>
string</td>
<td>
<code>cue.toolkit.fluxcd.io/v1beta2</code>
</td>
</tr>
<tr>
<td>
<code>kind</code><br>
string
</td>
<td>
<code>CueExport</code>
</td>
</tr>
<tr>
<td>
<code>metadata</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#objectmeta-v1-meta">
Kubernetes meta/v1.ObjectMeta
</a>
</em>
</td>
<td>
Refer to the Kubernetes API documentation for the fields of the
<code>metadata</code> field.
</td>
</tr>
<tr>
<td>
<code>spec</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CueExportSpec">
CueExportSpec
</a>
</em>
</td>
<td>
<br/>
<br/>
<table>
<tr>
<td>
<code>dependsOn</code><br>
<em>
<a href="https://godoc.org/github.com/fluxcd/pkg/apis/meta#NamespacedObjectReference">
[]github.com/fluxcd/pkg/apis/meta.NamespacedObjectReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>DependsOn may contain a meta.NamespacedObjectReference slice
with references to CueExport resources that must be ready before this
CueExport can be reconciled.</p>
</td>
</tr>
<tr>
<td>
<code>interval</code><br>
<em>
<a href="https://godoc.org/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration
</a>
</em>
</td>
<td>
<p>The interval at which to reconcile the CueExport.</p>
</td>
</tr>
<tr>
<td>
<code>retryInterval</code><br>
<em>
<a href="https://godoc.org/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>The interval at which to retry a previously failed reconciliation.
When not specified, the controller uses the CueExportSpec.Interval
value to retry failures.</p>
</td>
</tr>
<tr>
<td>
<code>kubeConfig</code><br>
<em>
<a href="https://godoc.org/github.com/fluxcd/pkg/apis/meta#KubeConfigReference">
github.com/fluxcd/pkg/apis/meta.KubeConfigReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>The KubeConfig for reconciling the CueExport on a remote cluster.
When used in combination with CueExportSpec.ServiceAccountName,
forces the controller to act on behalf of that Service Account at the
target cluster.
If the &ndash;default-service-account flag is set, its value will be used as
a controller level fallback for when CueExportSpec.ServiceAccountName
is empty.</p>
</td>
</tr>
<tr>
<td>
<code>root</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The module root of the CUE instance.</p>
</td>
</tr>
<tr>
<td>
<code>paths</code><br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The paths at which the CUE instances will be built from.</p>
</td>
</tr>
<tr>
<td>
<code>package</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The CUE package to use for the CUE instance. This is useful when applying
a CUE schema to plain yaml files.</p>
</td>
</tr>
<tr>
<td>
<code>tags</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.TagVar">
[]TagVar
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Tags that will be injected into the CUE instance.</p>
</td>
</tr>
<tr>
<td>
<code>tagVars</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.TagVar">
[]TagVar
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>TagVars that will be available to the CUE instance.</p>
</td>
</tr>
<tr>
<td>
<code>expressions</code><br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The CUE expression(s) to execute.</p>
</td>
</tr>
<tr>
<td>
<code>gates</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.GateExpr">
[]GateExpr
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>A list of CUE expressions that must be true for the CUE instance to be
reconciled</p>
</td>
</tr>
<tr>
<td>
<code>prune</code><br>
<em>
bool
</em>
</td>
<td>
<p>Prune enables garbage collection.</p>
</td>
</tr>
<tr>
<td>
<code>healthChecks</code><br>
<em>
<a href="https://godoc.org/github.com/fluxcd/pkg/apis/meta#NamespacedObjectKindReference">
[]github.com/fluxcd/pkg/apis/meta.NamespacedObjectKindReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>A list of resources to be included in the health assessment.</p>
</td>
</tr>
<tr>
<td>
<code>serviceAccountName</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The name of the Kubernetes service account to impersonate
when reconciling this CueExport.</p>
</td>
</tr>
<tr>
<td>
<code>sourceRef</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CrossNamespaceSourceReference">
CrossNamespaceSourceReference
</a>
</em>
</td>
<td>
<p>Reference of the source where the cue files are.</p>
</td>
</tr>
<tr>
<td>
<code>suspend</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>This flag tells the controller to suspend subsequent cue exports,
it does not apply to already started executions. Defaults to false.</p>
</td>
</tr>
<tr>
<td>
<code>timeout</code><br>
<em>
<a href="https://godoc.org/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Timeout for validation, apply and health checking operations.
Defaults to &lsquo;Interval&rsquo; duration.</p>
</td>
</tr>
<tr>
<td>
<code>force</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>Force instructs the controller to recreate resources
when patching fails due to an immutable field change.</p>
</td>
</tr>
<tr>
<td>
<code>wait</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>Wait instructs the controller to check the health of all the reconciled resources.
When enabled, the HealthChecks are ignored. Defaults to false.</p>
</td>
</tr>
</table>
</td>
</tr>
<tr>
<td>
<code>status</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CueExportStatus">
CueExportStatus
</a>
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cue.toolkit.fluxcd.io/v1beta2.CrossNamespaceSourceReference">CrossNamespaceSourceReference
</h3>
<p>
(<em>Appears on:</em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CueExportSpec">CueExportSpec</a>)
</p>
<p>CrossNamespaceSourceReference contains enough information to let you locate the
typed Kubernetes resource object at cluster level.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>apiVersion</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>API version of the referent.</p>
</td>
</tr>
<tr>
<td>
<code>kind</code><br>
<em>
string
</em>
</td>
<td>
<p>Kind of the referent.</p>
</td>
</tr>
<tr>
<td>
<code>name</code><br>
<em>
string
</em>
</td>
<td>
<p>Name of the referent.</p>
</td>
</tr>
<tr>
<td>
<code>namespace</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Namespace of the referent, defaults to the namespace of the Kubernetes resource object that contains the reference.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cue.toolkit.fluxcd.io/v1beta2.CueExportSpec">CueExportSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CueExport">CueExport</a>)
</p>
<p>CueExportSpec defines the configuration to calculate the desired state from a Source using Kustomize.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>dependsOn</code><br>
<em>
<a href="https://godoc.org/github.com/fluxcd/pkg/apis/meta#NamespacedObjectReference">
[]github.com/fluxcd/pkg/apis/meta.NamespacedObjectReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>DependsOn may contain a meta.NamespacedObjectReference slice
with references to CueExport resources that must be ready before this
CueExport can be reconciled.</p>
</td>
</tr>
<tr>
<td>
<code>interval</code><br>
<em>
<a href="https://godoc.org/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration
</a>
</em>
</td>
<td>
<p>The interval at which to reconcile the CueExport.</p>
</td>
</tr>
<tr>
<td>
<code>retryInterval</code><br>
<em>
<a href="https://godoc.org/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>The interval at which to retry a previously failed reconciliation.
When not specified, the controller uses the CueExportSpec.Interval
value to retry failures.</p>
</td>
</tr>
<tr>
<td>
<code>kubeConfig</code><br>
<em>
<a href="https://godoc.org/github.com/fluxcd/pkg/apis/meta#KubeConfigReference">
github.com/fluxcd/pkg/apis/meta.KubeConfigReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>The KubeConfig for reconciling the CueExport on a remote cluster.
When used in combination with CueExportSpec.ServiceAccountName,
forces the controller to act on behalf of that Service Account at the
target cluster.
If the &ndash;default-service-account flag is set, its value will be used as
a controller level fallback for when CueExportSpec.ServiceAccountName
is empty.</p>
</td>
</tr>
<tr>
<td>
<code>root</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The module root of the CUE instance.</p>
</td>
</tr>
<tr>
<td>
<code>paths</code><br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The paths at which the CUE instances will be built from.</p>
</td>
</tr>
<tr>
<td>
<code>package</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The CUE package to use for the CUE instance. This is useful when applying
a CUE schema to plain yaml files.</p>
</td>
</tr>
<tr>
<td>
<code>tags</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.TagVar">
[]TagVar
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Tags that will be injected into the CUE instance.</p>
</td>
</tr>
<tr>
<td>
<code>tagVars</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.TagVar">
[]TagVar
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>TagVars that will be available to the CUE instance.</p>
</td>
</tr>
<tr>
<td>
<code>expressions</code><br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The CUE expression(s) to execute.</p>
</td>
</tr>
<tr>
<td>
<code>gates</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.GateExpr">
[]GateExpr
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>A list of CUE expressions that must be true for the CUE instance to be
reconciled</p>
</td>
</tr>
<tr>
<td>
<code>prune</code><br>
<em>
bool
</em>
</td>
<td>
<p>Prune enables garbage collection.</p>
</td>
</tr>
<tr>
<td>
<code>healthChecks</code><br>
<em>
<a href="https://godoc.org/github.com/fluxcd/pkg/apis/meta#NamespacedObjectKindReference">
[]github.com/fluxcd/pkg/apis/meta.NamespacedObjectKindReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>A list of resources to be included in the health assessment.</p>
</td>
</tr>
<tr>
<td>
<code>serviceAccountName</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The name of the Kubernetes service account to impersonate
when reconciling this CueExport.</p>
</td>
</tr>
<tr>
<td>
<code>sourceRef</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CrossNamespaceSourceReference">
CrossNamespaceSourceReference
</a>
</em>
</td>
<td>
<p>Reference of the source where the cue files are.</p>
</td>
</tr>
<tr>
<td>
<code>suspend</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>This flag tells the controller to suspend subsequent cue exports,
it does not apply to already started executions. Defaults to false.</p>
</td>
</tr>
<tr>
<td>
<code>timeout</code><br>
<em>
<a href="https://godoc.org/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Timeout for validation, apply and health checking operations.
Defaults to &lsquo;Interval&rsquo; duration.</p>
</td>
</tr>
<tr>
<td>
<code>force</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>Force instructs the controller to recreate resources
when patching fails due to an immutable field change.</p>
</td>
</tr>
<tr>
<td>
<code>wait</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>Wait instructs the controller to check the health of all the reconciled resources.
When enabled, the HealthChecks are ignored. Defaults to false.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cue.toolkit.fluxcd.io/v1beta2.CueExportStatus">CueExportStatus
</h3>
<p>
(<em>Appears on:</em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CueExport">CueExport</a>)
</p>
<p>CueExportStatus defines the observed state of a CueExport.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>ReconcileRequestStatus</code><br>
<em>
<a href="https://godoc.org/github.com/fluxcd/pkg/apis/meta#ReconcileRequestStatus">
github.com/fluxcd/pkg/apis/meta.ReconcileRequestStatus
</a>
</em>
</td>
<td>
<p>
(Members of <code>ReconcileRequestStatus</code> are embedded into this type.)
</p>
</td>
</tr>
<tr>
<td>
<code>observedGeneration</code><br>
<em>
int64
</em>
</td>
<td>
<em>(Optional)</em>
<p>ObservedGeneration is the last reconciled generation.</p>
</td>
</tr>
<tr>
<td>
<code>conditions</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#condition-v1-meta">
[]Kubernetes meta/v1.Condition
</a>
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>lastAppliedRevision</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The last successfully applied revision.
Equals the Revision of the applied Artifact from the referenced Source.</p>
</td>
</tr>
<tr>
<td>
<code>lastAttemptedRevision</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>LastAttemptedRevision is the revision of the last reconciliation attempt.</p>
</td>
</tr>
<tr>
<td>
<code>inventory</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.ResourceInventory">
ResourceInventory
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Inventory contains the list of Kubernetes resource object references that have been successfully applied.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cue.toolkit.fluxcd.io/v1beta2.GateExpr">GateExpr
</h3>
<p>
(<em>Appears on:</em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CueExportSpec">CueExportSpec</a>)
</p>
<p>GateExpr defines a CUE expression that must be true for the CUE instance to be reconciled</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>expr</code><br>
<em>
string
</em>
</td>
<td>
<p>The CUE expression to evaluate.</p>
</td>
</tr>
<tr>
<td>
<code>name</code><br>
<em>
string
</em>
</td>
<td>
<p>The name of the gate.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cue.toolkit.fluxcd.io/v1beta2.ResourceInventory">ResourceInventory
</h3>
<p>
(<em>Appears on:</em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CueExportStatus">CueExportStatus</a>)
</p>
<p>ResourceInventory contains a list of Kubernetes resource object references that have been applied by a Kustomization.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>entries</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.ResourceRef">
[]ResourceRef
</a>
</em>
</td>
<td>
<p>Entries of Kubernetes resource object references.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cue.toolkit.fluxcd.io/v1beta2.ResourceRef">ResourceRef
</h3>
<p>
(<em>Appears on:</em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.ResourceInventory">ResourceInventory</a>)
</p>
<p>ResourceRef contains the information necessary to locate a resource within a cluster.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>id</code><br>
<em>
string
</em>
</td>
<td>
<p>ID is the string representation of the Kubernetes resource object&rsquo;s metadata,
in the format &lsquo;<namespace><em><name></em><group>_<kind>&rsquo;.</p>
</td>
</tr>
<tr>
<td>
<code>v</code><br>
<em>
string
</em>
</td>
<td>
<p>Version is the API version of the Kubernetes resource object&rsquo;s kind.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cue.toolkit.fluxcd.io/v1beta2.TagVar">TagVar
</h3>
<p>
(<em>Appears on:</em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.CueExportSpec">CueExportSpec</a>)
</p>
<p>TagVar is a tag variable with a required name and optional value</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>name</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>value</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>valueFrom</code><br>
<em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.TagVarSource">
TagVarSource
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Source for the environment variable&rsquo;s value. Cannot be used if value is not empty.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cue.toolkit.fluxcd.io/v1beta2.TagVarSource">TagVarSource
</h3>
<p>
(<em>Appears on:</em>
<a href="#cue.toolkit.fluxcd.io/v1beta2.TagVar">TagVar</a>)
</p>
<p>TagVarSource represents a source for the value of an EnvVar.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>configMapKeyRef</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#configmapkeyselector-v1-core">
Kubernetes core/v1.ConfigMapKeySelector
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Selects a key of a ConfigMap.</p>
</td>
</tr>
<tr>
<td>
<code>secretKeyRef</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Selects a key of a secret in the pod&rsquo;s namespace</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<div class="admonition note">
<p class="last">This page was automatically generated with <code>gen-crd-api-reference-docs</code></p>
</div>
