<h1>Kustomize API reference</h1>
<p>Packages:</p>
<ul class="simple">
<li>
<a href="#cuebuild.toolkit.fluxcd.io%2fv1alpha1">cuebuild.toolkit.fluxcd.io/v1alpha1</a>
</li>
</ul>
<h2 id="cuebuild.toolkit.fluxcd.io/v1alpha1">cuebuild.toolkit.fluxcd.io/v1alpha1</h2>
<p>Package v1alpha1 contains API Schema definitions for the cuebuild v1alpha1 API group</p>
Resource Types:
<ul class="simple"><li>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuild">CueBuild</a>
</li></ul>
<h3 id="cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuild">CueBuild
</h3>
<p>CueBuild is the Schema for the CueBuilds API.</p>
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
<code>cuebuild.toolkit.fluxcd.io/v1alpha1</code>
</td>
</tr>
<tr>
<td>
<code>kind</code><br>
string
</td>
<td>
<code>CueBuild</code>
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
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuildSpec">
CueBuildSpec
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
<a href="https://godoc.org/github.com/fluxcd/pkg/runtime/dependency#CrossNamespaceDependencyReference">
[]Runtime dependency.CrossNamespaceDependencyReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>DependsOn may contain a dependency.CrossNamespaceDependencyReference slice
with references to CueBuild resources that must be ready before this
CueBuild can be reconciled.</p>
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
<p>The interval at which to reconcile the CueBuild.</p>
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
When not specified, the controller uses the CueBuildSpec.Interval
value to retry failures.</p>
</td>
</tr>
<tr>
<td>
<code>kubeConfig</code><br>
<em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.KubeConfig">
KubeConfig
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>The KubeConfig for reconciling the CueBuild on a remote cluster.
When specified, KubeConfig takes precedence over ServiceAccountName.</p>
</td>
</tr>
<tr>
<td>
<code>packages</code><br>
<em>
[]string
</em>
</td>
<td>
<p>Packages to include in the cue build.</p>
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
when reconciling this CueBuild.</p>
</td>
</tr>
<tr>
<td>
<code>sourceRef</code><br>
<em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.CrossNamespaceSourceReference">
CrossNamespaceSourceReference
</a>
</em>
</td>
<td>
<p>Reference of the source where the cue packages are.</p>
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
<p>This flag tells the controller to suspend subsequent cue build executions,
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
<code>validation</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Validate the Kubernetes objects before applying them on the cluster.
The validation strategy can be &lsquo;client&rsquo; (local dry-run), &lsquo;server&rsquo;
(APIServer dry-run) or &lsquo;none&rsquo;.
When &lsquo;Force&rsquo; is &lsquo;true&rsquo;, validation will fallback to &lsquo;client&rsquo; if set to
&lsquo;server&rsquo; because server-side validation is not supported in this scenario.</p>
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
</table>
</td>
</tr>
<tr>
<td>
<code>status</code><br>
<em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuildStatus">
CueBuildStatus
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
<h3 id="cuebuild.toolkit.fluxcd.io/v1alpha1.CrossNamespaceSourceReference">CrossNamespaceSourceReference
</h3>
<p>
(<em>Appears on:</em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuildSpec">CueBuildSpec</a>)
</p>
<p>CrossNamespaceSourceReference contains enough information to let you locate the
typed referenced object at cluster level</p>
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
<p>API version of the referent</p>
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
<p>Kind of the referent</p>
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
<p>Name of the referent</p>
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
<p>Namespace of the referent, defaults to the CueBuild namespace</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuildSpec">CueBuildSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuild">CueBuild</a>)
</p>
<p>CueBuildSpec defines the desired state of a cueBuild.</p>
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
<a href="https://godoc.org/github.com/fluxcd/pkg/runtime/dependency#CrossNamespaceDependencyReference">
[]Runtime dependency.CrossNamespaceDependencyReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>DependsOn may contain a dependency.CrossNamespaceDependencyReference slice
with references to CueBuild resources that must be ready before this
CueBuild can be reconciled.</p>
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
<p>The interval at which to reconcile the CueBuild.</p>
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
When not specified, the controller uses the CueBuildSpec.Interval
value to retry failures.</p>
</td>
</tr>
<tr>
<td>
<code>kubeConfig</code><br>
<em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.KubeConfig">
KubeConfig
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>The KubeConfig for reconciling the CueBuild on a remote cluster.
When specified, KubeConfig takes precedence over ServiceAccountName.</p>
</td>
</tr>
<tr>
<td>
<code>packages</code><br>
<em>
[]string
</em>
</td>
<td>
<p>Packages to include in the cue build.</p>
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
when reconciling this CueBuild.</p>
</td>
</tr>
<tr>
<td>
<code>sourceRef</code><br>
<em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.CrossNamespaceSourceReference">
CrossNamespaceSourceReference
</a>
</em>
</td>
<td>
<p>Reference of the source where the cue packages are.</p>
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
<p>This flag tells the controller to suspend subsequent cue build executions,
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
<code>validation</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Validate the Kubernetes objects before applying them on the cluster.
The validation strategy can be &lsquo;client&rsquo; (local dry-run), &lsquo;server&rsquo;
(APIServer dry-run) or &lsquo;none&rsquo;.
When &lsquo;Force&rsquo; is &lsquo;true&rsquo;, validation will fallback to &lsquo;client&rsquo; if set to
&lsquo;server&rsquo; because server-side validation is not supported in this scenario.</p>
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
</tbody>
</table>
</div>
</div>
<h3 id="cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuildStatus">CueBuildStatus
</h3>
<p>
(<em>Appears on:</em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuild">CueBuild</a>)
</p>
<p>CueBuildStatus defines the observed state of a cueBuild.</p>
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
The revision format for Git sources is <branch|tag>/<commit-sha>.</p>
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
<code>snapshot</code><br>
<em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.Snapshot">
Snapshot
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>The last successfully applied revision metadata.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cuebuild.toolkit.fluxcd.io/v1alpha1.KubeConfig">KubeConfig
</h3>
<p>
(<em>Appears on:</em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuildSpec">CueBuildSpec</a>)
</p>
<p>KubeConfig references a Kubernetes secret that contains a kubeconfig file.</p>
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
<code>secretRef</code><br>
<em>
<a href="https://godoc.org/github.com/fluxcd/pkg/apis/meta#LocalObjectReference">
github.com/fluxcd/pkg/apis/meta.LocalObjectReference
</a>
</em>
</td>
<td>
<p>SecretRef holds the name to a secret that contains a &lsquo;value&rsquo; key with
the kubeconfig file as the value. It must be in the same namespace as
the CueBuild.
It is recommended that the kubeconfig is self-contained, and the secret
is regularly updated if credentials such as a cloud-access-token expire.
Cloud specific <code>cmd-path</code> auth helpers will not function without adding
binaries and credentials to the Pod that is responsible for reconciling
the CueBuild.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cuebuild.toolkit.fluxcd.io/v1alpha1.Snapshot">Snapshot
</h3>
<p>
(<em>Appears on:</em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.CueBuildStatus">CueBuildStatus</a>)
</p>
<p>Snapshot holds the metadata of the Kubernetes objects
generated for a source revision</p>
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
<code>checksum</code><br>
<em>
string
</em>
</td>
<td>
<p>The manifests sha1 checksum.</p>
</td>
</tr>
<tr>
<td>
<code>entries</code><br>
<em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.SnapshotEntry">
[]SnapshotEntry
</a>
</em>
</td>
<td>
<p>A list of Kubernetes kinds grouped by namespace.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="cuebuild.toolkit.fluxcd.io/v1alpha1.SnapshotEntry">SnapshotEntry
</h3>
<p>
(<em>Appears on:</em>
<a href="#cuebuild.toolkit.fluxcd.io/v1alpha1.Snapshot">Snapshot</a>)
</p>
<p>Snapshot holds the metadata of namespaced
Kubernetes objects</p>
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
<code>namespace</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>The namespace of this entry.</p>
</td>
</tr>
<tr>
<td>
<code>kinds</code><br>
<em>
map[string]string
</em>
</td>
<td>
<p>The list of Kubernetes kinds.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<div class="admonition note">
<p class="last">This page was automatically generated with <code>gen-crd-api-reference-docs</code></p>
</div>
