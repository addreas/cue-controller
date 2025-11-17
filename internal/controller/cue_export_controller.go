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

package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/load"
	"cuelang.org/go/cue/parser"
	securejoin "github.com/cyphar/filepath-securejoin"
	celtypes "github.com/google/cel-go/common/types"
	"github.com/opencontainers/go-digest"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	kuberecorder "k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/fluxcd/cli-utils/pkg/kstatus/polling"
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/engine"
	"github.com/fluxcd/cli-utils/pkg/object"
	apiacl "github.com/fluxcd/pkg/apis/acl"
	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/auth"
	authutils "github.com/fluxcd/pkg/auth/utils"
	"github.com/fluxcd/pkg/cache"
	"github.com/fluxcd/pkg/http/fetch"
	"github.com/fluxcd/pkg/runtime/acl"
	"github.com/fluxcd/pkg/runtime/cel"
	runtimeClient "github.com/fluxcd/pkg/runtime/client"
	"github.com/fluxcd/pkg/runtime/conditions"
	runtimeCtrl "github.com/fluxcd/pkg/runtime/controller"
	"github.com/fluxcd/pkg/runtime/jitter"
	"github.com/fluxcd/pkg/runtime/patch"
	"github.com/fluxcd/pkg/runtime/statusreaders"
	"github.com/fluxcd/pkg/ssa"
	"github.com/fluxcd/pkg/ssa/normalize"
	ssautil "github.com/fluxcd/pkg/ssa/utils"
	"github.com/fluxcd/pkg/tar"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	cuev1 "github.com/addreas/cue-controller/api/v1beta2"
	"github.com/addreas/cue-controller/internal/inventory"
)

// +kubebuilder:rbac:groups=cue.toolkit.fluxcd.io,resources=cueexports,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cue.toolkit.fluxcd.io,resources=cueexports/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cue.toolkit.fluxcd.io,resources=cueexports/finalizers,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=buckets;ocirepositories;gitrepositories,verbs=get;list;watch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=buckets/status;ocirepositories/status;gitrepositories/status,verbs=get
// +kubebuilder:rbac:groups="",resources=configmaps;secrets;serviceaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=serviceaccounts/token,verbs=create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// CueReconciler reconciles a CueExport object
type CueReconciler struct {
	client.Client
	kuberecorder.EventRecorder
	runtimeCtrl.Metrics

	// Kubernetes options

	APIReader      client.Reader
	ClusterReader  engine.ClusterReaderFactory
	ConcurrentSSA  int
	ControllerName string
	KubeConfigOpts runtimeClient.KubeConfigOptions
	Mapper         apimeta.RESTMapper
	StatusManager  string

	// Multi-tenancy and security options

	DefaultServiceAccount   string
	DisallowedFieldManagers []string
	NoCrossNamespaceRefs    bool
	NoRemoteBases           bool
	SOPSAgeSecret           string
	TokenCache              *cache.TokenCache

	HTTPRetry int
	// Retry and requeue options

	ArtifactFetchRetries      int
	DependencyRequeueInterval time.Duration

	// Feature gates

	AdditiveCELDependencyCheck     bool
	AllowExternalArtifact          bool
	CancelHealthCheckOnNewRevision bool
	FailFast                       bool
	GroupChangeLog                 bool
	StrictSubstitutions            bool
}

func (r *CueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	log := ctrl.LoggerFrom(ctx)
	reconcileStart := time.Now()

	obj := &cuev1.CueExport{}
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Initialize the runtime patcher with the current version of the object.
	patcher := patch.NewSerialPatcher(obj, r.Client)

	// Finalise the reconciliation and report the results.
	defer func() {
		// Patch finalizers, status and conditions.
		if err := r.finalizeStatus(ctx, obj, patcher); err != nil {
			retErr = kerrors.NewAggregate([]error{retErr, err})
		}

		// Record Prometheus metrics.
		r.Metrics.RecordDuration(ctx, obj, reconcileStart)

		// Do not proceed if the Kustomization is suspended
		if obj.Spec.Suspend {
			return
		}

		// Log and emit success event.
		if conditions.IsReady(obj) {
			msg := fmt.Sprintf("Reconciliation finished in %s, next run in %s",
				time.Since(reconcileStart).String(),
				obj.Spec.Interval.Duration.String())
			log.Info(msg, "revision", obj.Status.LastAttemptedRevision)
			r.event(obj, obj.Status.LastAppliedRevision, obj.Status.LastAppliedOriginRevision, eventv1.EventSeverityInfo, msg,
				map[string]string{
					cuev1.GroupVersion.Group + "/" + eventv1.MetaCommitStatusKey: eventv1.MetaCommitStatusUpdateValue,
				})
		}
	}()

	// Prune managed resources if the object is under deletion.
	if !obj.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, obj)
	}

	// Add finalizer first if it doesn't exist to avoid the race condition
	// between init and delete.
	// Note: Finalizers in general can only be added when the deletionTimestamp
	// is not set.
	if !controllerutil.ContainsFinalizer(obj, cuev1.CueExportFinalizer) {
		controllerutil.AddFinalizer(obj, cuev1.CueExportFinalizer)
		return ctrl.Result{Requeue: true}, nil
	}

	// Skip reconciliation if the object is suspended.
	if obj.Spec.Suspend {
		log.Info("Reconciliation is suspended for this object")
		return ctrl.Result{}, nil
	}

	// Configure custom health checks.
	statusReaders, err := cel.PollerWithCustomHealthChecks(ctx, obj.Spec.HealthCheckExprs)
	if err != nil {
		errMsg := fmt.Sprintf("%s: %v", TerminalErrorMessage, err)
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.InvalidCELExpressionReason, "%s", errMsg)
		conditions.MarkStalled(obj, meta.InvalidCELExpressionReason, "%s", errMsg)
		obj.Status.ObservedGeneration = obj.Generation
		r.event(obj, "", "", eventv1.EventSeverityError, errMsg, nil)
		return ctrl.Result{}, reconcile.TerminalError(err)
	}

	// Resolve the source reference and requeue the reconciliation if the source is not found.
	artifactSource, err := r.getSource(ctx, obj)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.ArtifactFailedReason, "%s", err)

		if apierrors.IsNotFound(err) {
			msg := fmt.Sprintf("Source '%s' not found", obj.Spec.SourceRef.String())
			log.Info(msg)
			return ctrl.Result{RequeueAfter: obj.GetRetryInterval()}, nil
		}

		if acl.IsAccessDenied(err) {
			conditions.MarkFalse(obj, meta.ReadyCondition, apiacl.AccessDeniedReason, "%s", err)
			conditions.MarkStalled(obj, apiacl.AccessDeniedReason, "%s", err)
			r.event(obj, "", "", eventv1.EventSeverityError, err.Error(), nil)
			return ctrl.Result{}, reconcile.TerminalError(err)
		}

		// Retry with backoff on transient errors.
		return ctrl.Result{}, err
	}

	// Requeue the reconciliation if the source artifact is not found.
	if artifactSource.GetArtifact() == nil {
		msg := fmt.Sprintf("Source artifact not found, retrying in %s", r.DependencyRequeueInterval.String())
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.ArtifactFailedReason, "%s", msg)
		log.Info(msg)
		return ctrl.Result{RequeueAfter: r.DependencyRequeueInterval}, nil
	}
	revision := artifactSource.GetArtifact().Revision
	originRevision := getOriginRevision(artifactSource)

	// Check dependencies and requeue the reconciliation if the check fails.
	if len(obj.Spec.DependsOn) > 0 {
		if err := r.checkDependencies(ctx, obj, artifactSource); err != nil {
			// Check if this is a terminal error that should not trigger retries
			if errors.Is(err, reconcile.TerminalError(nil)) {
				errMsg := fmt.Sprintf("%s: %v", TerminalErrorMessage, err)
				conditions.MarkFalse(obj, meta.ReadyCondition, meta.InvalidCELExpressionReason, "%s", errMsg)
				conditions.MarkStalled(obj, meta.InvalidCELExpressionReason, "%s", errMsg)
				obj.Status.ObservedGeneration = obj.Generation
				r.event(obj, revision, originRevision, eventv1.EventSeverityError, errMsg, nil)
				return ctrl.Result{}, err
			}

			// Retry on transient errors.
			conditions.MarkFalse(obj, meta.ReadyCondition, meta.DependencyNotReadyReason, "%s", err)
			msg := fmt.Sprintf("Dependencies do not meet ready condition, retrying in %s", r.DependencyRequeueInterval.String())
			log.Info(msg)
			r.event(obj, revision, originRevision, eventv1.EventSeverityInfo, msg, nil)
			return ctrl.Result{RequeueAfter: r.DependencyRequeueInterval}, nil
		}
		log.Info("All dependencies are ready, proceeding with reconciliation")
	}

	// Reconcile the latest revision.
	reconcileErr := r.reconcile(ctx, obj, artifactSource, patcher, statusReaders)

	// Requeue at the specified retry interval if the artifact tarball is not found.
	if errors.Is(reconcileErr, fetch.ErrFileNotFound) {
		msg := fmt.Sprintf("Source is not ready, artifact not found, retrying in %s", r.DependencyRequeueInterval.String())
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.ArtifactFailedReason, "%s", msg)
		log.Info(msg)
		return ctrl.Result{RequeueAfter: r.DependencyRequeueInterval}, nil
	}

	// Broadcast the reconciliation failure and requeue at the specified retry interval.
	if reconcileErr != nil {
		log.Error(reconcileErr, fmt.Sprintf("Reconciliation failed after %s, next try in %s",
			time.Since(reconcileStart).String(),
			obj.GetRetryInterval().String()),
			"revision",
			revision)
		r.event(obj, revision, originRevision, eventv1.EventSeverityError,
			reconcileErr.Error(), nil)
		return ctrl.Result{RequeueAfter: obj.GetRetryInterval()}, nil
	}

	// Requeue the reconciliation at the specified interval.
	return ctrl.Result{RequeueAfter: jitter.JitteredIntervalDuration(obj.GetRequeueAfter())}, nil
}

func (r *CueReconciler) reconcile(
	ctx context.Context,
	obj *cuev1.CueExport,
	src sourcev1.Source,
	patcher *patch.SerialPatcher,
	statusReaders []func(apimeta.RESTMapper) engine.StatusReader) error {
	reconcileStart := time.Now()
	log := ctrl.LoggerFrom(ctx)

	// Update status with the reconciliation progress.
	revision := src.GetArtifact().Revision
	originRevision := getOriginRevision(src)
	progressingMsg := fmt.Sprintf("Fetching manifests for revision %s with a timeout of %s", revision, obj.GetTimeout().String())
	conditions.MarkUnknown(obj, meta.ReadyCondition, meta.ProgressingReason, "%s", "Reconciliation in progress")
	conditions.MarkReconciling(obj, meta.ProgressingReason, "%s", progressingMsg)
	if err := r.patch(ctx, obj, patcher); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	// Create a snapshot of the current inventory.
	oldInventory := inventory.New()
	if obj.Status.Inventory != nil {
		obj.Status.Inventory.DeepCopyInto(oldInventory)
	}

	// Create tmp dir.
	tmpDir, err := MkdirTempAbs("", "cue-export-")
	if err != nil {
		err = fmt.Errorf("tmp dir error: %w", err)
		conditions.MarkFalse(obj, meta.ReadyCondition, sourcev1.DirCreationFailedReason, "%s", err)
		return err
	}

	defer func(path string) {
		if err := os.RemoveAll(path); err != nil {
			log.Error(err, "failed to remove tmp dir", "path", path)
		}
	}(tmpDir)

	// Set the artifact URL hostname override for localhost access.
	sourceLocalhost := os.Getenv("SOURCE_CONTROLLER_LOCALHOST")
	if strings.Contains(src.GetArtifact().URL, "//source-watcher") {
		sourceLocalhost = os.Getenv("SOURCE_WATCHER_LOCALHOST")
	}

	// Download artifact and extract files to the tmp dir.
	fetcher := fetch.New(
		fetch.WithLogger(ctrl.LoggerFrom(ctx)),
		fetch.WithRetries(r.ArtifactFetchRetries),
		fetch.WithMaxDownloadSize(tar.UnlimitedUntarSize),
		fetch.WithUntar(tar.WithMaxUntarSize(tar.UnlimitedUntarSize)),
		fetch.WithHostnameOverwrite(sourceLocalhost),
	)
	if err = fetcher.Fetch(src.GetArtifact().URL, src.GetArtifact().Digest, tmpDir); err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.ArtifactFailedReason, "%s", err)
		return err
	}

	moduleRootPath, err := securejoin.SecureJoin(tmpDir, obj.Spec.Root)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ArtifactFailedReason, "%s", err.Error())
		return err
	}

	if _, err := os.Stat(moduleRootPath); err != nil {
		err = fmt.Errorf("root path not found: %w", err)
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ArtifactFailedReason, "%s", err.Error())
	}

	// check build path exists
	for _, p := range obj.Spec.Paths {
		pp, err := securejoin.SecureJoin(tmpDir, p)
		if err != nil {
			conditions.MarkFalse(obj, meta.ReadyCondition, meta.ArtifactFailedReason, "%s", err)
			return err
		}

		if _, err := os.Stat(pp); err != nil {
			err = fmt.Errorf("path %s not found: %w", p, err)
			conditions.MarkFalse(obj, meta.ReadyCondition, meta.ArtifactFailedReason, "%s", err)
			return err
		}
	}

	// Report progress and set last attempted revision in status.
	obj.Status.LastAttemptedRevision = revision
	progressingMsg = fmt.Sprintf("Building manifests for revision %s with a timeout of %s", revision, obj.GetTimeout().String())
	conditions.MarkReconciling(obj, meta.ProgressingReason, "%s", progressingMsg)
	if err := r.patch(ctx, obj, patcher); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	// Configure the Kubernetes client for impersonation.
	var impersonatorOpts []runtimeClient.ImpersonatorOption
	var mustImpersonate bool
	if r.DefaultServiceAccount != "" || obj.Spec.ServiceAccountName != "" {
		mustImpersonate = true
		impersonatorOpts = append(impersonatorOpts,
			runtimeClient.WithServiceAccount(r.DefaultServiceAccount, obj.Spec.ServiceAccountName, obj.GetNamespace()))
	}
	if obj.Spec.KubeConfig != nil {
		mustImpersonate = true
		provider := r.getProviderRESTConfigFetcher(obj)
		impersonatorOpts = append(impersonatorOpts,
			runtimeClient.WithKubeConfig(obj.Spec.KubeConfig, r.KubeConfigOpts, obj.GetNamespace(), provider))
	}
	if r.ClusterReader != nil || len(statusReaders) > 0 {
		impersonatorOpts = append(impersonatorOpts,
			runtimeClient.WithPolling(r.ClusterReader, statusReaders...))
	}
	impersonation := runtimeClient.NewImpersonator(r.Client, impersonatorOpts...)

	// Create the Kubernetes client that runs under impersonation.
	var kubeClient client.Client
	var statusPoller *polling.StatusPoller
	if mustImpersonate {
		kubeClient, statusPoller, err = impersonation.GetClient(ctx)
	} else {
		kubeClient, statusPoller = r.getClientAndPoller(statusReaders)
	}
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.ReconciliationFailedReason, "%s", err)
		return fmt.Errorf("failed to build kube client: %w", err)
	}

	values, err := r.values(ctx, moduleRootPath, obj)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.BuildFailedReason, "%s", err)
		return err
	}

	resources, err := r.build(ctx, values, obj)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.BuildFailedReason, "%s", err)
		return err
	}

	err = r.checkGates(ctx, values, obj)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.BuildFailedReason, "%s", err)
		return err
	}

	// Convert the build result into Kubernetes unstructured objects.
	objects, err := ssautil.ReadObjects(bytes.NewReader(resources))
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.BuildFailedReason, "%s", err)
		return err
	}

	// Calculate the digest of the built resources for history tracking.
	checksum := digest.FromBytes(resources).String()
	historyMeta := map[string]string{"revision": revision}
	if originRevision != "" {
		historyMeta["originRevision"] = originRevision
	}

	// Create the server-side apply manager.
	resourceManager := ssa.NewResourceManager(kubeClient, statusPoller, ssa.Owner{
		Field: r.ControllerName,
		Group: cuev1.GroupVersion.Group,
	})
	resourceManager.SetOwnerLabels(objects, obj.GetName(), obj.GetNamespace())
	resourceManager.SetConcurrency(r.ConcurrentSSA)

	// Update status with the reconciliation progress.
	progressingMsg = fmt.Sprintf("Detecting drift for revision %s with a timeout of %s", revision, obj.GetTimeout().String())
	conditions.MarkReconciling(obj, meta.ProgressingReason, "%s", progressingMsg)
	if err := r.patch(ctx, obj, patcher); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	// Validate and apply resources in stages.
	drifted, changeSetWithSkipped, err := r.apply(ctx, resourceManager, obj, revision, originRevision, objects)
	if err != nil {
		obj.Status.History.Upsert(checksum, time.Now(), time.Since(reconcileStart), meta.ReconciliationFailedReason, historyMeta)
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.ReconciliationFailedReason, "%s", err)
		return err
	}

	// Filter out skipped entries from the change set.
	changeSet := ssa.NewChangeSet()
	skippedSet := make(map[object.ObjMetadata]struct{})
	for _, entry := range changeSetWithSkipped.Entries {
		if entry.Action == ssa.SkippedAction {
			skippedSet[entry.ObjMetadata] = struct{}{}
		} else {
			changeSet.Add(entry)
		}
	}

	// Create an inventory from the reconciled resources.
	newInventory := inventory.New()
	err = inventory.AddChangeSet(newInventory, changeSet)
	if err != nil {
		obj.Status.History.Upsert(checksum, time.Now(), time.Since(reconcileStart), meta.ReconciliationFailedReason, historyMeta)
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.ReconciliationFailedReason, "%s", err)
		return err
	}

	// Set last applied inventory in status.
	obj.Status.Inventory = newInventory

	// Detect stale resources which are subject to garbage collection.
	staleObjects, err := inventory.Diff(oldInventory, newInventory, skippedSet)
	if err != nil {
		obj.Status.History.Upsert(checksum, time.Now(), time.Since(reconcileStart), meta.ReconciliationFailedReason, historyMeta)
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.ReconciliationFailedReason, "%s", err)
		return err
	}

	// Run garbage collection for stale resources that do not have pruning disabled.
	if _, err := r.prune(ctx, resourceManager, obj, revision, originRevision, staleObjects); err != nil {
		obj.Status.History.Upsert(checksum, time.Now(), time.Since(reconcileStart), meta.PruneFailedReason, historyMeta)
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.PruneFailedReason, "%s", err)
		return err
	}

	// Run the health checks for the last applied resources.
	isNewRevision := !src.GetArtifact().HasRevision(obj.Status.LastAppliedRevision)
	if err := r.checkHealth(ctx,
		resourceManager,
		patcher,
		obj,
		revision,
		originRevision,
		isNewRevision,
		drifted,
		changeSet.ToObjMetadataSet()); err != nil {
		obj.Status.History.Upsert(checksum, time.Now(), time.Since(reconcileStart), meta.HealthCheckFailedReason, historyMeta)
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.HealthCheckFailedReason, "%s", err)
		return err
	}

	// Set last applied revisions.
	obj.Status.LastAppliedRevision = revision
	obj.Status.LastAppliedOriginRevision = originRevision

	// Mark the object as ready.
	conditions.MarkTrue(obj,
		meta.ReadyCondition,
		meta.ReconciliationSucceededReason,
		"Applied revision: %s", revision)
	obj.Status.History.Upsert(checksum,
		time.Now(),
		time.Since(reconcileStart),
		meta.ReconciliationSucceededReason,
		historyMeta)

	return nil
}

// checkDependencies checks if the dependencies of the current Kustomization are ready.
// To be considered ready, a dependencies must meet the following criteria:
// - The dependency exists in the API server.
// - The CEL expression (if provided) must evaluate to true.
// - The dependency observed generation must match the current generation.
// - The dependency Ready condition must be true.
// - The dependency last applied revision must match the current source artifact revision.
func (r *CueReconciler) checkDependencies(ctx context.Context,
	obj *cuev1.CueExport,
	source sourcev1.Source) error {

	// Convert the Kustomization object to Unstructured for CEL evaluation.
	objMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return fmt.Errorf("failed to convert CueExport to unstructured: %w", err)
	}

	for _, depRef := range obj.Spec.DependsOn {
		// Check if the dependency exists by querying
		// the API server bypassing the cache.
		if depRef.Namespace == "" {
			depRef.Namespace = obj.GetNamespace()
		}
		depName := types.NamespacedName{
			Namespace: depRef.Namespace,
			Name:      depRef.Name,
		}
		var dep cuev1.CueExport
		err := r.APIReader.Get(ctx, depName, &dep)
		if err != nil {
			return fmt.Errorf("dependency '%s' not found: %w", depName, err)
		}

		// Evaluate the CEL expression (if specified) to determine if the dependency is ready.
		if depRef.ReadyExpr != "" {
			ready, err := r.evalReadyExpr(ctx, depRef.ReadyExpr, objMap, &dep)
			if err != nil {
				return err
			}
			if !ready {
				return fmt.Errorf("dependency '%s' is not ready according to readyExpr eval", depName)
			}
		}

		// Skip the built-in readiness check if the CEL expression is provided
		// and the AdditiveCELDependencyCheck feature gate is not enabled.
		if depRef.ReadyExpr != "" && !r.AdditiveCELDependencyCheck {
			continue
		}

		// Check if the dependency observed generation is up to date
		// and if the dependency is in a ready state.
		if len(dep.Status.Conditions) == 0 || dep.Generation != dep.Status.ObservedGeneration {
			return fmt.Errorf("dependency '%s' is not ready", depName)
		}
		if !apimeta.IsStatusConditionTrue(dep.Status.Conditions, meta.ReadyCondition) {
			return fmt.Errorf("dependency '%s' is not ready", depName)
		}

		// Check if the dependency source matches the current source
		// and if so, verify that the last applied revision of the dependency
		// matches the current source artifact revision.
		srcNamespace := dep.Spec.SourceRef.Namespace
		if srcNamespace == "" {
			srcNamespace = dep.GetNamespace()
		}
		depSrcNamespace := obj.Spec.SourceRef.Namespace
		if depSrcNamespace == "" {
			depSrcNamespace = obj.GetNamespace()
		}
		if dep.Spec.SourceRef.Name == obj.Spec.SourceRef.Name &&
			srcNamespace == depSrcNamespace &&
			dep.Spec.SourceRef.Kind == obj.Spec.SourceRef.Kind &&
			!source.GetArtifact().HasRevision(dep.Status.LastAppliedRevision) {
			return fmt.Errorf("dependency '%s' revision is not up to date", depName)
		}
	}

	return nil
}

// evalReadyExpr evaluates the CEL expression for the dependency readiness check.
func (r *CueReconciler) evalReadyExpr(
	ctx context.Context,
	expr string,
	selfMap map[string]any,
	dep *cuev1.CueExport,
) (bool, error) {
	const (
		selfName = "self"
		depName  = "dep"
	)

	celExpr, err := cel.NewExpression(expr,
		cel.WithCompile(),
		cel.WithOutputType(celtypes.BoolType),
		cel.WithStructVariables(selfName, depName))
	if err != nil {
		return false, reconcile.TerminalError(fmt.Errorf("failed to evaluate dependency %s: %w", dep.Name, err))
	}

	depMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(dep)
	if err != nil {
		return false, fmt.Errorf("failed to convert %s object to map: %w", depName, err)
	}

	vars := map[string]any{
		selfName: selfMap,
		depName:  depMap,
	}

	return celExpr.EvaluateBoolean(ctx, vars)
}

// getSource resolves the source reference and returns the source object containing the artifact.
// It returns an error if the source is not found or if access is denied.
func (r *CueReconciler) getSource(ctx context.Context,
	obj *cuev1.CueExport) (sourcev1.Source, error) {
	var src sourcev1.Source
	sourceNamespace := obj.GetNamespace()
	if obj.Spec.SourceRef.Namespace != "" {
		sourceNamespace = obj.Spec.SourceRef.Namespace
	}
	namespacedName := types.NamespacedName{
		Namespace: sourceNamespace,
		Name:      obj.Spec.SourceRef.Name,
	}

	// Check if cross-namespace references are allowed.
	if r.NoCrossNamespaceRefs && sourceNamespace != obj.GetNamespace() {
		return src, acl.AccessDeniedError(
			fmt.Sprintf("can't access '%s/%s', cross-namespace references have been blocked",
				obj.Spec.SourceRef.Kind, namespacedName))
	}

	// Check if ExternalArtifact kind is allowed.
	if obj.Spec.SourceRef.Kind == sourcev1.ExternalArtifactKind && !r.AllowExternalArtifact {
		return src, acl.AccessDeniedError(
			fmt.Sprintf("can't access '%s/%s', %s feature gate is disabled",
				obj.Spec.SourceRef.Kind, namespacedName, "ExternalArtifact"))
	}

	switch obj.Spec.SourceRef.Kind {
	case sourcev1.OCIRepositoryKind:
		var repository sourcev1.OCIRepository
		err := r.Client.Get(ctx, namespacedName, &repository)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return src, err
			}
			return src, fmt.Errorf("unable to get source '%s': %w", namespacedName, err)
		}
		src = &repository
	case sourcev1.GitRepositoryKind:
		var repository sourcev1.GitRepository
		err := r.Client.Get(ctx, namespacedName, &repository)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return src, err
			}
			return src, fmt.Errorf("unable to get source '%s': %w", namespacedName, err)
		}
		src = &repository
	case sourcev1.BucketKind:
		var bucket sourcev1.Bucket
		err := r.Client.Get(ctx, namespacedName, &bucket)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return src, err
			}
			return src, fmt.Errorf("unable to get source '%s': %w", namespacedName, err)
		}
		src = &bucket
	case sourcev1.ExternalArtifactKind:
		var ea sourcev1.ExternalArtifact
		err := r.Client.Get(ctx, namespacedName, &ea)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return src, err
			}
			return src, fmt.Errorf("unable to get source '%s': %w", namespacedName, err)
		}
		src = &ea
	default:
		return src, fmt.Errorf("source `%s` kind '%s' not supported",
			obj.Spec.SourceRef.Name, obj.Spec.SourceRef.Kind)
	}
	return src, nil
}

func (r *CueReconciler) values(ctx context.Context, root string, obj *cuev1.CueExport) ([]cue.Value, error) {
	cctx := cuecontext.New()

	tags := make([]string, 0, len(obj.Spec.Tags))
	for _, t := range obj.Spec.Tags {
		if t.ValueFrom != nil {
			val, err := r.getValueFromSource(ctx, obj.Namespace, t.ValueFrom)
			if err != nil {
				return nil, err
			}
			tags = append(tags, fmt.Sprintf("%s=%s", t.Name, val))
		} else if t.Value != "" {
			tags = append(tags, fmt.Sprintf("%s=%s", t.Name, t.Value))
		} else {
			tags = append(tags, t.Name)
		}
	}

	cfg := &load.Config{
		Dir:        root,
		ModuleRoot: root,
		Package:    obj.Spec.Package,
		Tags:       tags,
		TagVars:    load.DefaultTagVars(),
	}

	ix := load.Instances(obj.Spec.Paths, cfg)

	for _, inst := range ix {
		if inst.Err != nil {
			return nil, inst.Err
		}
	}

	values, err := cctx.BuildInstances(ix)
	if err != nil {
		return nil, fmt.Errorf("failed to build instances: %w", err)
	}

	return values, nil
}

func (r *CueReconciler) getValueFromSource(ctx context.Context, namespace string, tag *cuev1.TagSource) (string, error) {
	if tag.ConfigMapKeyRef != nil {
		ref := types.NamespacedName{
			Namespace: namespace,
			Name:      tag.ConfigMapKeyRef.Name,
		}
		var cm corev1.ConfigMap
		if err := r.Get(ctx, ref, &cm); err != nil {
			return "", fmt.Errorf("failed to get configmap: %w", err)
		}
		val, ok := cm.Data[tag.ConfigMapKeyRef.Key]
		if !ok {
			return "", fmt.Errorf("missing key %s in ConfigMap %s", tag.ConfigMapKeyRef.Key, tag.ConfigMapKeyRef.Name)
		}
		return val, nil
	} else if tag.SecretKeyRef != nil {
		ref := types.NamespacedName{
			Namespace: namespace,
			Name:      tag.SecretKeyRef.Name,
		}
		var cm corev1.Secret
		if err := r.Get(ctx, ref, &cm); err != nil {
			return "", fmt.Errorf("failed to get secret: %w", err)
		}
		val, ok := cm.Data[tag.SecretKeyRef.Key]
		if !ok {
			return "", fmt.Errorf("missing key %s in Secret %s", tag.SecretKeyRef.Key, tag.SecretKeyRef.Name)
		}
		return string(val), nil
	}
	return "", fmt.Errorf("either ConfigMap or Secret has to be specified")
}

var (
	apiVersionPath   = cue.ParsePath("apiVersion")
	kindPath         = cue.ParsePath("kind")
	metadataNamePath = cue.ParsePath("metadata.name")
)

func (r *CueReconciler) build(ctx context.Context, values []cue.Value, obj *cuev1.CueExport) ([]byte, error) {
	log := ctrl.LoggerFrom(ctx)
	timeout := obj.GetTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	log.V(1).Info("building resources", "len(values)", len(values))

	resources := [][]byte{}
	errors := []error{}
	for _, value := range values {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to build cue values: %w", ctx.Err())
		}
		log.V(1).Info("searching for resources", "value.Kind", value.Kind())

		if len(obj.Spec.Exprs) > 0 {
			for _, e := range obj.Spec.Exprs {
				log.V(1).Info("evaluating expr", "expr", e)
				ex, err := parser.ParseExpr("", e)
				if err != nil {
					errors = append(errors, err)
				} else {
					result := value.Context().BuildExpr(ex, cue.Scope(value), cue.InferBuiltins(true))
					moreResources, err := marshalMaybeList(result)
					if err != nil {
						errors = append(errors, err)
					}
					resources = append(resources, moreResources...)
				}
			}
		} else {
			err := value.Err()
			log.V(1).Info("searching for apiVersion, kind, and metadata.name fields in value", "err", err)
			if err != nil {
				errors = append(errors, err)
			}
			value.Walk(func(v cue.Value) bool {
				if v.Kind() == cue.StructKind &&
					v.LookupPath(apiVersionPath).Exists() &&
					v.LookupPath(kindPath).Exists() &&
					v.LookupPath(metadataNamePath).Exists() {

					log.V(1).Info("found for apiVersion, kind, and metadata.name fields in struct", "path", v.Path())

					resource, err := v.MarshalJSON()
					if err != nil {
						errors = append(errors, err)
						log.V(1).Info("added err to to errors", "error", err)
					} else {
						resources = append(resources, resource)
						log.V(1).Info("added value to objects")
					}

					return false
				}
				return true
			}, nil)
		}
	}

	if len(errors) > 0 {
		return nil, kerrors.NewAggregate(errors)
	}

	if len(resources) == 0 {
		return nil, fmt.Errorf("found no objects in values")
	}

	return bytes.Join(resources, []byte("\n---\n")), nil
}

func (r *CueReconciler) checkGates(ctx context.Context, values []cue.Value, obj *cuev1.CueExport) error {
	log := ctrl.LoggerFrom(ctx)

	var errors []error
	for _, g := range obj.Spec.Gates {
		ex, err := parser.ParseExpr("", g.Expr)
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to parse gate expr: %w", err))
			continue
		}

		for _, value := range values {
			result := value.Context().BuildExpr(ex, cue.Scope(value), cue.InferBuiltins(true))

			valid := result.Validate()
			open, err := result.Bool()
			if !open || valid != nil {
				log.Info("gate check failed", "gate", g.Name, "expr", g.Expr, "result", result, "error", err)
				errors = append(errors, fmt.Errorf("%s failed: %w", g.Name, err))
			}
		}
	}

	return kerrors.NewAggregate(errors)
}

func marshalMaybeList(value cue.Value) ([][]byte, error) {
	errors := []error{}
	objects := [][]byte{}
	switch value.Kind() {
	case cue.StructKind:
		obj, err := value.MarshalJSON()
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	case cue.ListKind:
		objects := [][]byte{}
		items, err := value.List()
		if err != nil {
			return nil, err
		}
		for hasNext := items.Next(); hasNext; hasNext = items.Next() {
			object, err := items.Value().MarshalJSON()
			if err != nil {
				errors = append(errors, err)
			} else {
				objects = append(objects, object)
			}
		}
	default:
		return nil, fmt.Errorf("unknown kubernetes object kind %v", value)
	}
	return objects, kerrors.NewAggregate(errors)
}

func (r *CueReconciler) apply(ctx context.Context,
	manager *ssa.ResourceManager,
	obj *cuev1.CueExport,
	revision string,
	originRevision string,
	objects []*unstructured.Unstructured) (bool, *ssa.ChangeSet, error) {
	log := ctrl.LoggerFrom(ctx)

	if err := normalize.UnstructuredList(objects); err != nil {
		return false, nil, err
	}

	if cmeta := obj.Spec.CommonMetadata; cmeta != nil {
		ssautil.SetCommonMetadata(objects, cmeta.Labels, cmeta.Annotations)
	}

	applyOpts := ssa.DefaultApplyOptions()
	applyOpts.Force = obj.Spec.Force
	applyOpts.ExclusionSelector = map[string]string{
		fmt.Sprintf("%s/reconcile", cuev1.GroupVersion.Group): cuev1.DisabledValue,
		fmt.Sprintf("%s/ssa", cuev1.GroupVersion.Group):       cuev1.IgnoreValue,
	}
	applyOpts.IfNotPresentSelector = map[string]string{
		fmt.Sprintf("%s/ssa", cuev1.GroupVersion.Group): cuev1.IfNotPresentValue,
	}
	applyOpts.ForceSelector = map[string]string{
		fmt.Sprintf("%s/force", cuev1.GroupVersion.Group): cuev1.EnabledValue,
	}

	fieldManagers := []ssa.FieldManager{
		{
			// to undo changes made with 'kubectl apply --server-side --force-conflicts'
			Name:          "kubectl",
			OperationType: metav1.ManagedFieldsOperationApply,
		},
		{
			// to undo changes made with 'kubectl apply'
			Name:          "kubectl",
			OperationType: metav1.ManagedFieldsOperationUpdate,
		},
		{
			// to undo changes made with 'kubectl apply'
			Name:          "before-first-apply",
			OperationType: metav1.ManagedFieldsOperationUpdate,
		},
		{
			// to undo changes made by the controller before SSA
			Name:          r.ControllerName,
			OperationType: metav1.ManagedFieldsOperationUpdate,
		},
	}

	for _, fieldManager := range r.DisallowedFieldManagers {
		fieldManagers = append(fieldManagers, ssa.FieldManager{
			Name:          fieldManager,
			OperationType: metav1.ManagedFieldsOperationApply,
		})
		// to undo changes made by the controller before SSA
		fieldManagers = append(fieldManagers, ssa.FieldManager{
			Name:          fieldManager,
			OperationType: metav1.ManagedFieldsOperationUpdate,
		})
	}

	applyOpts.Cleanup = ssa.ApplyCleanupOptions{
		Annotations: []string{
			// remove the kubectl annotation
			corev1.LastAppliedConfigAnnotation,
		},
		FieldManagers: fieldManagers,
		Exclusions: map[string]string{
			fmt.Sprintf("%s/ssa", cuev1.GroupVersion.Group): cuev1.MergeValue,
		},
	}

	// contains the objects' metadata after apply
	resultSet := ssa.NewChangeSet()
	var changeSetLog strings.Builder

	if len(objects) > 0 {
		changeSet, err := manager.ApplyAllStaged(ctx, objects, applyOpts)

		if changeSet != nil && len(changeSet.Entries) > 0 {
			resultSet.Append(changeSet.Entries)

			// filter out the objects that have not changed
			for _, change := range changeSet.Entries {
				if HasChanged(change.Action) {
					changeSetLog.WriteString(change.String() + "\n")
				}
			}
		}

		// include the change log in the error message in case af a partial apply
		if err != nil {
			return false, nil, fmt.Errorf("%w\n%s", err, changeSetLog.String())
		}

		// log all applied objects
		if changeSet != nil && len(changeSet.Entries) > 0 {
			if r.GroupChangeLog {
				log.Info("server-side apply completed", "output", changeSet.ToGroupedMap(), "revision", revision)
			} else {
				log.Info("server-side apply completed", "output", changeSet.ToMap(), "revision", revision)
			}
		}
	}

	// emit event only if the server-side apply resulted in changes
	applyLog := strings.TrimSuffix(changeSetLog.String(), "\n")
	if applyLog != "" {
		r.event(obj, revision, originRevision, eventv1.EventSeverityInfo, applyLog, nil)
	}

	return applyLog != "", resultSet, nil
}

func (r *CueReconciler) checkHealth(ctx context.Context,
	manager *ssa.ResourceManager,
	patcher *patch.SerialPatcher,
	obj *cuev1.CueExport,
	revision string,
	originRevision string,
	isNewRevision bool,
	drifted bool,
	objects object.ObjMetadataSet) error {
	if len(obj.Spec.HealthChecks) == 0 && !obj.Spec.Wait {
		conditions.Delete(obj, meta.HealthyCondition)
		return nil
	}

	checkStart := time.Now()
	var err error
	if !obj.Spec.Wait {
		objects, err = inventory.ReferenceToObjMetadataSet(obj.Spec.HealthChecks)
		if err != nil {
			return err
		}
	}

	if len(objects) == 0 {
		conditions.Delete(obj, meta.HealthyCondition)
		return nil
	}

	// Guard against deadlock (waiting on itself).
	var toCheck []object.ObjMetadata
	for _, o := range objects {
		if o.GroupKind.Kind == cuev1.CueExportKind &&
			o.Name == obj.GetName() &&
			o.Namespace == obj.GetNamespace() {
			continue
		}
		toCheck = append(toCheck, o)
	}

	// Find the previous health check result.
	wasHealthy := apimeta.IsStatusConditionTrue(obj.Status.Conditions, meta.HealthyCondition)

	// Update status with the reconciliation progress.
	message := fmt.Sprintf("Running health checks for revision %s with a timeout of %s", revision, obj.GetTimeout().String())
	conditions.MarkReconciling(obj, meta.ProgressingReason, "%s", message)
	conditions.MarkUnknown(obj, meta.HealthyCondition, meta.ProgressingReason, "%s", message)
	if err := r.patch(ctx, obj, patcher); err != nil {
		return fmt.Errorf("unable to update the healthy status to progressing: %w", err)
	}

	// Check the health with a default timeout of 30sec shorter than the reconciliation interval.
	healthCtx := ctx
	if r.CancelHealthCheckOnNewRevision {
		// Create a cancellable context for health checks that monitors for new revisions
		var cancel context.CancelFunc
		healthCtx, cancel = context.WithCancel(ctx)
		defer cancel()

		// Start monitoring for new revisions to allow early cancellation
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-healthCtx.Done():
					return
				case <-ticker.C:
					// Get the latest source artifact
					latestSrc, err := r.getSource(ctx, obj)
					if err == nil && latestSrc.GetArtifact() != nil {
						if newRevision := latestSrc.GetArtifact().Revision; newRevision != revision {
							const msg = "New revision detected during health check, cancelling"
							r.event(obj, revision, originRevision, eventv1.EventSeverityInfo, msg, nil)
							ctrl.LoggerFrom(ctx).Info(msg, "current", revision, "new", newRevision)
							cancel()
							return
						}
					}
				}
			}
		}()
	}
	if err := manager.WaitForSetWithContext(healthCtx, toCheck, ssa.WaitOptions{
		Interval: 5 * time.Second,
		Timeout:  obj.GetTimeout(),
		FailFast: r.FailFast,
	}); err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, meta.HealthCheckFailedReason, "%s", err)
		conditions.MarkFalse(obj, meta.HealthyCondition, meta.HealthCheckFailedReason, "%s", err)
		return fmt.Errorf("health check failed after %s: %w", time.Since(checkStart).String(), err)
	}

	// Emit recovery event if the previous health check failed.
	msg := fmt.Sprintf("Health check passed in %s", time.Since(checkStart).String())
	if !wasHealthy || (isNewRevision && drifted) {
		r.event(obj, revision, originRevision, eventv1.EventSeverityInfo, msg, nil)
	}

	conditions.MarkTrue(obj, meta.HealthyCondition, meta.SucceededReason, "%s", msg)
	if err := r.patch(ctx, obj, patcher); err != nil {
		return fmt.Errorf("unable to update the healthy status to progressing: %w", err)
	}

	return nil
}

func (r *CueReconciler) prune(ctx context.Context,
	manager *ssa.ResourceManager,
	obj *cuev1.CueExport,
	revision string,
	originRevision string,
	objects []*unstructured.Unstructured) (bool, error) {
	if !obj.Spec.Prune {
		return false, nil
	}

	log := ctrl.LoggerFrom(ctx)

	opts := ssa.DeleteOptions{
		PropagationPolicy: metav1.DeletePropagationBackground,
		Inclusions:        manager.GetOwnerLabels(obj.Name, obj.Namespace),
		Exclusions: map[string]string{
			fmt.Sprintf("%s/prune", cuev1.GroupVersion.Group):     cuev1.DisabledValue,
			fmt.Sprintf("%s/reconcile", cuev1.GroupVersion.Group): cuev1.DisabledValue,
		},
	}

	changeSet, err := manager.DeleteAll(ctx, objects, opts)
	if err != nil {
		return false, err
	}

	// emit event only if the prune operation resulted in changes
	if changeSet != nil && len(changeSet.Entries) > 0 {
		log.Info(fmt.Sprintf("garbage collection completed: %s", changeSet.String()))
		r.event(obj, revision, originRevision, eventv1.EventSeverityInfo, changeSet.String(), nil)
		return true, nil
	}

	return false, nil
}

// finalizerShouldDeleteResources determines if resources should be deleted
// based on the object's inventory and deletion policy.
// A suspended CueExport or one without an inventory will not delete resources.
func finalizerShouldDeleteResources(obj *cuev1.CueExport) bool {
	if obj.Spec.Suspend {
		return false
	}

	if obj.Status.Inventory == nil || len(obj.Status.Inventory.Entries) == 0 {
		return false
	}

	switch obj.GetDeletionPolicy() {
	case cuev1.DeletionPolicyMirrorPrune:
		return obj.Spec.Prune
	case cuev1.DeletionPolicyDelete:
		return true
	case cuev1.DeletionPolicyWaitForTermination:
		return true
	default:
		return false
	}
}

// finalize handles the finalization logic for a Kustomization resource during its deletion process.
// Managed resources are pruned based on the deletion policy and suspended state of the Kustomization.
// When the policy is set to WaitForTermination, the function blocks and waits for the resources
// to be terminated by the Kubernetes Garbage Collector for the specified timeout duration.
// If the service account used for impersonation is no longer available or if a timeout occurs
// while waiting for resources to be terminated, an error is logged and the finalizer is removed.
func (r *CueReconciler) finalize(ctx context.Context,
	obj *cuev1.CueExport) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	if finalizerShouldDeleteResources(obj) {
		objects, _ := inventory.List(obj.Status.Inventory)

		var impersonatorOpts []runtimeClient.ImpersonatorOption
		var mustImpersonate bool
		if r.DefaultServiceAccount != "" || obj.Spec.ServiceAccountName != "" {
			mustImpersonate = true
			impersonatorOpts = append(impersonatorOpts,
				runtimeClient.WithServiceAccount(r.DefaultServiceAccount, obj.Spec.ServiceAccountName, obj.GetNamespace()))
		}
		if obj.Spec.KubeConfig != nil {
			mustImpersonate = true
			provider := r.getProviderRESTConfigFetcher(obj)
			impersonatorOpts = append(impersonatorOpts,
				runtimeClient.WithKubeConfig(obj.Spec.KubeConfig, r.KubeConfigOpts, obj.GetNamespace(), provider))
		}
		if r.ClusterReader != nil {
			impersonatorOpts = append(impersonatorOpts, runtimeClient.WithPolling(r.ClusterReader))
		}
		impersonation := runtimeClient.NewImpersonator(r.Client, impersonatorOpts...)
		if impersonation.CanImpersonate(ctx) {
			var kubeClient client.Client
			var err error
			if mustImpersonate {
				kubeClient, _, err = impersonation.GetClient(ctx)
			} else {
				kubeClient = r.Client
			}
			if err != nil {
				return ctrl.Result{}, err
			}

			resourceManager := ssa.NewResourceManager(kubeClient, nil, ssa.Owner{
				Field: r.ControllerName,
				Group: cuev1.GroupVersion.Group,
			})

			opts := ssa.DeleteOptions{
				PropagationPolicy: metav1.DeletePropagationBackground,
				Inclusions:        resourceManager.GetOwnerLabels(obj.Name, obj.Namespace),
				Exclusions: map[string]string{
					fmt.Sprintf("%s/prune", cuev1.GroupVersion.Group):     cuev1.DisabledValue,
					fmt.Sprintf("%s/reconcile", cuev1.GroupVersion.Group): cuev1.DisabledValue,
				},
			}

			changeSet, err := resourceManager.DeleteAll(ctx, objects, opts)
			if err != nil {
				r.event(obj, obj.Status.LastAppliedRevision, obj.Status.LastAppliedOriginRevision, eventv1.EventSeverityError, "pruning for deleted resource failed", nil)
				// Return the error so we retry the failed garbage collection
				return ctrl.Result{}, err
			}

			if changeSet != nil && len(changeSet.Entries) > 0 {
				// Emit event with the resources marked for deletion.
				r.event(obj, obj.Status.LastAppliedRevision, obj.Status.LastAppliedOriginRevision, eventv1.EventSeverityInfo, changeSet.String(), nil)

				// Wait for the resources marked for deletion to be terminated.
				if obj.GetDeletionPolicy() == cuev1.DeletionPolicyWaitForTermination {
					if err := resourceManager.WaitForSetTermination(changeSet, ssa.WaitOptions{
						Interval: 2 * time.Second,
						Timeout:  obj.GetTimeout(),
					}); err != nil {
						// Emit an event and log the error if a timeout occurs.
						msg := "failed to wait for resources termination"
						log.Error(err, msg)
						r.event(obj, obj.Status.LastAppliedRevision, obj.Status.LastAppliedOriginRevision, eventv1.EventSeverityError, msg, nil)
					}
				}
			}
		} else {
			// when the account to impersonate is gone, log the stale objects and continue with the finalization
			msg := fmt.Sprintf("unable to prune objects: \n%s", ssautil.FmtUnstructuredList(objects))
			log.Error(fmt.Errorf("skiping pruning, failed to find account to impersonate"), msg)
			r.event(obj, obj.Status.LastAppliedRevision, obj.Status.LastAppliedOriginRevision, eventv1.EventSeverityError, msg, nil)
		}
	}

	// Remove our finalizer from the list and update it
	controllerutil.RemoveFinalizer(obj, cuev1.CueExportFinalizer)

	// Cleanup caches.
	for _, op := range cuev1.AllMetrics {
		r.TokenCache.DeleteEventsForObject(cuev1.CueExportKind, obj.GetName(), obj.GetNamespace(), op)
	}

	// Stop reconciliation as the object is being deleted
	return ctrl.Result{}, nil
}

func (r *CueReconciler) event(obj *cuev1.CueExport,
	revision, originRevision, severity, msg string,
	metadata map[string]string) {
	if metadata == nil {
		metadata = map[string]string{}
	}
	if revision != "" {
		metadata[cuev1.GroupVersion.Group+"/"+eventv1.MetaRevisionKey] = revision
	}
	if originRevision != "" {
		metadata[cuev1.GroupVersion.Group+"/"+eventv1.MetaOriginRevisionKey] = originRevision
	}

	reason := severity
	if r := conditions.GetReason(obj, meta.ReadyCondition); r != "" {
		reason = r
	}

	eventtype := "Normal"
	if severity == eventv1.EventSeverityError {
		eventtype = "Warning"
	}

	r.EventRecorder.AnnotatedEventf(obj, metadata, eventtype, reason, msg)
}

func (r *CueReconciler) finalizeStatus(ctx context.Context,
	obj *cuev1.CueExport,
	patcher *patch.SerialPatcher) error {
	// Set the value of the reconciliation request in status.
	if v, ok := meta.ReconcileAnnotationValue(obj.GetAnnotations()); ok {
		obj.Status.LastHandledReconcileAt = v
	}

	// Remove the Reconciling condition and update the observed generation
	// if the reconciliation was successful.
	if conditions.IsTrue(obj, meta.ReadyCondition) {
		conditions.Delete(obj, meta.ReconcilingCondition)
		obj.Status.ObservedGeneration = obj.Generation
	}

	// Set the Reconciling reason to ProgressingWithRetry if the
	// reconciliation has failed.
	if conditions.IsFalse(obj, meta.ReadyCondition) &&
		conditions.Has(obj, meta.ReconcilingCondition) {
		rc := conditions.Get(obj, meta.ReconcilingCondition)
		rc.Reason = meta.ProgressingWithRetryReason
		conditions.Set(obj, rc)
	}

	// Patch finalizers, status and conditions.
	return r.patch(ctx, obj, patcher)
}

func (r *CueReconciler) patch(ctx context.Context,
	obj *cuev1.CueExport,
	patcher *patch.SerialPatcher) (retErr error) {

	// Configure the runtime patcher.
	patchOpts := []patch.Option{}
	ownedConditions := []string{
		meta.HealthyCondition,
		meta.ReadyCondition,
		meta.ReconcilingCondition,
		meta.StalledCondition,
	}
	patchOpts = append(patchOpts,
		patch.WithOwnedConditions{Conditions: ownedConditions},
		patch.WithForceOverwriteConditions{},
		patch.WithFieldOwner(r.StatusManager),
	)

	// Patch the object status, conditions and finalizers.
	if err := patcher.Patch(ctx, obj, patchOpts...); err != nil {
		if !obj.GetDeletionTimestamp().IsZero() {
			err = kerrors.FilterOut(err, func(e error) bool { return apierrors.IsNotFound(e) })
		}
		retErr = kerrors.NewAggregate([]error{retErr, err})
		if retErr != nil {
			return retErr
		}
	}

	return nil
}

// getClientAndPoller creates a status poller with the custom status readers
// from CEL expressions and the custom job status reader, and returns the
// Kubernetes client of the controller and the status poller.
// Should be used for reconciliations that are not configured to use
// ServiceAccount impersonation or kubeconfig.
func (r *CueReconciler) getClientAndPoller(
	readerCtors []func(apimeta.RESTMapper) engine.StatusReader,
) (client.Client, *polling.StatusPoller) {

	readers := make([]engine.StatusReader, 0, 1+len(readerCtors))
	readers = append(readers, statusreaders.NewCustomJobStatusReader(r.Mapper))
	for _, ctor := range readerCtors {
		readers = append(readers, ctor(r.Mapper))
	}

	poller := polling.NewStatusPoller(r.Client, r.Mapper, polling.Options{
		CustomStatusReaders:  readers,
		ClusterReaderFactory: r.ClusterReader,
	})

	return r.Client, poller
}

// getProviderRESTConfigFetcher returns a ProviderRESTConfigFetcher for the
// Kustomization object, which is used to fetch the kubeconfig for a ConfigMap
// reference in the Kustomization spec.
func (r *CueReconciler) getProviderRESTConfigFetcher(obj *cuev1.CueExport) runtimeClient.ProviderRESTConfigFetcher {
	var provider runtimeClient.ProviderRESTConfigFetcher
	if kc := obj.Spec.KubeConfig; kc != nil && kc.SecretRef == nil && kc.ConfigMapRef != nil {
		var opts []auth.Option
		if r.TokenCache != nil {
			involvedObject := cache.InvolvedObject{
				Kind:      cuev1.CueExportKind,
				Name:      obj.GetName(),
				Namespace: obj.GetNamespace(),
				Operation: cuev1.MetricFetchKubeConfig,
			}
			opts = append(opts, auth.WithCache(*r.TokenCache, involvedObject))
		}
		provider = runtimeClient.ProviderRESTConfigFetcher(authutils.GetRESTConfigFetcher(opts...))
	}
	return provider
}

// getOriginRevision returns the origin revision of the source artifact,
// or the empty string if it's not present, or if the artifact itself
// is not present.
func getOriginRevision(src sourcev1.Source) string {
	a := src.GetArtifact()
	if a == nil {
		return ""
	}
	return a.Metadata[OCIArtifactOriginRevisionAnnotation]
}
