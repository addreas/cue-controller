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

package controllers

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/load"
	"cuelang.org/go/cue/parser"
	securejoin "github.com/cyphar/filepath-securejoin"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	kuberecorder "k8s.io/client-go/tools/record"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling"
	"sigs.k8s.io/cli-utils/pkg/object"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/ratelimiter"
	"sigs.k8s.io/controller-runtime/pkg/source"

	apiacl "github.com/fluxcd/pkg/apis/acl"
	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/http/fetch"
	"github.com/fluxcd/pkg/runtime/acl"
	runtimeClient "github.com/fluxcd/pkg/runtime/client"
	"github.com/fluxcd/pkg/runtime/conditions"
	runtimeCtrl "github.com/fluxcd/pkg/runtime/controller"
	"github.com/fluxcd/pkg/runtime/patch"
	"github.com/fluxcd/pkg/runtime/predicates"
	"github.com/fluxcd/pkg/ssa"
	"github.com/fluxcd/pkg/tar"
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta2"

	cuev1 "github.com/addreas/cue-controller/api/v1beta2"
	"github.com/addreas/cue-controller/internal/inventory"
)

// +kubebuilder:rbac:groups=cue.toolkit.fluxcd.io,resources=cueexports,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cue.toolkit.fluxcd.io,resources=cueexports/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cue.toolkit.fluxcd.io,resources=cueexports/finalizers,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=buckets;ocirepositories;gitrepositories,verbs=get;list;watch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=buckets/status;ocirepositories/status;gitrepositories/status,verbs=get
// +kubebuilder:rbac:groups="",resources=configmaps;secrets;serviceaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// CueReconciler reconciles a CueExport object
type CueReconciler struct {
	client.Client
	kuberecorder.EventRecorder
	runtimeCtrl.Metrics

	artifactFetcher       *fetch.ArchiveFetcher
	requeueDependency     time.Duration
	StatusPoller          *polling.StatusPoller
	PollingOpts           polling.Options
	ControllerName        string
	statusManager         string
	NoCrossNamespaceRefs  bool
	NoRemoteBases         bool
	DefaultServiceAccount string
	KubeConfigOpts        runtimeClient.KubeConfigOptions
}

// CueReconcilerOptions contains options for the CueReconciler.
type CueReconcilerOptions struct {
	MaxConcurrentReconciles   int
	HTTPRetry                 int
	DependencyRequeueInterval time.Duration
	RateLimiter               ratelimiter.RateLimiter
}

func (r *CueReconciler) SetupWithManager(mgr ctrl.Manager, opts CueReconcilerOptions) error {
	const (
		ociRepositoryIndexKey string = ".metadata.ociRepository"
		gitRepositoryIndexKey string = ".metadata.gitRepository"
		bucketIndexKey        string = ".metadata.bucket"
	)

	// Index the CueExports by the OCIRepository references they (may) point at.
	if err := mgr.GetCache().IndexField(context.TODO(), &cuev1.CueExport{}, ociRepositoryIndexKey,
		r.indexBy(sourcev1.OCIRepositoryKind)); err != nil {
		return fmt.Errorf("failed setting index fields: %w", err)
	}

	// Index the CueExports by the GitRepository references they (may) point at.
	if err := mgr.GetCache().IndexField(context.TODO(), &cuev1.CueExport{}, gitRepositoryIndexKey,
		r.indexBy(sourcev1.GitRepositoryKind)); err != nil {
		return fmt.Errorf("failed setting index fields: %w", err)
	}

	// Index the CueExports by the Bucket references they (may) point at.
	if err := mgr.GetCache().IndexField(context.TODO(), &cuev1.CueExport{}, bucketIndexKey,
		r.indexBy(sourcev1.BucketKind)); err != nil {
		return fmt.Errorf("failed setting index fields: %w", err)
	}

	r.requeueDependency = opts.DependencyRequeueInterval
	r.statusManager = fmt.Sprintf("gotk-%s", r.ControllerName)
	r.artifactFetcher = fetch.NewArchiveFetcher(
		opts.HTTPRetry,
		tar.UnlimitedUntarSize,
		tar.UnlimitedUntarSize,
		os.Getenv("SOURCE_CONTROLLER_LOCALHOST"),
	)

	recoverPanic := true
	return ctrl.NewControllerManagedBy(mgr).
		For(&cuev1.CueExport{}, builder.WithPredicates(
			predicate.Or(predicate.GenerationChangedPredicate{}, predicates.ReconcileRequestedPredicate{}),
		)).
		Watches(
			&source.Kind{Type: &sourcev1.OCIRepository{}},
			handler.EnqueueRequestsFromMapFunc(r.requestsForRevisionChangeOf(ociRepositoryIndexKey)),
			builder.WithPredicates(SourceRevisionChangePredicate{}),
		).
		Watches(
			&source.Kind{Type: &sourcev1.GitRepository{}},
			handler.EnqueueRequestsFromMapFunc(r.requestsForRevisionChangeOf(gitRepositoryIndexKey)),
			builder.WithPredicates(SourceRevisionChangePredicate{}),
		).
		Watches(
			&source.Kind{Type: &sourcev1.Bucket{}},
			handler.EnqueueRequestsFromMapFunc(r.requestsForRevisionChangeOf(bucketIndexKey)),
			builder.WithPredicates(SourceRevisionChangePredicate{}),
		).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: opts.MaxConcurrentReconciles,
			RateLimiter:             opts.RateLimiter,
			RecoverPanic:            &recoverPanic,
		}).
		Complete(r)
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
		r.Metrics.RecordReadiness(ctx, obj)
		r.Metrics.RecordDuration(ctx, obj, reconcileStart)
		r.Metrics.RecordSuspend(ctx, obj, obj.Spec.Suspend)

		// Log and emit success event.
		if conditions.IsReady(obj) {
			msg := fmt.Sprintf("Reconciliation finished in %s, next run in %s",
				time.Since(reconcileStart).String(),
				obj.Spec.Interval.Duration.String())
			log.Info(msg, "revision", obj.Status.LastAttemptedRevision)
			r.event(obj, obj.Status.LastAppliedRevision, eventv1.EventSeverityInfo, msg,
				map[string]string{
					cuev1.GroupVersion.Group + "/" + eventv1.MetaCommitStatusKey: eventv1.MetaCommitStatusUpdateValue,
				})
		}
	}()

	// Add finalizer first if it doesn't exist to avoid the race condition
	// between init and delete.
	if !controllerutil.ContainsFinalizer(obj, cuev1.CueExportFinalizer) {
		controllerutil.AddFinalizer(obj, cuev1.CueExportFinalizer)
		return ctrl.Result{Requeue: true}, nil
	}

	// Prune managed resources if the object is under deletion.
	if !obj.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, obj)
	}

	// Skip reconciliation if the object is suspended.
	if obj.Spec.Suspend {
		log.Info("Reconciliation is suspended for this object")
		return ctrl.Result{}, nil
	}

	// Resolve the source reference and requeue the reconciliation if the source is not found.
	artifactSource, err := r.getSource(ctx, obj)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ArtifactFailedReason, err.Error())

		if apierrors.IsNotFound(err) {
			msg := fmt.Sprintf("Source '%s' not found", obj.Spec.SourceRef.String())
			log.Info(msg)
			return ctrl.Result{RequeueAfter: obj.GetRetryInterval()}, nil
		}

		if acl.IsAccessDenied(err) {
			conditions.MarkFalse(obj, meta.ReadyCondition, apiacl.AccessDeniedReason, err.Error())
			log.Error(err, "Access denied to cross-namespace source")
			r.event(obj, "unknown", eventv1.EventSeverityError, err.Error(), nil)
			return ctrl.Result{RequeueAfter: obj.GetRetryInterval()}, nil
		}

		// Retry with backoff on transient errors.
		return ctrl.Result{Requeue: true}, err
	}

	// Requeue the reconciliation if the source artifact is not found.
	if artifactSource.GetArtifact() == nil {
		msg := "Source is not ready, artifact not found"
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ArtifactFailedReason, msg)
		log.Info(msg)
		return ctrl.Result{RequeueAfter: obj.GetRetryInterval()}, nil
	}

	// Check dependencies and requeue the reconciliation if the check fails.
	if len(obj.Spec.DependsOn) > 0 {
		if err := r.checkDependencies(ctx, obj, artifactSource); err != nil {
			conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.DependencyNotReadyReason, err.Error())
			msg := fmt.Sprintf("Dependencies do not meet ready condition, retrying in %s", r.requeueDependency.String())
			log.Info(msg)
			r.event(obj, artifactSource.GetArtifact().Revision, eventv1.EventSeverityInfo, msg, nil)
			return ctrl.Result{RequeueAfter: r.requeueDependency}, nil
		}
		log.Info("All dependencies are ready, proceeding with reconciliation")
	}

	// Reconcile the latest revision.
	reconcileErr := r.reconcile(ctx, obj, artifactSource, patcher)

	// Requeue at the specified retry interval if the artifact tarball is not found.
	if reconcileErr == fetch.FileNotFoundError {
		msg := fmt.Sprintf("Source is not ready, artifact not found, retrying in %s", r.requeueDependency.String())
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ArtifactFailedReason, msg)
		log.Info(msg)
		return ctrl.Result{RequeueAfter: r.requeueDependency}, nil
	}

	// Broadcast the reconciliation failure and requeue at the specified retry interval.
	if reconcileErr != nil {
		log.Error(reconcileErr, fmt.Sprintf("Reconciliation failed after %s, next try in %s",
			time.Since(reconcileStart).String(),
			obj.GetRetryInterval().String()),
			"revision",
			artifactSource.GetArtifact().Revision)
		r.event(obj, artifactSource.GetArtifact().Revision, eventv1.EventSeverityError,
			reconcileErr.Error(), nil)
		return ctrl.Result{RequeueAfter: obj.GetRetryInterval()}, nil
	}

	// Requeue the reconciliation at the specified interval.
	return ctrl.Result{RequeueAfter: obj.Spec.Interval.Duration}, nil
}

func (r *CueReconciler) reconcile(
	ctx context.Context,
	obj *cuev1.CueExport,
	src sourcev1.Source,
	patcher *patch.SerialPatcher) error {

	// Update status with the reconciliation progress.
	revision := src.GetArtifact().Revision
	progressingMsg := fmt.Sprintf("Fetching manifests for revision %s with a timeout of %s", revision, obj.GetTimeout().String())
	conditions.MarkUnknown(obj, meta.ReadyCondition, meta.ProgressingReason, "Reconciliation in progress")
	conditions.MarkReconciling(obj, meta.ProgressingReason, progressingMsg)
	if err := r.patch(ctx, obj, patcher); err != nil {
		return fmt.Errorf("failed to update status, error: %w", err)
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
		conditions.MarkFalse(obj, meta.ReadyCondition, sourcev1.DirCreationFailedReason, err.Error())
		return err
	}

	defer os.RemoveAll(tmpDir)

	// Download artifact and extract files to the tmp dir.
	err = r.artifactFetcher.Fetch(src.GetArtifact().URL, src.GetArtifact().Checksum, tmpDir)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ArtifactFailedReason, err.Error())
		return err
	}

	moduleRootPath, err := securejoin.SecureJoin(tmpDir, obj.Spec.Root)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ArtifactFailedReason, err.Error())
		return err
	}

	if _, err := os.Stat(moduleRootPath); err != nil {
		err = fmt.Errorf("root path not found: %w", err)
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ArtifactFailedReason, err.Error())
		return err
	}

	// check build path exists
	for _, p := range obj.Spec.Paths {
		pp, err := securejoin.SecureJoin(tmpDir, p)
		if err != nil {
			conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ArtifactFailedReason, err.Error())
			return err
		}

		if _, err := os.Stat(pp); err != nil {
			err = fmt.Errorf("path %s not found: %w", p, err)
			conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ArtifactFailedReason, err.Error())
			return err
		}
	}

	// Report progress and set last attempted revision in status.
	obj.Status.LastAttemptedRevision = revision
	progressingMsg = fmt.Sprintf("Building manifests for revision %s with a timeout of %s", revision, obj.GetTimeout().String())
	conditions.MarkReconciling(obj, meta.ProgressingReason, progressingMsg)
	if err := r.patch(ctx, obj, patcher); err != nil {
		return fmt.Errorf("failed to update status, error: %w", err)
	}

	// Configure the Kubernetes client for impersonation.
	impersonation := runtimeClient.NewImpersonator(
		r.Client,
		r.StatusPoller,
		r.PollingOpts,
		obj.Spec.KubeConfig,
		r.KubeConfigOpts,
		r.DefaultServiceAccount,
		obj.Spec.ServiceAccountName,
		obj.GetNamespace(),
	)

	// Create the Kubernetes client that runs under impersonation.
	kubeClient, statusPoller, err := impersonation.GetClient(ctx)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ReconciliationFailedReason, err.Error())
		return fmt.Errorf("failed to build kube client: %w", err)
	}

	values, err := r.values(ctx, revision, moduleRootPath, obj)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.BuildFailedReason, err.Error())
		return err
	}

	objects, err := r.build(ctx, revision, values, obj)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.BuildFailedReason, err.Error())
		return err
	}

	err = r.checkGates(ctx, revision, values, obj)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.GateFailedReason, err.Error())
		return err
	}

	// Create the server-side apply manager.
	resourceManager := ssa.NewResourceManager(kubeClient, statusPoller, ssa.Owner{
		Field: r.ControllerName,
		Group: cuev1.GroupVersion.Group,
	})
	resourceManager.SetOwnerLabels(objects, obj.GetName(), obj.GetNamespace())

	// Update status with the reconciliation progress.
	progressingMsg = fmt.Sprintf("Detecting drift for revision %s with a timeout of %s", revision, obj.GetTimeout().String())
	conditions.MarkReconciling(obj, meta.ProgressingReason, progressingMsg)
	if err := r.patch(ctx, obj, patcher); err != nil {
		return fmt.Errorf("failed to update status, error: %w", err)
	}

	// Validate and apply resources in stages.
	drifted, changeSet, err := r.apply(ctx, resourceManager, obj, revision, objects)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ReconciliationFailedReason, err.Error())
		return err
	}

	// Create an inventory from the reconciled resources.
	newInventory := inventory.New()
	err = inventory.AddChangeSet(newInventory, changeSet)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ReconciliationFailedReason, err.Error())
		return err
	}

	// Set last applied inventory in status.
	obj.Status.Inventory = newInventory

	// Detect stale resources which are subject to garbage collection.
	staleObjects, err := inventory.Diff(oldInventory, newInventory)
	if err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.ReconciliationFailedReason, err.Error())
		return err
	}

	// Run garbage collection for stale resources that do not have pruning disabled.
	if _, err := r.prune(ctx, resourceManager, obj, revision, staleObjects); err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.PruneFailedReason, err.Error())
		return err
	}

	// Run the health checks for the last applied resources.
	isNewRevision := !src.GetArtifact().HasRevision(obj.Status.LastAppliedRevision)
	if err := r.checkHealth(ctx,
		resourceManager,
		patcher,
		obj,
		revision,
		isNewRevision,
		drifted,
		changeSet.ToObjMetadataSet()); err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.HealthCheckFailedReason, err.Error())
		return err
	}

	// Set last applied revision.
	obj.Status.LastAppliedRevision = revision

	// Mark the object as ready.
	conditions.MarkTrue(obj,
		meta.ReadyCondition,
		cuev1.ReconciliationSucceededReason,
		fmt.Sprintf("Applied revision: %s", revision))

	return nil
}

func (r *CueReconciler) checkDependencies(ctx context.Context,
	obj *cuev1.CueExport,
	source sourcev1.Source) error {
	for _, d := range obj.Spec.DependsOn {
		if d.Namespace == "" {
			d.Namespace = obj.GetNamespace()
		}
		dName := types.NamespacedName{
			Namespace: d.Namespace,
			Name:      d.Name,
		}
		var k cuev1.CueExport
		err := r.Get(ctx, dName, &k)
		if err != nil {
			return fmt.Errorf("dependency '%s' not found: %w", dName, err)
		}

		if len(k.Status.Conditions) == 0 || k.Generation != k.Status.ObservedGeneration {
			return fmt.Errorf("dependency '%s' is not ready", dName)
		}

		if !apimeta.IsStatusConditionTrue(k.Status.Conditions, meta.ReadyCondition) {
			return fmt.Errorf("dependency '%s' is not ready", dName)
		}

		if k.Spec.SourceRef.Name == obj.Spec.SourceRef.Name &&
			k.Spec.SourceRef.Namespace == obj.Spec.SourceRef.Namespace &&
			k.Spec.SourceRef.Kind == obj.Spec.SourceRef.Kind &&
			!source.GetArtifact().HasRevision(k.Status.LastAppliedRevision) {
			return fmt.Errorf("dependency '%s' revision is not up to date", dName)
		}
	}

	return nil
}

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

	if r.NoCrossNamespaceRefs && sourceNamespace != obj.GetNamespace() {
		return src, acl.AccessDeniedError(
			fmt.Sprintf("can't access '%s/%s', cross-namespace references have been blocked",
				obj.Spec.SourceRef.Kind, namespacedName))
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
	default:
		return src, fmt.Errorf("source `%s` kind '%s' not supported",
			obj.Spec.SourceRef.Name, obj.Spec.SourceRef.Kind)
	}
	return src, nil
}

func (r *CueReconciler) values(ctx context.Context,
	revision, root string,
	obj *cuev1.CueExport,
) ([]cue.Value, error) {
	cctx := cuecontext.New()

	tags := make([]string, 0, len(obj.Spec.Tags))
	for _, t := range obj.Spec.Tags {
		if t.ValueFrom != nil {
			val, err := getValueFromSource(t.ValueFrom)
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

		Module:      "",    // TODO: add to api spec
		DataFiles:   false, // TODO: add to api sepc
		AllCUEFiles: false, // TODO: add to api sepc
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

func getValueFromSource(tagvar *cuev1.TagVarSource) (string, error) {
	return "", fmt.Errorf("todo")
}

func (r *CueReconciler) build(ctx context.Context,
	revision string, values []cue.Value,
	obj *cuev1.CueExport,
) ([]*unstructured.Unstructured, error) {
	timeout := obj.GetTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	objects := make([]*unstructured.Unstructured, 0)
	errors := []error{}
	for _, value := range values {

		if len(obj.Spec.Exprs) > 0 {
			for _, e := range obj.Spec.Exprs {
				ex, err := parser.ParseExpr("", e)
				if err != nil {
					errors = append(errors, err)
				} else {
					result := value.Context().BuildExpr(ex, cue.Scope(value), cue.InferBuiltins(true))
					objects, err = appendUnstructuredValues(objects, result)
					if err != nil {
						errors = append(errors, err)
					}
				}
			}
		} else {
			value.Walk(func(v cue.Value) bool {
				if v.Kind() == cue.StructKind &&
					v.LookupPath(cue.ParsePath("apiVersion")).Exists() &&
					v.LookupPath(cue.ParsePath("kind")).Exists() &&
					v.LookupPath(cue.ParsePath("metadata.name")).Exists() {

					var err error
					objects, err = appendUnstructuredValue(objects, value)
					if err != nil {
						errors = append(errors, err)
					}

					return false
				}
				return true
			}, nil)
		}

	}

	return objects, nil
}

func (r *CueReconciler) checkGates(ctx context.Context,
	revision string, values []cue.Value,
	obj *cuev1.CueExport,
) error {
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

	return utilerrors.NewAggregate(errors)
}

func appendUnstructuredValue(objects []*unstructured.Unstructured, value cue.Value) ([]*unstructured.Unstructured, error) {
	resource := unstructured.Unstructured{}
	bytes, err := value.MarshalJSON()

	if err != nil {
		return objects, fmt.Errorf("failed to marshal json: %s", err)
	}

	err = resource.UnmarshalJSON(bytes)

	if err != nil {
		return objects, fmt.Errorf("failed to unmarshal json: %s", err)
	} else {
		return append(objects, &resource), nil
	}
}

func appendUnstructuredValues(objects []*unstructured.Unstructured, value cue.Value) ([]*unstructured.Unstructured, error) {
	errors := []error{}
	switch value.Kind() {
	case cue.StructKind:
		return appendUnstructuredValue(objects, value)
	case cue.ListKind:
		items, err := value.List()
		if err != nil {
			return objects, err
		}
		for hasNext := items.Next(); hasNext; hasNext = items.Next() {
			objects, err = appendUnstructuredValue(objects, items.Value())
			if err != nil {
				errors = append(errors, err)
			}
		}
		return objects, nil
	default:
		return objects, fmt.Errorf("Unknown kubernetes object kind %v", value)
	}
}

func (r *CueReconciler) apply(ctx context.Context,
	manager *ssa.ResourceManager,
	obj *cuev1.CueExport,
	revision string,
	objects []*unstructured.Unstructured) (bool, *ssa.ChangeSet, error) {
	log := ctrl.LoggerFrom(ctx)

	if err := ssa.SetNativeKindsDefaults(objects); err != nil {
		return false, nil, err
	}

	applyOpts := ssa.DefaultApplyOptions()
	applyOpts.Force = obj.Spec.Force
	applyOpts.ExclusionSelector = map[string]string{
		fmt.Sprintf("%s/reconcile", cuev1.GroupVersion.Group): cuev1.DisabledValue,
	}
	applyOpts.ForceSelector = map[string]string{
		fmt.Sprintf("%s/force", cuev1.GroupVersion.Group): cuev1.EnabledValue,
	}

	applyOpts.Cleanup = ssa.ApplyCleanupOptions{
		Annotations: []string{
			// remove the kubectl annotation
			corev1.LastAppliedConfigAnnotation,
		},
		FieldManagers: []ssa.FieldManager{
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
		},
		Exclusions: map[string]string{
			fmt.Sprintf("%s/ssa", cuev1.GroupVersion.Group): cuev1.MergeValue,
		},
	}

	// contains only CRDs and Namespaces
	var defStage []*unstructured.Unstructured

	// contains only Kubernetes Class types e.g.: RuntimeClass, PriorityClass,
	// StorageClas, VolumeSnapshotClass, IngressClass, GatewayClass, ClusterClass, etc
	var classStage []*unstructured.Unstructured

	// contains all objects except for CRDs, Namespaces and Class type objects
	var resStage []*unstructured.Unstructured

	// contains the objects' metadata after apply
	resultSet := ssa.NewChangeSet()

	for _, u := range objects {
		switch {
		case ssa.IsClusterDefinition(u):
			defStage = append(defStage, u)
		case strings.HasSuffix(u.GetKind(), "Class"):
			classStage = append(classStage, u)
		default:
			resStage = append(resStage, u)
		}

	}

	var changeSetLog strings.Builder

	// validate, apply and wait for CRDs and Namespaces to register
	if len(defStage) > 0 {
		changeSet, err := manager.ApplyAll(ctx, defStage, applyOpts)
		if err != nil {
			return false, nil, err
		}
		resultSet.Append(changeSet.Entries)

		if changeSet != nil && len(changeSet.Entries) > 0 {
			log.Info("server-side apply for cluster definitions completed", "output", changeSet.ToMap())
			for _, change := range changeSet.Entries {
				if change.Action != ssa.UnchangedAction {
					changeSetLog.WriteString(change.String() + "\n")
				}
			}

			if err := manager.WaitForSet(changeSet.ToObjMetadataSet(), ssa.WaitOptions{
				Interval: 2 * time.Second,
				Timeout:  obj.GetTimeout(),
			}); err != nil {
				return false, nil, err
			}
		}
	}

	// validate, apply and wait for Class type objects to register
	if len(classStage) > 0 {
		changeSet, err := manager.ApplyAll(ctx, classStage, applyOpts)
		if err != nil {
			return false, nil, err
		}
		resultSet.Append(changeSet.Entries)

		if changeSet != nil && len(changeSet.Entries) > 0 {
			log.Info("server-side apply for cluster class types completed", "output", changeSet.ToMap())
			for _, change := range changeSet.Entries {
				if change.Action != ssa.UnchangedAction {
					changeSetLog.WriteString(change.String() + "\n")
				}
			}

			if err := manager.WaitForSet(changeSet.ToObjMetadataSet(), ssa.WaitOptions{
				Interval: 2 * time.Second,
				Timeout:  obj.GetTimeout(),
			}); err != nil {
				return false, nil, err
			}
		}
	}

	// sort by kind, validate and apply all the others objects
	sort.Sort(ssa.SortableUnstructureds(resStage))
	if len(resStage) > 0 {
		changeSet, err := manager.ApplyAll(ctx, resStage, applyOpts)
		if err != nil {
			return false, nil, fmt.Errorf("%w\n%s", err, changeSetLog.String())
		}
		resultSet.Append(changeSet.Entries)

		if changeSet != nil && len(changeSet.Entries) > 0 {
			log.Info("server-side apply completed", "output", changeSet.ToMap(), "revision", revision)
			for _, change := range changeSet.Entries {
				if change.Action != ssa.UnchangedAction {
					changeSetLog.WriteString(change.String() + "\n")
				}
			}
		}
	}

	// emit event only if the server-side apply resulted in changes
	applyLog := strings.TrimSuffix(changeSetLog.String(), "\n")
	if applyLog != "" {
		r.event(obj, revision, eventv1.EventSeverityInfo, applyLog, nil)
	}

	return applyLog != "", resultSet, nil
}

func (r *CueReconciler) checkHealth(ctx context.Context,
	manager *ssa.ResourceManager,
	patcher *patch.SerialPatcher,
	obj *cuev1.CueExport,
	revision string,
	isNewRevision bool,
	drifted bool,
	objects object.ObjMetadataSet) error {
	if len(obj.Spec.HealthChecks) == 0 && !obj.Spec.Wait {
		conditions.Delete(obj, cuev1.HealthyCondition)
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
		conditions.Delete(obj, cuev1.HealthyCondition)
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
	wasHealthy := apimeta.IsStatusConditionTrue(obj.Status.Conditions, cuev1.HealthyCondition)

	// Update status with the reconciliation progress.
	message := fmt.Sprintf("Running health checks for revision %s with a timeout of %s", revision, obj.GetTimeout().String())
	conditions.MarkReconciling(obj, meta.ProgressingReason, message)
	conditions.MarkUnknown(obj, cuev1.HealthyCondition, meta.ProgressingReason, message)
	if err := r.patch(ctx, obj, patcher); err != nil {
		return fmt.Errorf("unable to update the healthy status to progressing, error: %w", err)
	}

	// Check the health with a default timeout of 30sec shorter than the reconciliation interval.
	if err := manager.WaitForSet(toCheck, ssa.WaitOptions{
		Interval: 5 * time.Second,
		Timeout:  obj.GetTimeout(),
	}); err != nil {
		conditions.MarkFalse(obj, meta.ReadyCondition, cuev1.HealthCheckFailedReason, err.Error())
		conditions.MarkFalse(obj, cuev1.HealthyCondition, cuev1.HealthCheckFailedReason, err.Error())
		return fmt.Errorf("Health check failed after %s: %w", time.Since(checkStart).String(), err)
	}

	// Emit recovery event if the previous health check failed.
	msg := fmt.Sprintf("Health check passed in %s", time.Since(checkStart).String())
	if !wasHealthy || (isNewRevision && drifted) {
		r.event(obj, revision, eventv1.EventSeverityInfo, msg, nil)
	}

	conditions.MarkTrue(obj, cuev1.HealthyCondition, meta.SucceededReason, msg)
	if err := r.patch(ctx, obj, patcher); err != nil {
		return fmt.Errorf("unable to update the healthy status to progressing, error: %w", err)
	}

	return nil
}

func (r *CueReconciler) prune(ctx context.Context,
	manager *ssa.ResourceManager,
	obj *cuev1.CueExport,
	revision string,
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
		r.event(obj, revision, eventv1.EventSeverityInfo, changeSet.String(), nil)
		return true, nil
	}

	return false, nil
}

func (r *CueReconciler) finalize(ctx context.Context,
	obj *cuev1.CueExport) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	if obj.Spec.Prune &&
		!obj.Spec.Suspend &&
		obj.Status.Inventory != nil &&
		obj.Status.Inventory.Entries != nil {
		objects, _ := inventory.List(obj.Status.Inventory)

		impersonation := runtimeClient.NewImpersonator(
			r.Client,
			r.StatusPoller,
			r.PollingOpts,
			obj.Spec.KubeConfig,
			r.KubeConfigOpts,
			r.DefaultServiceAccount,
			obj.Spec.ServiceAccountName,
			obj.GetNamespace(),
		)
		if impersonation.CanImpersonate(ctx) {
			kubeClient, _, err := impersonation.GetClient(ctx)
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
				r.event(obj, obj.Status.LastAppliedRevision, eventv1.EventSeverityError, "pruning for deleted resource failed", nil)
				// Return the error so we retry the failed garbage collection
				return ctrl.Result{}, err
			}

			if changeSet != nil && len(changeSet.Entries) > 0 {
				r.event(obj, obj.Status.LastAppliedRevision, eventv1.EventSeverityInfo, changeSet.String(), nil)
			}
		} else {
			// when the account to impersonate is gone, log the stale objects and continue with the finalization
			msg := fmt.Sprintf("unable to prune objects: \n%s", ssa.FmtUnstructuredList(objects))
			log.Error(fmt.Errorf("skiping pruning, failed to find account to impersonate"), msg)
			r.event(obj, obj.Status.LastAppliedRevision, eventv1.EventSeverityError, msg, nil)
		}
	}

	// Remove our finalizer from the list and update it
	controllerutil.RemoveFinalizer(obj, cuev1.CueExportFinalizer)
	// Stop reconciliation as the object is being deleted
	return ctrl.Result{}, nil
}

func (r *CueReconciler) event(obj *cuev1.CueExport,
	revision, severity, msg string,
	metadata map[string]string) {
	if metadata == nil {
		metadata = map[string]string{}
	}
	if revision != "" {
		metadata[cuev1.GroupVersion.Group+"/revision"] = revision
	}

	reason := severity
	conditions.GetReason(obj, meta.ReadyCondition)
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
		rc.Reason = cuev1.ProgressingWithRetryReason
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
		cuev1.HealthyCondition,
		meta.ReadyCondition,
		meta.ReconcilingCondition,
		meta.StalledCondition,
	}
	patchOpts = append(patchOpts,
		patch.WithOwnedConditions{Conditions: ownedConditions},
		patch.WithForceOverwriteConditions{},
		patch.WithFieldOwner(r.statusManager),
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
