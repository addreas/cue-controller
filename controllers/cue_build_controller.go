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
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/load"
	"github.com/go-logr/logr"
	"github.com/hashicorp/go-retryablehttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kuberecorder "k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling"
	"sigs.k8s.io/cli-utils/pkg/object"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/runtime/events"
	"github.com/fluxcd/pkg/runtime/metrics"
	"github.com/fluxcd/pkg/runtime/predicates"
	"github.com/fluxcd/pkg/ssa"
	"github.com/fluxcd/pkg/untar"
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta1"

	cuebuildv1 "github.com/addreas/cuebuild-controller/api/v1alpha2"
)

// +kubebuilder:rbac:groups=cuebuild.toolkit.fluxcd.io,resources=cuebuilds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cuebuild.toolkit.fluxcd.io,resources=cuebuilds/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cuebuild.toolkit.fluxcd.io,resources=cuebuilds/finalizers,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=buckets;gitrepositories,verbs=get;list;watch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=buckets/status;gitrepositories/status,verbs=get
// +kubebuilder:rbac:groups="",resources=configmaps;secrets;serviceaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// CueBuildReconciler reconciles a CueBuild object
type CueBuildReconciler struct {
	client.Client
	httpClient            *retryablehttp.Client
	requeueDependency     time.Duration
	Scheme                *runtime.Scheme
	EventRecorder         kuberecorder.EventRecorder
	ExternalEventRecorder *events.Recorder
	MetricsRecorder       *metrics.Recorder
	StatusPoller          *polling.StatusPoller
	ControllerName        string
}

type CueBuildReconcilerOptions struct {
	MaxConcurrentReconciles   int
	HTTPRetry                 int
	DependencyRequeueInterval time.Duration
}

func (r *CueBuildReconciler) SetupWithManager(mgr ctrl.Manager, opts CueBuildReconcilerOptions) error {
	const (
		gitRepositoryIndexKey string = ".metadata.gitRepository"
		bucketIndexKey        string = ".metadata.bucket"
	)

	// Index the Cuebuilds by the GitRepository references they (may) point at.
	if err := mgr.GetCache().IndexField(context.TODO(), &cuebuildv1.CueBuild{}, gitRepositoryIndexKey,
		r.indexBy(sourcev1.GitRepositoryKind)); err != nil {
		return fmt.Errorf("failed setting index fields: %w", err)
	}

	// Index the Cuebuilds by the Bucket references they (may) point at.
	if err := mgr.GetCache().IndexField(context.TODO(), &cuebuildv1.CueBuild{}, bucketIndexKey,
		r.indexBy(sourcev1.BucketKind)); err != nil {
		return fmt.Errorf("failed setting index fields: %w", err)
	}

	r.requeueDependency = opts.DependencyRequeueInterval

	// Configure the retryable http client used for fetching artifacts.
	// By default it retries 10 times within a 3.5 minutes window.
	httpClient := retryablehttp.NewClient()
	httpClient.RetryWaitMin = 5 * time.Second
	httpClient.RetryWaitMax = 30 * time.Second
	httpClient.RetryMax = opts.HTTPRetry
	httpClient.Logger = nil
	r.httpClient = httpClient

	return ctrl.NewControllerManagedBy(mgr).
		For(&cuebuildv1.CueBuild{}, builder.WithPredicates(
			predicate.Or(predicate.GenerationChangedPredicate{}, predicates.ReconcileRequestedPredicate{}),
		)).
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
		WithOptions(controller.Options{MaxConcurrentReconciles: opts.MaxConcurrentReconciles}).
		Complete(r)
}

func (r *CueBuildReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logr.FromContext(ctx)
	reconcileStart := time.Now()

	var cueBuild cuebuildv1.CueBuild
	if err := r.Get(ctx, req.NamespacedName, &cueBuild); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Record suspended status metric
	defer r.recordSuspension(ctx, cueBuild)

	// Add our finalizer if it does not exist
	if !controllerutil.ContainsFinalizer(&cueBuild, cuebuildv1.CueBuildFinalizer) {
		controllerutil.AddFinalizer(&cueBuild, cuebuildv1.CueBuildFinalizer)
		if err := r.Update(ctx, &cueBuild); err != nil {
			log.Error(err, "unable to register finalizer")
			return ctrl.Result{}, err
		}
	}

	// Examine if the object is under deletion
	if !cueBuild.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, cueBuild)
	}

	// Return early if the CueBuild is suspended.
	if cueBuild.Spec.Suspend {
		log.Info("Reconciliation is suspended for this object")
		return ctrl.Result{}, nil
	}

	// resolve source reference
	source, err := r.getSource(ctx, cueBuild)
	if err != nil {
		if apierrors.IsNotFound(err) {
			msg := fmt.Sprintf("Source '%s' not found", cueBuild.Spec.SourceRef.String())
			cueBuild = cuebuildv1.CueBuildNotReady(cueBuild, "", cuebuildv1.ArtifactFailedReason, msg)
			if err := r.patchStatus(ctx, req, cueBuild.Status); err != nil {
				log.Error(err, "unable to update status for source not found")
				return ctrl.Result{Requeue: true}, err
			}
			r.recordReadiness(ctx, cueBuild)
			log.Info(msg)
			// do not requeue immediately, when the source is created the watcher should trigger a reconciliation
			return ctrl.Result{RequeueAfter: cueBuild.GetRetryInterval()}, nil
		} else {
			// retry on transient errors
			return ctrl.Result{Requeue: true}, err
		}
	}

	if source.GetArtifact() == nil {
		msg := "Source is not ready, artifact not found"
		cueBuild = cuebuildv1.CueBuildNotReady(cueBuild, "", cuebuildv1.ArtifactFailedReason, msg)
		if err := r.patchStatus(ctx, req, cueBuild.Status); err != nil {
			log.Error(err, "unable to update status for artifact not found")
			return ctrl.Result{Requeue: true}, err
		}
		r.recordReadiness(ctx, cueBuild)
		log.Info(msg)
		// do not requeue immediately, when the artifact is created the watcher should trigger a reconciliation
		return ctrl.Result{RequeueAfter: cueBuild.GetRetryInterval()}, nil
	}

	// check dependencies
	if len(cueBuild.Spec.DependsOn) > 0 {
		if err := r.checkDependencies(cueBuild); err != nil {
			cueBuild = cuebuildv1.CueBuildNotReady(
				cueBuild, source.GetArtifact().Revision, meta.DependencyNotReadyReason, err.Error())
			if err := r.patchStatus(ctx, req, cueBuild.Status); err != nil {
				log.Error(err, "unable to update status for dependency not ready")
				return ctrl.Result{Requeue: true}, err
			}
			// we can't rely on exponential backoff because it will prolong the execution too much,
			// instead we requeue on a fix interval.
			msg := fmt.Sprintf("Dependencies do not meet ready condition, retrying in %s", r.requeueDependency.String())
			log.Info(msg)
			r.event(ctx, cueBuild, source.GetArtifact().Revision, events.EventSeverityInfo, msg, nil)
			r.recordReadiness(ctx, cueBuild)
			return ctrl.Result{RequeueAfter: r.requeueDependency}, nil
		}
		log.Info("All dependencies are ready, proceeding with reconciliation")
	}

	// record reconciliation duration
	if r.MetricsRecorder != nil {
		objRef, err := reference.GetReference(r.Scheme, &cueBuild)
		if err != nil {
			return ctrl.Result{}, err
		}
		defer r.MetricsRecorder.RecordDuration(*objRef, reconcileStart)
	}

	// set the reconciliation status to progressing
	cueBuild = cuebuildv1.CueBuildProgressing(cueBuild, "reconciliation in progress")
	if err := r.patchStatus(ctx, req, cueBuild.Status); err != nil {
		log.Error(err, "unable to update status to progressing")
		return ctrl.Result{Requeue: true}, err
	}
	r.recordReadiness(ctx, cueBuild)

	// reconcile cueBuild by applying the latest revision
	reconciledCueBuild, reconcileErr := r.reconcile(ctx, *cueBuild.DeepCopy(), source)
	if err := r.patchStatus(ctx, req, reconciledCueBuild.Status); err != nil {
		log.Error(err, "unable to update status after reconciliation")
		return ctrl.Result{Requeue: true}, err
	}
	r.recordReadiness(ctx, reconciledCueBuild)

	// broadcast the reconciliation failure and requeue at the specified retry interval
	if reconcileErr != nil {
		log.Error(reconcileErr, fmt.Sprintf("Reconciliation failed after %s, next try in %s",
			time.Now().Sub(reconcileStart).String(),
			cueBuild.GetRetryInterval().String()),
			"revision",
			source.GetArtifact().Revision)
		r.event(ctx, reconciledCueBuild, source.GetArtifact().Revision, events.EventSeverityError,
			reconcileErr.Error(), nil)
		return ctrl.Result{RequeueAfter: cueBuild.GetRetryInterval()}, nil
	}

	// broadcast the reconciliation result and requeue at the specified interval
	msg := fmt.Sprintf("Reconciliation finished in %s, next run in %s",
		time.Now().Sub(reconcileStart).String(),
		cueBuild.Spec.Interval.Duration.String())
	log.Info(fmt.Sprintf(msg),
		"revision",
		source.GetArtifact().Revision,
	)
	r.event(ctx, reconciledCueBuild, source.GetArtifact().Revision, events.EventSeverityInfo,
		msg, map[string]string{"commit_status": "update"})
	return ctrl.Result{RequeueAfter: cueBuild.Spec.Interval.Duration}, nil
}

func (r *CueBuildReconciler) reconcile(
	ctx context.Context,
	cueBuild cuebuildv1.CueBuild,
	source sourcev1.Source) (cuebuildv1.CueBuild, error) {
	// record the value of the reconciliation request, if any
	if v, ok := meta.ReconcileAnnotationValue(cueBuild.GetAnnotations()); ok {
		cueBuild.Status.SetLastHandledReconcileRequest(v)
	}

	revision := source.GetArtifact().Revision

	// create tmp dir
	tmpDir, err := os.MkdirTemp("", cueBuild.Name)
	if err != nil {
		err = fmt.Errorf("tmp dir error: %w", err)
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			revision,
			sourcev1.StorageOperationFailedReason,
			err.Error(),
		), err
	}
	defer os.RemoveAll(tmpDir)

	// download artifact and extract files
	err = r.download(source.GetArtifact(), tmpDir)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			revision,
			cuebuildv1.ArtifactFailedReason,
			err.Error(),
		), err
	}

	cuectx := cuecontext.New()
	instances := load.Instances(cueBuild.Spec.Packages, &load.Config{
		Dir: tmpDir,
	})

	for _, instance := range instances {
		if instance.Err != nil {
			return cuebuildv1.CueBuildNotReady(
				cueBuild,
				revision,
				cuebuildv1.BuildFailedReason,
				instance.Err.Error(),
			), fmt.Errorf("failed to load instance: %w", instance.Err)
		}
	}

	values, err := cuectx.BuildInstances(instances)

	if err != nil {
		err = fmt.Errorf("failed to build cue instances: %w", err)
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			revision,
			cuebuildv1.BuildFailedReason,
			err.Error(),
		), err
	}

	// setup the Kubernetes client for impersonation
	impersonation := NewCueBuildImpersonation(cueBuild, r.Client, r.StatusPoller, tmpDir)
	kubeClient, statusPoller, err := impersonation.GetClient(ctx)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			revision,
			meta.ReconciliationFailedReason,
			err.Error(),
		), fmt.Errorf("failed to build kube client: %w", err)
	}

	// build the cueBuild
	resources, err := r.build(ctx, cueBuild, values)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			revision,
			cuebuildv1.BuildFailedReason,
			fmt.Sprint("failed to build", err),
		), err
	}

	// convert the build result into Kubernetes unstructured objects
	objects, err := ssa.ReadObjects(bytes.NewReader(resources))
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			revision,
			cuebuildv1.BuildFailedReason,
			err.Error(),
		), err
	}

	// create a snapshot of the current inventory
	oldStatus := cueBuild.Status.DeepCopy()

	// create the server-side apply manager
	resourceManager := ssa.NewResourceManager(kubeClient, statusPoller, ssa.Owner{
		Field: r.ControllerName,
		Group: cuebuildv1.GroupVersion.Group,
	})
	resourceManager.SetOwnerLabels(objects, cueBuild.GetName(), cueBuild.GetNamespace())

	// validate and apply resources in stages
	drifted, changeSet, err := r.apply(ctx, resourceManager, cueBuild, revision, objects)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			revision,
			meta.ReconciliationFailedReason,
			err.Error(),
		), err
	}

	// create an inventory of objects to be reconciled
	newInventory := NewInventory()
	err = AddObjectsToInventory(newInventory, changeSet)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			revision,
			meta.ReconciliationFailedReason,
			err.Error(),
		), err
	}

	// detect stale objects which are subject to garbage collection
	var staleObjects []*unstructured.Unstructured
	if oldStatus.Inventory != nil {
		diffObjects, err := DiffInventory(oldStatus.Inventory, newInventory)
		if err != nil {
			return cuebuildv1.CueBuildNotReady(
				cueBuild,
				revision,
				meta.ReconciliationFailedReason,
				err.Error(),
			), err
		}

		// TODO: remove this workaround after kustomize-controller 0.18 release
		// skip objects that were wrongly marked as namespaced
		// https://github.com/fluxcd/kustomize-controller/issues/466
		newObjects, _ := ListObjectsInInventory(newInventory)
		for _, obj := range diffObjects {
			preserve := false
			if obj.GetNamespace() != "" {
				for _, newObj := range newObjects {
					if newObj.GetNamespace() == "" &&
						obj.GetKind() == newObj.GetKind() &&
						obj.GetAPIVersion() == newObj.GetAPIVersion() &&
						obj.GetName() == newObj.GetName() {
						preserve = true
						break
					}
				}
			}
			if !preserve {
				staleObjects = append(staleObjects, obj)
			}
		}
	}

	// run garbage collection for stale objects that do not have pruning disabled
	if _, err := r.prune(ctx, resourceManager, cueBuild, revision, staleObjects); err != nil {
		return cuebuildv1.CueBuildNotReadyInventory(
			cueBuild,
			newInventory,
			revision,
			cuebuildv1.PruneFailedReason,
			err.Error(),
		), err
	}

	// health assessment
	if err := r.checkHealth(ctx, resourceManager, cueBuild, revision, drifted, changeSet.ToObjMetadataSet()); err != nil {
		return cuebuildv1.CueBuildNotReadyInventory(
			cueBuild,
			newInventory,
			revision,
			cuebuildv1.HealthCheckFailedReason,
			err.Error(),
		), err
	}

	return cuebuildv1.CueBuildReadyInventory(
		cueBuild,
		newInventory,
		revision,
		meta.ReconciliationSucceededReason,
		fmt.Sprintf("Applied revision: %s", revision),
	), nil
}

func (r *CueBuildReconciler) checkDependencies(cueBuild cuebuildv1.CueBuild) error {
	for _, d := range cueBuild.Spec.DependsOn {
		if d.Namespace == "" {
			d.Namespace = cueBuild.GetNamespace()
		}
		dName := types.NamespacedName(d)
		var k cuebuildv1.CueBuild
		err := r.Get(context.Background(), dName, &k)
		if err != nil {
			return fmt.Errorf("unable to get '%s' dependency: %w", dName, err)
		}

		if len(k.Status.Conditions) == 0 || k.Generation != k.Status.ObservedGeneration {
			return fmt.Errorf("dependency '%s' is not ready", dName)
		}

		if !apimeta.IsStatusConditionTrue(k.Status.Conditions, meta.ReadyCondition) {
			return fmt.Errorf("dependency '%s' is not ready", dName)
		}
	}

	return nil
}

func (r *CueBuildReconciler) download(artifact *sourcev1.Artifact, tmpDir string) error {
	artifactURL := artifact.URL
	if hostname := os.Getenv("SOURCE_CONTROLLER_LOCALHOST"); hostname != "" {
		u, err := url.Parse(artifactURL)
		if err != nil {
			return err
		}
		u.Host = hostname
		artifactURL = u.String()
	}

	req, err := retryablehttp.NewRequest(http.MethodGet, artifactURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create a new request: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download artifact, error: %w", err)
	}
	defer resp.Body.Close()

	// check response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download artifact from %s, status: %s", artifactURL, resp.Status)
	}

	var buf bytes.Buffer

	// verify checksum matches origin
	if err := r.verifyArtifact(artifact, &buf, resp.Body); err != nil {
		return err
	}

	// extract
	if _, err = untar.Untar(&buf, tmpDir); err != nil {
		return fmt.Errorf("failed to untar artifact, error: %w", err)
	}

	return nil
}

func (r *CueBuildReconciler) verifyArtifact(artifact *sourcev1.Artifact, buf *bytes.Buffer, reader io.Reader) error {
	hasher := sha256.New()

	// for backwards compatibility with source-controller v0.17.2 and older
	if len(artifact.Checksum) == 40 {
		hasher = sha1.New()
	}

	// compute checksum
	mw := io.MultiWriter(hasher, buf)
	if _, err := io.Copy(mw, reader); err != nil {
		return err
	}

	if checksum := fmt.Sprintf("%x", hasher.Sum(nil)); checksum != artifact.Checksum {
		return fmt.Errorf("failed to verify artifact: computed checksum '%s' doesn't match advertised '%s'",
			checksum, artifact.Checksum)
	}

	return nil
}

func (r *CueBuildReconciler) getSource(ctx context.Context, cueBuild cuebuildv1.CueBuild) (sourcev1.Source, error) {
	var source sourcev1.Source
	sourceNamespace := cueBuild.GetNamespace()
	if cueBuild.Spec.SourceRef.Namespace != "" {
		sourceNamespace = cueBuild.Spec.SourceRef.Namespace
	}
	namespacedName := types.NamespacedName{
		Namespace: sourceNamespace,
		Name:      cueBuild.Spec.SourceRef.Name,
	}
	switch cueBuild.Spec.SourceRef.Kind {
	case sourcev1.GitRepositoryKind:
		var repository sourcev1.GitRepository
		err := r.Client.Get(ctx, namespacedName, &repository)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return source, err
			}
			return source, fmt.Errorf("unable to get source '%s': %w", namespacedName, err)
		}
		source = &repository
	case sourcev1.BucketKind:
		var bucket sourcev1.Bucket
		err := r.Client.Get(ctx, namespacedName, &bucket)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return source, err
			}
			return source, fmt.Errorf("unable to get source '%s': %w", namespacedName, err)
		}
		source = &bucket
	default:
		return source, fmt.Errorf("source `%s` kind '%s' not supported",
			cueBuild.Spec.SourceRef.Name, cueBuild.Spec.SourceRef.Kind)
	}
	return source, nil
}

func (r *CueBuildReconciler) build(ctx context.Context, cueBuild cuebuildv1.CueBuild, values []cue.Value) ([]byte, error) {
	timeout := cueBuild.GetTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	errors := make([]string, 0)
	resources := bytes.Buffer{}
	for _, inst := range values {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		inst.Walk(func(v cue.Value) bool {
			if v.Kind() == cue.StructKind &&
				v.LookupPath(cue.ParsePath("apiVersion")).Exists() &&
				v.LookupPath(cue.ParsePath("kind")).Exists() &&
				v.LookupPath(cue.ParsePath("metadata.name")).Exists() {

				bytes, err := v.MarshalJSON()
				if err != nil {
					errors = append(errors, fmt.Sprintf("failed to json marshal: %s", err))
				}
				resources.Write(bytes)
				resources.WriteString("\n")
				return false
			}
			return true
		}, nil)
	}

	if len(errors) > 0 {
		return nil, fmt.Errorf(strings.Join(errors, "\n"))
	}

	return resources.Bytes(), nil
}

func (r *CueBuildReconciler) apply(ctx context.Context, manager *ssa.ResourceManager, cueBuild cuebuildv1.CueBuild, revision string, objects []*unstructured.Unstructured) (bool, *ssa.ChangeSet, error) {
	log := logr.FromContext(ctx)

	if err := ssa.SetNativeKindsDefaults(objects); err != nil {
		return false, nil, err
	}

	applyOpts := ssa.DefaultApplyOptions()
	applyOpts.Force = cueBuild.Spec.Force
	applyOpts.Exclusions = map[string]string{
		fmt.Sprintf("%s/reconcile", cuebuildv1.GroupVersion.Group): cuebuildv1.DisabledValue,
	}

	// contains only CRDs and Namespaces
	var stageOne []*unstructured.Unstructured

	// contains all objects except for CRDs and Namespaces
	var stageTwo []*unstructured.Unstructured

	// contains the objects' metadata after apply
	resultSet := ssa.NewChangeSet()

	for _, u := range objects {
		if ssa.IsClusterDefinition(u) {
			u.SetNamespace("")
			stageOne = append(stageOne, u)
		} else {
			stageTwo = append(stageTwo, u)
		}
	}

	var changeSetLog strings.Builder

	// validate, apply and wait for CRDs and Namespaces to register
	if len(stageOne) > 0 {
		changeSet, err := manager.ApplyAll(ctx, stageOne, applyOpts)
		if err != nil {
			return false, nil, err
		}
		resultSet.Append(changeSet.Entries)

		if changeSet != nil && len(changeSet.Entries) > 0 {
			log.Info("server-side apply completed", "output", changeSet.ToMap())
			for _, change := range changeSet.Entries {
				if change.Action != string(ssa.UnchangedAction) {
					changeSetLog.WriteString(change.String() + "\n")
				}
			}
		}

		if err := manager.Wait(stageOne, ssa.WaitOptions{
			Interval: 2 * time.Second,
			Timeout:  cueBuild.GetTimeout(),
		}); err != nil {
			return false, nil, err
		}
	}

	// sort by kind, validate and apply all the others objects
	sort.Sort(ssa.SortableUnstructureds(stageTwo))
	if len(stageTwo) > 0 {
		changeSet, err := manager.ApplyAll(ctx, stageTwo, applyOpts)
		if err != nil {
			return false, nil, fmt.Errorf("%w\n%s", err, changeSetLog.String())
		}
		resultSet.Append(changeSet.Entries)

		if changeSet != nil && len(changeSet.Entries) > 0 {
			log.Info("server-side apply completed", "output", changeSet.ToMap())
			for _, change := range changeSet.Entries {
				if change.Action != string(ssa.UnchangedAction) {
					changeSetLog.WriteString(change.String() + "\n")
				}
			}
		}
	}

	// emit event only if the server-side apply resulted in changes
	applyLog := strings.TrimSuffix(changeSetLog.String(), "\n")
	if applyLog != "" {
		r.event(ctx, cueBuild, revision, events.EventSeverityInfo, applyLog, nil)
	}

	return applyLog != "", resultSet, nil
}

func (r *CueBuildReconciler) checkHealth(ctx context.Context, manager *ssa.ResourceManager, cueBuild cuebuildv1.CueBuild, revision string, drifted bool, objects object.ObjMetadataSet) error {
	if len(cueBuild.Spec.HealthChecks) == 0 && !cueBuild.Spec.Wait {
		return nil
	}

	checkStart := time.Now()
	var err error
	if !cueBuild.Spec.Wait {
		objects, err = referenceToObjMetadataSet(cueBuild.Spec.HealthChecks)
		if err != nil {
			return err
		}
	}

	if len(objects) == 0 {
		return nil
	}

	// guard against deadlock (waiting on itself)
	var toCheck []object.ObjMetadata
	for _, object := range objects {
		if object.GroupKind.Kind == cuebuildv1.CueBuildKind &&
			object.Name == cueBuild.GetName() &&
			object.Namespace == cueBuild.GetNamespace() {
			continue
		}
		toCheck = append(toCheck, object)
	}

	// find the previous health check result
	wasHealthy := apimeta.IsStatusConditionTrue(cueBuild.Status.Conditions, cuebuildv1.HealthyCondition)

	// set the Healthy and Ready conditions to progressing
	message := fmt.Sprintf("running health checks with a timeout of %s", cueBuild.GetTimeout().String())
	k := cuebuildv1.CueBuildProgressing(cueBuild, message)
	cuebuildv1.SetCueBuildHealthiness(&k, metav1.ConditionUnknown, meta.ProgressingReason, message)
	if err := r.patchStatus(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&cueBuild)}, k.Status); err != nil {
		return fmt.Errorf("unable to update the healthy status to progressing, error: %w", err)
	}

	// check the health with a default timeout of 30sec shorter than the reconciliation interval
	if err := manager.WaitForSet(toCheck, ssa.WaitOptions{
		Interval: 5 * time.Second,
		Timeout:  cueBuild.GetTimeout(),
	}); err != nil {
		return fmt.Errorf("Health check failed after %s, %w", time.Now().Sub(checkStart).String(), err)
	}

	// emit event if the previous health check failed
	if !wasHealthy || (cueBuild.Status.LastAppliedRevision != revision && drifted) {
		r.event(ctx, cueBuild, revision, events.EventSeverityInfo,
			fmt.Sprintf("Health check passed in %s", time.Now().Sub(checkStart).String()), nil)
	}

	return nil
}

func (r *CueBuildReconciler) prune(ctx context.Context, manager *ssa.ResourceManager, cueBuild cuebuildv1.CueBuild, revision string, objects []*unstructured.Unstructured) (bool, error) {
	if !cueBuild.Spec.Prune {
		return false, nil
	}

	log := logr.FromContext(ctx)

	opts := ssa.DeleteOptions{
		PropagationPolicy: metav1.DeletePropagationBackground,
		Inclusions:        manager.GetOwnerLabels(cueBuild.Name, cueBuild.Namespace),
		Exclusions: map[string]string{
			fmt.Sprintf("%s/prune", cuebuildv1.GroupVersion.Group):     cuebuildv1.DisabledValue,
			fmt.Sprintf("%s/reconcile", cuebuildv1.GroupVersion.Group): cuebuildv1.DisabledValue,
		},
	}

	changeSet, err := manager.DeleteAll(ctx, objects, opts)
	if err != nil {
		return false, err
	}

	// emit event only if the prune operation resulted in changes
	if changeSet != nil && len(changeSet.Entries) > 0 {
		log.Info(fmt.Sprintf("garbage collection completed: %s", changeSet.String()))
		r.event(ctx, cueBuild, revision, events.EventSeverityInfo, changeSet.String(), nil)
		return true, nil
	}

	return false, nil
}

func (r *CueBuildReconciler) finalize(ctx context.Context, cueBuild cuebuildv1.CueBuild) (ctrl.Result, error) {
	log := logr.FromContext(ctx)
	if cueBuild.Spec.Prune &&
		!cueBuild.Spec.Suspend &&
		cueBuild.Status.Inventory != nil &&
		cueBuild.Status.Inventory.Entries != nil {
		objects, err := ListObjectsInInventory(cueBuild.Status.Inventory)

		impersonation := NewCueBuildImpersonation(cueBuild, r.Client, r.StatusPoller, "")
		kubeClient, _, err := impersonation.GetClient(ctx)
		if err != nil {
			// when impersonation fails, log the stale objects and continue with the finalization
			msg := fmt.Sprintf("unable to prune objects: \n%s", ssa.FmtUnstructuredList(objects))
			log.Error(fmt.Errorf("failed to build kube client: %w", err), msg)
			r.event(ctx, cueBuild, cueBuild.Status.LastAppliedRevision, events.EventSeverityError, msg, nil)
		} else {
			resourceManager := ssa.NewResourceManager(kubeClient, nil, ssa.Owner{
				Field: r.ControllerName,
				Group: cuebuildv1.GroupVersion.Group,
			})

			opts := ssa.DeleteOptions{
				PropagationPolicy: metav1.DeletePropagationBackground,
				Inclusions:        resourceManager.GetOwnerLabels(cueBuild.Name, cueBuild.Namespace),
				Exclusions: map[string]string{
					fmt.Sprintf("%s/prune", cuebuildv1.GroupVersion.Group):     cuebuildv1.DisabledValue,
					fmt.Sprintf("%s/reconcile", cuebuildv1.GroupVersion.Group): cuebuildv1.DisabledValue,
				},
			}

			changeSet, err := resourceManager.DeleteAll(ctx, objects, opts)
			if err != nil {
				r.event(ctx, cueBuild, cueBuild.Status.LastAppliedRevision, events.EventSeverityError, "pruning for deleted resource failed", nil)
				// Return the error so we retry the failed garbage collection
				return ctrl.Result{}, err
			}

			if changeSet != nil && len(changeSet.Entries) > 0 {
				r.event(ctx, cueBuild, cueBuild.Status.LastAppliedRevision, events.EventSeverityInfo, changeSet.String(), nil)
			}
		}
	}

	// Record deleted status
	r.recordReadiness(ctx, cueBuild)

	// Remove our finalizer from the list and update it
	controllerutil.RemoveFinalizer(&cueBuild, cuebuildv1.CueBuildFinalizer)
	if err := r.Update(ctx, &cueBuild); err != nil {
		return ctrl.Result{}, err
	}

	// Stop reconciliation as the object is being deleted
	return ctrl.Result{}, nil
}

func (r *CueBuildReconciler) event(ctx context.Context, cueBuild cuebuildv1.CueBuild, revision, severity, msg string, metadata map[string]string) {
	log := logr.FromContext(ctx)

	annotations := map[string]string{
		cuebuildv1.GroupVersion.Group + "/revision": revision,
	}

	eventtype := "Normal"
	if severity == events.EventSeverityError {
		eventtype = "Warning"
	}

	r.EventRecorder.AnnotatedEventf(&cueBuild, annotations, eventtype, severity, msg)

	if r.ExternalEventRecorder != nil {
		objRef, err := reference.GetReference(r.Scheme, &cueBuild)
		if err != nil {
			log.Error(err, "unable to send event")
			return
		}
		if metadata == nil {
			metadata = map[string]string{}
		}
		if revision != "" {
			metadata["revision"] = revision
		}

		reason := severity
		if c := apimeta.FindStatusCondition(cueBuild.Status.Conditions, meta.ReadyCondition); c != nil {
			reason = c.Reason
		}

		if err := r.ExternalEventRecorder.Eventf(*objRef, metadata, severity, reason, msg); err != nil {
			log.Error(err, "unable to send event")
			return
		}
	}
}

func (r *CueBuildReconciler) recordReadiness(ctx context.Context, cueBuild cuebuildv1.CueBuild) {
	if r.MetricsRecorder == nil {
		return
	}
	log := logr.FromContext(ctx)

	objRef, err := reference.GetReference(r.Scheme, &cueBuild)
	if err != nil {
		log.Error(err, "unable to record readiness metric")
		return
	}
	if rc := apimeta.FindStatusCondition(cueBuild.Status.Conditions, meta.ReadyCondition); rc != nil {
		r.MetricsRecorder.RecordCondition(*objRef, *rc, !cueBuild.DeletionTimestamp.IsZero())
	} else {
		r.MetricsRecorder.RecordCondition(*objRef, metav1.Condition{
			Type:   meta.ReadyCondition,
			Status: metav1.ConditionUnknown,
		}, !cueBuild.DeletionTimestamp.IsZero())
	}
}

func (r *CueBuildReconciler) recordSuspension(ctx context.Context, cueBuild cuebuildv1.CueBuild) {
	if r.MetricsRecorder == nil {
		return
	}
	log := logr.FromContext(ctx)

	objRef, err := reference.GetReference(r.Scheme, &cueBuild)
	if err != nil {
		log.Error(err, "unable to record suspended metric")
		return
	}

	if !cueBuild.DeletionTimestamp.IsZero() {
		r.MetricsRecorder.RecordSuspend(*objRef, false)
	} else {
		r.MetricsRecorder.RecordSuspend(*objRef, cueBuild.Spec.Suspend)
	}
}
func (r *CueBuildReconciler) patchStatus(ctx context.Context, req ctrl.Request, newStatus cuebuildv1.CueBuildStatus) error {
	var cueBuild cuebuildv1.CueBuild
	if err := r.Get(ctx, req.NamespacedName, &cueBuild); err != nil {
		return err
	}

	patch := client.MergeFrom(cueBuild.DeepCopy())
	cueBuild.Status = newStatus

	return r.Status().Patch(ctx, &cueBuild, patch)
}
