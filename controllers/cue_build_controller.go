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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/runtime/events"
	"github.com/fluxcd/pkg/runtime/metrics"
	"github.com/fluxcd/pkg/runtime/predicates"
	"github.com/fluxcd/pkg/untar"
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta1"
	"github.com/go-logr/logr"
	"github.com/hashicorp/go-retryablehttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kuberecorder "k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/kustomize/api/filesys"

	cuebuildv1 "github.com/fluxcd/cuebuild-controller/api/v1alpha1"
)

// +kubebuilder:rbac:groups=cuebuild.toolkit.fluxcd.io,resources=cuebuilds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cuebuild.toolkit.fluxcd.io,resources=cuebuilds/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cuebuild.toolkit.fluxcd.io,resources=cuebuilds/finalizers,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=buckets;gitrepositories,verbs=get;list;watch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=buckets/status;gitrepositories/status,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets;serviceaccounts,verbs=get;list;watch
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
}

type CueBuildReconcilerOptions struct {
	MaxConcurrentReconciles   int
	HTTPRetry                 int
	DependencyRequeueInterval time.Duration
}

func (r *CueBuildReconciler) SetupWithManager(mgr ctrl.Manager, opts CueBuildReconcilerOptions) error {
	// Index the Cuebuilds by the GitRepository references they (may) point at.
	if err := mgr.GetCache().IndexField(context.TODO(), &cuebuildv1.CueBuild{}, cuebuildv1.GitRepositoryIndexKey,
		r.indexByGitRepository); err != nil {
		return fmt.Errorf("failed setting index fields: %w", err)
	}

	// Index the Cuebuilds by the Bucket references they (may) point at.
	if err := mgr.GetCache().IndexField(context.TODO(), &cuebuildv1.CueBuild{}, cuebuildv1.BucketIndexKey,
		r.indexByBucket); err != nil {
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
			handler.EnqueueRequestsFromMapFunc(r.requestsForGitRepositoryRevisionChange),
			builder.WithPredicates(SourceRevisionChangePredicate{}),
		).
		Watches(
			&source.Kind{Type: &sourcev1.Bucket{}},
			handler.EnqueueRequestsFromMapFunc(r.requestsForBucketRevisionChange),
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
		return r.reconcileDelete(ctx, cueBuild)
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
			log.Error(err, msg)
			r.event(ctx, cueBuild, source.GetArtifact().Revision, events.EventSeverityInfo, msg, nil)
			r.recordReadiness(ctx, cueBuild)
			return ctrl.Result{RequeueAfter: r.requeueDependency}, nil
		}
		log.Info("All dependencies area ready, proceeding with reconciliation")
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
	cueBuild = cuebuildv1.CueBuildProgressing(cueBuild)
	if err := r.patchStatus(ctx, req, cueBuild.Status); err != nil {
		(logr.FromContext(ctx)).Error(err, "unable to update status to progressing")
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
	log.Info(fmt.Sprintf("Reconciliation finished in %s, next run in %s",
		time.Now().Sub(reconcileStart).String(),
		cueBuild.Spec.Interval.Duration.String()),
		"revision",
		source.GetArtifact().Revision,
	)
	r.event(ctx, reconciledCueBuild, source.GetArtifact().Revision, events.EventSeverityInfo,
		"Update completed", map[string]string{"commit_status": "update"})
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

	// create tmp dir
	tmpDir, err := ioutil.TempDir("", cueBuild.Name)
	if err != nil {
		err = fmt.Errorf("tmp dir error: %w", err)
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			source.GetArtifact().Revision,
			sourcev1.StorageOperationFailedReason,
			err.Error(),
		), err
	}
	defer os.RemoveAll(tmpDir)

	// download artifact and extract files
	err = r.download(source.GetArtifact().URL, tmpDir)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			source.GetArtifact().Revision,
			cuebuildv1.ArtifactFailedReason,
			err.Error(),
		), err
	}

	// check build path exists
	dirPath, err := securejoin.SecureJoin(tmpDir, cueBuild.Spec.Paths[0])
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			source.GetArtifact().Revision,
			cuebuildv1.ArtifactFailedReason,
			err.Error(),
		), err
	}
	if _, err := os.Stat(dirPath); err != nil {
		err = fmt.Errorf("cueBuild path not found: %w", err)
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			source.GetArtifact().Revision,
			cuebuildv1.ArtifactFailedReason,
			err.Error(),
		), err
	}

	// create any necessary kube-clients for impersonation
	impersonation := NewCueBuildImpersonation(cueBuild, r.Client, r.StatusPoller, dirPath)
	kubeClient, statusPoller, err := impersonation.GetClient(ctx)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			source.GetArtifact().Revision,
			meta.ReconciliationFailedReason,
			err.Error(),
		), fmt.Errorf("failed to build kube client: %w", err)
	}

	// generate cueBuild.yaml and calculate the manifests checksum
	checksum, err := r.generate(ctx, kubeClient, cueBuild, dirPath)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			source.GetArtifact().Revision,
			cuebuildv1.BuildFailedReason,
			err.Error(),
		), err
	}

	// build the cueBuild and generate the GC snapshot
	snapshot, err := r.build(ctx, cueBuild, checksum, dirPath)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			source.GetArtifact().Revision,
			cuebuildv1.BuildFailedReason,
			err.Error(),
		), err
	}

	// dry-run apply
	err = r.validate(ctx, cueBuild, impersonation, dirPath)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			source.GetArtifact().Revision,
			cuebuildv1.ValidationFailedReason,
			err.Error(),
		), err
	}

	// apply
	changeSet, err := r.applyWithRetry(ctx, cueBuild, impersonation, source.GetArtifact().Revision, dirPath, 5*time.Second)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			source.GetArtifact().Revision,
			meta.ReconciliationFailedReason,
			err.Error(),
		), err
	}

	// prune
	err = r.prune(ctx, kubeClient, cueBuild, checksum)
	if err != nil {
		return cuebuildv1.CueBuildNotReady(
			cueBuild,
			source.GetArtifact().Revision,
			cuebuildv1.PruneFailedReason,
			err.Error(),
		), err
	}

	// health assessment
	err = r.checkHealth(ctx, statusPoller, cueBuild, source.GetArtifact().Revision, changeSet != "")
	if err != nil {
		return cuebuildv1.CueBuildNotReadySnapshot(
			cueBuild,
			snapshot,
			source.GetArtifact().Revision,
			cuebuildv1.HealthCheckFailedReason,
			err.Error(),
		), err
	}

	return cuebuildv1.CueBuildReady(
		cueBuild,
		snapshot,
		source.GetArtifact().Revision,
		meta.ReconciliationSucceededReason,
		"Applied revision: "+source.GetArtifact().Revision,
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

func (r *KustomizationReconciler) download(artifactURL string, tmpDir string) error {
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

	// extract
	if _, err = untar.Untar(resp.Body, tmpDir); err != nil {
		return fmt.Errorf("failed to untar artifact, error: %w", err)
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

func (r *CueBuildReconciler) generate(ctx context.Context, kubeClient client.Client, cueBuild cuebuildv1.CueBuild, dirPath string) (string, error) {
	gen := NewGenerator(cueBuild, kubeClient)
	return gen.WriteFile(ctx, dirPath)
}

func (r *CueBuildReconciler) build(ctx context.Context, cueBuild cuebuildv1.CueBuild, checksum, dirPath string) (*cuebuildv1.Snapshot, error) {
	timeout := cueBuild.GetTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fs := filesys.MakeFsOnDisk()
	m, err := buildCueBuild(fs, dirPath)
	if err != nil {
		return nil, fmt.Errorf("cuebuild build failed: %w", err)
	}

	resources, err := m.AsYaml()
	if err != nil {
		return nil, fmt.Errorf("cuebuild build failed: %w", err)
	}

	manifestsFile := filepath.Join(dirPath, fmt.Sprintf("%s.yaml", cueBuild.GetUID()))
	if err := fs.WriteFile(manifestsFile, resources); err != nil {
		return nil, err
	}

	return cuebuildv1.NewSnapshot(resources, checksum)
}

func (r *CueBuildReconciler) validate(ctx context.Context, cueBuild cuebuildv1.CueBuild, imp *CueBuildImpersonation, dirPath string) error {
	if cueBuild.Spec.Validation == "" || cueBuild.Spec.Validation == "none" {
		return nil
	}

	timeout := cueBuild.GetTimeout() + (time.Second * 1)
	applyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	validation := cueBuild.Spec.Validation
	if validation == "server" && cueBuild.Spec.Force {
		// Use client-side validation with force
		validation = "client"
		(logr.FromContext(ctx)).Info(fmt.Sprintf("Server-side validation is configured, falling-back to client-side validation since 'force' is enabled"))
	}

	cmd := fmt.Sprintf("cd %s && kubectl apply -f %s.yaml --timeout=%s --dry-run=%s --cache-dir=/tmp --force=%t",
		dirPath, cueBuild.GetUID(), cueBuild.GetTimeout().String(), validation, cueBuild.Spec.Force)

	if cueBuild.Spec.KubeConfig != nil {
		kubeConfig, err := imp.WriteKubeConfig(ctx)
		if err != nil {
			return err
		}
		cmd = fmt.Sprintf("%s --kubeconfig=%s", cmd, kubeConfig)
	} else {
		// impersonate SA
		if cueBuild.Spec.ServiceAccountName != "" {
			saToken, err := imp.GetServiceAccountToken(ctx)
			if err != nil {
				return fmt.Errorf("service account impersonation failed: %w", err)
			}

			cmd = fmt.Sprintf("%s --token %s", cmd, saToken)
		}
	}

	command := exec.CommandContext(applyCtx, "/bin/sh", "-c", cmd)
	output, err := command.CombinedOutput()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("validation timeout: %w", err)
		}
		return fmt.Errorf("validation failed: %s", parseApplyError(output))
	}
	return nil
}

func (r *CueBuildReconciler) apply(ctx context.Context, cueBuild cuebuildv1.CueBuild, imp *CueBuildImpersonation, dirPath string) (string, error) {
	start := time.Now()
	timeout := cueBuild.GetTimeout() + (time.Second * 1)
	applyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	fieldManager := "cuebuild-controller"

	cmd := fmt.Sprintf("cd %s && kubectl apply --field-manager=%s -f %s.yaml --timeout=%s --cache-dir=/tmp --force=%t",
		dirPath, fieldManager, cueBuild.GetUID(), cueBuild.Spec.Interval.Duration.String(), cueBuild.Spec.Force)

	if cueBuild.Spec.KubeConfig != nil {
		kubeConfig, err := imp.WriteKubeConfig(ctx)
		if err != nil {
			return "", err
		}
		cmd = fmt.Sprintf("%s --kubeconfig=%s", cmd, kubeConfig)
	} else {
		// impersonate SA
		if cueBuild.Spec.ServiceAccountName != "" {
			saToken, err := imp.GetServiceAccountToken(ctx)
			if err != nil {
				return "", fmt.Errorf("service account impersonation failed: %w", err)
			}

			cmd = fmt.Sprintf("%s --token %s", cmd, saToken)
		}
	}

	command := exec.CommandContext(applyCtx, "/bin/sh", "-c", cmd)
	output, err := command.CombinedOutput()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", fmt.Errorf("apply timeout: %w", err)
		}

		if string(output) == "" {
			return "", fmt.Errorf("apply failed: %w, kubectl process was killed, probably due to OOM", err)
		}

		return "", fmt.Errorf("apply failed: %s", parseApplyError(output))
	}

	resources := parseApplyOutput(output)
	(logr.FromContext(ctx)).Info(
		fmt.Sprintf("CueBuild applied in %s",
			time.Now().Sub(start).String()),
		"output", resources,
	)

	changeSet := ""
	for obj, action := range resources {
		if action != "" && action != "unchanged" {
			changeSet += obj + " " + action + "\n"
		}
	}
	return changeSet, nil
}

func (r *CueBuildReconciler) applyWithRetry(ctx context.Context, cueBuild cuebuildv1.CueBuild, imp *CueBuildImpersonation, revision, dirPath string, delay time.Duration) (string, error) {
	changeSet, err := r.apply(ctx, cueBuild, imp, dirPath)
	if err != nil {
		// retry apply due to CRD/CR race
		if strings.Contains(err.Error(), "could not find the requested resource") ||
			strings.Contains(err.Error(), "no matches for kind") {
			(logr.FromContext(ctx)).Info("retrying apply", "error", err.Error())
			time.Sleep(delay)
			if changeSet, err := r.apply(ctx, cueBuild, imp, dirPath); err != nil {
				return "", err
			} else {
				if changeSet != "" {
					r.event(ctx, cueBuild, revision, events.EventSeverityInfo, changeSet, nil)
				}
			}
		} else {
			return "", err
		}
	} else {
		if changeSet != "" && cueBuild.Status.LastAppliedRevision != revision {
			r.event(ctx, cueBuild, revision, events.EventSeverityInfo, changeSet, nil)
		}
	}
	return changeSet, nil
}

func (r *CueBuildReconciler) prune(ctx context.Context, kubeClient client.Client, cueBuild cuebuildv1.CueBuild, newChecksum string) error {
	if !cueBuild.Spec.Prune || cueBuild.Status.Snapshot == nil {
		return nil
	}
	if cueBuild.DeletionTimestamp.IsZero() && cueBuild.Status.Snapshot.Checksum == newChecksum {
		return nil
	}

	gc := NewGarbageCollector(kubeClient, *cueBuild.Status.Snapshot, newChecksum, logr.FromContext(ctx))

	if output, ok := gc.Prune(cueBuild.GetTimeout(),
		cueBuild.GetName(),
		cueBuild.GetNamespace(),
	); !ok {
		return fmt.Errorf("garbage collection failed: %s", output)
	} else {
		if output != "" {
			(logr.FromContext(ctx)).Info(fmt.Sprintf("garbage collection completed: %s", output))
			r.event(ctx, cueBuild, newChecksum, events.EventSeverityInfo, output, nil)
		}
	}
	return nil
}

func (r *CueBuildReconciler) checkHealth(ctx context.Context, statusPoller *polling.StatusPoller, cueBuild cuebuildv1.CueBuild, revision string, changed bool) error {
	if len(cueBuild.Spec.HealthChecks) == 0 {
		return nil
	}

	hc := NewHealthCheck(cueBuild, statusPoller)

	if err := hc.Assess(1 * time.Second); err != nil {
		return err
	}

	healthiness := apimeta.FindStatusCondition(cueBuild.Status.Conditions, cuebuildv1.HealthyCondition)
	healthy := healthiness != nil && healthiness.Status == metav1.ConditionTrue

	if !healthy || (cueBuild.Status.LastAppliedRevision != revision && changed) {
		r.event(ctx, cueBuild, revision, events.EventSeverityInfo, "Health check passed", nil)
	}
	return nil
}

func (r *CueBuildReconciler) reconcileDelete(ctx context.Context, cueBuild cuebuildv1.CueBuild) (ctrl.Result, error) {
	if cueBuild.Spec.Prune && !cueBuild.Spec.Suspend {
		// create any necessary kube-clients
		imp := NewCueBuildImpersonation(cueBuild, r.Client, r.StatusPoller, "")
		client, _, err := imp.GetClient(ctx)
		if err != nil {
			err = fmt.Errorf("failed to build kube client for CueBuild: %w", err)
			(logr.FromContext(ctx)).Error(err, "Unable to prune for finalizer")
			return ctrl.Result{}, err
		}
		if err := r.prune(ctx, client, cueBuild, ""); err != nil {
			r.event(ctx, cueBuild, cueBuild.Status.LastAppliedRevision, events.EventSeverityError, "pruning for deleted resource failed", nil)
			// Return the error so we retry the failed garbage collection
			return ctrl.Result{}, err
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
	r.EventRecorder.Event(&cueBuild, "Normal", severity, msg)
	objRef, err := reference.GetReference(r.Scheme, &cueBuild)
	if err != nil {
		(logr.FromContext(ctx)).WithValues(
			strings.ToLower(cueBuild.Kind),
			fmt.Sprintf("%s/%s", cueBuild.GetNamespace(), cueBuild.GetName()),
		).Error(err, "unable to send event")
		return
	}

	if r.ExternalEventRecorder != nil {
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
			(logr.FromContext(ctx)).Error(err, "unable to send event")
			return
		}
	}
}

func (r *CueBuildReconciler) recordReadiness(ctx context.Context, cueBuild cuebuildv1.CueBuild) {
	if r.MetricsRecorder == nil {
		return
	}

	objRef, err := reference.GetReference(r.Scheme, &cueBuild)
	if err != nil {
		(logr.FromContext(ctx)).Error(err, "unable to record readiness metric")
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

func (r *CueBuildReconciler) patchStatus(ctx context.Context, req ctrl.Request, newStatus cuebuildv1.CueBuildStatus) error {
	var cueBuild cuebuildv1.CueBuild
	if err := r.Get(ctx, req.NamespacedName, &cueBuild); err != nil {
		return err
	}

	patch := client.MergeFrom(cueBuild.DeepCopy())
	cueBuild.Status = newStatus

	return r.Status().Patch(ctx, &cueBuild, patch)
}
