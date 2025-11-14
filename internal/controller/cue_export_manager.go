/*
Copyright 2025 The Flux authors

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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/fluxcd/pkg/runtime/predicates"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	cuev1 "github.com/addreas/cue-controller/api/v1beta2"
)

// CueExportReconcilerOptions contains options for the CueReconciler.
type CueReconcilerOptions struct {
	RateLimiter            workqueue.TypedRateLimiter[reconcile.Request]
	WatchConfigsPredicate  predicate.Predicate
	WatchExternalArtifacts bool
}

// SetupWithManager sets up the controller with the Manager.
// It indexes the CueExports by the source references, and sets up watches for
// changes in those sources, as well as for ConfigMaps and Secrets that the CueExports depend on.
func (r *CueReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager, opts CueReconcilerOptions) error {
	const (
		indexExternalArtifact = ".metadata.externalArtifact"
		indexOCIRepository    = ".metadata.ociRepository"
		indexGitRepository    = ".metadata.gitRepository"
		indexBucket           = ".metadata.bucket"
		indexConfigMap        = ".metadata.configMap"
		indexSecret           = ".metadata.secret"
	)

	// Index the CueExports by the OCIRepository references they (may) point at.
	if err := mgr.GetCache().IndexField(ctx, &cuev1.CueExport{}, indexOCIRepository,
		r.indexBy(sourcev1.OCIRepositoryKind)); err != nil {
		return fmt.Errorf("failed creating index %s: %w", indexOCIRepository, err)
	}

	// Index the CueExports by the GitRepository references they (may) point at.
	if err := mgr.GetCache().IndexField(ctx, &cuev1.CueExport{}, indexGitRepository,
		r.indexBy(sourcev1.GitRepositoryKind)); err != nil {
		return fmt.Errorf("failed creating index %s: %w", indexGitRepository, err)
	}

	// Index the CueExports by the Bucket references they (may) point at.
	if err := mgr.GetCache().IndexField(ctx, &cuev1.CueExport{}, indexBucket,
		r.indexBy(sourcev1.BucketKind)); err != nil {
		return fmt.Errorf("failed creating index %s: %w", indexBucket, err)
	}

	// Index the CueExports by the ExternalArtifact references they (may) point at (if enabled).
	if opts.WatchExternalArtifacts {
		if err := mgr.GetCache().IndexField(ctx, &cuev1.CueExport{}, indexExternalArtifact,
			r.indexBy(sourcev1.ExternalArtifactKind)); err != nil {
			return fmt.Errorf("failed creating index %s: %w", indexExternalArtifact, err)
		}
	}

	// Index the CueExport by the ConfigMap references they point to.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &cuev1.CueExport{}, indexConfigMap,
		func(o client.Object) []string {
			obj := o.(*cuev1.CueExport)
			namespace := obj.GetNamespace()
			var keys []string
			if kc := obj.Spec.KubeConfig; kc != nil && kc.ConfigMapRef != nil {
				keys = append(keys, fmt.Sprintf("%s/%s", namespace, kc.ConfigMapRef.Name))
			}
			if tags := obj.Spec.Tags; tags != nil {
				for _, ref := range tags {
					if ref.ValueFrom != nil && ref.ValueFrom.ConfigMapKeyRef != nil {
						keys = append(keys, fmt.Sprintf("%s/%s", namespace, ref.ValueFrom.ConfigMapKeyRef.Name))
					}
				}
			}
			return keys
		},
	); err != nil {
		return fmt.Errorf("failed creating index %s: %w", indexConfigMap, err)
	}

	// Index the CueExport by the Secret references they point to.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &cuev1.CueExport{}, indexSecret,
		func(o client.Object) []string {
			obj := o.(*cuev1.CueExport)
			namespace := obj.GetNamespace()
			var keys []string

			if kc := obj.Spec.KubeConfig; kc != nil && kc.SecretRef != nil {
				keys = append(keys, fmt.Sprintf("%s/%s", namespace, kc.SecretRef.Name))
			}
			if tags := obj.Spec.Tags; tags != nil {
				for _, ref := range tags {
					if ref.ValueFrom != nil && ref.ValueFrom.SecretKeyRef != nil {
						keys = append(keys, fmt.Sprintf("%s/%s", namespace, ref.ValueFrom.SecretKeyRef.Name))
					}
				}
			}
			return keys
		},
	); err != nil {
		return fmt.Errorf("failed creating index %s: %w", indexSecret, err)
	}

	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).
		For(&cuev1.CueExport{}, builder.WithPredicates(
			predicate.Or(predicate.GenerationChangedPredicate{}, predicates.ReconcileRequestedPredicate{}),
		)).
		Watches(
			&sourcev1.OCIRepository{},
			handler.EnqueueRequestsFromMapFunc(r.requestsForRevisionChangeOf(indexOCIRepository)),
			builder.WithPredicates(SourceRevisionChangePredicate{}),
		).
		Watches(
			&sourcev1.GitRepository{},
			handler.EnqueueRequestsFromMapFunc(r.requestsForRevisionChangeOf(indexGitRepository)),
			builder.WithPredicates(SourceRevisionChangePredicate{}),
		).
		Watches(
			&sourcev1.Bucket{},
			handler.EnqueueRequestsFromMapFunc(r.requestsForRevisionChangeOf(indexBucket)),
			builder.WithPredicates(SourceRevisionChangePredicate{}),
		).
		WatchesMetadata(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.requestsForConfigDependency(indexConfigMap)),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}, opts.WatchConfigsPredicate),
		).
		WatchesMetadata(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.requestsForConfigDependency(indexSecret)),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}, opts.WatchConfigsPredicate),
		)

	if opts.WatchExternalArtifacts {
		ctrlBuilder = ctrlBuilder.Watches(
			&sourcev1.ExternalArtifact{},
			handler.EnqueueRequestsFromMapFunc(r.requestsForRevisionChangeOf(indexExternalArtifact)),
			builder.WithPredicates(SourceRevisionChangePredicate{}),
		)
	}

	return ctrlBuilder.WithOptions(controller.Options{RateLimiter: opts.RateLimiter}).Complete(r)
}
