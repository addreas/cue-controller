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
	"io/ioutil"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	cuebuildv1 "github.com/fluxcd/cuebuild-controller/api/v1alpha1"
)

type CueBuildImpersonation struct {
	workdir      string
	cueBuild     cuebuildv1.CueBuild
	statusPoller *polling.StatusPoller
	client.Client
}

func NewCueBuildImpersonation(
	cueBuild cuebuildv1.CueBuild,
	kubeClient client.Client,
	statusPoller *polling.StatusPoller,
	workdir string) *CueBuildImpersonation {
	return &CueBuildImpersonation{
		workdir:      workdir,
		cueBuild:     cueBuild,
		statusPoller: statusPoller,
		Client:       kubeClient,
	}
}

func (cbi *CueBuildImpersonation) GetServiceAccountToken(ctx context.Context) (string, error) {
	namespacedName := types.NamespacedName{
		Namespace: cbi.cueBuild.Namespace,
		Name:      cbi.cueBuild.Spec.ServiceAccountName,
	}

	var serviceAccount corev1.ServiceAccount
	err := cbi.Client.Get(ctx, namespacedName, &serviceAccount)
	if err != nil {
		return "", err
	}

	secretName := types.NamespacedName{
		Namespace: cbi.cueBuild.Namespace,
		Name:      cbi.cueBuild.Spec.ServiceAccountName,
	}

	for _, secret := range serviceAccount.Secrets {
		if strings.HasPrefix(secret.Name, fmt.Sprintf("%s-token", serviceAccount.Name)) {
			secretName.Name = secret.Name
			break
		}
	}

	var secret corev1.Secret
	err = cbi.Client.Get(ctx, secretName, &secret)
	if err != nil {
		return "", err
	}

	var token string
	if data, ok := secret.Data["token"]; ok {
		token = string(data)
	} else {
		return "", fmt.Errorf("the service account secret '%s' does not containt a token", secretName.String())
	}

	return token, nil
}

// GetClient creates a controller-runtime client for talcbing to a Kubernetes API server.
// If KubeConfig is set, will use the kubeconfig bytes from the Kubernetes secret.
// If ServiceAccountName is set, will use the cluster provided kubeconfig impersonating the SA.
// If --kubeconfig is set, will use the kubeconfig file at that location.
// Otherwise will assume running in cluster and use the cluster provided kubeconfig.
func (cbi *CueBuildImpersonation) GetClient(ctx context.Context) (client.Client, *polling.StatusPoller, error) {
	if cbi.cueBuild.Spec.KubeConfig == nil {
		if cbi.cueBuild.Spec.ServiceAccountName != "" {
			return cbi.clientForServiceAccount(ctx)
		}

		return cbi.Client, cbi.statusPoller, nil
	}
	return cbi.clientForKubeConfig(ctx)
}

func (cbi *CueBuildImpersonation) clientForServiceAccount(ctx context.Context) (client.Client, *polling.StatusPoller, error) {
	token, err := cbi.GetServiceAccountToken(ctx)
	if err != nil {
		return nil, nil, err
	}
	restConfig, err := config.GetConfig()
	if err != nil {
		return nil, nil, err
	}
	restConfig.BearerToken = token
	restConfig.BearerTokenFile = "" // Clear, as it overrides BearerToken

	restMapper, err := apiutil.NewDynamicRESTMapper(restConfig)
	if err != nil {
		return nil, nil, err
	}

	client, err := client.New(restConfig, client.Options{Mapper: restMapper})
	if err != nil {
		return nil, nil, err
	}

	statusPoller := polling.NewStatusPoller(client, restMapper)
	return client, statusPoller, err

}

func (cbi *CueBuildImpersonation) clientForKubeConfig(ctx context.Context) (client.Client, *polling.StatusPoller, error) {
	kubeConfigBytes, err := cbi.getKubeConfig(ctx)
	if err != nil {
		return nil, nil, err
	}

	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeConfigBytes)
	if err != nil {
		return nil, nil, err
	}

	restMapper, err := apiutil.NewDynamicRESTMapper(restConfig)
	if err != nil {
		return nil, nil, err
	}

	client, err := client.New(restConfig, client.Options{Mapper: restMapper})
	if err != nil {
		return nil, nil, err
	}

	statusPoller := polling.NewStatusPoller(client, restMapper)

	return client, statusPoller, err
}

func (cbi *CueBuildImpersonation) WriteKubeConfig(ctx context.Context) (string, error) {
	secretName := types.NamespacedName{
		Namespace: cbi.cueBuild.GetNamespace(),
		Name:      cbi.cueBuild.Spec.KubeConfig.SecretRef.Name,
	}

	kubeConfig, err := cbi.getKubeConfig(ctx)
	if err != nil {
		return "", err
	}

	f, err := ioutil.TempFile(cbi.workdir, "kubeconfig")
	defer f.Close()
	if err != nil {
		return "", fmt.Errorf("unable to write KubeConfig secret '%s' to storage: %w", secretName.String(), err)
	}
	if _, err := f.Write(kubeConfig); err != nil {
		return "", fmt.Errorf("unable to write KubeConfig secret '%s' to storage: %w", secretName.String(), err)
	}
	return f.Name(), nil
}

func (cbi *CueBuildImpersonation) getKubeConfig(ctx context.Context) ([]byte, error) {
	secretName := types.NamespacedName{
		Namespace: cbi.cueBuild.GetNamespace(),
		Name:      cbi.cueBuild.Spec.KubeConfig.SecretRef.Name,
	}

	var secret corev1.Secret
	if err := cbi.Get(ctx, secretName, &secret); err != nil {
		return nil, fmt.Errorf("unable to read KubeConfig secret '%s' error: %w", secretName.String(), err)
	}

	kubeConfig, ok := secret.Data["value"]
	if !ok {
		return nil, fmt.Errorf("KubeConfig secret '%s' doesn't contain a 'value' key ", secretName.String())
	}

	return kubeConfig, nil
}
