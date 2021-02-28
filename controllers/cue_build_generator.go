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
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/kustomize/api/filesys"
	"sigs.k8s.io/kustomize/api/konfig"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/k8sdeps/kunstruct"
	"sigs.k8s.io/yaml"

	cuebuildv1 "github.com/addreas/cuebuild-controller/api/v1alpha1"
)

const (
	transformerFileName = "cuebuild-gc-labels.yaml"
)

type CueBuildGenerator struct {
	cueBuild cuebuildv1.CueBuild
	client.Client
}

func NewGenerator(cueBuild cuebuildv1.CueBuild, kubeClient client.Client) *CueBuildGenerator {
	return &CueBuildGenerator{
		cueBuild: cueBuild,
		Client:   kubeClient,
	}
}

func (kg *CueBuildGenerator) WriteFile(ctx context.Context, dirPath string) (string, error) {
	kfile := filepath.Join(dirPath, konfig.DefaultCueBuildFileName())

	checksum, err := kg.checksum(ctx, dirPath)
	if err != nil {
		return "", err
	}

	if err := kg.generateLabelTransformer(checksum, dirPath); err != nil {
		return "", err
	}

	data, err := ioutil.ReadFile(kfile)
	if err != nil {
		return "", err
	}

	kus := kustypes.CueBuild{
		TypeMeta: kustypes.TypeMeta{
			APIVersion: kustypes.CueBuildVersion,
			Kind:       kustypes.CueBuildKind,
		},
	}

	if err := yaml.Unmarshal(data, &kus); err != nil {
		return "", err
	}

	if len(kus.Transformers) == 0 {
		kus.Transformers = []string{transformerFileName}
	} else {
		var exists bool
		for _, transformer := range kus.Transformers {
			if transformer == transformerFileName {
				exists = true
				break
			}
		}
		if !exists {
			kus.Transformers = append(kus.Transformers, transformerFileName)
		}
	}

	kd, err := yaml.Marshal(kus)
	if err != nil {
		return "", err
	}

	return checksum, ioutil.WriteFile(kfile, kd, os.ModePerm)
}

func checkCueBuildImageExists(images []kustypes.Image, imageName string) (bool, int) {
	for i, image := range images {
		if imageName == image.Name {
			return true, i
		}
	}

	return false, -1
}

func (kg *CueBuildGenerator) generateCueBuild(dirPath string) error {
	fs := filesys.MakeFsOnDisk()

	// Determine if there already is a CueBuild file at the root,
	// as this means we do not have to generate one.
	for _, kfilename := range konfig.RecognizedCueBuildFileNames() {
		if kpath := filepath.Join(dirPath, kfilename); fs.Exists(kpath) && !fs.IsDir(kpath) {
			return nil
		}
	}

	scan := func(base string) ([]string, error) {
		var paths []string
		uf := kunstruct.NewKunstructuredFactoryImpl()
		err := fs.Walk(base, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if path == base {
				return nil
			}
			if info.IsDir() {
				// If a sub-directory contains an existing cueBuild file add the
				// directory as a resource and do not decend into it.
				for _, kfilename := range konfig.RecognizedCueBuildFileNames() {
					if kpath := filepath.Join(path, kfilename); fs.Exists(kpath) && !fs.IsDir(kpath) {
						paths = append(paths, path)
						return filepath.SkipDir
					}
				}
				return nil
			}

			extension := filepath.Ext(path)
			if !containsString([]string{".yaml", ".yml"}, extension) {
				return nil
			}

			fContents, err := fs.ReadFile(path)
			if err != nil {
				return err
			}

			if _, err := uf.SliceFromBytes(fContents); err != nil {
				return fmt.Errorf("failed to decode Kubernetes YAML from %s: %w", path, err)
			}
			paths = append(paths, path)
			return nil
		})
		return paths, err
	}

	abs, err := filepath.Abs(dirPath)
	if err != nil {
		return err
	}

	files, err := scan(abs)
	if err != nil {
		return err
	}

	kfile := filepath.Join(dirPath, konfig.DefaultCueBuildFileName())
	f, err := fs.Create(kfile)
	if err != nil {
		return err
	}
	f.Close()

	kus := kustypes.CueBuild{
		TypeMeta: kustypes.TypeMeta{
			APIVersion: kustypes.CueBuildVersion,
			Kind:       kustypes.CueBuildKind,
		},
	}

	var resources []string
	for _, file := range files {
		resources = append(resources, strings.Replace(file, abs, ".", 1))
	}

	kus.Resources = resources
	kd, err := yaml.Marshal(kus)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(kfile, kd, os.ModePerm)
}

func (kg *CueBuildGenerator) checksum(ctx context.Context, dirPath string) (string, error) {
	if err := kg.generateCueBuild(dirPath); err != nil {
		return "", fmt.Errorf("cueBuild create failed: %w", err)
	}

	fs := filesys.MakeFsOnDisk()
	m, err := buildCueBuild(fs, dirPath)
	if err != nil {
		return "", fmt.Errorf("cueBuild build failed: %w", err)
	}

	resources, err := m.AsYaml()
	if err != nil {
		return "", fmt.Errorf("cueBuild build failed: %w", err)
	}

	return fmt.Sprintf("%x", sha1.Sum(resources)), nil
}

func (kg *CueBuildGenerator) generateLabelTransformer(checksum, dirPath string) error {
	labels := selectorLabels(kg.cueBuild.GetName(), kg.cueBuild.GetNamespace())

	// add checksum label only if GC is enabled
	if kg.cueBuild.Spec.Prune {
		labels = gcLabels(kg.cueBuild.GetName(), kg.cueBuild.GetNamespace(), checksum)
	}

	var lt = struct {
		ApiVersion string `json:"apiVersion" yaml:"apiVersion"`
		Kind       string `json:"kind" yaml:"kind"`
		Metadata   struct {
			Name string `json:"name" yaml:"name"`
		} `json:"metadata" yaml:"metadata"`
		Labels     map[string]string    `json:"labels,omitempty" yaml:"labels,omitempty"`
		FieldSpecs []kustypes.FieldSpec `json:"fieldSpecs,omitempty" yaml:"fieldSpecs,omitempty"`
	}{
		ApiVersion: "builtin",
		Kind:       "LabelTransformer",
		Metadata: struct {
			Name string `json:"name" yaml:"name"`
		}{
			Name: kg.cueBuild.GetName(),
		},
		Labels: labels,
		FieldSpecs: []kustypes.FieldSpec{
			{Path: "metadata/labels", CreateIfNotPresent: true},
		},
	}

	data, err := yaml.Marshal(lt)
	if err != nil {
		return err
	}

	labelsFile := filepath.Join(dirPath, transformerFileName)
	if err := ioutil.WriteFile(labelsFile, data, os.ModePerm); err != nil {
		return err
	}

	return nil
}

func adaptSelector(selector *cuebuild.Selector) (output *kustypes.Selector) {
	if selector != nil {
		output = &kustypes.Selector{}
		output.Gvk.Group = selector.Group
		output.Gvk.Kind = selector.Kind
		output.Gvk.Version = selector.Version
		output.Name = selector.Name
		output.Namespace = selector.Namespace
		output.LabelSelector = selector.LabelSelector
		output.AnnotationSelector = selector.AnnotationSelector
	}
	return
}

// buildCueBuild wraps krusty.MakeCueBuildr with the following settings:
// - disable kyaml due to critical bugs like:
//	 - https://github.com/kubernetes-sigs/cueBuild/issues/3446
//	 - https://github.com/kubernetes-sigs/cueBuild/issues/3480
// - reorder the resources just before output (Namespaces and Cluster roles/role bindings first, CRDs before CRs, Webhooks last)
// - load files from outside the cueBuild.yaml root
// - disable plugins except for the builtin ones
// - prohibit changes to resourceIds, patch name/kind don't overwrite target name/kind
func buildCueBuild(fs filesys.FileSystem, dirPath string) (resmap.ResMap, error) {
	buildOptions := &krusty.Options{
		UseKyaml:               false,
		DoLegacyResourceSort:   true,
		LoadRestrictions:       kustypes.LoadRestrictionsNone,
		AddManagedbyLabel:      false,
		DoPrune:                false,
		PluginConfig:           konfig.DisabledPluginConfig(),
		AllowResourceIdChanges: false,
	}

	k := krusty.MakeCueBuildr(fs, buildOptions)
	return k.Run(dirPath)
}
