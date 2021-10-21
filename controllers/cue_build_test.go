package controllers

import (
	"fmt"
	"testing"

	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/load"
)

func TestCueBuildHomelab(t *testing.T) {

	cuectx := cuecontext.New()
	instances := load.Instances([]string{
		"./resources/bitwarden",
		"./resources/default",
		"./resources/fogis",
		"./resources/grrrr",
		"./resources/hass",
		"./resources/monitoring",
		"./resources/unifi",
		"./resources/system/cert-manager",
		"./resources/system/flux-system",
		"./resources/system/kube-system",
		"./resources/system/longhorn-system",
		"./resources/system/metallb-system",
	}, &load.Config{
		Dir: "/home/addem/Documents/k8s/homelab/",
	})
	for _, instance := range instances {
		fmt.Printf("Instance: %+v\n", instance)
	}
	values, err := cuectx.BuildInstances(instances)
	fmt.Printf("Values: %+v\n", values)
	fmt.Printf("Err: %+v\n", err)
}
