package main

import (
	"db-operator/controller"
	"db-operator/thirdparty"
	"log"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	k8sGroup            = "tt.yagatito.com"
	k8sVersion          = "v1alpha1"
	k8sResourceName     = "manageddatabases"
	provisioningBaseUrl = "http://localhost:8080"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	log.Println("Starting K8s Dynamic Watcher...")

	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	kubeconfig := filepath.Join(home, ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	provClient := thirdparty.NewProvisionerClient(provisioningBaseUrl)
	operator := controller.NewKubeOperator(dynamicClient, provClient)

	err = operator.Watch(schema.GroupVersionResource{
		Group:    k8sGroup,
		Version:  k8sVersion,
		Resource: k8sResourceName,
	})
	if err != nil {
		return err
	}

	return nil
}
