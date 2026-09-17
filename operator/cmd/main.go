package main

import (
	"db-operator/controller"
	"log"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	K8SGroup        = "tt.yagatito.com"
	K8SVersion      = "v1alpha1"
	K8SResourceName = "manageddatabases"
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

	operator := controller.NewKubeOperator(dynamicClient)

	operator.Watch(schema.GroupVersionResource{
		Group:    K8SGroup,
		Version:  K8SVersion,
		Resource: K8SResourceName,
	})

	return nil
}
