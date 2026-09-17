package controller

import (
	"context"
	"fmt"
	"log"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type KubeOperator struct {
	client dynamic.Interface
}

func NewKubeOperator(client dynamic.Interface) *KubeOperator {
	return &KubeOperator{
		client: client,
	}
}

func (ko *KubeOperator) Watch(gvr schema.GroupVersionResource) error {
	watcher, err := ko.client.Resource(gvr).Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	defer watcher.Stop()

	log.Println("Watcher is active and stable! Ready to catch events. Press Ctrl+C to exit.")

	for event := range watcher.ResultChan() {
		unstructObj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			log.Println("Received unexpected event type or error status")
			continue
		}

		name := unstructObj.GetName()

		engine, _, _ := unstructured.NestedString(unstructObj.Object, "spec", "engine")
		sizeGB, _, _ := unstructured.NestedInt64(unstructObj.Object, "spec", "sizeGB")
		state, _, _ := unstructured.NestedString(unstructObj.Object, "status", "state")

		switch event.Type {
		case watch.Added:
			fmt.Printf("\n[ADDED] Catch new database request!\n")
			fmt.Printf("Name: %s | Engine: %s | Size: %d GB\n", name, engine, sizeGB)

		case watch.Modified:
			fmt.Printf("\n[MODIFIED] Database object changed in K8s!\n")
			fmt.Printf("Name: %s | Current Status State: %s\n", name, state)

		case watch.Deleted:
			fmt.Printf("\n[DELETED] Database object removed from K8s!\n")
			fmt.Printf("Name: %s\n", name)
		}
	}
	return nil
}
