package controller

import (
	"context"
	"db-operator/thirdparty"
	"fmt"
	"log"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/workqueue"
)

type KubeOperator struct {
	client       dynamic.Interface
	dbProvClient thirdparty.DbProvisionerClient
	queue        workqueue.TypedRateLimitingInterface[string]
}

func NewKubeOperator(client dynamic.Interface, dbProvClient thirdparty.DbProvisionerClient) *KubeOperator {
	return &KubeOperator{
		client:       client,
		dbProvClient: dbProvClient,
		queue:        workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
	}
}

func (ko *KubeOperator) Watch(gvr schema.GroupVersionResource) error {
	watcher, err := ko.client.Resource(gvr).Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	defer watcher.Stop()

	go ko.runWorker(gvr)

	log.Println("Watcher and Worker are active! Ready to process.")

	for event := range watcher.ResultChan() {
		unstructObj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			log.Println("Received unexpected event type or error status")
			continue
		}

		key := fmt.Sprintf("%s/%s", unstructObj.GetNamespace(), unstructObj.GetName())

		switch event.Type {
		case watch.Added, watch.Modified:
			ko.queue.Add(key)

		case watch.Deleted:
			ko.delete(unstructObj)
		}
	}
	return nil
}

func (ko *KubeOperator) runWorker(gvr schema.GroupVersionResource) {
	defer ko.queue.ShutDown()

	for {
		key, shutdown := ko.queue.Get()
		if shutdown {
			return
		}

		err := ko.reconcile(gvr, key)

		if err != nil {
			log.Printf("Error syncing %s: %v", key, err)
			ko.queue.AddRateLimited(key)
		} else {
			ko.queue.Forget(key)
		}

		ko.queue.Done(key)
	}
}

func (ko *KubeOperator) reconcile(gvr schema.GroupVersionResource, key string) error {
	var namespace, name string
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid key")
	}
	namespace = parts[0]
	name = parts[1]

	unstructObj, err := ko.client.Resource(gvr).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return nil
	}

	engine, _, _ := unstructured.NestedString(unstructObj.Object, "spec", "engine")
	sizeGB, _, _ := unstructured.NestedInt64(unstructObj.Object, "spec", "sizeGB")
	state, _, _ := unstructured.NestedString(unstructObj.Object, "status", "state")
	dbID, foundID, _ := unstructured.NestedString(unstructObj.Object, "status", "id")

	// CASE: DB is not created
	if !foundID {
		log.Printf("[Reconcile] Creating external DB for %s...", name)
		provRes, err := ko.dbProvClient.CreateDatabase(name, engine, int(sizeGB))
		if err != nil {
			return fmt.Errorf("error creating DB: %w", err)
		}

		unstructured.SetNestedField(unstructObj.Object, provRes.ID, "status", "id")
		unstructured.SetNestedField(unstructObj.Object, provRes.State, "status", "state")

		_, err = ko.client.Resource(gvr).Namespace(namespace).UpdateStatus(context.Background(), unstructObj, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to apply new status: %w", err)
		}

		ko.queue.AddAfter(key, 20*time.Second)
		return nil
	}

	// CASE: DB is in PROVISIONING state
	if state == thirdparty.ProvisioningState {
		log.Printf("[Reconcile] Checking status for DB ID %s...", dbID)

		externalDB, err := ko.dbProvClient.GetDatabase(dbID)
		if err != nil {
			return fmt.Errorf("error retrieving DB: %w", err)
		}

		if externalDB.State != thirdparty.ReadyState {
			log.Printf("[Reconcile] DB %s is still %s. Will check again in 20s.", name, externalDB.State)
			ko.queue.AddAfter(key, 20*time.Second)
			return nil
		}

		unstructured.SetNestedField(unstructObj.Object, thirdparty.ReadyState, "status", "state")
		unstructured.SetNestedField(unstructObj.Object, externalDB.Endpoint, "status", "endpoint")

		_, err = ko.client.Resource(gvr).Namespace(namespace).UpdateStatus(context.Background(), unstructObj, metav1.UpdateOptions{})
		return err
	}

	return nil
}

func (ko *KubeOperator) delete(obj *unstructured.Unstructured) {
	dbID, foundID, _ := unstructured.NestedString(obj.Object, "status", "id")
	if foundID {
		log.Printf("[DELETE] Removing external DB ID: %s", dbID)
		_ = ko.dbProvClient.DeleteDatabase(dbID)
	}
}
