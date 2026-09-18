package controller

import (
	"context"
	"db-operator/thirdparty"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	dbFinalizer  = "manageddatabase.tt.yagatito.com/finalizer"
	dbAnnotation = "manageddatabase.tt.yagatito.com/annotation"

	baseRateLimitingDelay = 1 * time.Second
	maxRateLimitingDelay  = 600 * time.Second

	ProvisioningState = "PROVISIONING"
	FailedState       = "FAILED"
	ReadyState        = "READY"
	UnknownState      = "UNKNOWN"
)

type KubeOperator struct {
	client       dynamic.Interface
	dbProvClient thirdparty.DbProvisionerClient
	queue        workqueue.TypedRateLimitingInterface[string]
}

func NewKubeOperator(client dynamic.Interface, dbProvClient thirdparty.DbProvisionerClient) *KubeOperator {
	rateLimiter := workqueue.NewTypedItemExponentialFailureRateLimiter[string](baseRateLimitingDelay, maxRateLimitingDelay)

	return &KubeOperator{
		client:       client,
		dbProvClient: dbProvClient,
		queue:        workqueue.NewTypedRateLimitingQueue(rateLimiter),
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

		case watch.Error:
			log.Printf("[Error] Watcher error received: %v", event.Object)
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
			log.Printf("[Error] syncing failed %s: %v", key, err)
			ko.queue.AddRateLimited(key)
		} else {
			ko.queue.Forget(key)
		}

		ko.queue.Done(key)
	}
}

func (ko *KubeOperator) reconcile(gvr schema.GroupVersionResource, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	unstructObj, err := ko.client.Resource(gvr).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("invalid resource format: %w", err)
	}

	// CASE: DB marked as to be deleted
	if unstructObj.GetDeletionTimestamp() != nil {
		return ko.handleFinalizer(gvr, namespace, name, unstructObj)
	}

	// CASE: DB is to be provisioned/ready
	finalizers := unstructObj.GetFinalizers()
	if !slices.Contains(finalizers, dbFinalizer) {
		unstructObj.SetFinalizers(append(finalizers, dbFinalizer))
		_, err = ko.client.Resource(gvr).Namespace(namespace).Update(context.Background(), unstructObj, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to add finalizer: %w", err)
		}
		return nil
	}

	engine, _, _ := unstructured.NestedString(unstructObj.Object, "spec", "engine")
	sizeGB, _, _ := unstructured.NestedInt64(unstructObj.Object, "spec", "sizeGB")
	k8Sstate, _, _ := unstructured.NestedString(unstructObj.Object, "status", "state")
	dbID, _, _ := unstructured.NestedString(unstructObj.Object, "status", "id")

	// CASE: DB is not created
	if dbID == "" && k8Sstate != UnknownState {
		log.Printf("[Reconcile] Creating external DB for %s...", name)
		provRes, err := ko.dbProvClient.CreateDatabase(name, engine, int(sizeGB))

		if err != nil {
			// Retry
			if errors.Is(err, thirdparty.ServiceUnavailableError) {
				return fmt.Errorf("error creating DB: %w", err)
			}

			// Unknown ID: to be patched manually
			if errors.Is(err, thirdparty.InternalServiceError) {
				unstructured.SetNestedField(unstructObj.Object, UnknownState, "status", "state")

				unstructObj, err = ko.client.Resource(gvr).Namespace(namespace).UpdateStatus(context.Background(), unstructObj, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to update id %s in status: %w", provRes.ID, err)
				}

				annotations := unstructObj.GetAnnotations()
				if annotations == nil {
					annotations = make(map[string]string)
				}
				annotations[dbAnnotation] =
					"API returned 500. Check your cloud provider console. If the DB was created, patch resource status with the ID, otherwise update any spec field."
				unstructObj.SetAnnotations(annotations)

				_, err = ko.client.Resource(gvr).Namespace(namespace).Update(context.Background(), unstructObj, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to write instruction to annotations: %w", err)
				}

				log.Printf("[Finalizer] UNKNOWN state was set to resource due-to error while creating DB: %s", err)
				return nil

			} else {
				log.Printf("[Finalizer] Unexpected error received: %s", err)
			}
		}

		unstructured.SetNestedField(unstructObj.Object, provRes.ID, "status", "id")
		unstructured.SetNestedField(unstructObj.Object, provRes.State, "status", "state")

		_, err = ko.client.Resource(gvr).Namespace(namespace).UpdateStatus(context.Background(), unstructObj, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update id %s in status: %w", provRes.ID, err)
		}

		return nil
	}

	// CASE: DB is in PROVISIONING state or DB id was manually patched by user.
	if k8Sstate == ProvisioningState || (dbID != "" && k8Sstate == UnknownState) {
		log.Printf("[Reconcile] Checking status for DB ID %s...", dbID)

		externalDB, err := ko.dbProvClient.GetDatabase(dbID)
		if err != nil {
			if errors.Is(err, thirdparty.ServiceUnavailableError) {
				return fmt.Errorf("error retrieving DB: %w", err)
			} else {
				log.Printf("[Finalizer] Unexpected error received: %s", err)
			}
		}

		// CASE: still PROVISIONING state
		if externalDB.State == ProvisioningState {
			log.Printf("[Reconcile] DB %s is still %s. Will check again in 10s.", name, externalDB.State)

			ko.queue.AddAfter(key, 10*time.Second)
			return nil
		}

		// CASE: FAILED state
		if externalDB.State == FailedState {
			log.Printf("[Reconcile] DB provisioning failed. Deleting previous DB and re-requesting new one..")

			err := ko.dbProvClient.DeleteDatabase(dbID)
			if err != nil {
				if errors.Is(err, thirdparty.ServiceUnavailableError) {
					return fmt.Errorf("error deleting DB: %w", err)
				} else {
					log.Printf("[Finalizer] Unexpected error received: %s", err)
				}
			} else {
				log.Printf("[Finalizer] External DB deleted successfully. Removing finalizer from K8s...")
			}

			unstructured.SetNestedField(unstructObj.Object, "", "status", "id")
			unstructured.SetNestedField(unstructObj.Object, "", "status", "state")

			_, err = ko.client.Resource(gvr).Namespace(namespace).UpdateStatus(context.Background(), unstructObj, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to re-request DB: %w", err)
			}

			return nil
		}

		unstructured.SetNestedField(unstructObj.Object, ReadyState, "status", "state")
		unstructured.SetNestedField(unstructObj.Object, externalDB.Endpoint, "status", "endpoint")

		_, err = ko.client.Resource(gvr).Namespace(namespace).UpdateStatus(context.Background(), unstructObj, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to apply ready status: %w", err)
		}

		log.Println("[Reconcile] K8S DB state is updated to READY! DONE")
		return nil
	}

	// CASE: DB is in READY state, so SPEC modification of manifiest should NOT be applied
	if k8Sstate == ReadyState {
		externalDB, err := ko.dbProvClient.GetDatabase(dbID)
		if err != nil {
			if errors.Is(err, thirdparty.ServiceUnavailableError) {
				return fmt.Errorf("error retrieving DB: %w", err)
			} else {
				log.Printf("[Finalizer] Unexpected error received: %s", err)
			}
		}

		changed := false

		if engine != externalDB.Engine {
			log.Printf("[Spec Protection] Attempt to change engine from %s to %s. Rolling back.", externalDB.Engine, engine)
			unstructured.SetNestedField(unstructObj.Object, externalDB.Engine, "spec", "engine")
			changed = true
		}

		if sizeGB != int64(externalDB.SizeGB) {
			log.Printf("[Spec Protection] Attempt to change sizeGB from %d to %d. Rolling back.", externalDB.SizeGB, sizeGB)
			unstructured.SetNestedField(unstructObj.Object, int64(externalDB.SizeGB), "spec", "sizeGB")
			changed = true
		}

		if changed {
			_, err = ko.client.Resource(gvr).Namespace(namespace).Update(context.Background(), unstructObj, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to rollback immutable spec fields: %w", err)
			}

			return nil
		}
	}

	log.Printf("[Reconcile] skip (already synced & protected): %s", key)
	return nil
}

func (ko *KubeOperator) handleFinalizer(gvr schema.GroupVersionResource, namespace, name string, obj *unstructured.Unstructured) error {
	finalizers := obj.GetFinalizers()

	if !slices.Contains(finalizers, dbFinalizer) {
		return nil
	}

	dbID, _, _ := unstructured.NestedString(obj.Object, "status", "id")

	if dbID != "" {
		log.Printf("[Finalizer] Deleting external DB ID: %s for resource %s", dbID, name)

		err := ko.dbProvClient.DeleteDatabase(dbID)
		if err != nil {
			if errors.Is(err, thirdparty.ServiceUnavailableError) {
				return fmt.Errorf("error deleting DB: %w", err)
			} else {
				log.Printf("[Finalizer] Unexpected error received: %s", err)
			}
		} else {
			log.Printf("[Finalizer] External DB deleted successfully. Removing finalizer from K8s...")
		}

	} else {
		log.Printf("[Finalizer] Finilizing resource with id %s was not found in k8s", dbID)
	}

	finalizers = slices.DeleteFunc(finalizers, func(s string) bool { return s == dbFinalizer })
	obj.SetFinalizers(finalizers)

	_, err := ko.client.Resource(gvr).Namespace(namespace).Update(context.Background(), obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer: %w", err)
	}

	return nil
}
