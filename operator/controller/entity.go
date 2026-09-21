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
	pollingInterval       = 10 * time.Second

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

type kubeParams struct {
	gvr       schema.GroupVersionResource
	namespace string
	name      string

	obj *unstructured.Unstructured
}

func NewKubeOperator(
	client dynamic.Interface,
	dbProvClient thirdparty.DbProvisionerClient,
) *KubeOperator {
	rateLimiter := workqueue.NewTypedItemExponentialFailureRateLimiter[string](
		baseRateLimitingDelay,
		maxRateLimitingDelay,
	)

	return &KubeOperator{
		client:       client,
		dbProvClient: dbProvClient,
		queue:        workqueue.NewTypedRateLimitingQueue(rateLimiter),
	}
}

// Watch starts a live watcher on the given GVR and dispatches events to the worker queue.
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

		key, err := cache.MetaNamespaceKeyFunc(unstructObj)
		if err != nil {
			return err
		}

		switch event.Type {
		case watch.Added, watch.Modified:
			ko.queue.Add(key)

		case watch.Error:
			log.Printf("[Error] Watcher error received: %v", event.Object)
		}
	}
	return nil
}

// runWorker drains the queue and calls reconcile for each key. Errors are retried via rate-limited requeue.
func (ko *KubeOperator) runWorker(gvr schema.GroupVersionResource) {
	defer ko.queue.ShutDown()

	for {
		key, shutdown := ko.queue.Get()
		if shutdown {
			return
		}

		err := ko.reconcile(gvr, key)

		if err != nil {
			// Retry
			log.Printf("[Error] syncing failed %s: %v", key, err)
			ko.queue.AddRateLimited(key)
		} else {
			ko.queue.Forget(key)
		}

		ko.queue.Done(key)
	}
}

// reconcile is the core state-machine driver. It transitions resources through
// UNKNOWN -> PROVISIONING -> READY/FAILED, or handles deletion via finalizers.
func (ko *KubeOperator) reconcile(gvr schema.GroupVersionResource, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	unstructObj, err := ko.client.Resource(gvr).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("invalid resource format: %w", err)
	}

	// CASE: DB is marked to be deleted
	if unstructObj.GetDeletionTimestamp() != nil {
		return ko.handleFinalizer(kubeParams{
			gvr:       gvr,
			namespace: namespace,
			name:      name,
			obj:       unstructObj,
		})
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

	k8Sstate, _, err := unstructured.NestedString(unstructObj.Object, "status", "state")
	if err != nil {
		return fmt.Errorf("failed to get nested state: %w", err)
	}

	dbID, _, _ := unstructured.NestedString(unstructObj.Object, "status", "id")

	// CASE: DB is not created
	if k8Sstate != UnknownState && dbID == "" {
		return ko.handleInitDbState(kubeParams{
			gvr:       gvr,
			namespace: namespace,
			name:      name,
			obj:       unstructObj,
		})
	}

	// CASE: DB is in PROVISIONING state
	if k8Sstate == ProvisioningState {
		return ko.handleProvisionedDbState(kubeParams{
			gvr:       gvr,
			namespace: namespace,
			name:      name,
			obj:       unstructObj,
		}, dbID)
	}

	// CASE: DB id was manually patched by user
	if k8Sstate == UnknownState && dbID != "" {
		return ko.handleProvisionedDbState(kubeParams{
			gvr:       gvr,
			namespace: namespace,
			name:      name,
			obj:       unstructObj,
		}, dbID)
	}

	// CASE: DB is in READY state, so SPEC modification of manifiest should NOT be applied
	if k8Sstate == ReadyState {
		return ko.handleReadyDbState(kubeParams{
			gvr:       gvr,
			namespace: namespace,
			name:      name,
			obj:       unstructObj,
		}, dbID)
	}

	log.Printf("[Reconcile] skip (already synced & protected): %s", key)
	return nil
}

// handleInitDbState calls the provisioner to create a DB and writes back the result ID/state.
func (ko *KubeOperator) handleInitDbState(args kubeParams) error {
	engine, _, err := unstructured.NestedString(args.obj.Object, "spec", "engine")
	if err != nil {
		return fmt.Errorf("failed to get engine fld: %w", err)
	}
	sizeGB, _, err := unstructured.NestedInt64(args.obj.Object, "spec", "sizeGB")
	if err != nil {
		return fmt.Errorf("failed to get sizeGb fld: %w", err)
	}

	log.Printf("[Reconcile] Creating external DB for %s...", args.name)
	provRes, err := ko.dbProvClient.CreateDatabase(args.name, engine, int(sizeGB))

	if err != nil {
		if errors.Is(err, thirdparty.ErrServiceUnavailable) {
			return fmt.Errorf("error creating DB: %w", err)
		}

		// Unknown ID: to be patched manually
		if errors.Is(err, thirdparty.ErrInternalService) {
			err = unstructured.SetNestedField(args.obj.Object, UnknownState, "status", "state")
			if err != nil {
				return fmt.Errorf("failed to set state in status: %w", err)
			}

			args.obj, err = ko.client.Resource(args.gvr).Namespace(args.namespace).UpdateStatus(context.Background(), args.obj, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to update state %s in status: %w", provRes.ID, err)
			}

			annotations := args.obj.GetAnnotations()
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations[dbAnnotation] =
				"API returned 500. Check your cloud provider console." +
					"If the DB was created, patch resource status with the ID," +
					" otherwise update any spec field."

			args.obj.SetAnnotations(annotations)

			_, err = ko.client.Resource(args.gvr).Namespace(args.namespace).Update(context.Background(), args.obj, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to write instruction to annotations: %w", err)
			}
			log.Printf("[Finalizer] UNKNOWN state was set to resource due-to internal server error while creating DB")

			return nil
		} else {
			log.Printf("[Finalizer] Unexpected error received: %s", err)
		}
	}

	err = unstructured.SetNestedField(args.obj.Object, provRes.ID, "status", "id")
	if err != nil {
		return fmt.Errorf("failed to set result id in status: %w", err)
	}
	err = unstructured.SetNestedField(args.obj.Object, provRes.State, "status", "state")
	if err != nil {
		return fmt.Errorf("failed to set result state in status: %w", err)
	}

	_, err = ko.client.Resource(args.gvr).Namespace(args.namespace).UpdateStatus(context.Background(), args.obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update id %s in status: %w", provRes.ID, err)
	}

	return nil
}

// handleProvisionedDbState polls the provisioner until the DB is READY or FAILED.
// On success it writes endpoint & state.
// On failure it cleans up and resets status and produces error to trigger retry;
func (ko *KubeOperator) handleProvisionedDbState(args kubeParams, databaseID string) error {
	log.Printf("[Reconcile] Checking status for DB ID %s...", databaseID)

	externalDB, err := ko.dbProvClient.GetDatabase(databaseID)
	if err != nil {
		if errors.Is(err, thirdparty.ErrServiceUnavailable) {
			return fmt.Errorf("error retrieving DB: %w", err)
		} else {
			log.Printf("[Finalizer] Unexpected error received: %s", err)
		}
	}

	// CASE: still PROVISIONING state
	if externalDB.State == ProvisioningState {
		log.Printf("[Reconcile] DB %s is still %s. Will check again in 10s.", args.name, externalDB.State)

		key, err := cache.MetaNamespaceKeyFunc(args.obj)
		if err != nil {
			return fmt.Errorf("failed to construct key: %w", err)
		}

		ko.queue.AddAfter(key, pollingInterval)
		return nil
	}

	// CASE: FAILED state
	if externalDB.State == FailedState {
		log.Printf("[Reconcile] DB provisioning failed. Deleting previous DB and re-requesting new one..")

		err := ko.dbProvClient.DeleteDatabase(databaseID)
		if err != nil {
			if errors.Is(err, thirdparty.ErrServiceUnavailable) {
				return fmt.Errorf("error deleting DB: %w", err)
			} else {
				log.Printf("[Finalizer] Unexpected error received: %s", err)
			}
		} else {
			log.Printf("[Finalizer] External DB deleted successfully. Removing finalizer from K8s...")
		}

		err = unstructured.SetNestedField(args.obj.Object, "", "status", "id")
		if err != nil {
			return fmt.Errorf("failed to set empty id in status: %w", err)
		}
		err = unstructured.SetNestedField(args.obj.Object, "", "status", "state")
		if err != nil {
			return fmt.Errorf("failed to set empty state in status: %w", err)
		}

		_, err = ko.client.Resource(args.gvr).Namespace(args.namespace).UpdateStatus(context.Background(), args.obj, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to re-request DB: %w", err)
		}

		return nil
	}

	err = unstructured.SetNestedField(args.obj.Object, externalDB.State, "status", "state")
	if err != nil {
		return fmt.Errorf("failed to update ready state: %w", err)
	}
	err = unstructured.SetNestedField(args.obj.Object, externalDB.Endpoint, "status", "endpoint")
	if err != nil {
		return fmt.Errorf("failed to update endpoint: %w", err)
	}

	_, err = ko.client.Resource(args.gvr).Namespace(args.namespace).UpdateStatus(context.Background(), args.obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to apply ready status: %w", err)
	}

	log.Println("[Reconcile] K8S DB state is updated to READY! DONE")
	return nil
}

// handleReadyDbState enforces spec immutability after creation, rolling back any changes
// and adding a status condition if a rollback occurred.
func (ko *KubeOperator) handleReadyDbState(args kubeParams, databaseID string) error {
	engine, _, err := unstructured.NestedString(args.obj.Object, "spec", "engine")
	if err != nil {
		return fmt.Errorf("failed to get engine fld: %w", err)
	}
	sizeGB, _, err := unstructured.NestedInt64(args.obj.Object, "spec", "sizeGB")
	if err != nil {
		return fmt.Errorf("failed to get sizeGb fld: %w", err)
	}

	externalDB, err := ko.dbProvClient.GetDatabase(databaseID)
	if err != nil {
		if errors.Is(err, thirdparty.ErrServiceUnavailable) {
			return fmt.Errorf("error retrieving DB: %w", err)
		} else {
			log.Printf("[Finalizer] Unexpected error received: %s", err)
		}
	}

	changed := false

	if engine != externalDB.Engine {
		log.Printf("[Spec Protection] Attempt to change engine from %s to %s. Rolling back.", externalDB.Engine, engine)
		err = unstructured.SetNestedField(args.obj.Object, externalDB.Engine, "spec", "engine")
		if err != nil {
			return fmt.Errorf("failed to rollback engine: %w", err)
		}
		changed = true
	}

	if sizeGB != int64(externalDB.SizeGB) {
		log.Printf("[Spec Protection] Attempt to change sizeGB from %d to %d. Rolling back.", externalDB.SizeGB, sizeGB)
		err = unstructured.SetNestedField(args.obj.Object, int64(externalDB.SizeGB), "spec", "sizeGB")
		if err != nil {
			return fmt.Errorf("failed to rollback sizeGb: %w", err)
		}

		changed = true
	}

	// Rollback
	args.obj, err = ko.client.Resource(args.gvr).Namespace(args.namespace).Update(context.Background(), args.obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to rollback immutable spec fields: %w", err)
	}

	if changed {
		// Condition to inform about immutable field change attept
		condition := map[string]any{
			"type":               "Ready",
			"status":             "False",
			"reason":             "FieldIsImmutable",
			"message":            "The sizeGB field cannot be modified after database creation. Rolling back to the original value.",
			"lastTransitionTime": time.Now().Format(time.RFC3339),
		}

		_, err = ko.addCondition(args, condition)
		if err != nil {
			return fmt.Errorf("failed to add condition: %w", err)
		}
	}

	return nil
}

// addCondition appends a status condition to the resource and persists it to K8s.
func (ko *KubeOperator) addCondition(args kubeParams, cond map[string]any) (*unstructured.Unstructured, error) {
	conditions, _, _ := unstructured.NestedSlice(args.obj.Object, "status", "conditions")
	if conditions == nil {
		conditions = make([]any, 1)
		conditions[0] = cond
	} else {
		conditions = append(conditions, cond)
	}

	err := unstructured.SetNestedSlice(args.obj.Object, conditions, "status", "conditions")
	if err != nil {
		return nil, fmt.Errorf("failed to set status.conditions: %w", err)
	}

	args.obj, err = ko.client.Resource(args.gvr).Namespace(args.namespace).UpdateStatus(context.Background(), args.obj, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to apply status.conditions: %w", err)
	}

	return args.obj, nil
}

// handleFinalizer cleans up the external DB when the K8s resource is deleted
// (detected via DeletionTimestamp) and removes the managed finalizer.
func (ko *KubeOperator) handleFinalizer(args kubeParams) error {
	finalizers := args.obj.GetFinalizers()

	if !slices.Contains(finalizers, dbFinalizer) {
		return nil
	}

	dbID, _, err := unstructured.NestedString(args.obj.Object, "status", "id")
	if err != nil {
		return fmt.Errorf("failed to get id from unstructured:%w", err)
	}

	if dbID != "" {
		log.Printf("[Finalizer] Deleting external DB ID: %s for resource %s", dbID, args.name)

		err := ko.dbProvClient.DeleteDatabase(dbID)
		if err != nil {
			if errors.Is(err, thirdparty.ErrServiceUnavailable) {
				return fmt.Errorf("error deleting DB: %w", err)
			} else {
				log.Printf("[Finalizer] Unexpected error received: %s", err)
			}
		} else {
			log.Printf(
				"[Finalizer] External DB deleted successfully. Removing finalizer from K8s...",
			)
		}
	} else {
		log.Printf("[Finalizer] Finilizing resource with id %s was not found in k8s", dbID)
	}

	finalizers = slices.DeleteFunc(finalizers, func(s string) bool { return s == dbFinalizer })
	args.obj.SetFinalizers(finalizers)

	_, err = ko.client.Resource(args.gvr).Namespace(args.namespace).Update(context.Background(), args.obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer: %w", err)
	}

	return nil
}
