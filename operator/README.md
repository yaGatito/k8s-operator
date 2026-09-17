# Take-home: ManagedDatabase operator

We would much rather see 200 lines you can defend than 2000 you cannot.

## What you are building

This repository contains a running "provisioning API": a fake external service
that creates databases. Your task is to write a Kubernetes operator in Go so
that people can order databases declaratively:

```sh
kubectl apply -f - <<'EOF'
apiVersion: demo.example.com/v1alpha1
kind: ManagedDatabase
metadata:
  name: orders
spec:
  engine: postgres
  sizeGB: 20
EOF
```

```
$ kubectl get manageddatabase
NAME     ENGINE     SIZE   STATE   AGE
orders   postgres   20     Ready   47s
```

Deleting the resource must delete the database in the external API.

## Getting started

You need Docker, `kind`, `kubectl` and Go.

```sh
make up      # kind cluster named "take-home" + provisioning API on :8080
make check   # both are alive
make logs    # follow the API log, useful while debugging
make reset   # wipe the API's state, keep the cluster
make down    # tear everything down
```

The API is reachable from your machine at `http://localhost:8080`. Running your
controller locally against the cluster is fine and expected; you do not need to
containerise it.

If building the container image is awkward on your network, run the API
straight from source instead. It has no dependencies beyond the standard
library:

```sh
make provisioner-local
```

Read [`provisioner/README.md`](provisioner/README.md) before you start. It
describes the endpoints and, more importantly, the ways the service misbehaves.

## Requirements

1. `status` reflects reality. Someone running `kubectl get` and
   `kubectl describe` can tell whether the database is being provisioned, ready
   or failed, and how to connect to it once it is ready.
2. Deleting the custom resource deletes the external database.
3. **One custom resource must never result in two databases.** This has to hold
   even if your controller is killed at an arbitrary moment and restarted.
4. The controller keeps working while the API is unreliable.

Requirement 3 is the interesting one. Read the provisioner README carefully
before deciding how to satisfy it.

## Explicitly not required

No admission webhooks. No multi-tenancy. No metrics, dashboards or tracing. No
production hardening. A single namespace is fine. A couple of unit tests on the
core decision logic is enough; we are not looking for coverage.

Scaffolding the project with `kubebuilder` or `operator-sdk` is fine.

## Deliverables

**1. The code**, in a git repository with a readable history.

**2. `DECISIONS.md`**, one page maximum, answering these five questions:

- **a.** The API can create a database and then lose the response. How does
  your controller avoid creating a duplicate on the next reconcile? What risk
  remains that you did not eliminate?

- **b.** What would you change about this external API to make your job easier?
  How would you argue for it to the team that owns it and has its own backlog?

- **c.** Deleting the external database can keep failing. Do you block deletion
  of the custom resource indefinitely, or give up at some point and let it go?
  Justify the choice you made.

- **d.** Someone edits `sizeGB` after the database exists. The API has no
  resize operation. What does your controller do, and what does the user see?

- **e.** What did you deliberately leave out, and what would you do next?

**3. How to run it.** A few lines in the README. A short screen recording is
welcome but not required.

## On AI tools

Use whatever you like. 

