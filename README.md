# How to run worker
**1. Provisioner and cluster can be raised by default makefile target.**

**2. To start worker run this command from a root folder of the project:**
```sh
cd operator && go run cmd/main.go
```
<br>

# K8S commands
### Init CRD schema:
```sh
kubectl apply -f crd.yml
```

### Apply CR schema:
```sh
kubectl apply -f - <<'EOF'
apiVersion: tt.yagatito.com/v1alpha1
kind: ManagedDatabase
metadata:
  name: orders
spec:
  engine: postgres
  sizeGB: 20
EOF
```

### Get CR schema:
```sh
kubectl get manageddatabase.tt.yagatito.com
```

### Describe CR schema:
```sh
kubectl describe manageddatabases.tt.yagatito.com orders
```
### Delete CR schema:
```sh
kubectl delete manageddatabases.tt.yagatito.com orders
```

### Patch CR schema with ID:
```sh
kubectl patch manageddatabases.tt.yagatito.com orders \
  --subresource=status \
  --type=merge \
  -p '{"status":{"id":"db-b0cf16a6"}}'
```