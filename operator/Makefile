IMAGE   := provisioner:latest
CLUSTER := take-home
PORT    := 8080

.PHONY: up down cluster provisioner provisioner-local reset logs check

up: cluster provisioner

cluster:
	@kind get clusters 2>/dev/null | grep -qx $(CLUSTER) \
		|| kind create cluster --name $(CLUSTER)
	@kubectl config use-context kind-$(CLUSTER)

provisioner:
	@docker build -q -t $(IMAGE) ./provisioner
	@docker rm -f provisioner >/dev/null 2>&1 || true
	@docker run -d --name provisioner -p $(PORT):8080 $(IMAGE) >/dev/null
	@echo "provisioner is listening on http://localhost:$(PORT)"

provisioner-local:
	@echo "provisioner will listen on http://localhost:$(PORT), stop it with ctrl-c"
	@cd provisioner && go run . -addr=:$(PORT)

reset:
	@docker rm -f provisioner >/dev/null 2>&1 || true
	@$(MAKE) --no-print-directory provisioner

logs:
	@docker logs -f provisioner

check:
	@curl -sf http://localhost:$(PORT)/healthz >/dev/null && echo "provisioner: ok" || echo "provisioner: down"
	@kubectl cluster-info --context kind-$(CLUSTER) >/dev/null 2>&1 && echo "cluster: ok" || echo "cluster: down"

down:
	@docker rm -f provisioner >/dev/null 2>&1 || true
	@kind delete cluster --name $(CLUSTER)
