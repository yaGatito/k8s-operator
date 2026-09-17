# Provisioning API

A fake external service that creates databases. It runs in a container, keeps
everything in memory, and is deliberately unreliable.

Base URL: `http://localhost:8080`

## Endpoints

### `POST /databases`

```json
{ "name": "orders", "engine": "postgres", "sizeGB": 20 }
```

`201 Created`:

```json
{ "id": "db-9c69e07d", "name": "orders", "engine": "postgres",
  "sizeGB": 20, "state": "PROVISIONING" }
```

`400` if `name`, `engine` or `sizeGB` is missing or empty.

### `GET /databases/{id}`

`200 OK`:

```json
{ "id": "db-9c69e07d", "name": "orders", "engine": "postgres", "sizeGB": 20,
  "state": "READY", "endpoint": "db-9c69e07d.db.internal:5432" }
```

`404` if the id is unknown. `endpoint` is present only in state `READY`.

### `DELETE /databases/{id}`

`204 No Content` on success, `404` if the id is unknown.

### `GET /databases`

`501 Not Implemented`. Listing is not supported. This is intentional.

## States

```
PROVISIONING -> READY
PROVISIONING -> FAILED
```

Provisioning takes 20 to 60 seconds. A small share of databases end in
`FAILED`. States never change after `READY` or `FAILED`.

## What this service does to you

These are not bugs. They model a real remote dependency and they are the point
of the exercise.

| Behaviour | Rate | Consequence |
|---|---|---|
| Any call rejected with `503` before it is processed | ~10% | Retries are unavoidable |
| A create succeeds, then its response is lost | ~15% of creates | An error does not mean nothing happened |
| Provisioning ends in `FAILED` | ~5% | The happy path is not the only path |

Two further properties, always on:

- **`POST` is not idempotent.** Two calls with the same name create two
  databases with two different ids.
- **There is no way to find a database by name.** Only by id. If you lose an
  id, the database is unreachable through this API.

`DELETE` is irreversible. There is no undo and no recycle bin.

## Diagnostics

`GET /_debug/databases` returns everything the service currently holds, sorted
by name. Neither this endpoint nor `/healthz` is subject to the failure
injection above.

```sh
curl -s localhost:8080/_debug/databases | jq
```

Use it to verify your own work, in particular that one resource never produced
two databases.

**Your controller must not call `/_debug/`.** It exists because a real operator
would have a support tool, not because the API supports listing.

## Tuning

Every rate is a flag, so the exercise can be made harsher or gentler:

```sh
docker run -p 8080:8080 provisioner:latest \
  -unavailable-rate=0.3 \
  -lost-response-rate=0.5 \
  -provision-fail-rate=0 \
  -min-provisioning=5s -max-provisioning=10s
```

| Flag | Default |
|---|---|
| `-addr` | `:8080` |
| `-unavailable-rate` | `0.10` |
| `-lost-response-rate` | `0.15` |
| `-provision-fail-rate` | `0.05` |
| `-min-provisioning` | `20s` |
| `-max-provisioning` | `60s` |

Setting every rate to `0` gives a clean API, which is useful while you get the
basic flow working. Set them back before you call the exercise finished.

State is in memory only. `make reset` restarts the service with an empty store.
