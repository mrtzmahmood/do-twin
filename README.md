# DoTwin — backend + docker-compose

This adds a real backend (FastAPI) and a docker-compose stack around the
existing single-file app, so the browser no longer talks to MinIO /
RabbitMQ / MQTT directly (which is what caused the CORS / `Origin: null` /
mixed-content problems).

## Services

| Service    | Purpose                                   | Ports                  |
|------------|--------------------------------------------|------------------------|
| minio      | Object storage (pipelines, registry, jobs) | 9000 (API), 9001 (UI)  |
| rabbitmq   | Job queue                                  | 5672 (AMQP), 15672 (UI)|
| mosquitto  | MQTT broker (raw + WebSocket)              | 1883, 9883             |
| postgres   | Sample DB for the pipeline's SQL Source node | 5432                 |
| mysql      | Sample DB for the pipeline's SQL Source node | 3306                 |
| worker     | Consumes queued runs, actually trains models, reports progress | — |
| backend    | FastAPI — proxies storage/queue/mqtt/db    | 8000                   |
| nginx      | Serves the frontend, reverse-proxies /api  | 8080                   |

## Run it

```bash
docker compose up --build
```

Then open **http://localhost:8080** — not `file://…` and not opening the
HTML directly, since that's exactly what triggered the RabbitMQ/CORS issue.

- MinIO console: http://localhost:9001 (user/pass from `.env` or the
  defaults below)
- RabbitMQ management UI: http://localhost:15672
- Backend API docs (Swagger): http://localhost:8000/docs

Defaults (override via a `.env` file next to `docker-compose.yml`):
```
MINIO_ROOT_USER=dotwin
MINIO_ROOT_PASSWORD=dotwin12345
RABBITMQ_USER=dotwin
RABBITMQ_PASSWORD=dotwin12345
POSTGRES_USER=dotwin / POSTGRES_PASSWORD=dotwin12345 / POSTGRES_DB=dotwin_sample
MYSQL_USER=dotwin / MYSQL_PASSWORD=dotwin12345 / MYSQL_DATABASE=dotwin_sample
```

> **RabbitMQ credentials aren't templated.** `rabbitmq/definitions.json` is a
> static file RabbitMQ imports at boot (it creates the `dotwin` user, the `/`
> vhost, and the exchange/queues) — it can't read `.env` variables. If you
> change `RABBITMQ_USER`/`RABBITMQ_PASSWORD`, also edit the `users`/
> `permissions` entries in `rabbitmq/definitions.json` to match, or the
> backend will try to authenticate with credentials RabbitMQ doesn't have.

> **If RabbitMQ crashes on first boot** with `Please create virtual host "/"
> prior to importing definitions.` — this happens when `definitions.json`
> doesn't explicitly list the `/` vhost and a fresh (empty) `rabbitmq-data`
> volume tries to import definitions before vhost recovery finishes. This is
> already fixed in `rabbitmq/definitions.json` (it declares `"vhosts":
> [{"name": "/"}]` up front) — if you still hit it, remove the
> `rabbitmq-data` docker volume and start fresh: `docker compose down -v`.

## What's wired so far

- **Storage (MinIO)** — fully switched over. The frontend's storage client
  (`CaldexMinio`) now calls the backend's `/api/storage/*` endpoints instead
  of signing S3 requests in the browser. Nothing else in the frontend
  changed — same function signatures, so every page that saves/loads
  pipelines, the model registry, etc. just works through the backend now.
  Settings → Storage no longer needs any credentials — it just checks the
  backend is reachable.
- **Pipeline execution** — `worker/` is a real RabbitMQ consumer. It picks
  up `{runId, pipelineId, bucket, specKey}` pointers from the
  `dotwin.pipeline.runs` queue, fetches the job spec from MinIO, walks the
  node graph (`csv` → `power_transform`/`robust_scaler` → `xgboost` →
  `confusion`/`roc`/`metrics` → `registry`/`share`), and reports each
  node's start/done/error over MQTT on the exact topics the frontend
  listens on, then writes `result.json` to MinIO and publishes `done` with
  its key. This is genuinely training a model (scikit-learn preprocessing +
  XGBoost), not a simulation — the node types not yet listed above are
  reported as "skipped" rather than crashing the run, so pipelines using
  newer node types still complete (just without that step's effect).

## What's NOT wired yet (next steps)

- **RabbitMQ** — the frontend's Settings → Job queue panel still talks
  directly to RabbitMQ's Management API from the browser. The backend
  already has a working proxy (`/api/queue/health`, `/api/queue/exchange`,
  `/api/queue/publish`) — swapping the frontend over is the same pattern as
  storage: rewrite the three fetch calls in `QueuePanel`'s `probe()`/`save()`
  to hit `/api/queue/...` instead of RabbitMQ directly, and update
  `dispatchViaQueue()` in the pipeline/federated builders to call
  `/api/queue/publish` instead of building the RabbitMQ auth header itself.
- **MQTT** — same story. The backend exposes a WebSocket relay at
  `/api/mqtt/ws?topics=a,b,c` (protocol documented in
  `backend/app/services/mqtt_bridge.py`) plus a one-shot
  `POST /api/mqtt/publish`. The frontend's `useTelemetry()` (Digital Twin
  dashboard), the pipeline's `runOnBackend()`, and the federated builder's
  `build()` all currently call `mqtt.connect()` directly — swapping each to
  open a `new WebSocket("/api/mqtt/ws?topics=...")` instead removes the
  bundled `mqtt.js` library entirely and fixes the WebSocket-listener /
  Origin gotchas for good, since it becomes a plain server-to-server MQTT
  connection.
- **SQL Source node** — the backend's `/api/db/test` and `/api/db/query`
  are ready (Postgres + MySQL, SELECT-only), but the pipeline's SQL Source
  node in the frontend doesn't call them yet — it just stores connection
  params. Wiring "Test connection" and preview/run in that node to these
  endpoints is a contained follow-up.
- **Federated Learning builds** — the `dotwin.federated.builds` queue exists
  (definitions.json) but nothing consumes it yet; `worker/consumer.py` only
  listens on `dotwin.pipeline.runs`. Same shape of work as the pipeline
  runner, just aggregating multiple registered models instead of training
  one from a CSV.
- **Hyperparameter Tuning is its own node now**, not a fixed-value tab on
  a model. Drop a "Hyperparameter Tuning" node (its own category, between
  a model and its evaluate node) downstream of an XGBoost node: pick
  grid/random search, cross-validation folds, and the metric to optimize,
  and give a comma-separated candidate list for any of `n_estimators`,
  `max_depth`, `learning_rate`, `subsample` (leave one blank to keep the
  model node's own fixed value instead of searching it). The worker runs
  a real `GridSearchCV`/`RandomizedSearchCV` and keeps whichever trial won.
  Only XGBoost is wired up server-side right now — other model types report
  a clear "not supported yet" error instead of silently ignoring the node.
- **Feature engineering nodes work in the Pipeline (batch, training-time)
  only.** The Pipeline Builder's "Feature engineering" group has visual,
  form-based nodes — no code required for the common cases:
    - *Time series*: Resample (up/downsample), Rolling Window (mean/std/
      min/max/median/ewm), Lag & Diff, Datetime Split (with cyclical
      sin/cos encoding).
    - *Domain formulas*: Custom Formula — a single pandas-eval expression
      over existing columns, for the one-off case the visual nodes don't
      cover.
    - *Domain libraries*: Fuzzy Inference (scikit-fuzzy) and Customer
      Lifetime/BTYD (the `lifetimes` package) — pluggable, industry-specific
      building blocks rather than anything hardcoded to one dataset.
  All of them live in `worker/feature_engine.py` and run over the full
  train/test split during a real pipeline run, same as any other transform
  node. Using the *same* recipe for live inference in a Data Flow needs flow
  execution to move server-side too (see the next point) — until then,
  a Data Flow's own nodes (still client-side JS) don't share this engine.
- **Data Flows execution** — still entirely client-side/tab-scoped (see the
  earlier discussion) — moving it to the backend as a persistent service is
  a separate, larger piece of work. Doing so would let a Data Flow reuse
  `worker/feature_engine.py` directly (most of these transforms have a
  natural "update with one new row" incremental form — rolling window and
  lag/diff especially), keeping the exact same recipe consistent between
  training and live inference.
- **Auth** stays exactly as-is (browser-side IndexedDB) — untouched, as
  requested.
