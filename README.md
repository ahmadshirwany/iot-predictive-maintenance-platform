# IoT Predictive Maintenance Platform

An end-to-end streaming telemetry and live monitoring platform built around the Azure Predictive Maintenance dataset (`PdM_telemetry.csv`). Simulated industrial machines publish hourly sensor readings over MQTT with realistic field faults (duplicates, delays, outages, impossible sensor values, and firmware schema evolution), which flow through Redpanda (Kafka) into PostgreSQL and a live Django + HTMX fleet dashboard.

## Architecture

```text
PdM_telemetry.csv
       │
       ▼
[Device Simulator] ──MQTT (1883)──▶ [Mosquitto] ──▶ [MQTT-to-Kafka Bridge]
  • fault injection                                    • validates JSON
  • firmware v1 → v2                                   • malformed → DLQ topic
                                                              │
                                                              ▼ Kafka (19092)
                                                         [Redpanda]
                                                     (Console UI: :8080)
                                                              │
                                                              ▼
                                                     [PostgreSQL Sink]
                                                       • idempotent inserts
                                                       • out-of-order safe upsert
                                                       • alert extraction
                                                              │
                                                              ▼ PostgreSQL (5434)
                                                         [PostgreSQL]
                                                              │
                                                              ▼
                                                  [Django + HTMX Dashboard]
                                                    • live fleet view (:8000)
                                                    • Chart.js device detail
                                                    • JSON API (/api/status/)
                                                    • read-only Django Admin
```

## Key Capabilities

- **Realistic Telemetry Simulator** (`src/simulator/`): Replays readings for 100 machines with configurable speed and injects duplicates, delayed/out-of-order messages, temporary device outages, negative pressure faults, and a `v1` → `v2` firmware rollout that introduces a `temperature` field.
- **MQTT-to-Kafka Bridge** (`src/bridge/`): Subscribes to `factory/+/telemetry` on Mosquitto and forwards valid JSON payloads to Redpanda topic `machine-telemetry` (keyed by `device_id` with idempotent producer settings). Malformed payloads are routed to `machine-telemetry-dlq`.
- **Idempotent PostgreSQL Sink** (`src/sinks/postgres/`): Consumes batches from Kafka, applies [schema.sql](src/sinks/postgres/schema.sql), and writes `telemetry_reading`, `device_status`, and `sensor_alert` atomically before committing Kafka offsets. Uses `ON CONFLICT` deduplication and guards `device_status` updates with `WHERE device_status.last_event_time <= EXCLUDED.last_event_time` so late messages never overwrite newer state.
- **Live Fleet Dashboard** (`dashboard/`): Django web app reading from the pipeline-owned PostgreSQL tables (`managed = False`):
  - **Fleet Overview (`/`)**: Auto-refreshes every 2 seconds via HTMX (`/partials/fleet/`), showing stream freshness (wall-clock), fleet watermark (newest event time), status KPIs (`FAULT`, `OFFLINE`, `WARNING`, `OK`), machine grid, and recent alerts.
  - **Device Detail (`/devices/<device_id>/`)**: Displays latest sensor values, a Chart.js plot of the last 72 readings, and device-specific alerts.
  - **JSON API (`/api/status/`)**: Machine-readable fleet snapshot.
  - **Read-only Admin (`/admin/`)**: Browse `DeviceStatus`, `TelemetryReading`, and `SensorAlert`.

## Project Structure

```text
.
├── dashboard/                  # Django monitoring web app
│   ├── config/                 # Django settings, URLs, WSGI/ASGI
│   ├── monitoring/             # Unmanaged models, status logic, views, templates, admin, tests
│   └── manage.py
├── data/raw/                   # Local dataset folder (gitignored; place PdM_telemetry.csv here)
├── infra/mosquitto/            # Local Mosquitto broker configuration
├── src/
│   ├── bridge/                 # MQTT-to-Kafka bridge
│   ├── simulator/              # Telemetry replay + fault injection
│   └── sinks/postgres/         # Kafka-to-PostgreSQL sink, parser, schema.sql
├── tests/                      # Pytest unit & PostgreSQL integration tests
├── docker-compose.yml          # Mosquitto, Redpanda, Redpanda Console, PostgreSQL
├── pyproject.toml              # Package, pytest, and ruff configuration
├── requirements.txt            # Runtime dependencies
└── requirements-dev.txt        # Dev dependencies (pytest, ruff)
```

## Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Dataset file `PdM_telemetry.csv` placed at `data/raw/PdM_telemetry.csv` (from the Microsoft Azure Predictive Maintenance dataset)

## Setup

1. **Create and activate a virtual environment:**

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

2. **Install dependencies and the local package:**

   ```powershell
   pip install -r requirements.txt -r requirements-dev.txt
   pip install -e .
   ```

3. **Start infrastructure containers** (Mosquitto, Redpanda, Redpanda Console, and PostgreSQL):

   ```powershell
   docker compose --profile postgres up -d
   ```

4. **Run Django migrations** (creates Django's built-in tables; telemetry tables are created automatically by the PostgreSQL sink):

   ```powershell
   python dashboard/manage.py migrate
   ```

   *(Optional)* Create an admin user to access `/admin/`:

   ```powershell
   python dashboard/manage.py createsuperuser
   ```

## Running the Pipeline

Open separate terminals (with `.venv` activated) to run each service:

1. **Start the MQTT-to-Kafka bridge:**

   ```powershell
   python -m bridge.mqtt_to_kafka
   ```

2. **Start the Kafka-to-PostgreSQL sink:**

   ```powershell
   python -m sinks.postgres.kafka_to_postgres
   ```

3. **Start the Django dashboard:**

   ```powershell
   python dashboard/manage.py runserver
   ```

4. **Start the device simulator:**

   ```powershell
   python -m simulator.run_simulator --start 2015-01-01 --end 2015-01-15 --hour-seconds 0.2
   ```

   Useful simulator flags:
   - `--csv`: Path to telemetry CSV (default: `data/raw/PdM_telemetry.csv`)
   - `--start` / `--end`: Replay date window (defaults: `2015-01-01` to `2015-01-15`)
   - `--hour-seconds`: Real seconds per simulated hour (default: `0.2`)
   - `--firmware-v2-from`: Date when devices switch from `v1` to `v2` (default: `2015-01-08`)
   - `--seed`: RNG seed for reproducible fault injection (default: `42`)

### Local Service Endpoints

| Service | Address / Port |
| --- | --- |
| Fleet Dashboard | http://localhost:8000 |
| Fleet Status JSON API | http://localhost:8000/api/status/ |
| Django Admin | http://localhost:8000/admin/ |
| Redpanda Console | http://localhost:8080 |
| Mosquitto (MQTT) | `localhost:1883` |
| Redpanda (Kafka external) | `localhost:19092` |
| PostgreSQL | `localhost:5434` (`db=iot`, `user=iot`, `password=iot`) |

## Environment Variables

Both the PostgreSQL sink and Django dashboard support these optional overrides:

| Variable | Default | Description |
| --- | --- | --- |
| `POSTGRES_HOST` | `localhost` | PostgreSQL hostname |
| `POSTGRES_PORT` | `5434` | PostgreSQL port |
| `POSTGRES_DB` | `iot` | Database name |
| `POSTGRES_USER` | `iot` | Database user |
| `POSTGRES_PASSWORD` | `iot` | Database password |
| `DJANGO_SECRET_KEY` | `dev-only-not-secret` | Django secret key |
| `DJANGO_DEBUG` | `1` | Set to `0` to disable debug mode |

## Testing & Linting

- **Pipeline unit & Postgres integration tests** (integration tests in `tests/test_pg_writer.py` run in an isolated temporary schema and are skipped automatically if PostgreSQL is unreachable):

  ```powershell
  pytest
  ```

- **Dashboard tests** (requires PostgreSQL running on port `5434`):

  ```powershell
  python dashboard/manage.py test monitoring
  ```

- **Linter (Ruff):**

  ```powershell
  ruff check src tests dashboard
  ```
