# YT_ELT — YouTube ELT Pipeline

An end-to-end ELT pipeline that extracts video performance data for a selected YouTube channel, loads the raw data into PostgreSQL, and is orchestrated with Apache Airflow in a fully Dockerized environment. Data quality is monitored with SODA, the code is covered by pytest tests, and validation is automated with GitHub Actions.

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Infrastructure](#infrastructure)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Getting Started](#getting-started)
- [Pipeline (DAGs)](#pipeline-dags)
- [Data Model](#data-model)
- [Data Quality and Testing](#data-quality-and-testing)
- [CI/CD](#cicd)
- [Security Notes](#security-notes)
- [Future Improvements](#future-improvements)

## Architecture

The project follows an **Extract → Load → Transform** approach:

1. **Extract** — video statistics for a selected channel are retrieved from the YouTube Data API v3 using the channel handle.
2. **Load** — raw data is stored in a PostgreSQL database that acts as a mock data warehouse.
3. **Transform** — stored data is prepared for further processing and analysis.

```
YouTube Data API v3
        │
        ▼
 Airflow (CeleryExecutor)  ──►  PostgreSQL (ELT database)  ──►  SODA data quality checks
   │            ▲
   ▼            │
 Redis (broker) ┘
```

## Tech Stack

| Component | Purpose |
|---|---|
| Python | Data extraction and pipeline logic |
| Apache Airflow 2.9.2 | Workflow orchestration |
| CeleryExecutor | Distributed task execution |
| Redis 7.2 | Celery message broker |
| PostgreSQL 13 | Airflow metadata, Celery results, and ELT data |
| Docker & Docker Compose | Containerization and service management |
| SODA Core (`soda-core-postgres` 3.3.14) | Data quality monitoring |
| pytest 8.3.2 | Unit and integration tests |
| GitHub Actions | CI/CD automation |

## Repository Structure

```
YT_ELT/
├── dags/                  # Airflow DAG definitions
├── data/                  # Data files used/produced by the pipeline
├── docker/postgres/       # Postgres init script (creates the multiple databases)
├── include/soda/          # SODA configuration and checks
├── tests/                 # Unit and integration tests
├── docker-compose.yaml    # Airflow + Celery + Redis + PostgreSQL stack
├── dockerfile             # Custom Airflow image
├── requirements.txt       # Extra Python dependencies (SODA, pytest)
└── .gitignore
```

## Infrastructure

`docker-compose.yaml` defines the following services:

| Service | Description |
|---|---|
| `airflow-webserver` | Airflow UI (port `8080`) |
| `airflow-scheduler` | Schedules DAGs and manages task execution |
| `airflow-worker` | Celery worker that executes tasks |
| `airflow-init` | Migrates the Airflow DB and creates the admin user |
| `postgres` | PostgreSQL 13 (port `5432`) |
| `redis` | Celery message broker |
| `airflow-cli` | Optional debug container (`--profile debug`) |

The `postgres` container creates **three separate databases** on first start via `docker/postgres/init-multiple-databases.sh`:

- **Metadata database** — Airflow metadata
- **Celery backend database** — Celery task results
- **ELT database** — project data (accessed in Airflow via the connection `POSTGRES_DB_YT_ELT`)

The API key and channel handle are exposed to DAGs as Airflow Variables (`API_KEY`, `CHANNEL_HANDLE`), and the ELT database as an Airflow Connection, both injected through environment variables.

> The stack uses the Airflow image defined by `DOCKERHUB_NAMESPACE/DOCKERHUB_REPOSITORY:IMAGE_TAG` (built from the `dockerfile` and pushed to Docker Hub). The Triggerer and Flower services are present in the compose file but commented out.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose v2
- At least 4 GB RAM and 2 CPUs available to Docker (checked by `airflow-init`)
- A YouTube Data API v3 key ([how to get one](https://developers.google.com/youtube/v3/getting-started))
- A Fernet key for Airflow (see below)
- A Docker Hub account/repository for the custom Airflow image

## Configuration

Create a `.env` file in the project root. **Never commit it.**

```env
# --- Airflow image ---
DOCKERHUB_NAMESPACE=your-dockerhub-username
DOCKERHUB_REPOSITORY=your-image-repo
IMAGE_TAG=latest
AIRFLOW_UID=50000

# --- Airflow admin user ---
AIRFLOW_WWW_USER_USERNAME=admin
AIRFLOW_WWW_USER_PASSWORD=change-me
FERNET_KEY=generate-with-command-below

# --- PostgreSQL server ---
POSTGRES_CONN_HOST=postgres
POSTGRES_CONN_PORT=5432
POSTGRES_CONN_USERNAME=postgres
POSTGRES_CONN_PASSWORD=change-me

# --- Airflow metadata DB ---
METADATA_DATABASE_NAME=airflow_metadata_db
METADATA_DATABASE_USERNAME=metadata_user
METADATA_DATABASE_PASSWORD=change-me

# --- Celery results DB ---
CELERY_BACKEND_NAME=celery_results_db
CELERY_BACKEND_USERNAME=celery_user
CELERY_BACKEND_PASSWORD=change-me

# --- ELT data DB ---
ELT_DATABASE_NAME=elt_db
ELT_DATABASE_USERNAME=elt_user
ELT_DATABASE_PASSWORD=change-me

# --- YouTube ---
API_KEY=your-youtube-api-key
CHANNEL_HANDLE=MrBeast
```

Generate a Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Getting Started

```bash
# 1. Clone the repository
git clone https://github.com/nemm22/YT_ELT.git
cd YT_ELT

# 2. Create and fill in the .env file (see Configuration)

# 3. Build and push the custom Airflow image (or use an already published one)
docker build -t $DOCKERHUB_NAMESPACE/$DOCKERHUB_REPOSITORY:$IMAGE_TAG -f dockerfile .
docker push $DOCKERHUB_NAMESPACE/$DOCKERHUB_REPOSITORY:$IMAGE_TAG

# 4. Initialize Airflow (DB migration + admin user)
docker compose up airflow-init

# 5. Start all services
docker compose up -d
```

Then open the Airflow UI at <http://localhost:8080> and sign in with `AIRFLOW_WWW_USER_USERNAME` / `AIRFLOW_WWW_USER_PASSWORD`. DAGs are created **paused** by default (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`), so unpause the DAG you want to run and trigger it manually or wait for its schedule.

To stop the stack:

```bash
docker compose down        # keep data
docker compose down -v     # also remove the Postgres volume
```

## Pipeline (DAGs)

<!-- TODO: fill in from the files in dags/ -->

| DAG | Description | Schedule |
|---|---|---|
| `TODO_dag_name` | e.g. extracts video stats from the YouTube API and loads them into the ELT database | `TODO` |

Describe the task order here (e.g. `extract → load → data quality check`).

## Data Model

<!-- TODO: fill in with the real schema -->

| Table / Schema | Description | Key columns |
|---|---|---|
| `TODO` | e.g. raw video statistics | e.g. `video_id`, `title`, `published_at`, `views`, `likes`, `comments` |

## Data Quality and Testing

**SODA** checks live in `include/soda/` and are run against the ELT database.

<!-- TODO: list what the checks validate (e.g. no null video_id, unique video_id, non-negative view counts) -->

**pytest** tests live in `tests/` (unit tests for individual components and integration tests that talk to PostgreSQL). Since `./tests` and `./include` are mounted into the Airflow containers, tests can be run inside the stack:

```bash
docker exec -it airflow-scheduler pytest /opt/airflow/tests -v
```

## CI/CD

GitHub Actions validates and tests the project on changes.

<!-- TODO: name the workflow file(s) in .github/workflows and describe the steps -->

> Note: `docker-compose.yaml` contains comments marking which lines (`env_file`, Postgres `environment` entries) must be switched when running under GitHub Actions, where secrets are provided as repository secrets instead of a `.env` file.

## Security Notes

- Keep `.env`, API keys, and passwords out of version control.
- Port `5432` is published for local development. Do not expose it publicly; remove the port mapping if you don't need host access.
- This setup is intended for **local development**, not production.

## Future Improvements

- Introduce **dbt** for modular SQL transformations and data modeling.
- Add dbt tests and generated documentation.
- Expand data quality checks and monitoring.
- Improve pipeline observability and error handling.

## Repository

[github.com/nemm22/YT_ELT](https://github.com/nemm22/YT_ELT)

## Running the Project

The project uses Docker Compose to run its services. Before starting, configure the required environment variables and ensure Docker and Docker Compose are installed.

Initialize and start the environment using the commands appropriate for the repository's configuration.

The Airflow UI is exposed on port `8080`, and PostgreSQL is exposed on port `5432`.

## Future Improvements

- Introduce dbt for modular SQL transformations and data modeling.
- Add dbt tests and generated documentation.
- Expand data quality checks and monitoring.
- Improve pipeline observability and error handling.

## Repository

[GitHub — YT_ELT](https://github.com/nemm22/YT_ELT)
