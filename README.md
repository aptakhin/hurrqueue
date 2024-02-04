snailqueue - is a database (PostgreSQL) asyncio task queue in Python. Toolkit for asynchronous executions.

Context of appropriate using of this library:

* Not a heavy loads, less than 10 RPS.
* Limited amounts of queues, as implementation relies on tables.
* Not a big tasks context.
* Without dependencies or with minimal amount of them. If you need dependencies my advice to look into the Workflow solutions or [Temporal.io](https://temporal.io/)

* Tasks are stateful.
* Flexible to any tasks storages: dict, pydantic BaseModel, dataclass, attrs or any generic.

```bash
pip install snailqueue
```

## Usage

## Development

```bash
poetry install
```

### Precommits and environment

```bash
pre-commit install

docker compose -f environments/docker-compose-postgres.yaml up -d
# or
make run-infra
```

### Running tests for development

```bash
PYTHONPATH=. pytest-watcher --now .
# or
make ptw
```

### Running checkers

```bash
./shell/mypy.sh
./shell/pytest.sh
```

## Plans

Concentrate on SQLAlchemy.
Prometheus metrics support.
Configurable pip installation.
More abstraction on status logic. Statemachine? Timeouts of messages handling.
Retries logic.
Schedulers.
All async execution. API generator. Docs.
Jsonschema check pydantic is subset. Transports. SQS, Azure Storage Queue. Various messages – one channel.
Base versioning.
