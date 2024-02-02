snailqueue - is a database (PostgreSQL) queue in Python.

Context of appropriate using of this library:

* Not a heavy loads, less than 10 RPS.
* Limited amounts of queues, as implementation relies on tables.
* Not a big messages context.


* Messages are stateful.
* Flexible to any message storages: dict, pydantic BaseModel, dataclass, attrs and any generic.

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
```

### Running tests for development

```bash
PYTHONPATH=. pytest-watcher --now .
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
