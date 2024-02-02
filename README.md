```bash
pip install hurrqueue
```

```bash
poetry install
```

```bash
pre-commit install

docker compose -f environments/docker-compose-postgres.yaml up -d
```

```bash
PYTHONPATH=. pytest-watcher --now .
```

```bash
./shell/mypy.sh
./shell/pytest.sh
```
