# qcutils

Python utils per i progetti educational di Quantia Consulting.

Companion runtime dell'immagine `qc-platform`: fornisce gestione materiali
(compress/deliver/restore su S3), helper Spark↔S3 e Kafka, il tutto configurato
**a runtime** via `init()` — nessun segreto nel pacchetto.

## Uso

```python
import qcutils
qcutils.init()                  # legge endpoint/segreti da env (o argomenti)
qcutils.create_kafka_topic("demo")
qcutils.deliver_bootcamp()
```

`init()` legge, con precedenza `argomenti > env var > default self-contained`:
`KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`), `SCHEMA_REGISTRY_URL`,
`FLINK_REST_URL`, `KAFKA_SASL_USERNAME/PASSWORD`, `AWS_ACCESS_KEY_ID/SECRET`,
`AWS_DEFAULT_REGION`, `JUPYTERHUB_USER`, `GITHUB_BRANCH`, `GITHUB_REPO`.

Sul laptop: `qcutils.init(interactive=True)` chiede i segreti AWS con prompt sicuro.

## Build & pubblicazione

```bash
python -m build                       # genera dist/*.whl e *.tar.gz
python -m twine upload dist/*         # pubblica su PyPI
```
