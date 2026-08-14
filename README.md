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

`init()` accetta solo argomenti keyword-only e legge, con precedenza
`argomenti > env var > default self-contained`:
`KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`), `SCHEMA_REGISTRY_URL`,
`FLINK_REST_URL`, `KAFKA_SASL_USERNAME/PASSWORD`, `AWS_ACCESS_KEY_ID/SECRET`,
`AWS_DEFAULT_REGION`, `JUPYTERHUB_USER`, `GITHUB_BRANCH`, `GITHUB_REPO`.

`init()` ritorna un oggetto `Config` immutabile; `qcutils.get_config()` ritorna
lo stesso singleton. I segreti (password SASL, chiavi AWS) sono esclusi dal
`repr` di `Config`.

Sul laptop: `qcutils.init(interactive=True)` chiede i segreti AWS con prompt sicuro.

### Config esplicita (funzioni pure)

Le helper accettano un parametro keyword-only `config: Config | None = None`.
Se omesso usano il singleton popolato da `init()` (comportamento storico),
altrimenti la Config passata:

```python
cfg = qcutils.Config.from_env(kafka_bootstrap="broker:9092")
print(qcutils.kafka_srv_description(config=cfg))
```

## Credenziali AWS

Di default `init()` **non** scrive `~/.aws/credentials`: boto3 e Spark leggono
le credenziali dall'ambiente / default credential chain. Per il comportamento
storico (scrittura del file) usa `qcutils.init(write_aws=True)`.

## Installazione ed extra

Il core è senza dipendenze; le librerie pesanti sono extra e vengono importate
in modo *lazy* (così `import qcutils` e `init()` funzionano anche senza extra):

```bash
pip install qcutils            # core
pip install "qcutils[all]"     # pyyaml + tabulate + boto3
pip install "qcutils[s3]"      # boto3
pip install "qcutils[viz]"     # tabulate (tabella endpoint)
pip install "qcutils[config]"  # pyyaml (read_config_value)
pip install "qcutils[kafka]"   # confluent-kafka (create_kafka_topic)
```

## Novità della 1.2.0 (tutte retrocompatibili)

Nessuna firma storica è cambiata: le aggiunte sono parametri *keyword-only* con
default che riproducono il comportamento precedente.

**Eccezioni tipizzate.** `QcutilsError`, `S3UploadError`, `S3DownloadError` sono
sottoclassi di `RuntimeError`: ogni `except RuntimeError` esistente continua a
funzionare, ma ora si può distinguere.

**La chiave S3 può conoscere il corso.** `push_to_remote(..., prefix=...)`.
Il default resta il nome del branch, com'è sempre stato — ma il branch è un
*proxy* del corso, non il corso: due corsi sullo stesso branch (`student`,
`main`) si scrivono sopra, e lo stesso studente in due edizioni sovrascrive la
propria consegna precedente. `Config` ora legge anche `COURSE_NAME`:

```python
cfg = qcutils.init()
qcutils.persist_user_materials()                    # come prima: prefisso = branch
qcutils.push_to_remote(bucket, path, prefix=cfg.course)   # separato per corso
```

**Guardia di dimensione.** `push_to_remote(..., max_size_mb=500)` solleva *prima*
di caricare, invece di far scoprire dopo dieci minuti che si stava spedendo una
home intera. Default `None` = nessun limite, cioè come prima.

**Upload di un file singolo.** `push_file_to_remote(bucket, path)`: per un
artefatto già compresso — un `.gguf`, uno zip — comprimerlo di nuovo è lavoro e
spazio buttati.

**`qcutils.doctor()`.** Ritorna un dizionario e stampa un riepilogo: versione,
extra installati, configurazione effettiva, raggiungibilità di Kafka e Schema
Registry. I segreti non compaiono mai. La stessa diagnosi era riscritta a mano
nella CLI `qc` dell'immagine, che poteva solo controllare se il modulo si
importava.

**`py.typed`.** Il codice era già interamente annotato, ma senza il marker mypy e
gli IDE trattavano la libreria come non tipizzata.

## Retrocompatibilità

Ogni release preserva l'intera API pubblica storica. La 1.0.0: le firme delle
funzioni chiamate dai notebook restano identiche, incluse
`read_config_value(key, github_user, github_token, remote_cf_version, cf_path)`
(i primi tre parametri sono conservati per compatibilità ma ignorati).
`kafka_srv_description()` ora **ritorna** la stringa della tabella (i notebook
la usano con `print(...)`). Le funzioni non più utilizzate
(`read_config_value`, `restore_user_bootcamp`, `list_s3_bucket_objects`,
`print_s3_bucket_object`) restano funzionanti ma emettono `DeprecationWarning`.

## Sviluppo & test

```bash
pip install -e ".[test]"
pytest
```

## Build & pubblicazione

```bash
python -m build                       # genera dist/*.whl e *.tar.gz
python -m twine upload dist/*         # pubblica su PyPI
```
