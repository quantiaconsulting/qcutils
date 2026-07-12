"""qcutils — utilities per i progetti educational di Quantia Consulting.

Modello runtime (self-contained + iniezione a runtime):

    import qcutils
    qcutils.init()                 # legge endpoint/segreti da env (o argomenti)
    qcutils.create_kafka_topic("demo")
    qcutils.deliver_bootcamp()

Precedenza configurazione:  argomenti espliciti > variabili d'ambiente > default.

I segreti NON sono mai nel pacchetto né vanno scritti nei notebook: arrivano a
runtime (JupyterHub spawner env, `docker run -e ...`, oppure `init(interactive=True)`
per il laptop). In assenza di credenziali/endpoint condivisi, qcutils resta
utilizzabile sul sandbox locale (Kafka su localhost:9092, Spark locale).

Retrocompatibilità
------------------
La API pubblica storica è preservata: le firme delle funzioni chiamate dai
notebook restano identiche. Le funzioni probabilmente non più usate emettono un
`DeprecationWarning` ma continuano a funzionare. Le dipendenze pesanti
(boto3, tabulate, confluent-kafka) sono importate in modo *lazy*, così che
`import qcutils` e `qcutils.init()` funzionino anche senza extra installati.
"""
from __future__ import annotations

import configparser
import functools
import getpass
import logging
import os
import shutil
import subprocess
import tarfile
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar

logger = logging.getLogger("qcutils")

__version__ = "1.0.1"

_F = TypeVar("_F", bound=Callable[..., Any])

__all__ = [
    "__version__",
    "Config",
    "init",
    "get_config",
    "read_config_value",
    "kafka_srv_description",
    "create_kafka_topic",
    "init_spark_session",
    "deliver_bootcamp",
    "persist_user_materials",
    "restore_user_materials",
    "restore_user_bootcamp",
    "update_materials",
    "compress_folder",
    "push_to_remote",
    "pull_from_remote",
    "list_s3_bucket_objects",
    "print_s3_bucket_object",
]


# =============================================================================
# Deprecation helper
# =============================================================================

def _deprecated(msg: str) -> Callable[[_F], _F]:
    """Decorator riusabile: emette un ``DeprecationWarning`` all'invocazione."""

    def decorator(func: _F) -> _F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        return wrapper

    return decorator


# =============================================================================
# Config & init
# =============================================================================

@dataclass(frozen=True, slots=True)
class Config:
    """Stato di runtime di qcutils. Niente segreti su disco se non necessario.

    Immutabile: usa :meth:`from_env` o :func:`init` per costruirla. I segreti
    (password SASL, chiavi AWS) sono esclusi dal ``repr`` per evitarne il leak
    accidentale nei log o nei notebook.
    """

    kafka_bootstrap: str = "localhost:9092"
    schema_registry_url: str = ""
    flink_rest_url: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = field(default="", repr=False)
    jupyterhub_user: str = ""
    github_branch: str = ""
    github_repo: str = ""
    aws_region: str = ""
    aws_access_key_id: str = field(default="", repr=False)
    aws_secret_access_key: str = field(default="", repr=False)
    initialized: bool = False

    @classmethod
    def from_env(cls, **overrides: Any) -> "Config":
        """Costruisce una Config con precedenza ``override > env > default``.

        Gli ``overrides`` con valore *falsy* (``None`` o stringa vuota) sono
        ignorati, così un override omesso ricade sull'env var e poi sul default.
        """
        g = os.environ.get

        def pick(key: str, env: str, default: str = "") -> str:
            ov = overrides.get(key)
            if ov:
                return ov
            return g(env, default)

        return cls(
            kafka_bootstrap=pick("kafka_bootstrap", "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            schema_registry_url=pick("schema_registry_url", "SCHEMA_REGISTRY_URL"),
            flink_rest_url=pick("flink_rest_url", "FLINK_REST_URL"),
            kafka_sasl_username=pick("kafka_sasl_username", "KAFKA_SASL_USERNAME"),
            kafka_sasl_password=pick("kafka_sasl_password", "KAFKA_SASL_PASSWORD"),
            jupyterhub_user=g("JUPYTERHUB_USER", ""),
            github_branch=g("GITHUB_BRANCH", ""),
            github_repo=g("GITHUB_REPO", ""),
            aws_region=pick("aws_region", "AWS_DEFAULT_REGION"),
            aws_access_key_id=pick("aws_access_key_id", "AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=pick("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY"),
            initialized=True,
        )


# Singleton interno per il façade retrocompatibile. Popolato da init().
_config: Config = Config()


def init(
    *,
    kafka_bootstrap: str | None = None,
    schema_registry_url: str | None = None,
    flink_rest_url: str | None = None,
    kafka_sasl_username: str | None = None,
    kafka_sasl_password: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_region: str | None = None,
    interactive: bool = False,
    write_aws: bool = False,
) -> Config:
    """Inizializza qcutils con endpoint e segreti, a runtime. Idempotente.

    Args (tutti opzionali e keyword-only; se omessi si leggono dalle env var):
        kafka_bootstrap:        KAFKA_BOOTSTRAP_SERVERS (default localhost:9092)
        schema_registry_url:    SCHEMA_REGISTRY_URL
        flink_rest_url:         FLINK_REST_URL
        kafka_sasl_username:    KAFKA_SASL_USERNAME
        kafka_sasl_password:    KAFKA_SASL_PASSWORD
        aws_access_key_id:      AWS_ACCESS_KEY_ID
        aws_secret_access_key:  AWS_SECRET_ACCESS_KEY
        aws_region:             AWS_DEFAULT_REGION
        interactive:    se True, chiede via prompt sicuro i segreti AWS mancanti
        write_aws:      se True scrive ~/.aws/credentials quando le chiavi sono
                        fornite e il file non esiste. Default False: boto3 e
                        Spark leggono le credenziali dall'ambiente.
    Ritorna l'oggetto Config. Non stampa mai i segreti.
    """
    global _config

    ak = aws_access_key_id or os.environ.get("AWS_ACCESS_KEY_ID", "")
    sk = aws_secret_access_key or os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    if interactive:
        if not ak:
            ak = input("AWS_ACCESS_KEY_ID: ").strip()
        if not sk:
            sk = getpass.getpass("AWS_SECRET_ACCESS_KEY: ").strip()

    _config = Config.from_env(
        kafka_bootstrap=kafka_bootstrap,
        schema_registry_url=schema_registry_url,
        flink_rest_url=flink_rest_url,
        kafka_sasl_username=kafka_sasl_username,
        kafka_sasl_password=kafka_sasl_password,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        aws_region=aws_region,
    )

    creds_path = os.path.expanduser("~/.aws/credentials")
    if write_aws and ak and sk and not os.path.exists(creds_path):
        _write_aws_credentials(ak, sk, _config.aws_region)

    logger.info(
        "qcutils inizializzato (kafka=%s, sasl=%s, sr=%s)",
        _config.kafka_bootstrap,
        "yes" if _config.kafka_sasl_username else "no",
        _config.schema_registry_url or "-",
    )
    return _config


def get_config() -> Config:
    """Ritorna l'oggetto Config corrente (il singleton popolato da ``init()``)."""
    return _config


def _resolve_config(config: Config | None) -> Config:
    """Ritorna la Config esplicita o il singleton interno come fallback."""
    return config if config is not None else _config


def _write_aws_credentials(key: str, secret: str, region: str = "") -> None:
    aws_dir = os.path.expanduser("~/.aws")
    os.makedirs(aws_dir, exist_ok=True)
    creds_file = os.path.join(aws_dir, "credentials")
    with open(creds_file, "w") as f:
        f.write(f"[default]\naws_access_key_id={key}\naws_secret_access_key={secret}\n")
    if region:
        with open(os.path.join(aws_dir, "config"), "w") as f:
            f.write(f"[default]\nregion={region}\n")
    os.chmod(aws_dir, 0o700)
    os.chmod(creds_file, 0o600)
    logger.info("AWS credentials scritte in ~/.aws/credentials")


# =============================================================================
# Funzioni private
# =============================================================================

def _search_sub_node(node: Any, lst: list[str]) -> Any:
    pname = lst.pop(0)
    subnode = node[pname]
    if len(lst) > 0:
        return _search_sub_node(subnode, lst)
    return subnode


def _make_tarfile(source_dir: str, output_path: str) -> bool:
    if not output_path.endswith(".tar.gz"):
        logger.error("output_path deve terminare con il nome dell'archivio .tar.gz")
        return False
    output_filename = output_path.rsplit("/", 1)[-1]
    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(source_dir, arcname=output_filename)
    logger.info("Archivio creato: %s", output_path)
    return True


def _aws_credentials(config: Config | None = None) -> dict[str, str]:
    """Ricava le credenziali AWS con precedenza Config > env > ~/.aws.

    Ritorna un dict adatto a essere passato come ``**kwargs`` a
    ``boto3.client``/``boto3.resource``. Se non trova nulla ritorna un dict
    vuoto, così boto3 ricade sulla propria default credential chain (env var,
    ~/.aws, IAM role) senza che noi imponiamo credenziali fittizie.
    """
    cfg = _resolve_config(config)
    key = cfg.aws_access_key_id or os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = cfg.aws_secret_access_key or os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    region = cfg.aws_region or os.environ.get("AWS_DEFAULT_REGION", "")

    if not (key and secret):
        creds_path = os.path.expanduser("~/.aws/credentials")
        if os.path.exists(creds_path):
            parser = configparser.RawConfigParser()
            parser.read(creds_path)
            if parser.has_section("default"):
                key = key or parser["default"].get("aws_access_key_id", "")
                secret = secret or parser["default"].get("aws_secret_access_key", "")

    creds: dict[str, str] = {}
    if key and secret:
        creds["aws_access_key_id"] = key
        creds["aws_secret_access_key"] = secret
    if region:
        creds["region_name"] = region
    return creds


def _s3_client(config: Config | None = None) -> Any:
    """Crea un client boto3 S3 usando le credenziali della Config (o default chain)."""
    import boto3

    return boto3.client("s3", **_aws_credentials(config))


def _s3_resource(config: Config | None = None) -> Any:
    """Crea una resource boto3 S3 usando le credenziali della Config (o default chain)."""
    import boto3

    return boto3.resource("s3", **_aws_credentials(config))


def _upload_file_s3(file_name: str, bucket: str, object_name: str | None = None,
                    *, config: Config | None = None) -> bool:
    # BotoCoreError copre anche NoCredentialsError ("Unable to locate credentials"),
    # che NON è una ClientError: senza questo, l'assenza di credenziali sfuggirebbe.
    from botocore.exceptions import BotoCoreError, ClientError

    if object_name is None:
        object_name = file_name
    s3_client = _s3_client(config)
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except (ClientError, BotoCoreError) as e:
        logger.error("Upload S3 fallito: %s", e)
        return False
    return True


def _download_file_s3(file_name: str, bucket: str, object_name: str | None = None,
                      *, config: Config | None = None) -> bool:
    from botocore.exceptions import BotoCoreError, ClientError

    if object_name is None:
        object_name = file_name
    s3_client = _s3_client(config)
    try:
        s3_client.download_file(bucket, object_name, file_name)
    except (ClientError, BotoCoreError) as e:
        logger.error("Download S3 fallito: %s", e)
        return False
    return True


# =============================================================================
# Config file (compat) — SOLO lettura di un file locale, niente token-in-URL
# =============================================================================

@_deprecated(
    "read_config_value() è deprecata: usa qcutils.init() con le env var del "
    "deployment. I parametri github_user/github_token/remote_cf_version sono "
    "ignorati (legge solo il file YAML locale in cf_path)."
)
def read_config_value(
    key: str,
    github_user: str = "",
    github_token: str = "",
    remote_cf_version: str = "0.6.0",
    cf_path: str = "/home/jovyan/utils/config.yaml",
) -> Any:
    """[Compat] Legge un valore da un config YAML LOCALE.

    Il vecchio download da repo privato con token nell'URL è stato rimosso
    (anti-pattern): i parametri ``github_user``, ``github_token`` e
    ``remote_cf_version`` sono conservati per retrocompatibilità di firma ma
    ignorati. Per gli endpoint condivisi usa ``qcutils.init()`` con le env var
    iniettate dal deployment. Se serve un file di config, montalo/scaricalo a
    runtime in ``cf_path`` e questa funzione lo leggerà.
    """
    import yaml

    if not os.path.exists(cf_path):
        raise FileNotFoundError(
            f"Config file '{cf_path}' non trovato. Usa qcutils.init() con le env "
            "var del deployment, oppure monta/scarica il file di config a runtime."
        )
    with open(cf_path) as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    return _search_sub_node(cfg, key.split("."))


# =============================================================================
# Materiali: compress / update / deliver / persist / restore
# =============================================================================

def compress_folder(path: str = "/home/jovyan/materials", *, config: Config | None = None) -> str:
    """Comprime la cartella indicata in un archivio .tar.gz nella home.

    Ritorna il percorso dell'archivio creato. Solleva in caso di errore.
    """
    cfg = _resolve_config(config)
    folder_name = path.rsplit("/", 1)[-1]
    logger.info("Compressing %s folder....", folder_name)
    jhub_user = cfg.jupyterhub_user or os.environ.get("JUPYTERHUB_USER", "")
    output_filename = folder_name + "_" + jhub_user.replace(".", "_") + ".tar.gz"
    output_path = "/home/jovyan/" + output_filename
    if not _make_tarfile(path, output_path):
        raise RuntimeError(f"Creazione archivio fallita per '{path}'")
    logger.info("Archivio disponibile nella home: %s", output_filename)
    return output_path


def update_materials(*, config: Config | None = None) -> None:
    """Aggiorna /home/jovyan/materials dal repo GitHub della classe (gitpuller)."""
    cfg = _resolve_config(config)
    logger.info("updating materials folder....")
    repo = cfg.github_repo or os.environ.get("GITHUB_REPO", "")
    branch = cfg.github_branch or os.environ.get("GITHUB_BRANCH", "")
    subprocess.run(
        ["gitpuller", repo, branch, "/home/jovyan/materials"],
        check=True,
    )


def deliver_bootcamp(path: str = "/home/jovyan/materials/bootcamp", *, config: Config | None = None) -> None:
    """Comprime e carica la cartella sul bucket S3 quantia-bootcamp-results."""
    push_to_remote("quantia-bootcamp-results", path, config=config)


def persist_user_materials(path: str = "/home/jovyan/materials", *, config: Config | None = None) -> None:
    """Comprime e carica la cartella sul bucket S3 quantia-platform-users."""
    push_to_remote("quantia-platform-users", path, config=config)


def _restore_from_bucket(bucket: str, local_file_path: str, target_dir: str,
                         source_index: int, *, config: Config | None = None) -> None:
    """Recupera un archivio utente da S3 e lo estrae in ``target_dir``."""
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)

    tar_file = pull_from_remote(bucket, local_file_path, config=config)
    with tarfile.open(tar_file) as my_tar:
        for member in my_tar.getmembers():
            if ".ipynb_checkpoints" not in member.name:
                my_tar.extract(member, path="/home/jovyan/tmp")

    os.makedirs(target_dir, exist_ok=True)
    source_dir = "/home/jovyan/tmp/" + tar_file.split("/")[source_index]
    for file_name in os.listdir(source_dir):
        shutil.move(os.path.join(source_dir, file_name), target_dir)

    os.remove(tar_file)
    shutil.rmtree("/home/jovyan/tmp/")


def restore_user_materials(bucket: str = "quantia-platform-users",
                           local_file_path: str = "/home/jovyan/",
                           *, config: Config | None = None) -> None:
    """Recupera la cartella utente da S3 e la estrae in persistent-materials."""
    _restore_from_bucket(
        bucket, local_file_path, "/home/jovyan/persistent-materials",
        source_index=3, config=config,
    )


@_deprecated("restore_user_bootcamp() è deprecata e potrebbe essere rimossa in futuro.")
def restore_user_bootcamp(bucket: str = "quantia-bootcamp-results",
                          local_file_path: str = "/home/jovyan/",
                          *, config: Config | None = None) -> None:
    """Recupera il bootcamp dell'utente da S3 e lo estrae in materials/bootcamp."""
    _restore_from_bucket(
        bucket, local_file_path, "/home/jovyan/materials/bootcamp",
        source_index=-1, config=config,
    )


# =============================================================================
# Spark
# =============================================================================

def init_spark_session(spark_session: Any, *, config: Config | None = None) -> None:
    """Configura una SparkSession per leggere da S3 via s3a.

    Le credenziali AWS vengono prese da Config/env; in fallback dal file
    ~/.aws/credentials se presente. Non scrive segreti su disco.
    """
    cfg = _resolve_config(config)
    creds = _aws_credentials(cfg)
    aws_key = creds.get("aws_access_key_id", "")
    aws_secret = creds.get("aws_secret_access_key", "")

    hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
    if aws_key and aws_secret:
        hadoop_conf.set("fs.s3a.access.key", aws_key)
        hadoop_conf.set("fs.s3a.secret.key", aws_secret)
    else:
        logger.warning(
            "Nessuna credenziale AWS trovata (Config/env/~/.aws): "
            "s3a userà la default credential chain."
        )
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_session.conf.set("spark.sql.repl.eagerEval.enabled", True)


# =============================================================================
# Kafka — usa la Config (init), self-contained per default (localhost:9092)
# =============================================================================

def _kafka_admin_conf(config: Config | None = None) -> dict[str, str]:
    cfg = _resolve_config(config)
    bootstrap = cfg.kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "") or "localhost:9092"
    conf: dict[str, str] = {"bootstrap.servers": bootstrap}
    if cfg.kafka_sasl_username:
        conf.update({
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username": cfg.kafka_sasl_username,
            "sasl.password": cfg.kafka_sasl_password,
        })
    return conf


def kafka_srv_description(*, config: Config | None = None) -> str:
    """Ritorna una tabella (stringa) con gli endpoint streaming configurati.

    Nota: ritorna la stringa (non stampa). I notebook usano
    ``print(qcutils.kafka_srv_description())``.
    """
    cfg = _resolve_config(config)
    rows = [
        ["Kafka", cfg.kafka_bootstrap or "localhost:9092"],
        ["Schema Registry", cfg.schema_registry_url or "-"],
        ["Flink REST", cfg.flink_rest_url or "-"],
    ]
    headers = ["Service", "Endpoint"]
    try:
        from tabulate import tabulate

        return tabulate(rows, headers=headers, tablefmt="pretty")
    except ImportError:
        # Fallback senza dipendenza: tabella testuale minimale.
        width = max(len(headers[0]), *(len(r[0]) for r in rows))
        lines = [f"{headers[0]:<{width}}  {headers[1]}"]
        lines += [f"{r[0]:<{width}}  {r[1]}" for r in rows]
        return "\n".join(lines)


def create_kafka_topic(topic: str, partitions: int = 1, replication: int = 1,
                       *, config: Config | None = None) -> None:
    """Crea un topic Kafka sul broker configurato (init/env, default localhost)."""
    from confluent_kafka import KafkaError
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(_kafka_admin_conf(config))
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=replication)]
    )
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info("Topic %s created", topic_name)
        except Exception as e:  # confluent_kafka.KafkaException
            if e.args and hasattr(e.args[0], "code") and e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.info("Topic %s già esistente", topic_name)
            else:
                logger.error("Failed to create topic %s: %s", topic_name, e)
                raise


# =============================================================================
# S3
# =============================================================================

def push_to_remote(bucket: str, path: str = "/home/jovyan/materials",
                   *, config: Config | None = None) -> None:
    """Comprime la cartella e carica l'archivio sul bucket S3 indicato."""
    cfg = _resolve_config(config)
    compress_folder(path, config=cfg)
    folder_name = path.rsplit("/", 1)[-1]
    logger.info("Sending compressed %s to qc repo....", folder_name)
    ghb = cfg.github_branch or os.environ.get("GITHUB_BRANCH", "")
    jhub_user = cfg.jupyterhub_user or os.environ.get("JUPYTERHUB_USER", "")
    file_name = folder_name + "_" + jhub_user.replace(".", "_") + ".tar.gz"
    remote_key = ghb + "/" + file_name
    if not _upload_file_s3("/home/jovyan/" + file_name, bucket, remote_key, config=cfg):
        raise RuntimeError(
            f"Upload su s3://{bucket}/{remote_key} fallito. Verifica le credenziali "
            "AWS (env AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, ~/.aws/credentials, "
            "IAM role del pod, oppure qcutils.init(aws_access_key_id=..., ...))."
        )
    logger.info("%s is now on qc remote repo -> %s", folder_name, remote_key)


def pull_from_remote(bucket: str, local_file_path: str, *, config: Config | None = None) -> str:
    """Recupera l'archivio dell'utente dal bucket S3 indicato."""
    cfg = _resolve_config(config)
    if not local_file_path.endswith("/"):
        local_file_path = local_file_path + "/"
    ghb = cfg.github_branch or os.environ.get("GITHUB_BRANCH", "")
    jhub_user = cfg.jupyterhub_user or os.environ.get("JUPYTERHUB_USER", "")
    file_name = jhub_user.replace(".", "_") + ".tar.gz"

    s3_rs = _s3_resource(cfg)
    s3_client = _s3_client(cfg)
    for obj in s3_rs.Bucket(bucket).objects.filter(Prefix=ghb + "/"):
        if obj.key.endswith(file_name):
            dest = local_file_path + os.path.basename(obj.key)
            s3_client.download_file(bucket, obj.key, dest)
            return dest
    raise FileNotFoundError(f"Nessun archivio per l'utente in s3://{bucket}/{ghb}")


@_deprecated("list_s3_bucket_objects() è deprecata e potrebbe essere rimossa in futuro.")
def list_s3_bucket_objects(bucket_name: str = "quantia-master", prefix: str = "training",
                           limit: int = 10) -> list[str]:
    """Elenca gli oggetti in un bucket/prefix S3. Ritorna la lista delle key."""
    objects = _s3_client().list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    keys = [obj.get("Key") for obj in (objects.get("Contents") or [])[:limit]]
    for key in keys:
        logger.info("%s", key)
    return keys


@_deprecated("print_s3_bucket_object() è deprecata e potrebbe essere rimossa in futuro.")
def print_s3_bucket_object(key: str, bucket_name: str = "quantia-master",
                           size: int = 1000, decode: bool = True) -> str | bytes:
    """Ritorna il contenuto (parziale) di un oggetto S3."""
    obj = _s3_client().get_object(Bucket=bucket_name, Key=key)
    body = obj.get("Body").read(size)
    return body.decode(encoding="utf-8", errors="ignore") if decode else body
