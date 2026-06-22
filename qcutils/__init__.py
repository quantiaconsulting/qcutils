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
"""
import os
import logging
import shutil
import tarfile
import getpass
import traceback
import configparser
from dataclasses import dataclass

logger = logging.getLogger("qcutils")

__version__ = "0.7.0"

# =============================================================================
# Config & init
# =============================================================================

@dataclass
class Config:
    """Stato di runtime di qcutils. Niente segreti su disco se non necessario."""
    kafka_bootstrap: str = "localhost:9092"
    schema_registry_url: str = ""
    flink_rest_url: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    jupyterhub_user: str = ""
    github_branch: str = ""
    github_repo: str = ""
    aws_region: str = ""
    initialized: bool = False


_config = Config()


def init(*, kafka_bootstrap=None, schema_registry_url=None, flink_rest_url=None,
         kafka_sasl_username=None, kafka_sasl_password=None,
         aws_access_key_id=None, aws_secret_access_key=None, aws_region=None,
         interactive=False, write_aws=True):
    """Inizializza qcutils con endpoint e segreti, a runtime. Idempotente.

    Args (tutti opzionali; se omessi si leggono dalle env var corrispondenti):
        kafka_bootstrap:        KAFKA_BOOTSTRAP_SERVERS (default localhost:9092)
        schema_registry_url:    SCHEMA_REGISTRY_URL
        flink_rest_url:         FLINK_REST_URL
        kafka_sasl_username:    KAFKA_SASL_USERNAME
        kafka_sasl_password:    KAFKA_SASL_PASSWORD
        aws_access_key_id:      AWS_ACCESS_KEY_ID
        aws_secret_access_key:  AWS_SECRET_ACCESS_KEY
        aws_region:             AWS_DEFAULT_REGION
        interactive:    se True, chiede via prompt sicuro i segreti AWS mancanti
        write_aws:      se True (default) scrive ~/.aws/credentials quando le
                        chiavi sono fornite e il file non esiste già
    Ritorna l'oggetto Config. Non stampa mai i segreti.
    """
    g = os.environ.get
    _config.kafka_bootstrap = kafka_bootstrap or g("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    _config.schema_registry_url = schema_registry_url or g("SCHEMA_REGISTRY_URL", "")
    _config.flink_rest_url = flink_rest_url or g("FLINK_REST_URL", "")
    _config.kafka_sasl_username = kafka_sasl_username or g("KAFKA_SASL_USERNAME", "")
    _config.kafka_sasl_password = kafka_sasl_password or g("KAFKA_SASL_PASSWORD", "")
    _config.jupyterhub_user = g("JUPYTERHUB_USER", "")
    _config.github_branch = g("GITHUB_BRANCH", "")
    _config.github_repo = g("GITHUB_REPO", "")
    _config.aws_region = aws_region or g("AWS_DEFAULT_REGION", "")

    ak = aws_access_key_id or g("AWS_ACCESS_KEY_ID", "")
    sk = aws_secret_access_key or g("AWS_SECRET_ACCESS_KEY", "")
    if interactive:
        if not ak:
            ak = input("AWS_ACCESS_KEY_ID: ").strip()
        if not sk:
            sk = getpass.getpass("AWS_SECRET_ACCESS_KEY: ").strip()
    creds_path = os.path.expanduser("~/.aws/credentials")
    if write_aws and ak and sk and not os.path.exists(creds_path):
        _write_aws_credentials(ak, sk, _config.aws_region)

    _config.initialized = True
    logger.info("qcutils inizializzato (kafka=%s, sasl=%s, sr=%s)",
                _config.kafka_bootstrap,
                "yes" if _config.kafka_sasl_username else "no",
                _config.schema_registry_url or "-")
    return _config


def get_config():
    """Ritorna l'oggetto Config corrente."""
    return _config


def _write_aws_credentials(key, secret, region=""):
    aws_dir = os.path.expanduser("~/.aws")
    os.makedirs(aws_dir, exist_ok=True)
    with open(os.path.join(aws_dir, "credentials"), "w") as f:
        f.write("[default]\naws_access_key_id={}\naws_secret_access_key={}\n".format(key, secret))
    if region:
        with open(os.path.join(aws_dir, "config"), "w") as f:
            f.write("[default]\nregion={}\n".format(region))
    os.chmod(aws_dir, 0o700)
    os.chmod(os.path.join(aws_dir, "credentials"), 0o600)
    logger.info("AWS credentials scritte in ~/.aws/credentials")


def _resolve(value, cfg_attr, env_name):
    """Risolve un valore: esplicito > Config (da init) > env var."""
    if value:
        return value
    cfg_val = getattr(_config, cfg_attr, "")
    if cfg_val:
        return cfg_val
    return os.environ.get(env_name, "")


# =============================================================================
# Funzioni private
# =============================================================================

def __search_sub_node(node, lst):
    pname = lst.pop(0)
    subnode = node[pname]
    if len(lst) > 0:
        return __search_sub_node(subnode, lst)
    return subnode


def __make_tarfile(source_dir, output_path):
    try:
        if output_path.endswith(".tar.gz"):
            output_filename = output_path.split("/")[-1]
            with tarfile.open(output_path, "w:gz") as tar:
                tar.add(source_dir, arcname=output_filename)
            print("OK")
            return True
        print("The output_path must contain the name of the output .tar.gz archive")
        return False
    except Exception:
        traceback.print_exc()
        return False


def __upload_file_s3(file_name, bucket, object_name=None):
    import boto3
    from botocore.exceptions import ClientError
    if object_name is None:
        object_name = file_name
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def __download_file_s3(file_name, bucket, object_name=None):
    import boto3
    from botocore.exceptions import ClientError
    if object_name is None:
        object_name = file_name
    s3_client = boto3.client("s3")
    try:
        s3_client.download_file(bucket, object_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# =============================================================================
# Config file (compat) — SOLO lettura di un file locale, niente token-in-URL
# =============================================================================

def read_config_value(key, github_user="", github_token="", remote_cf_version="0.6.0",
                       cf_path="/home/jovyan/utils/config.yaml"):
    """[Compat] Legge un valore da un config YAML LOCALE.

    Il vecchio download da repo privato con token nell'URL è stato rimosso
    (anti-pattern). Per gli endpoint condivisi usa `qcutils.init()` con le env
    var iniettate dal deployment. Se serve un file di config, montalo/scaricalo
    a runtime in `cf_path` (es. da JupyterHub) e questa funzione lo leggerà.
    """
    import yaml
    if not os.path.exists(cf_path):
        raise FileNotFoundError(
            "Config file '{}' non trovato. Usa qcutils.init() con le env var "
            "del deployment, oppure monta/scarica il file di config a runtime."
            .format(cf_path)
        )
    with open(cf_path) as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    return __search_sub_node(cfg, key.split("."))


# =============================================================================
# Materiali: compress / update / deliver / persist / restore
# =============================================================================

def compress_folder(path="/home/jovyan/materials"):
    """Comprime la cartella indicata in un archivio .tar.gz nella home."""
    try:
        print("Compressing {} folder....".format(path.split("/")[-1]))
        jhub_user = _resolve("", "jupyterhub_user", "JUPYTERHUB_USER")
        output_filename = path.split("/")[-1] + "_" + jhub_user.replace(".", "_") + ".tar.gz"
        if __make_tarfile(path, "/home/jovyan/" + output_filename):
            print("You can find your {} in your home folder".format(output_filename))
        else:
            raise Exception("tar creation failed")
    except Exception:
        traceback.print_exc()


def update_materials():
    """Aggiorna /home/jovyan/materials dal repo GitHub della classe (gitpuller)."""
    print("updating materials folder....")
    repo = _resolve("", "github_repo", "GITHUB_REPO")
    branch = _resolve("", "github_branch", "GITHUB_BRANCH")
    os.system("gitpuller {} {} /home/jovyan/materials".format(repo, branch))


def deliver_bootcamp(path="/home/jovyan/materials/bootcamp"):
    """Comprime e carica la cartella sul bucket S3 quantia-bootcamp-results."""
    push_to_remote("quantia-bootcamp-results", path)


def persist_user_materials(path="/home/jovyan/materials"):
    """Comprime e carica la cartella sul bucket S3 quantia-platform-users."""
    push_to_remote("quantia-platform-users", path)


def restore_user_materials(bucket="quantia-platform-users", local_file_path="/home/jovyan/"):
    """Recupera la cartella utente da S3 e la estrae in persistent-materials."""
    folder_path = "/home/jovyan/persistent-materials"
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)

    tar_file = pull_from_remote(bucket, local_file_path)
    my_tar = tarfile.open(tar_file)
    for member in my_tar.getmembers():
        if ".ipynb_checkpoints" not in member.name:
            my_tar.extract(member, path="/home/jovyan/tmp")
    my_tar.close()

    os.makedirs("/home/jovyan/persistent-materials", exist_ok=True)
    source_dir = "/home/jovyan/tmp/" + tar_file.split("/")[3]
    target_dir = "/home/jovyan/persistent-materials"
    for file_name in os.listdir(source_dir):
        shutil.move(os.path.join(source_dir, file_name), target_dir)

    os.remove(tar_file)
    shutil.rmtree("/home/jovyan/tmp/")


def restore_user_bootcamp(bucket="quantia-bootcamp-results", local_file_path="/home/jovyan/"):
    """Recupera il bootcamp dell'utente da S3 e lo estrae in materials/bootcamp."""
    folder_path = "/home/jovyan/materials/bootcamp"
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)

    tar_file = pull_from_remote(bucket, local_file_path)
    my_tar = tarfile.open(tar_file)
    for member in my_tar.getmembers():
        if ".ipynb_checkpoints" not in member.name:
            my_tar.extract(member, path="/home/jovyan/tmp")
    my_tar.close()

    os.makedirs("/home/jovyan/materials/bootcamp", exist_ok=True)
    source_dir = "/home/jovyan/tmp/" + tar_file.split("/")[-1]
    target_dir = "/home/jovyan/materials/bootcamp"
    for file_name in os.listdir(source_dir):
        shutil.move(os.path.join(source_dir, file_name), target_dir)

    os.remove(tar_file)
    shutil.rmtree("/home/jovyan/tmp/")


# =============================================================================
# Spark
# =============================================================================

def init_spark_session(spark_session):
    """Configura una SparkSession per leggere da S3 via s3a (creds da ~/.aws)."""
    config = configparser.RawConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))
    aws_key = config["default"]["aws_access_key_id"]
    aws_secret = config["default"]["aws_secret_access_key"]

    hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_session.conf.set("spark.sql.repl.eagerEval.enabled", True)


# =============================================================================
# Kafka — usa la Config (init), self-contained per default (localhost:9092)
# =============================================================================

def _kafka_admin_conf():
    conf = {"bootstrap.servers": _resolve("", "kafka_bootstrap", "KAFKA_BOOTSTRAP_SERVERS")
            or "localhost:9092"}
    if _config.kafka_sasl_username:
        conf.update({
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username": _config.kafka_sasl_username,
            "sasl.password": _config.kafka_sasl_password,
        })
    return conf


def kafka_srv_description():
    """Mostra una tabella con gli endpoint streaming configurati."""
    from tabulate import tabulate
    rows = [
        ["Kafka", _resolve("", "kafka_bootstrap", "KAFKA_BOOTSTRAP_SERVERS") or "localhost:9092"],
        ["Schema Registry", _config.schema_registry_url or "-"],
        ["Flink REST", _config.flink_rest_url or "-"],
    ]
    print(tabulate(rows, headers=["Service", "Endpoint"], tablefmt="pretty"))


def create_kafka_topic(topic, partitions=1, replication=1):
    """Crea un topic Kafka sul broker configurato (init/env, default localhost)."""
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaError

    a = AdminClient(_kafka_admin_conf())
    fs = a.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replication)])
    for topic, f in fs.items():
        try:
            f.result()
            print("Topic {} created".format(topic))
        except Exception as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print("{}".format(e))
            else:
                print("Failed to create topic {}: {}".format(topic, e))


# =============================================================================
# S3
# =============================================================================

def push_to_remote(bucket, path="/home/jovyan/materials"):
    """Comprime la cartella e carica l'archivio sul bucket S3 indicato."""
    compress_folder(path)
    print("Sending compressed {} to qc repo....".format(path.split("/")[-1]))
    ghb = _resolve("", "github_branch", "GITHUB_BRANCH")
    jhub_user = _resolve("", "jupyterhub_user", "JUPYTERHUB_USER")
    file_name = path.split("/")[-1] + "_" + jhub_user.replace(".", "_") + ".tar.gz"
    if __upload_file_s3("/home/jovyan/" + file_name, bucket, ghb + "/" + file_name):
        print("{} is now on qc remote repo -> {}".format(path.split("/")[-1], ghb + "/" + file_name))


def pull_from_remote(bucket, local_file_path):
    """Recupera l'archivio dell'utente dal bucket S3 indicato."""
    import boto3
    if not local_file_path.endswith("/"):
        local_file_path = local_file_path + "/"
    ghb = _resolve("", "github_branch", "GITHUB_BRANCH")
    jhub_user = _resolve("", "jupyterhub_user", "JUPYTERHUB_USER")
    file_name = jhub_user.replace(".", "_") + ".tar.gz"

    s3_rs = boto3.resource("s3")
    s3_client = boto3.client("s3")
    for obj in s3_rs.Bucket(bucket).objects.filter(Prefix=ghb + "/"):
        if obj.key.endswith(file_name):
            s3_client.download_file(bucket, obj.key, local_file_path + obj.key.split("/")[1])
            return local_file_path + obj.key.split("/")[1]
    raise FileNotFoundError("Nessun archivio per l'utente in s3://{}/{}".format(bucket, ghb))


def list_s3_bucket_objects(bucket_name="quantia-master", prefix="training", limit=10):
    """Elenca gli oggetti in un bucket/prefix S3."""
    import boto3
    objects = boto3.client("s3").list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    for obj in (objects.get("Contents") or [])[:limit]:
        print(obj.get("Key"))


def print_s3_bucket_object(key, bucket_name="quantia-master", size=1000, decode=True):
    """Stampa il contenuto (parziale) di un oggetto S3."""
    import boto3
    obj = boto3.client("s3").get_object(Bucket=bucket_name, Key=key)
    body = obj.get("Body").read(size)
    print(body.decode(encoding="utf-8", errors="ignore") if decode else body)
