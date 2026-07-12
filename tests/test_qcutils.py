"""Test di retrocompatibilità e modernizzazione per qcutils.

Nessuna rete reale: boto3/confluent-kafka sono mockati o saltati.
"""
from __future__ import annotations

import sys
import types
import warnings

import pytest

import qcutils


# ---------------------------------------------------------------------------
# Versione e API pubblica
# ---------------------------------------------------------------------------

def test_version_is_1_0_1():
    assert qcutils.__version__ == "1.0.1"


def test_public_api_present():
    for name in (
        "init", "get_config", "read_config_value", "kafka_srv_description",
        "init_spark_session", "deliver_bootcamp", "persist_user_materials",
        "restore_user_materials", "Config",
    ):
        assert hasattr(qcutils, name), name


# ---------------------------------------------------------------------------
# Config.from_env
# ---------------------------------------------------------------------------

def test_config_from_env_defaults(monkeypatch):
    for var in (
        "KAFKA_BOOTSTRAP_SERVERS", "SCHEMA_REGISTRY_URL", "FLINK_REST_URL",
        "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD", "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = qcutils.Config.from_env()
    assert cfg.kafka_bootstrap == "localhost:9092"
    assert cfg.schema_registry_url == ""
    assert cfg.initialized is True


def test_config_from_env_reads_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://sr:8081")
    cfg = qcutils.Config.from_env()
    assert cfg.kafka_bootstrap == "broker:9092"
    assert cfg.schema_registry_url == "http://sr:8081"


def test_config_from_env_precedence(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env-broker:9092")
    cfg = qcutils.Config.from_env(kafka_bootstrap="override:9092")
    assert cfg.kafka_bootstrap == "override:9092"


def test_config_frozen_and_secret_hidden(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "supersecret")
    cfg = qcutils.Config.from_env()
    # frozen: immutabile
    with pytest.raises(Exception):
        cfg.kafka_bootstrap = "x"  # type: ignore[misc]
    # segreto assente dal repr
    assert "supersecret" not in repr(cfg)
    assert cfg.kafka_sasl_password == "supersecret"


# ---------------------------------------------------------------------------
# init() / get_config()
# ---------------------------------------------------------------------------

def test_init_returns_config_and_idempotent(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    cfg1 = qcutils.init()
    assert isinstance(cfg1, qcutils.Config)
    assert cfg1.initialized is True
    assert qcutils.get_config() is cfg1
    cfg2 = qcutils.init()
    assert cfg2.kafka_bootstrap == cfg1.kafka_bootstrap


def test_init_does_not_write_aws_by_default(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAFAKE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secretfake")
    qcutils.init()
    assert not (tmp_path / ".aws" / "credentials").exists()


def test_init_write_aws_opt_in(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    qcutils.init(aws_access_key_id="AKIAFAKE", aws_secret_access_key="secretfake", write_aws=True)
    creds = tmp_path / ".aws" / "credentials"
    assert creds.exists()
    assert "AKIAFAKE" in creds.read_text()


# ---------------------------------------------------------------------------
# kafka_srv_description()
# ---------------------------------------------------------------------------

def test_kafka_srv_description_returns_nonempty_string(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    qcutils.init()
    out = qcutils.kafka_srv_description()
    assert isinstance(out, str)
    assert out.strip() != ""
    assert "Kafka" in out


def test_kafka_srv_description_uses_explicit_config():
    cfg = qcutils.Config.from_env(kafka_bootstrap="mybroker:9092")
    out = qcutils.kafka_srv_description(config=cfg)
    assert "mybroker:9092" in out


# ---------------------------------------------------------------------------
# read_config_value()
# ---------------------------------------------------------------------------

def test_read_config_value_missing_file_raises_and_warns(tmp_path):
    missing = tmp_path / "nope.yaml"
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        with pytest.raises(FileNotFoundError):
            qcutils.read_config_value("a.b", cf_path=str(missing))
    assert any(issubclass(x.category, DeprecationWarning) for x in w)


def test_read_config_value_reads_local_yaml(tmp_path):
    pytest.importorskip("yaml")
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("kafka:\n  bootstrap: broker:9092\n")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        val = qcutils.read_config_value("kafka.bootstrap", cf_path=str(cfg_file))
    assert val == "broker:9092"


def test_read_config_value_full_legacy_signature(tmp_path):
    """La firma storica con i 3 parametri 'morti' resta accettata."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("x: 1\n")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        val = qcutils.read_config_value(
            "x", github_user="u", github_token="t",
            remote_cf_version="0.6.0", cf_path=str(cfg_file),
        )
    assert val == 1


# ---------------------------------------------------------------------------
# Deprecation warnings sulle API morte
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("func_name", [
    "read_config_value", "restore_user_bootcamp",
    "list_s3_bucket_objects", "print_s3_bucket_object",
])
def test_deprecated_functions_emit_warning(func_name, monkeypatch, tmp_path):
    func = getattr(qcutils, func_name)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        # Chiamate che falliscono presto ma dopo aver emesso il warning.
        try:
            if func_name == "read_config_value":
                func("k", cf_path=str(tmp_path / "missing.yaml"))
            else:
                # boto3 non installato -> ImportError dopo il warning
                func()
        except Exception:
            pass
    assert any(issubclass(x.category, DeprecationWarning) for x in w), func_name


def test_deprecated_decorator_reusable():
    @qcutils._deprecated("obsoleto")
    def foo():
        return 42

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert foo() == 42
    assert any(issubclass(x.category, DeprecationWarning) for x in w)


# ---------------------------------------------------------------------------
# Credenziali AWS: precedenza e propagazione a boto3 (regressione 1.0.1)
# ---------------------------------------------------------------------------

def test_aws_credentials_from_config():
    cfg = qcutils.Config.from_env(
        aws_access_key_id="AKIATEST",
        aws_secret_access_key="s3cr3t",
        aws_region="eu-west-1",
    )
    creds = qcutils._aws_credentials(cfg)
    assert creds["aws_access_key_id"] == "AKIATEST"
    assert creds["aws_secret_access_key"] == "s3cr3t"
    assert creds["region_name"] == "eu-west-1"


def test_aws_credentials_empty_falls_back_to_default_chain(monkeypatch, tmp_path):
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))  # nessun ~/.aws/credentials
    cfg = qcutils.Config.from_env()
    assert qcutils._aws_credentials(cfg) == {}


def test_s3_client_passes_config_credentials(monkeypatch):
    """Le credenziali della Config devono arrivare a boto3.client (bug 1.0.0)."""
    captured = {}

    def fake_client(service, **kwargs):
        captured["service"] = service
        captured["kwargs"] = kwargs
        return object()

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = fake_client
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    cfg = qcutils.Config.from_env(
        aws_access_key_id="AKIATEST", aws_secret_access_key="s3cr3t", aws_region="eu-west-1",
    )
    qcutils._s3_client(cfg)
    assert captured["service"] == "s3"
    assert captured["kwargs"]["aws_access_key_id"] == "AKIATEST"
    assert captured["kwargs"]["region_name"] == "eu-west-1"


def test_push_to_remote_raises_on_upload_failure(monkeypatch, tmp_path):
    """Una consegna fallita deve sollevare, non fallire in silenzio (bug 1.0.0)."""
    monkeypatch.setattr(qcutils, "compress_folder", lambda path, config=None: str(tmp_path / "x.tar.gz"))
    monkeypatch.setattr(qcutils, "_upload_file_s3", lambda *a, **k: False)
    cfg = qcutils.Config.from_env()
    with pytest.raises(RuntimeError):
        qcutils.push_to_remote("quantia-bootcamp-results", "/home/jovyan/materials/bootcamp", config=cfg)
