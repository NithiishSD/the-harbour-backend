import json
import logging

import pytest

from common import config
from common.log import JsonFormatter


@pytest.fixture(autouse=True)
def fresh_settings():
    config.settings.cache_clear()
    yield
    config.settings.cache_clear()


def test_defaults(monkeypatch):
    for key in ("KAFKA_BOOTSTRAP", "LIVE_BBOX", "DARK_TIMEOUT_HOURS"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(config, "_load_dotenv", lambda path: None)
    s = config.settings()
    assert s.kafka_bootstrap == "localhost:9092"
    assert s.live_bboxes == ()  # the boxes along the sea route are used
    assert s.dark_timeout_hours == 6


def test_env_overrides(monkeypatch):
    monkeypatch.setattr(config, "_load_dotenv", lambda path: None)
    monkeypatch.setenv("LIVE_BBOX", "33.4,-118.6,33.9,-117.9; 1.0,103.5,1.5,104.3")
    monkeypatch.setenv("HBASE_REST_URL", "http://hbase:8080/")
    s = config.settings()
    assert s.live_bboxes == ((33.4, -118.6, 33.9, -117.9), (1.0, 103.5, 1.5, 104.3))
    assert s.hbase_rest_url == "http://hbase:8080"


def test_bad_bbox_rejected(monkeypatch):
    monkeypatch.setattr(config, "_load_dotenv", lambda path: None)
    monkeypatch.setenv("LIVE_BBOX", "1,2,3")
    with pytest.raises(ValueError):
        config.settings()


def test_dotenv_does_not_override_env(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("# comment\nFOO_HW=from_file\nBAR_HW='quoted'\n")
    monkeypatch.setenv("FOO_HW", "from_env")
    monkeypatch.delenv("BAR_HW", raising=False)
    config._load_dotenv(env_file)
    import os

    assert os.environ["FOO_HW"] == "from_env"
    assert os.environ["BAR_HW"] == "quoted"
    monkeypatch.delenv("BAR_HW")


def test_json_log_line():
    record = logging.LogRecord("ingest.replay", logging.INFO, "", 0, "sent %d", (5,), None)
    record.data = {"rate": 12.5}
    line = json.loads(JsonFormatter().format(record))
    assert line["component"] == "ingest.replay"
    assert line["msg"] == "sent 5"
    assert line["rate"] == 12.5
