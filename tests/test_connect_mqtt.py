import builtins
import logging
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import airmar_reader


def test_connect_mqtt_failure_logs_error(capsys, monkeypatch):
    # Create a fake MQTT client to avoid real network operations
    events = {}

    class FakeClient:
        def __init__(self, client_id=None):
            self.client_id = client_id
            self.on_connect = None

        def username_pw_set(self, username, password):
            pass

        def connect(self, broker, port):
            # store parameters for inspection if needed
            events['connect_called'] = True

    # Patch mqtt_client.Client to return FakeClient
    monkeypatch.setattr(airmar_reader.mqtt_client, 'Client', lambda *_args, **_kwargs: FakeClient())

    # Patch exit to avoid exiting test process
    monkeypatch.setattr(builtins, 'exit', lambda *_args, **_kwargs: None)
    client = airmar_reader.connect_mqtt()
    assert isinstance(client, FakeClient)

    # Attach a temporary stream handler so logging goes to stderr
    handler = logging.StreamHandler()
    logging.getLogger().addHandler(handler)

    # simulate mqtt connection callback with non-zero rc
    client.on_connect(client, None, None, rc=1)

    logging.getLogger().removeHandler(handler)

    captured = capsys.readouterr()
    assert "Connection failed:" in captured.err
