"""Cron listener config belongs to its listener, not the enclosing request."""

from agent.runtime_scope import bind_profile_scope
from agent.secret_scope import get_secret
from hermes_cli.web_server_cron import _listener_api_server_extra
from hermes_scope import current_scope


def test_listener_read_restores_the_enclosing_profile(tmp_path, monkeypatch):
    caller = tmp_path / "caller"
    listener = tmp_path / "listener"
    for home, value in ((caller, "caller-value"), (listener, "listener-value")):
        home.mkdir()
        (home / ".env").write_text(f"LISTENER_KEY={value}\n", encoding="utf-8")
        (home / "config.yaml").write_text(
            "platforms:\n  api_server:\n    extra:\n      key: ${LISTENER_KEY}\n",
            encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "launch"))
    with bind_profile_scope(caller, hydrate_secrets=False):
        owner = current_scope()
        assert _listener_api_server_extra(listener)["key"] == "listener-value"
        assert current_scope() is owner
        assert get_secret("LISTENER_KEY") == "caller-value"
        assert _listener_api_server_extra(listener / "missing") == {}
        assert current_scope() is owner
    assert current_scope() is None
