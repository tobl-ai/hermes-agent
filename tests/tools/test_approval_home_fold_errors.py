"""Home normalization tolerates filesystem failures, not isolation failures."""

from unittest.mock import patch

import pytest

from hermes_scope import ScopeSwitchRefused
from tools.approval_home_fold import _rewrite_resolved_hermes_home


@pytest.mark.parametrize("error", [OSError("unreadable home"), RuntimeError("symlink loop")])
def test_unresolvable_home_keeps_the_command(error):
    with patch("pathlib.Path.resolve", side_effect=error):
        assert _rewrite_resolved_hermes_home("echo safe") == "echo safe"


def test_scope_refusal_is_not_silently_ignored():
    with patch("hermes_constants.get_hermes_home", side_effect=ScopeSwitchRefused("foreign scope")):
        with pytest.raises(ScopeSwitchRefused):
            _rewrite_resolved_hermes_home("echo safe")
