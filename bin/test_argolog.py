import pytest

from argolog import init_log


def test_init_log_raise_on_none_filename():
    with pytest.raises(TypeError) as excinfo:
        init_log(log_mode='file', log_file=None, log_level='INFO', log_name='argo.test')
    assert "Log filename is NoneType" in str(excinfo.value)

