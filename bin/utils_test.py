
from utils import *


TEST_CONF_CONTENTS = r"""
[default]
mongo_host=127.0.0.1 
mongo_port=27017
mode=local

[logging]    
log_mode=file
log_file=/var/log/ar-compute.log
log_level=debug

[jobs]
tenants=TenantA,TenantB
TenantA_jobs=JobA,JobB
TenantB_jobs=JobC,JobD

[connectors]
sync_exec=/usr/libexec/argo-egi-connectors
sync_path=/var/lib/argo-connectors
"""


def test_get_date_under():
    date_input = "2015-05-11"
    expected = "2015_05_11"
    assert expected == get_date_under(date_input)


def test_get_actual_date():
    date_input = "2015-05-11"
    expected = datetime(2015, 05, 11)
    assert expected == get_actual_date(date_input)


def test_get_date_str():
    date_input = datetime(2015, 05, 11)
    expected = "2015-05-11"
    assert expected == get_date_str(date_input)


def test_get_date_range():
    expected = [datetime(2015, 05, 11), datetime(2015, 05, 12), datetime(2015, 05, 13)]
    start_date = "2015-05-11"
    end_date = "2015-05-13"
    assert get_date_range(start_date, end_date) == expected


def test_load_configuration(tmpdir):
    """ 
    tmpdir creates a directory unique to this test's invocation
    and returns a LocalPath object with the ability to write 
    files with data during tests. tmpdir is used here to create
    a temporary test configuration resourse file to load in 
    ArgoConfiguration object
    """

    res_file = tmpdir.join("ar-compute-test.conf")

    res_file.write(TEST_CONF_CONTENTS)

    cfg = ArgoConfiguration(res_file.strpath)

    expected_tenants = ["TenantA", "TenantB"]
    expected_jobs = {"TenantA": ["JobA", "JobB"], "TenantB": ["JobC", "JobD"]}

    assert cfg.mongo_host == "127.0.0.1"
    assert cfg.mongo_port == "27017"
    assert cfg.log_mode == "file"
    assert cfg.log_file == "/var/log/ar-compute.log"
    assert cfg.log_level == "debug"

    assert cfg.tenants == expected_tenants
    assert cfg.jobs == expected_jobs

    assert cfg.sync_exec == "/usr/libexec/argo-egi-connectors"
    assert cfg.sync_path == "/var/lib/argo-connectors"
