import pytest
import mock
import logging
import recompute
import json
from bson.objectid import ObjectId
from datetime import datetime

FOO_RECOMPUTATION_STR = r"""
{
    "excluded" : [
        "WCSS"
    ],
    "end_time" : "2015-03-15T23:00:00Z",
    "reason" : "2nd recomputation test 3rd key",
    "status" : "pending",
    "start_time" : "2015-03-10T12:00:00Z",
    "timestamp" : "2015-04-01 14:58:40",
    "id":"SOME_UUID",
    "requester_name":"TestName",
    "requester_email":"starwars@starwars.gr"
}
"""


@mock.patch('recompute.run_cmd')
def test_do_recompute(mock_run_cmd):

    log = logging.getLogger()

    ar_exec = "/usr/libexec/ar-compute/bin"
    date = "2015-03-11"
    tenant = "tenant_FOO"
    job = "job_BAR"
    recompute.do_recompute(ar_exec, date, tenant, job, log)
    expected_call_args = ['/usr/libexec/ar-compute/bin/job_ar.py',
                          '-d', '2015-03-11', '-t', 'tenant_FOO', '-j', 'job_BAR']
    # Assert expected call list in one
    mock_run_cmd.assert_called_with(expected_call_args, log)


@mock.patch('recompute.do_recompute')
def test_loop_recompute(mock_do_recompute):
    log = logging.getLogger()

    ar_exec = "/usr/libexec/ar-compute/bin"
    date_range = [datetime(2015, 03, 10), datetime(2015, 03, 11), datetime(2015, 03, 12)]
    tenant = "tenant_FOO"
    job = "job_a"
    recompute.loop_recompute(ar_exec, date_range, tenant, job, log)

    # do_recompute mock method will be called in iteration.
    # prepare a list with all the expected mock calls that
    # the method will recieve
    call_1a = mock.call(ar_exec, "2015-03-10", "tenant_FOO", "job_a", log)
    call_2a = mock.call(ar_exec, "2015-03-11", "tenant_FOO", "job_a", log)
    call_3a = mock.call(ar_exec, "2015-03-12", "tenant_FOO", "job_a", log)

    calls = [call_1a, call_2a, call_3a]
    # Assert expected call list in one
    mock_do_recompute.assert_has_calls(calls, any_order=True)


@mock.patch('recompute.MongoClient')
def test_get_collection(mock_client):
    log = logging.getLogger()
    mock_client.return_value = {"db_test": {"collection_test": "recalculations"}}
    col_str = recompute.get_mongo_collection(
        "mongo_host", "27017", "db_test", "collection_test", log)

    mock_client.assert_called_with("mongo_host", 27017)
    assert col_str == "recalculations"


def test_get_recomputation():
    log = logging.getLogger()
    mock_collection = mock.MagicMock()
    expected_recomputation = json.loads(FOO_RECOMPUTATION_STR)
    mock_collection.find_one.return_value = expected_recomputation
    results = recompute.get_recomputation(mock_collection, "551bdd701c8a97e78635a911", log)

    # Assert with a valid Object Id
    mock_collection.find_one.assert_called_with({'id': '551bdd701c8a97e78635a911'})
    assert results == expected_recomputation



def test_get_time_period():
    input_recomputation = json.loads(FOO_RECOMPUTATION_STR)

    expected = [datetime(2015, 03, 10), datetime(2015, 03, 11), datetime(
        2015, 03, 12), datetime(2015, 03, 13), datetime(2015, 03, 14), datetime(2015, 03, 15)]
    result = recompute.get_time_period(input_recomputation)
    assert result == expected


def test_update_status():
    log = logging.getLogger()
    mock_collection = mock.MagicMock()
    timestamp = datetime.now()
    recompute.update_status(mock_collection, "551bdd701c8a97e78635a911", "FOO", timestamp, log)

    query_id = {'id': '551bdd701c8a97e78635a911'}
    query_update = {'$set': {'status': 'FOO','timestamp':timestamp.strftime("%Y-%m-%d %H:%M:%S")}}

    mock_collection.update_one.assert_called_with(query_id, query_update)

   
