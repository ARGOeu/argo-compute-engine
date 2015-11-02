
import mock
import logging
import mongo_recompute
import json


@mock.patch('mongo_recompute.MongoClient')
def test_get_collection(mock_client):
    log = logging.getLogger()
    mock_client.return_value = {"db_test": {"collection_test": "recalculations"}}
    col_str = mongo_recompute.get_mongo_collection(
        "mongo_host", "27017", "db_test", "collection_test", log)

    mock_client.assert_called_with("mongo_host", 27017)
    assert col_str == "recalculations"


def test_get_results():
    mock_collection = mock.MagicMock()
    mock_collection.find.return_value = ["result1", "result2"]
    results = mongo_recompute.get_mongo_results(mock_collection, "2015-02-05","critical")
    query_range = {"report":"critical",
        '$where': "'2015-02-05' >= this.start_time.split('T')[0] && '2015-02-05' <= this.end_time.split('T')[0]"}
    query_projection = {'_id': 0}

    mock_collection.find.assert_called_with(query_range, query_projection)
    assert results == ["result1", "result2"]


def test_write_output(tmpdir):
    """
    tmpdir creates a directory unique to this test's invocation
    and returns a LocalPath object with the ability to write
    files with data during tests. tmpdir is used here to write
    temporarily output to a file
    """
    results = {"result1": "value1", "result2": "value2"}
    arsync_lib = tmpdir.strpath
    tenant = "foo"
    date_under = "2015_02_03"
    mongo_recompute.write_output(results, tenant, date_under, arsync_lib)
    res_file = tmpdir.join("recomputations_foo_2015_02_03.json")

    # Assert File exists
    assert res_file.check()

    # Load file content and re-create json objecct
    file_txt = res_file.read()
    file_json = json.loads(file_txt)

    # Assert result json equals json from file
    assert file_json == results
