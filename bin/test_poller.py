from mock import MagicMock, patch

from poller_ar import run_recomputation, main, ObjectId


def test_run_recomputation():
    """
    Check that the recomputation function looks for a single pending
    request and submits it to be recomputed
    """
    pen_recalc, col = MagicMock(), MagicMock()
    pen_recalc.__getitem__.return_value = ObjectId('5559ed3306f6233c190bc851')
    col.find_one.return_value = pen_recalc
    run_recomputation(col)
    col.find_one.assert_called_with({'s': 'pending'})
    col.update.assert_called_with({'_id': ObjectId('5559ed3306f6233c190bc851')}, {'$set': {'s': 'running'}})
    #TODO: when the actual recomputation call is implemented, add some kind of test


def test_main():
    """
    Check that the recomputation is called if the threshold of
    running recomputations is not exceeded
    """
    with patch('poller_ar.get_poller_config', return_value=[MagicMock()]*4) as get_poller_config,\
         patch('poller_ar.get_mongo_collection') as get_mongo_collection, \
         patch('poller_ar.get_pending_and_running', return_value=[MagicMock()]*2) as get_pending_and_running,\
         patch('poller_ar.run_recomputation') as run_recomputation:
        get_pending_and_running.return_value[1] = 2
        get_poller_config.return_value[3] = 3
        main()
        run_recomputation.assert_called_once_with(get_mongo_collection.return_value)


if __name__ == '__main__':
    test_run_recomputation()
    test_main()