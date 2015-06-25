import pytest
import mock
from mock import MagicMock
from poller_ar import run_recomputation
from bson.objectid import ObjectId


@mock.patch('poller_ar.subprocess.Popen')
def test_run_recomputation(mock_popen):
    """
    Check that the recomputation function looks for a single pending
    request and submits it to be recomputed
    """
    pen_recalc, col = MagicMock(), MagicMock()
    pen_recalc.__getitem__.return_value = ObjectId('5559ed3306f6233c190bc851')
    col.find_one.return_value = pen_recalc
    run_recomputation(col, "FOO_tenant", 1, 1, 3)
    col.find_one.assert_called_with({'s': 'pending'})

    # Assert that the actual sys call is called with the correct arguments
    mock_popen.assert_called_with(
        ['./recompute.py', '-i', '5559ed3306f6233c190bc851', '-t', 'FOO_tenant'])


@mock.patch('poller_ar.subprocess.Popen')
def test_zero_pending(mock_popen):
    """
    In the case that are zero pending recomputations
    check that appropriate exception is raised 
    """
    pen_recalc, col = MagicMock(), MagicMock()
    pen_recalc.__getitem__.return_value = ObjectId('5559ed3306f6233c190bc851')
    col.find_one.return_value = pen_recalc
    with pytest.raises(ValueError) as excinfo:
        run_recomputation(col, "FOO_tenant", 0, 0, 3)
        col.find_one.assert_called_with({'s': 'pending'})
    assert 'Zero pending recomputations' in str(excinfo.value)

    # Assert that sys call was not date
    assert not mock_popen.called


@mock.patch('poller_ar.subprocess.Popen')
def test_over_threshold(mock_popen):
    """
    In the case that number of running > threshold
    check that appropriate exception is raised 
    """
    pen_recalc, col = MagicMock(), MagicMock()
    pen_recalc.__getitem__.return_value = ObjectId('5559ed3306f6233c190bc851')
    col.find_one.return_value = pen_recalc
    with pytest.raises(ValueError) as excinfo:
        run_recomputation(col, "FOO_tenant", 5, 1, 3)
        col.find_one.assert_called_with({'s': 'pending'})
    assert 'Over threshold; no recomputation will be executed.' in str(excinfo.value)

    # Assert that sys call was not date
    assert not mock_popen.called
