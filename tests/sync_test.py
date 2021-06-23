from tap_xactly.sync import metrics, overall_metrics, get_elapsed_time, average_rps


def test_metrics():
    info, rps = metrics(0.0, 10.0, 6)

    assert isinstance(info, str)
    assert isinstance(rps, float)
    assert rps == 0.6


def test_overall_metrics():
    records = [10, 5, 5]
    rps_list = [0.6, 0.1, 0.4, 0.9, 1.5]
    expected = overall_metrics(records, rps_list)

    assert isinstance(expected, float)
    assert expected == 1.1666666666666667


def test_get_elapsed_time():
    start = 0.0
    end = 10.6
    expected = get_elapsed_time(end, start)

    assert isinstance(expected, float)
    assert expected == 10.6


def test_average_rps():
    records = 3
    elapsed_time = 12
    expected = average_rps(records, elapsed_time)

    assert isinstance(expected, float)
    assert expected == 0.25
